# cogs/sentinel.py
from __future__ import annotations

import io
import json
import os
import random
import re
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import SentinelBotTrustRow, SentinelEventRow
from services.db import sessions


SENTINEL_NAME = "SENTINEL"
SENTINEL_ICON = "https://cdn.discordapp.com/emojis/1086355036920744057.png"

DEFAULT_LOG_CHANNEL_ID = 1461111447194042510

CONFIG_PATH = "data/sentinel_config.json"

UTC = timezone.utc

# Cache: store only enough to reconstruct deletes
CACHE_PER_CHANNEL = 3000
CACHE_GUILD_LIMIT = 35000

# Bulk delete preview behavior
BULK_PREVIEW_LINES = 0  # 0 = never spam preview; inspect case instead

# Bot trust tuning
TRUST_SCORE_FAMILIAR = 120
TRUST_MIN_DAYS = 21
TRUST_MIN_EVENTS = 30

# Raid heuristics
RAID_MODE_SECONDS = 600
JOIN_BURST_WINDOW_S = 35
JOIN_BURST_THRESHOLD = 6
NEWACCT_WINDOW_S = 70
NEWACCT_THRESHOLD = 4
NEWACCT_DAYS = 7

# Suspicious user heuristics (join-time)
SUSPICIOUS_ACCT_DAYS = 3
SUSPICIOUS_NO_AVATAR = True
SUSPICIOUS_DEFAULT_NAME = True  # username like "user1234" or similar

# Retention
DEFAULT_RETENTION_DAYS = 30

# --------------------------------
# Staff alert noise control
# --------------------------------

# Join staff alert scoring
STAFF_JOIN_ALERT_MIN_SCORE = 4          # minimum score to post join alert to staff
STAFF_JOIN_ALERT_ALWAYS_IF_RAID = True  # always post join alerts to staff when raid mode active

JOIN_SCORE_ACCOUNT_0D = 5
JOIN_SCORE_ACCOUNT_1D = 4
JOIN_SCORE_ACCOUNT_2D = 3
JOIN_SCORE_ACCOUNT_3D = 2
JOIN_SCORE_DEFAULT_NAME = 2
JOIN_SCORE_NO_AVATAR = 1
JOIN_SCORE_RAID_MODE = 3

# Staff alert burst suppression by category
STAFF_ALERT_WINDOW_S = 90
STAFF_ALERT_LIMIT_JOIN = 4          # max join alerts per window
STAFF_ALERT_LIMIT_EXT_OUTCOME = 6   # max external outcomes per window

# External app outcomes logging
LOG_EXTERNAL_SLASH_OUTCOMES = True
LOG_EXTERNAL_SLASH_OUTCOMES_TO_STAFF_DURING_RAID = True
LOG_EXTERNAL_SLASH_OUTCOMES_SKIP_FAMILIAR_BOTS = True  # reduces spam a lot

# Optional: only log external outcomes in specific channels (empty = no restriction)
EXTERNAL_OUTCOME_CHANNEL_ALLOWLIST: List[int] = []


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _now() -> int:
    return int(time.time())


def _dt(ts: Optional[int] = None) -> str:
    ts = _now() if ts is None else int(ts)
    return f"<t:{ts}:F>  <t:{ts}:R>"


def _clamp(s: str, n: int) -> str:
    s = s or ""
    if len(s) <= n:
        return s
    return s[: n - 3] + "..."


def _safe_str(x: Any) -> str:
    try:
        return str(x)
    except Exception:
        return repr(x)


def _clean_content_for_log(s: str) -> str:
    s = re.sub(r"@everyone", "@\u200beveryone", s or "")
    s = re.sub(r"@here", "@\u200bhere", s or "")
    return s


def _make_case_id() -> str:
    return f"SNT-{_now()}-{random.randrange(0, 0xFFFF):04X}"


def _role_list(roles: Iterable[discord.Role], limit: int = 14) -> str:
    rs = [r.mention for r in roles if r.name != "@everyone"]
    if not rs:
        return "None"
    if len(rs) <= limit:
        return ", ".join(rs)
    return ", ".join(rs[:limit]) + f"  (+{len(rs) - limit} more)"


def _acct_age_days(member: discord.Member) -> int:
    created_ts = int(member.created_at.timestamp())
    return max(0, int((_now() - created_ts) // 86400))


def _ch_mention(ch: Optional[discord.abc.GuildChannel]) -> str:
    if not ch:
        return "Unknown"
    if hasattr(ch, "mention"):
        return f"{ch.mention}"
    return f"`{getattr(ch, 'id', 0)}`"


def _jump_url_from_ids(guild_id: int, channel_id: int, message_id: int) -> str:
    return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"


def _fmt_deleted_line(ts: int, author_id: int, author_tag: str, msg_id: int, content: str) -> str:
    content = _clean_content_for_log(content or "")
    content = content.replace("\n", " ").strip()
    content = _clamp(content, 260)
    if not content:
        content = "*no text*"
    return f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts))}Z] msg `{msg_id}` | {author_tag} `{author_id}` | {content}"


def _looks_default_name(u: discord.abc.User) -> bool:
    s = str(u)
    if not s:
        return False
    s2 = s.lower()
    if re.fullmatch(r"user\d{3,8}", s2):
        return True
    if re.fullmatch(r"[a-z]{3,10}\d{4}", s2):
        return True
    return False


@dataclass
class CachedMsg:
    msg_id: int
    ts: int
    author_id: int
    author_tag: str
    channel_id: int
    content: str
    attachments: List[dict]
    stickers: List[dict]
    embed_summaries: List[dict]
    reply_to: Optional[int]
    webhook_id: Optional[int]
    is_bot: bool


@dataclass
class RaidSignal:
    reason: str
    member_ids: List[int]
    window_s: int
    count: int


class Sentinel(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

        self._cfg: Dict[str, Any] = {"guilds": {}}
        self._log_ch_by_guild: Dict[int, int] = {}
        self._staff_ch_by_guild: Dict[int, int] = {}
        self._load_cfg()

        self._cache_by_channel: Dict[int, Deque[CachedMsg]] = defaultdict(lambda: deque(maxlen=CACHE_PER_CHANNEL))
        self._cache_index: Dict[int, Dict[int, CachedMsg]] = defaultdict(dict)
        self._cache_total: int = 0

        self._recent_joins: Dict[int, Deque[Tuple[int, int]]] = defaultdict(lambda: deque(maxlen=500))
        self._recent_newacct_joins: Dict[int, Deque[Tuple[int, int]]] = defaultdict(lambda: deque(maxlen=500))
        self._raid_until: Dict[int, int] = defaultdict(int)

        # spam guard for staff pings
        self._last_staff_ping_at: Dict[int, int] = defaultdict(int)

        # staff spam suppression by category
        self._staff_alert_events: Dict[Tuple[int, str], Deque[int]] = defaultdict(lambda: deque(maxlen=2000))

    # ----------------------------
    # Config
    # ----------------------------
    def _load_cfg(self) -> None:
        os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
        if os.path.exists(CONFIG_PATH):
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    self._cfg = json.load(f) or {"guilds": {}}
            except Exception:
                self._cfg = {"guilds": {}}
        else:
            self._cfg = {"guilds": {}}
            self._save_cfg()

        for gid_str, data in self._cfg.get("guilds", {}).items():
            try:
                gid = int(gid_str)
                cid = int(data.get("log_channel_id", DEFAULT_LOG_CHANNEL_ID))
                self._log_ch_by_guild[gid] = cid
                staff_cid = data.get("staff_channel_id")
                if staff_cid is not None:
                    self._staff_ch_by_guild[gid] = int(staff_cid)
            except Exception:
                continue

    def _save_cfg(self) -> None:
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(self._cfg, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def _get_log_channel_id(self, guild_id: int) -> int:
        return self._log_ch_by_guild.get(guild_id, DEFAULT_LOG_CHANNEL_ID)

    def _set_log_channel_id(self, guild_id: int, channel_id: int) -> None:
        self._log_ch_by_guild[guild_id] = channel_id
        self._cfg.setdefault("guilds", {})
        self._cfg["guilds"].setdefault(str(guild_id), {})
        self._cfg["guilds"][str(guild_id)]["log_channel_id"] = int(channel_id)
        self._save_cfg()

    def _get_staff_channel_id(self, guild_id: int) -> int:
        return self._staff_ch_by_guild.get(guild_id, self._get_log_channel_id(guild_id))

    def _set_staff_channel_id(self, guild_id: int, channel_id: int) -> None:
        self._staff_ch_by_guild[guild_id] = channel_id
        self._cfg.setdefault("guilds", {})
        self._cfg["guilds"].setdefault(str(guild_id), {})
        self._cfg["guilds"][str(guild_id)]["staff_channel_id"] = int(channel_id)
        self._save_cfg()

    async def _log_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        cid = self._get_log_channel_id(guild.id)
        ch = guild.get_channel(cid)
        if isinstance(ch, discord.TextChannel):
            return ch
        try:
            fetched = await guild.fetch_channel(cid)
            if isinstance(fetched, discord.TextChannel):
                return fetched
        except Exception:
            return None
        return None

    async def _staff_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        cid = self._get_staff_channel_id(guild.id)
        ch = guild.get_channel(cid)
        if isinstance(ch, discord.TextChannel):
            return ch
        try:
            fetched = await guild.fetch_channel(cid)
            if isinstance(fetched, discord.TextChannel):
                return fetched
        except Exception:
            return None
        return None

    def _staff_role_mention(self, guild: discord.Guild) -> Tuple[str, discord.AllowedMentions]:
        role = discord.utils.find(lambda r: r.name.lower() == "staff", guild.roles)
        if role:
            return role.mention, discord.AllowedMentions(roles=True, users=False, everyone=False, replied_user=False)
        return "@staff", discord.AllowedMentions.none()

    # ----------------------------
    # Staff alert suppression
    # ----------------------------
    def _staff_allow_category(self, guild_id: int, category: str, limit: int) -> bool:
        now = _now()
        key = (int(guild_id), str(category))
        dq = self._staff_alert_events[key]

        while dq and (now - dq[0]) > STAFF_ALERT_WINDOW_S:
            dq.popleft()

        if len(dq) >= int(limit):
            return False

        dq.append(now)
        return True

    # ----------------------------
    # Pretty embeds
    # ----------------------------
    def _embed(self, guild: discord.Guild, title: str, severity: str, *, case_id: str, ts: Optional[int] = None) -> discord.Embed:
        palette = {
            "INFO": 0x5865F2,
            "OK": 0x57F287,
            "WARN": 0xFEE75C,
            "BAD": 0xED4245,
            "WEIRD": 0xEB459E,
        }
        sev = (severity or "INFO").upper()
        t = int(ts or _now())
        e = discord.Embed(
            title=f"{SENTINEL_NAME} • {title}",
            description=f"{_dt(t)}\nCase: `{case_id}`",
            color=palette.get(sev, 0x5865F2),
            timestamp=discord.utils.utcnow(),
        )
        try:
            e.set_author(name=SENTINEL_NAME, icon_url=SENTINEL_ICON)
        except Exception:
            pass
        e.set_footer(text=f"{guild.name} • {sev}")
        return e

    async def _send_embed(self, guild: discord.Guild, embed: discord.Embed) -> None:
        ch = await self._log_channel(guild)
        if not ch:
            return
        try:
            await ch.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            pass

    async def _send_staff_alert(self, guild: discord.Guild, embed: discord.Embed, *, force_ping: bool = False) -> None:
        ch = await self._staff_channel(guild)
        if not ch:
            return

        now = _now()
        last = int(self._last_staff_ping_at.get(guild.id, 0))
        should_ping = force_ping or (now - last >= 90)

        mention, allowed = self._staff_role_mention(guild)
        content = mention if should_ping else None
        if should_ping:
            self._last_staff_ping_at[guild.id] = now

        try:
            await ch.send(content=content, embed=embed, allowed_mentions=allowed)
        except Exception:
            pass

    # ----------------------------
    # DB
    # ----------------------------
    async def _store_event(
        self,
        *,
        guild_id: int,
        event_type: str,
        severity: str,
        summary: str,
        payload: dict,
        actor_user_id: Optional[int] = None,
        target_user_id: Optional[int] = None,
        channel_id: Optional[int] = None,
        message_id: Optional[int] = None,
        case_id: Optional[str] = None,
    ) -> str:
        cid = case_id or _make_case_id()

        async with self.sessionmaker() as session:
            async with session.begin():
                row = SentinelEventRow(
                    guild_id=int(guild_id),
                    case_id=str(cid),
                    event_type=str(event_type),
                    severity=str(severity).upper(),
                    actor_user_id=int(actor_user_id) if actor_user_id is not None else None,
                    target_user_id=int(target_user_id) if target_user_id is not None else None,
                    channel_id=int(channel_id) if channel_id is not None else None,
                    message_id=int(message_id) if message_id is not None else None,
                    summary=_clamp(summary, 512),
                    payload_json=dict(payload or {}),
                )
                session.add(row)
                await session.flush()

        return cid

    async def _fetch_case(self, *, guild_id: int, case_id: str) -> Optional[SentinelEventRow]:
        async with self.sessionmaker() as session:
            return await session.scalar(
                select(SentinelEventRow).where(
                    SentinelEventRow.guild_id == int(guild_id),
                    SentinelEventRow.case_id == str(case_id),
                )
            )

    # ----------------------------
    # Bot trust
    # ----------------------------
    async def _bot_trust_bump(self, *, guild_id: int, bot_user_id: int, kind: str) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                row = await session.scalar(
                    select(SentinelBotTrustRow)
                    .where(
                        SentinelBotTrustRow.guild_id == int(guild_id),
                        SentinelBotTrustRow.bot_user_id == int(bot_user_id),
                    )
                    .with_for_update()
                )
                if row is None:
                    row = SentinelBotTrustRow(guild_id=int(guild_id), bot_user_id=int(bot_user_id), trust_score=0)
                    session.add(row)
                    await session.flush()

                if kind == "app":
                    row.app_commands_seen = int(getattr(row, "app_commands_seen", 0)) + 1
                    row.trust_score = int(getattr(row, "trust_score", 0)) + 3
                else:
                    row.interactions_seen = int(getattr(row, "interactions_seen", 0)) + 1
                    row.trust_score = int(getattr(row, "trust_score", 0)) + 1

    async def _bot_is_familiar(self, *, guild_id: int, bot_user_id: int) -> bool:
        async with self.sessionmaker() as session:
            row = await session.scalar(
                select(SentinelBotTrustRow).where(
                    SentinelBotTrustRow.guild_id == int(guild_id),
                    SentinelBotTrustRow.bot_user_id == int(bot_user_id),
                )
            )
            if row is None:
                return False

            if bool(getattr(row, "is_whitelisted", False)):
                return True

            if int(getattr(row, "trust_score", 0)) >= TRUST_SCORE_FAMILIAR:
                return True

            first_seen = getattr(row, "first_seen_at", None)
            if first_seen is not None:
                age_days = max(int((_utc_now() - first_seen).total_seconds() // 86400), 0)
                seen = int(getattr(row, "app_commands_seen", 0)) + int(getattr(row, "interactions_seen", 0))
                if age_days >= TRUST_MIN_DAYS and seen >= TRUST_MIN_EVENTS:
                    return True

            return False

    # ----------------------------
    # Cache (only for delete reconstruction)
    # ----------------------------
    def _cache_put(self, msg: discord.Message) -> None:
        if not msg.guild or not msg.channel:
            return
        if not isinstance(msg.channel, (discord.TextChannel, discord.Thread)):
            return

        ch_id = msg.channel.id
        mid = msg.id
        ts = int(msg.created_at.timestamp()) if msg.created_at else _now()

        author = msg.author
        author_id = int(getattr(author, "id", 0))
        author_tag = _safe_str(author) if author else "Unknown"
        content = msg.content or ""

        reply_to = None
        try:
            if msg.reference and msg.reference.message_id:
                reply_to = int(msg.reference.message_id)
        except Exception:
            reply_to = None

        attachments: List[dict] = []
        try:
            for a in (msg.attachments or [])[:10]:
                attachments.append({"id": a.id, "filename": a.filename, "size": a.size, "url": a.url})
        except Exception:
            attachments = []

        stickers: List[dict] = []
        try:
            for s in (msg.stickers or [])[:10]:
                stickers.append({"id": s.id, "name": s.name})
        except Exception:
            stickers = []

        embed_summaries: List[dict] = []
        try:
            for e in (msg.embeds or [])[:6]:
                embed_summaries.append(
                    {
                        "type": getattr(e, "type", None),
                        "title": _clamp(getattr(e, "title", "") or "", 200),
                        "description": _clamp(getattr(e, "description", "") or "", 300),
                    }
                )
        except Exception:
            embed_summaries = []

        cached = CachedMsg(
            msg_id=mid,
            ts=ts,
            author_id=author_id,
            author_tag=author_tag,
            channel_id=ch_id,
            content=content,
            attachments=attachments,
            stickers=stickers,
            embed_summaries=embed_summaries,
            reply_to=reply_to,
            webhook_id=getattr(msg, "webhook_id", None),
            is_bot=bool(getattr(author, "bot", False)),
        )

        if mid in self._cache_index[ch_id]:
            self._cache_index[ch_id][mid] = cached
            return

        self._cache_by_channel[ch_id].append(cached)
        self._cache_index[ch_id][mid] = cached
        self._cache_total += 1

        if self._cache_total > CACHE_GUILD_LIMIT:
            for _ in range(2500):
                biggest = None
                blen = 0
                for cid, dq in self._cache_by_channel.items():
                    if len(dq) > blen:
                        biggest = cid
                        blen = len(dq)
                if not biggest or blen <= 0:
                    break
                old = self._cache_by_channel[biggest].popleft()
                self._cache_index[biggest].pop(old.msg_id, None)
                self._cache_total = max(0, self._cache_total - 1)

    def _cache_get(self, channel_id: int, msg_id: int) -> Optional[CachedMsg]:
        return self._cache_index.get(channel_id, {}).get(msg_id)

    def _cache_get_many(self, channel_id: int, msg_ids: List[int]) -> List[CachedMsg]:
        idx = self._cache_index.get(channel_id, {})
        out: List[CachedMsg] = []
        for mid in msg_ids:
            c = idx.get(mid)
            if c:
                out.append(c)
        return out

    # ----------------------------
    # Raid
    # ----------------------------
    def _raid_active(self, guild_id: int) -> bool:
        return _now() < int(self._raid_until.get(guild_id, 0))

    def _raid_enable(self, guild_id: int, seconds: int = RAID_MODE_SECONDS) -> None:
        self._raid_until[guild_id] = max(int(self._raid_until.get(guild_id, 0)), _now() + int(seconds))

    def _raid_check(self, guild_id: int) -> Optional[RaidSignal]:
        now = _now()

        joins = self._recent_joins[guild_id]
        while joins and now - joins[0][1] > JOIN_BURST_WINDOW_S:
            joins.popleft()
        if len(joins) >= JOIN_BURST_THRESHOLD:
            return RaidSignal(
                reason=f"Join burst ({len(joins)} in {JOIN_BURST_WINDOW_S}s)",
                member_ids=[m for m, _ in list(joins)],
                window_s=JOIN_BURST_WINDOW_S,
                count=len(joins),
            )

        newjoins = self._recent_newacct_joins[guild_id]
        while newjoins and now - newjoins[0][1] > NEWACCT_WINDOW_S:
            newjoins.popleft()
        if len(newjoins) >= NEWACCT_THRESHOLD:
            return RaidSignal(
                reason=f"New account burst ({len(newjoins)} in {NEWACCT_WINDOW_S}s, <= {NEWACCT_DAYS} days old)",
                member_ids=[m for m, _ in list(newjoins)],
                window_s=NEWACCT_WINDOW_S,
                count=len(newjoins),
            )

        return None

    def _suspicious_join_reasons(self, member: discord.Member) -> List[str]:
        reasons: List[str] = []
        age = _acct_age_days(member)

        if age <= SUSPICIOUS_ACCT_DAYS:
            reasons.append(f"Account is {age}d old")

        if SUSPICIOUS_NO_AVATAR:
            try:
                if member.avatar is None and member.default_avatar is not None:
                    reasons.append("No custom avatar")
            except Exception:
                pass

        if SUSPICIOUS_DEFAULT_NAME and _looks_default_name(member):
            reasons.append("Looks like a throwaway username")

        if self._raid_active(member.guild.id):
            reasons.append("Joined while raid mode is active")

        return reasons

    def _join_score(self, member: discord.Member, reasons: List[str]) -> int:
        score = 0
        age = _acct_age_days(member)

        if age <= 0:
            score += JOIN_SCORE_ACCOUNT_0D
        elif age == 1:
            score += JOIN_SCORE_ACCOUNT_1D
        elif age == 2:
            score += JOIN_SCORE_ACCOUNT_2D
        elif age == 3:
            score += JOIN_SCORE_ACCOUNT_3D
        elif age <= SUSPICIOUS_ACCT_DAYS:
            score += 1

        if any("throwaway" in r.lower() for r in reasons):
            score += JOIN_SCORE_DEFAULT_NAME

        if any("avatar" in r.lower() for r in reasons):
            score += JOIN_SCORE_NO_AVATAR

        if self._raid_active(member.guild.id):
            score += JOIN_SCORE_RAID_MODE

        return score

    def _should_staff_alert_join(self, guild_id: int, score: int, reasons: List[str]) -> bool:
        if STAFF_JOIN_ALERT_ALWAYS_IF_RAID and self._raid_active(guild_id):
            return True

        # hard suppress ultra low signal
        if reasons and all(r == "No custom avatar" for r in reasons):
            return False

        return score >= STAFF_JOIN_ALERT_MIN_SCORE

    # ----------------------------
    # External slash outcomes (other bots)
    # ----------------------------
    def _message_interaction_meta(self, message: discord.Message) -> Optional[dict]:
        # discord.py exposes this differently across versions/forks.
        # Prefer the modern attribute and only use the legacy payload as a
        # low-level fallback so we do not trigger deprecation warnings.
        meta = getattr(message, "interaction_metadata", None)
        if meta is not None:
            user = getattr(meta, "user", None)
            name = getattr(meta, "name", None)
            if user is not None:
                return {"user_id": int(user.id), "user_tag": str(user), "command_name": name}

        inter = getattr(message, "__dict__", {}).get("interaction")
        if inter is not None:
            user = getattr(inter, "user", None)
            name = getattr(inter, "name", None)
            if user is not None:
                return {"user_id": int(user.id), "user_tag": str(user), "command_name": name}

        return None

    async def _log_external_slash_outcome(self, message: discord.Message, meta: dict) -> None:
        if message.guild is None or message.channel is None:
            return
        if not LOG_EXTERNAL_SLASH_OUTCOMES:
            return

        if EXTERNAL_OUTCOME_CHANNEL_ALLOWLIST:
            cid = int(getattr(message.channel, "id", 0))
            if cid not in set(int(x) for x in EXTERNAL_OUTCOME_CHANNEL_ALLOWLIST):
                return

        g = message.guild
        ch_id = int(getattr(message.channel, "id", 0))
        msg_id = int(message.id)

        bot_user = getattr(message, "author", None)
        bot_id = int(getattr(bot_user, "id", 0))
        bot_tag = str(bot_user) if bot_user else "Unknown"

        if self.bot.user and bot_user and bot_user.id == self.bot.user.id:
            # avoid double logging our own stuff (we already log app commands elsewhere)
            return

        # trust bump on the responding bot
        if bot_id:
            await self._bot_trust_bump(guild_id=g.id, bot_user_id=bot_id, kind="app")

        if LOG_EXTERNAL_SLASH_OUTCOMES_SKIP_FAMILIAR_BOTS and bot_id:
            if await self._bot_is_familiar(guild_id=g.id, bot_user_id=bot_id):
                # Still allow during raid mode, because context matters
                if not self._raid_active(g.id):
                    return

        actor_id = int(meta.get("user_id", 0))
        actor_tag = str(meta.get("user_tag", "Unknown"))

        cmd = meta.get("command_name") or "unknown"

        content_preview = _clamp(_clean_content_for_log(message.content or ""), 900)
        embed_summaries: List[dict] = []
        try:
            for e in (message.embeds or [])[:6]:
                embed_summaries.append(
                    {
                        "type": getattr(e, "type", None),
                        "title": _clamp(getattr(e, "title", "") or "", 200),
                        "description": _clamp(getattr(e, "description", "") or "", 300),
                    }
                )
        except Exception:
            embed_summaries = []

        summary = f"External outcome: /{cmd} by {actor_tag} via {bot_tag} in #{getattr(message.channel, 'name', 'unknown')}"

        payload = {
            "actor": {"id": actor_id, "tag": actor_tag},
            "source_bot": {"id": bot_id, "tag": bot_tag},
            "channel": {"id": ch_id, "name": getattr(message.channel, "name", None)},
            "message": {
                "id": msg_id,
                "jump": message.jump_url,
                "content": content_preview,
                "embeds": embed_summaries,
            },
            "raid_mode": self._raid_active(g.id),
        }

        case_id = await self._store_event(
            guild_id=g.id,
            event_type="EXT_APP_OUTCOME",
            severity="INFO",
            summary=summary,
            actor_user_id=actor_id if actor_id else None,
            channel_id=ch_id,
            message_id=msg_id,
            payload=payload,
        )

        e = self._embed(g, "External Slash Outcome", "INFO", case_id=case_id)
        e.add_field(name="User", value=f"<@{actor_id}>\n`{actor_id}`", inline=True)
        e.add_field(name="Command", value=f"`/{cmd}`", inline=True)
        e.add_field(name="Bot", value=f"{_clamp(bot_tag, 80)}\n`{bot_id}`", inline=True)
        e.add_field(name="Channel", value=f"<#{ch_id}>\n`{ch_id}`", inline=True)
        e.add_field(name="Jump", value=message.jump_url, inline=False)
        if content_preview:
            e.add_field(name="Message", value=_clamp(content_preview, 900), inline=False)
        e.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)

        await self._send_embed(g, e)

        if LOG_EXTERNAL_SLASH_OUTCOMES_TO_STAFF_DURING_RAID and self._raid_active(g.id):
            if self._staff_allow_category(g.id, "ext_outcome", STAFF_ALERT_LIMIT_EXT_OUTCOME):
                ae = self._embed(g, "External Command During Raid Mode", "WEIRD", case_id=case_id)
                ae.add_field(name="User", value=f"<@{actor_id}>\n`{actor_id}`", inline=True)
                ae.add_field(name="Command", value=f"`/{cmd}`", inline=True)
                ae.add_field(name="Bot", value=f"{_clamp(bot_tag, 80)}\n`{bot_id}`", inline=True)
                ae.add_field(name="Channel", value=f"<#{ch_id}>", inline=True)
                ae.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)
                await self._send_staff_alert(g, ae, force_ping=False)

    # ----------------------------
    # Slash commands
    # ----------------------------
    sentinel_group = app_commands.Group(name="sentinel", description="Sentinel forensics tools.")

    @sentinel_group.command(name="raidtest", description="Send a test raid alert to staff chat (does not ban anyone).")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def sentinel_raidtest(self, interaction: discord.Interaction, simulate_mode_seconds: Optional[int] = 120):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)

        g = interaction.guild
        ts = _now()
        secs = int(simulate_mode_seconds or 120)
        if secs < 30:
            secs = 30
        if secs > 900:
            secs = 900

        self._raid_enable(g.id, secs)

        case_id = await self._store_event(
            guild_id=g.id,
            event_type="RAID_TEST",
            severity="WEIRD",
            summary=f"Raid test triggered by {interaction.user}",
            actor_user_id=interaction.user.id,
            payload={"actor": {"id": interaction.user.id, "tag": str(interaction.user)}, "enabled_seconds": secs},
        )

        e = self._embed(g, "RAID ALERT (TEST)", "WEIRD", case_id=case_id, ts=ts)
        e.add_field(name="This is a test", value="No action was taken. This validates staff alert routing.", inline=False)
        e.add_field(name="Raid Mode", value=f"Enabled for `{secs}s`", inline=True)
        e.add_field(name="Triggered By", value=f"{interaction.user.mention}\n`{interaction.user.id}`", inline=True)
        e.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)

        await interaction.response.send_message("Sent test alert to staff chat.", ephemeral=True)
        await self._send_staff_alert(g, e, force_ping=True)

    @sentinel_group.command(name="setlogchannel", description="Set the Sentinel log channel for this server.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def sentinel_setlogchannel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)

        self._set_log_channel_id(interaction.guild.id, channel.id)

        case_id = await self._store_event(
            guild_id=interaction.guild.id,
            event_type="CONFIG_LOG_CHANNEL",
            severity="OK",
            summary=f"Log channel set to #{channel.name} ({channel.id})",
            actor_user_id=interaction.user.id,
            channel_id=channel.id,
            payload={
                "actor": {"id": interaction.user.id, "tag": str(interaction.user)},
                "channel": {"id": channel.id, "name": channel.name},
                "interaction": interaction.data or {},
            },
        )

        e = self._embed(interaction.guild, "Log Channel Updated", "OK", case_id=case_id)
        e.add_field(name="Set By", value=f"{interaction.user.mention}\n`{interaction.user.id}`", inline=True)
        e.add_field(name="New Channel", value=f"{channel.mention}\n`{channel.id}`", inline=True)

        await interaction.response.send_message(f"Done. Logging to {channel.mention}.", ephemeral=True)
        await self._send_embed(interaction.guild, e)

    @sentinel_group.command(name="setstaffchannel", description="Set the Sentinel staff alert channel for this server.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def sentinel_setstaffchannel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)

        self._set_staff_channel_id(interaction.guild.id, channel.id)

        case_id = await self._store_event(
            guild_id=interaction.guild.id,
            event_type="CONFIG_STAFF_CHANNEL",
            severity="OK",
            summary=f"Staff channel set to #{channel.name} ({channel.id})",
            actor_user_id=interaction.user.id,
            channel_id=channel.id,
            payload={
                "actor": {"id": interaction.user.id, "tag": str(interaction.user)},
                "channel": {"id": channel.id, "name": channel.name},
                "interaction": interaction.data or {},
            },
        )

        e = self._embed(interaction.guild, "Staff Alert Channel Updated", "OK", case_id=case_id)
        e.add_field(name="Set By", value=f"{interaction.user.mention}\n`{interaction.user.id}`", inline=True)
        e.add_field(name="New Channel", value=f"{channel.mention}\n`{channel.id}`", inline=True)

        await interaction.response.send_message(f"Done. Staff alerts now go to {channel.mention}.", ephemeral=True)
        await self._send_embed(interaction.guild, e)

    @sentinel_group.command(name="last", description="Show recent Sentinel cases.")
    @app_commands.checks.has_permissions(view_audit_log=True)
    async def sentinel_last(self, interaction: discord.Interaction, count: Optional[int] = 10):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)

        n = int(count or 10)
        if n < 1:
            n = 1
        if n > 25:
            n = 25

        await interaction.response.defer(thinking=True, ephemeral=True)

        async with self.sessionmaker() as session:
            res = await session.execute(
                select(SentinelEventRow)
                .where(SentinelEventRow.guild_id == int(interaction.guild.id))
                .order_by(SentinelEventRow.created_at.desc(), SentinelEventRow.id.desc())
                .limit(n)
            )
            rows = list(res.scalars().all())

        if not rows:
            return await interaction.followup.send("No cases stored yet.", ephemeral=True)

        lines: List[str] = []
        for r in rows:
            created_ts = int(r.created_at.timestamp()) if r.created_at else _now()
            lines.append(f"`{r.case_id}`  {r.severity}  `{r.event_type}`  <t:{created_ts}:R>  {_clamp(r.summary, 80)}")

        e = self._embed(interaction.guild, f"Recent Cases ({len(rows)})", "INFO", case_id=rows[0].case_id)
        e.description = f"{_dt(_now())}\nUse `/sentinel inspect case_id:<id>` for full details."
        e.clear_fields()
        e.add_field(name="Cases", value=_clamp("\n".join(lines), 1024), inline=False)

        await interaction.followup.send(embed=e, ephemeral=True)

    @sentinel_group.command(name="inspect", description="Inspect a case (shows details + attaches raw JSON).")
    @app_commands.checks.has_permissions(view_audit_log=True)
    async def sentinel_inspect(self, interaction: discord.Interaction, case_id: str):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)

        cid = (case_id or "").strip()
        if not cid:
            return await interaction.response.send_message("Provide a case_id.", ephemeral=True)

        await interaction.response.defer(thinking=True, ephemeral=True)

        row = await self._fetch_case(guild_id=interaction.guild.id, case_id=cid)
        if row is None:
            return await interaction.followup.send("Case not found.", ephemeral=True)

        created_ts = int(row.created_at.timestamp()) if row.created_at else _now()
        e = self._embed(interaction.guild, "Case Inspect", row.severity, case_id=row.case_id, ts=created_ts)

        e.add_field(name="Type", value=f"`{row.event_type}`", inline=True)
        e.add_field(name="Severity", value=f"`{row.severity}`", inline=True)
        e.add_field(name="When", value=f"<t:{created_ts}:F>", inline=True)

        if row.actor_user_id:
            e.add_field(name="Actor", value=f"<@{row.actor_user_id}>\n`{row.actor_user_id}`", inline=True)
        if row.target_user_id:
            e.add_field(name="Target", value=f"<@{row.target_user_id}>\n`{row.target_user_id}`", inline=True)
        if row.channel_id:
            e.add_field(name="Channel", value=f"<#{row.channel_id}>\n`{row.channel_id}`", inline=True)
        if row.message_id:
            if row.channel_id:
                e.add_field(
                    name="Message",
                    value=f"`{row.message_id}`\n{_jump_url_from_ids(interaction.guild.id, int(row.channel_id), int(row.message_id))}",
                    inline=False,
                )
            else:
                e.add_field(name="Message", value=f"`{row.message_id}`", inline=False)

        e.add_field(name="Summary", value=_clamp(row.summary or "None", 1024), inline=False)

        raw = json.dumps(row.payload_json or {}, ensure_ascii=False, indent=2, default=_safe_str).encode("utf-8", errors="replace")
        file = discord.File(fp=io.BytesIO(raw), filename=f"sentinel_{row.case_id}.json")

        await interaction.followup.send(embed=e, file=file, ephemeral=True)

    @sentinel_group.command(name="search", description="Search cases by actor or target user.")
    @app_commands.checks.has_permissions(view_audit_log=True)
    async def sentinel_search(self, interaction: discord.Interaction, user: discord.User, count: Optional[int] = 15):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)

        n = int(count or 15)
        if n < 1:
            n = 1
        if n > 25:
            n = 25

        await interaction.response.defer(thinking=True, ephemeral=True)

        uid = int(user.id)
        async with self.sessionmaker() as session:
            res = await session.execute(
                select(SentinelEventRow)
                .where(
                    SentinelEventRow.guild_id == int(interaction.guild.id),
                    (SentinelEventRow.actor_user_id == uid) | (SentinelEventRow.target_user_id == uid),
                )
                .order_by(SentinelEventRow.created_at.desc(), SentinelEventRow.id.desc())
                .limit(n)
            )
            rows = list(res.scalars().all())

        if not rows:
            return await interaction.followup.send("No cases found for that user.", ephemeral=True)

        lines: List[str] = []
        for r in rows:
            created_ts = int(r.created_at.timestamp()) if r.created_at else _now()
            lines.append(f"`{r.case_id}`  {r.severity}  `{r.event_type}`  <t:{created_ts}:R>  {_clamp(r.summary, 80)}")

        e = self._embed(interaction.guild, f"Cases for {user}", "INFO", case_id=rows[0].case_id)
        e.add_field(name="Results", value=_clamp("\n".join(lines), 1024), inline=False)
        await interaction.followup.send(embed=e, ephemeral=True)

    @sentinel_group.command(name="prune", description="Delete stored cases older than N days.")
    @app_commands.checks.has_permissions(administrator=True)
    async def sentinel_prune(self, interaction: discord.Interaction, days: Optional[int] = DEFAULT_RETENTION_DAYS):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)

        d = int(days or DEFAULT_RETENTION_DAYS)
        if d < 1:
            d = 1
        if d > 365:
            d = 365

        cutoff_dt = datetime.fromtimestamp(_utc_now().timestamp() - (d * 86400), tz=UTC)

        await interaction.response.defer(thinking=True, ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    delete(SentinelEventRow).where(
                        SentinelEventRow.guild_id == int(interaction.guild.id),
                        SentinelEventRow.created_at < cutoff_dt,
                    )
                )

        await interaction.followup.send(f"Pruned cases older than {d} days.", ephemeral=True)

    # ----------------------------
    # Lifecycle
    # ----------------------------
    @commands.Cog.listener()
    async def on_ready(self):
        try:
            self.bot.tree.add_command(self.sentinel_group)
        except Exception:
            pass

        for g in self.bot.guilds:
            self._log_ch_by_guild.setdefault(g.id, DEFAULT_LOG_CHANNEL_ID)

    # ----------------------------
    # Cache feed + external bot outcomes (quiet unless raid)
    # ----------------------------
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.guild:
            self._cache_put(message)

        if message.guild is None:
            return
        if not bool(getattr(getattr(message, "author", None), "bot", False)):
            return

        meta = self._message_interaction_meta(message)
        if not meta:
            return

        await self._log_external_slash_outcome(message, meta)

    # ----------------------------
    # Member join/leave + staff alerts
    # ----------------------------
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        g = member.guild
        ts = _now()

        self._recent_joins[g.id].append((member.id, ts))
        if _acct_age_days(member) <= NEWACCT_DAYS:
            self._recent_newacct_joins[g.id].append((member.id, ts))

        suspicious_reasons = self._suspicious_join_reasons(member)
        score = self._join_score(member, suspicious_reasons)
        is_suspicious = score >= STAFF_JOIN_ALERT_MIN_SCORE

        case_id = await self._store_event(
            guild_id=g.id,
            event_type="MEMBER_JOIN",
            severity="WEIRD" if (suspicious_reasons or self._raid_active(g.id)) else "INFO",
            summary=f"{member} joined (acct age {_acct_age_days(member)}d, score {score})",
            actor_user_id=member.id,
            target_user_id=member.id,
            payload={
                "member": {
                    "id": member.id,
                    "tag": str(member),
                    "created_at": int(member.created_at.timestamp()),
                    "joined_at": int(member.joined_at.timestamp()) if member.joined_at else None,
                    "account_age_days": _acct_age_days(member),
                    "roles": [r.id for r in member.roles],
                    "has_avatar": bool(member.avatar),
                    "suspicious_reasons": suspicious_reasons,
                    "join_score": score,
                },
                "raid_mode": self._raid_active(g.id),
            },
        )

        e = self._embed(g, "Member Joined", "WEIRD" if (suspicious_reasons or self._raid_active(g.id)) else "INFO", case_id=case_id, ts=ts)
        e.add_field(name="Member", value=f"{member.mention}\n`{member.id}`", inline=True)
        e.add_field(name="Account", value=f"<t:{int(member.created_at.timestamp())}:R>", inline=True)
        e.add_field(name="Score", value=f"`{score}`", inline=True)

        if suspicious_reasons:
            e.add_field(name="Signals", value=_clamp("• " + "\n• ".join(suspicious_reasons), 900), inline=False)

        if member.avatar:
            try:
                e.set_thumbnail(url=member.avatar.url)
            except Exception:
                pass

        e.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)
        await self._send_embed(g, e)

        # Staff alert rules:
        # - Never ping for joins
        # - Only send to staff when score is high enough, or raid mode is active
        # - Burst suppress join staff alerts
        if self._should_staff_alert_join(g.id, score, suspicious_reasons):
            if self._staff_allow_category(g.id, "join", STAFF_ALERT_LIMIT_JOIN):
                ae = self._embed(g, "Join Alert", "WEIRD", case_id=case_id, ts=ts)
                ae.add_field(name="Member", value=f"{member.mention}\n`{member.id}`", inline=True)
                ae.add_field(name="Account", value=f"<t:{int(member.created_at.timestamp())}:R>", inline=True)
                ae.add_field(name="Score", value=f"`{score}`", inline=True)
                if suspicious_reasons:
                    ae.add_field(name="Why", value=_clamp("• " + "\n• ".join(suspicious_reasons), 900), inline=False)
                ae.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)
                await self._send_staff_alert(g, ae, force_ping=False)

        sig = self._raid_check(g.id)
        if sig:
            self._raid_enable(g.id, RAID_MODE_SECONDS)
            await self._log_raid_signal(g, sig)

    async def _log_raid_signal(self, guild: discord.Guild, sig: RaidSignal) -> None:
        ts = _now()
        case_id = await self._store_event(
            guild_id=guild.id,
            event_type="RAID_SIGNAL",
            severity="WEIRD",
            summary=sig.reason,
            payload={
                "reason": sig.reason,
                "window_s": sig.window_s,
                "count": sig.count,
                "member_ids": sig.member_ids,
            },
        )

        e = self._embed(guild, "Raid Signal", "WEIRD", case_id=case_id, ts=ts)
        e.add_field(name="Signal", value=sig.reason, inline=False)
        e.add_field(name="Raid Mode", value=f"Enabled for `{RAID_MODE_SECONDS}s`", inline=True)
        e.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)

        await self._send_embed(guild, e)

        ae = self._embed(guild, "RAID ALERT", "BAD", case_id=case_id, ts=ts)
        ae.add_field(name="Signal", value=sig.reason, inline=False)
        ae.add_field(name="What to do", value="Turn on slowmode, lock down invites, verify newcomers, start banning.", inline=False)
        ae.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)
        await self._send_staff_alert(guild, ae, force_ping=True)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        g = member.guild
        ts = _now()

        case_id = await self._store_event(
            guild_id=g.id,
            event_type="MEMBER_LEAVE",
            severity="WARN",
            summary=f"{member} left",
            actor_user_id=member.id,
            target_user_id=member.id,
            payload={
                "member": {"id": member.id, "tag": str(member)},
                "roles_last_known": [r.id for r in member.roles],
                "joined_at": int(member.joined_at.timestamp()) if member.joined_at else None,
            },
        )

        e = self._embed(g, "Member Left", "WARN", case_id=case_id, ts=ts)
        e.add_field(name="Member", value=f"{member.mention}\n`{member.id}`", inline=True)
        if member.joined_at:
            e.add_field(name="Joined", value=f"<t:{int(member.joined_at.timestamp())}:R>", inline=True)
        e.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)
        await self._send_embed(g, e)

    # ----------------------------
    # Moderation outcomes (signal)
    # ----------------------------
    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.User):
        ts = _now()
        case_id = await self._store_event(
            guild_id=guild.id,
            event_type="MEMBER_BAN",
            severity="BAD",
            summary=f"{user} banned",
            target_user_id=user.id,
            payload={"target": {"id": user.id, "tag": str(user)}},
        )

        e = self._embed(guild, "Member Banned", "BAD", case_id=case_id, ts=ts)
        e.add_field(name="Target", value=f"<@{user.id}>\n`{user.id}`", inline=True)
        e.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)
        await self._send_embed(guild, e)

    @commands.Cog.listener()
    async def on_member_unban(self, guild: discord.Guild, user: discord.User):
        ts = _now()
        case_id = await self._store_event(
            guild_id=guild.id,
            event_type="MEMBER_UNBAN",
            severity="WARN",
            summary=f"{user} unbanned",
            target_user_id=user.id,
            payload={"target": {"id": user.id, "tag": str(user)}},
        )

        e = self._embed(guild, "Member Unbanned", "WARN", case_id=case_id, ts=ts)
        e.add_field(name="Target", value=f"<@{user.id}>\n`{user.id}`", inline=True)
        e.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)
        await self._send_embed(guild, e)

    # ----------------------------
    # Message edit + delete logging
    # ----------------------------
    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        if after.guild is None or after.channel is None:
            return
        if before.content == after.content:
            return

        g = after.guild
        ts = _now()

        before_txt = _clean_content_for_log(before.content or "")
        after_txt = _clean_content_for_log(after.content or "")

        if not before_txt and not after_txt:
            return

        case_id = await self._store_event(
            guild_id=g.id,
            event_type="MESSAGE_EDIT",
            severity="WARN",
            summary=f"Message edited by {after.author} in #{getattr(after.channel, 'name', 'unknown')}",
            actor_user_id=after.author.id if after.author else None,
            channel_id=after.channel.id,
            message_id=after.id,
            payload={
                "author": {"id": after.author.id, "tag": str(after.author)} if after.author else None,
                "channel": {"id": after.channel.id, "name": getattr(after.channel, "name", None)},
                "message": {"id": after.id, "jump": after.jump_url},
                "before": before_txt,
                "after": after_txt,
            },
        )

        e = self._embed(g, "Message Edited", "WARN", case_id=case_id, ts=ts)
        e.add_field(name="Author", value=f"{after.author.mention}\n`{after.author.id}`", inline=True)
        e.add_field(name="Channel", value=f"{_ch_mention(after.channel)}\n`{after.channel.id}`", inline=True)
        e.add_field(name="Jump", value=after.jump_url, inline=False)
        e.add_field(name="Before", value=_clamp(before_txt or "(no text)", 900), inline=False)
        e.add_field(name="After", value=_clamp(after_txt or "(no text)", 900), inline=False)
        e.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)

        await self._send_embed(g, e)

        if self._raid_active(g.id):
            ae = self._embed(g, "Edit During Raid Mode", "WEIRD", case_id=case_id, ts=ts)
            ae.add_field(name="Author", value=f"{after.author.mention}\n`{after.author.id}`", inline=True)
            ae.add_field(name="Channel", value=f"{_ch_mention(after.channel)}\n`{after.channel.id}`", inline=True)
            ae.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)
            await self._send_staff_alert(g, ae, force_ping=False)

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        if message.guild is None or message.channel is None:
            return

        g = message.guild
        ts = _now()
        channel_id = int(getattr(message.channel, "id", 0))
        message_id = int(getattr(message, "id", 0))

        cached = self._cache_get(channel_id, message_id)

        author = message.author
        author_id = int(getattr(author, "id", 0)) if author else (cached.author_id if cached else 0)
        author_tag = str(author) if author else (cached.author_tag if cached else "Unknown")

        raw_content = message.content or ""
        if (not raw_content.strip()) and cached:
            raw_content = cached.content or ""

        content = _clean_content_for_log(raw_content)

        payload: dict = {
            "channel": {"id": channel_id, "name": getattr(message.channel, "name", None)},
            "author": {"id": author_id, "tag": author_tag},
            "message": {
                "id": message_id,
                "created_at": int(message.created_at.timestamp()) if message.created_at else None,
                "content": content,
                "jump_guess": _jump_url_from_ids(g.id, channel_id, message_id),
                "webhook_id": getattr(message, "webhook_id", None) or (cached.webhook_id if cached else None),
                "reply_to": (
                    int(message.reference.message_id)
                    if (message.reference and message.reference.message_id)
                    else (cached.reply_to if cached else None)
                ),
                "attachments": (cached.attachments if cached else []),
                "stickers": (cached.stickers if cached else []),
                "embeds": (cached.embed_summaries if cached else []),
                "cache_hit": bool(cached),
            },
            "raid_mode": self._raid_active(g.id),
        }

        summary = f"Deleted message in #{getattr(message.channel, 'name', 'unknown')} by {author_tag}"

        case_id = await self._store_event(
            guild_id=g.id,
            event_type="MESSAGE_DELETE",
            severity="BAD",
            summary=summary,
            target_user_id=author_id if author_id else None,
            channel_id=channel_id,
            message_id=message_id,
            payload=payload,
        )

        e = self._embed(g, "Message Deleted", "BAD", case_id=case_id, ts=ts)
        e.add_field(name="Author", value=f"<@{author_id}>\n`{author_id}`", inline=True)
        e.add_field(name="Channel", value=f"<#{channel_id}>\n`{channel_id}`", inline=True)

        if content.strip():
            e.add_field(name="Content", value=_clamp(content, 1000), inline=False)
        else:
            e.add_field(name="Content", value="(no text)", inline=False)

        if cached and cached.attachments:
            names = [a.get("filename", "file") for a in cached.attachments[:6]]
            e.add_field(name="Attachments", value=_clamp("\n".join([f"• {n}" for n in names]), 800), inline=False)

        e.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)

        await self._send_embed(g, e)

        if self._raid_active(g.id):
            ae = self._embed(g, "Delete During Raid Mode", "BAD", case_id=case_id, ts=ts)
            ae.add_field(name="Author", value=f"<@{author_id}>\n`{author_id}`", inline=True)
            ae.add_field(name="Channel", value=f"<#{channel_id}>\n`{channel_id}`", inline=True)
            ae.add_field(name="Content", value=_clamp(content or "(no text)", 900), inline=False)
            ae.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)
            await self._send_staff_alert(g, ae, force_ping=False)

    @commands.Cog.listener()
    async def on_bulk_message_delete(self, messages: List[discord.Message]):
        if not messages:
            return

        first = messages[0]
        if first.guild is None or first.channel is None:
            return

        g = first.guild
        ch = first.channel
        ts = _now()

        channel_id = int(getattr(ch, "id", 0))
        count = len(messages)

        msg_ids = [m.id for m in messages]
        cached = self._cache_get_many(channel_id, msg_ids)

        merged: Dict[int, CachedMsg] = {c.msg_id: c for c in cached}
        for m in messages:
            if m.id in merged:
                continue
            author_id = int(getattr(getattr(m, "author", None), "id", 0))
            merged[m.id] = CachedMsg(
                msg_id=m.id,
                ts=int(m.created_at.timestamp()) if m.created_at else ts,
                author_id=author_id,
                author_tag=_safe_str(m.author) if getattr(m, "author", None) else "Unknown",
                channel_id=channel_id,
                content=m.content or "",
                attachments=[],
                stickers=[],
                embed_summaries=[],
                reply_to=int(m.reference.message_id) if (m.reference and m.reference.message_id) else None,
                webhook_id=getattr(m, "webhook_id", None),
                is_bot=bool(getattr(getattr(m, "author", None), "bot", False)),
            )

        merged_list = sorted(merged.values(), key=lambda x: (x.ts, x.msg_id))
        lines = [_fmt_deleted_line(c.ts, c.author_id, c.author_tag, c.msg_id, c.content) for c in merged_list]

        case_id = await self._store_event(
            guild_id=g.id,
            event_type="BULK_DELETE",
            severity="BAD",
            summary=f"Bulk delete in #{getattr(ch, 'name', 'unknown')} ({count} messages)",
            channel_id=channel_id,
            payload={
                "channel": {"id": channel_id, "name": getattr(ch, "name", None)},
                "count": count,
                "cache_coverage": {"cached": len(cached), "total": count},
                "deleted_lines": lines,
                "raid_mode": self._raid_active(g.id),
            },
        )

        e = self._embed(g, "Bulk Delete", "BAD", case_id=case_id, ts=ts)
        e.add_field(name="Channel", value=f"<#{channel_id}>\n`{channel_id}`", inline=True)
        e.add_field(name="Count", value=f"`{count}`", inline=True)
        e.add_field(name="Coverage", value=f"`{len(cached)}/{count}`", inline=True)
        e.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)

        await self._send_embed(g, e)

        ae = self._embed(g, "Bulk Delete Alert", "BAD", case_id=case_id, ts=ts)
        ae.add_field(name="Channel", value=f"<#{channel_id}>\n`{channel_id}`", inline=True)
        ae.add_field(name="Count", value=f"`{count}`", inline=True)
        ae.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)
        await self._send_staff_alert(g, ae, force_ping=self._raid_active(g.id))

        if BULK_PREVIEW_LINES > 0:
            preview = lines[:BULK_PREVIEW_LINES]
            blob = "\n".join(preview).encode("utf-8", errors="replace")
            file = discord.File(fp=io.BytesIO(blob), filename=f"sentinel_{case_id}_bulk_preview.txt")
            logch = await self._log_channel(g)
            if logch:
                try:
                    await logch.send(file=file, allowed_mentions=discord.AllowedMentions.none())
                except Exception:
                    pass

    # ----------------------------
    # App command logging (your bot only, filtered by bot familiarity)
    # ----------------------------
    def _extract_app_command(self, interaction: discord.Interaction, command: app_commands.Command) -> dict:
        data = interaction.data or {}

        options = []
        try:
            options = data.get("options") or []
        except Exception:
            options = []

        name = getattr(command, "qualified_name", None) or getattr(command, "name", None) or "unknown"

        target_user_id: Optional[int] = None
        reason: Optional[str] = None

        try:
            for opt in options:
                if not isinstance(opt, dict):
                    continue
                k = str(opt.get("name") or "")
                v = opt.get("value")
                if k in ("user", "member", "target") and v is not None:
                    try:
                        target_user_id = int(v)
                    except Exception:
                        pass
                if k in ("reason", "note") and v is not None:
                    reason = str(v)
        except Exception:
            pass

        return {
            "command_name": name,
            "raw_data": data,
            "options": options,
            "target_user_id_guess": target_user_id,
            "reason_guess": reason,
        }

    @commands.Cog.listener()
    async def on_app_command_completion(self, interaction: discord.Interaction, command: app_commands.Command):
        if interaction.guild is None:
            return

        g = interaction.guild
        ts = _now()

        actor = interaction.user
        if actor is None:
            return

        actor_id = int(actor.id)
        actor_is_bot = bool(getattr(actor, "bot", False))

        if actor_is_bot:
            await self._bot_trust_bump(guild_id=g.id, bot_user_id=actor_id, kind="app")
            if await self._bot_is_familiar(guild_id=g.id, bot_user_id=actor_id):
                return

        bits = self._extract_app_command(interaction, command)
        cmd_name = bits.get("command_name", "unknown")
        target_guess = bits.get("target_user_id_guess")
        reason_guess = bits.get("reason_guess")

        summary = f"{cmd_name} used by {actor} in #{getattr(interaction.channel, 'name', 'unknown')}"

        case_id = await self._store_event(
            guild_id=g.id,
            event_type="APP_COMMAND",
            severity="INFO",
            summary=summary,
            actor_user_id=actor_id,
            target_user_id=int(target_guess) if target_guess else None,
            channel_id=getattr(interaction.channel, "id", None),
            payload={
                "actor": {"id": actor_id, "tag": str(actor), "is_bot": actor_is_bot},
                "channel": {
                    "id": getattr(interaction.channel, "id", None),
                    "name": getattr(interaction.channel, "name", None),
                },
                "command": {
                    "name": cmd_name,
                    "target_user_id_guess": target_guess,
                    "reason_guess": reason_guess,
                },
                "interaction": {
                    "type": _safe_str(getattr(interaction, "type", None)),
                    "data": interaction.data or {},
                },
            },
        )

        e = self._embed(g, "Command Used", "INFO", case_id=case_id, ts=ts)
        e.add_field(name="User", value=f"{actor.mention}\n`{actor_id}`", inline=True)
        if interaction.channel is not None:
            e.add_field(name="Channel", value=f"{_ch_mention(interaction.channel)}\n`{interaction.channel.id}`", inline=True)
        e.add_field(name="Command", value=f"`{cmd_name}`", inline=True)

        if target_guess:
            e.add_field(name="Target", value=f"<@{int(target_guess)}>\n`{int(target_guess)}`", inline=True)
        if reason_guess:
            e.add_field(name="Reason", value=_clamp(_clean_content_for_log(str(reason_guess)), 600), inline=False)

        e.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)

        await self._send_embed(g, e)

        if self._raid_active(g.id):
            ae = self._embed(g, "Command During Raid Mode", "WEIRD", case_id=case_id, ts=ts)
            ae.add_field(name="User", value=f"{actor.mention}\n`{actor_id}`", inline=True)
            ae.add_field(name="Command", value=f"`{cmd_name}`", inline=True)
            if interaction.channel is not None:
                ae.add_field(name="Channel", value=f"{_ch_mention(interaction.channel)}\n`{interaction.channel.id}`", inline=True)
            ae.add_field(name="Inspect", value=f"`/sentinel inspect case_id:{case_id}`", inline=False)
            await self._send_staff_alert(g, ae, force_ping=False)

    # ----------------------------
    # Non-command interactions (mostly for bot trust, not noisy)
    # ----------------------------
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return
        if interaction.type == discord.InteractionType.application_command:
            return

        actor = interaction.user
        if actor is None:
            return

        if bool(getattr(actor, "bot", False)):
            await self._bot_trust_bump(guild_id=interaction.guild.id, bot_user_id=int(actor.id), kind="interaction")


async def setup(bot: commands.Bot):
    await bot.add_cog(Sentinel(bot))
