from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional


import discord
from discord import app_commands
from discord.ext import commands, tasks
from sqlalchemy import func, select, text

from db.models import ActivityDailyRow, UserAchievementRow, WalletRow, XpRow
from services.achievement_catalog import ACHIEVEMENT_CATALOG, AchievementTier
from services.db import sessions
from services.xp import get_xp_progress


SEASON_START_UTC = date(2026, 1, 25)

EXCLUDED_USER_IDS: set[int] = {
    326498486335963137,  # Mavis
    537375301915901975,  # Kai
}


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _first_of_month_utc(d: date) -> date:
    return date(d.year, d.month, 1)


def _month_label(d: date) -> str:
    return d.strftime("%B %Y")


def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _fmt_duration(seconds: int) -> str:
    s = max(int(seconds), 0)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    if h > 0:
        return f"{h:,}h {m:02d}m"
    if m > 0:
        return f"{m:,}m {sec:02d}s"
    return f"{sec:,}s"


def _medal(i: int) -> str:
    if i == 1:
        return "🥇"
    if i == 2:
        return "🥈"
    if i == 3:
        return "🥉"
    return "▫️"


async def _display_name(bot: commands.Bot, guild: discord.Guild, user_id: int) -> str:
    m = guild.get_member(int(user_id))
    if m is not None:
        return m.display_name
    u = bot.get_user(int(user_id))
    if u is not None:
        return u.name
    try:
        u2 = await bot.fetch_user(int(user_id))
        return u2.name
    except Exception:
        return f"User {user_id}"


def _embed_fingerprint(e: discord.Embed) -> str:
    title = e.title or ""
    desc = e.description or ""
    footer = (e.footer.text if e.footer and e.footer.text else "") or ""
    thumb = (e.thumbnail.url if e.thumbnail and e.thumbnail.url else "") or ""
    fields = "|".join(f"{f.name}:{f.value}:{int(bool(f.inline))}" for f in e.fields)
    return f"{title}||{desc}||{fields}||{footer}||{thumb}"


def _msg_embed_fingerprint(msg: discord.Message) -> str:
    if not msg.embeds:
        return ""
    e = msg.embeds[0]
    title = e.title or ""
    desc = e.description or ""
    footer = (e.footer.text if e.footer and e.footer.text else "") or ""
    thumb = (e.thumbnail.url if e.thumbnail and e.thumbnail.url else "") or ""
    fields = "|".join(f"{f.name}:{f.value}:{int(bool(f.inline))}" for f in e.fields)
    return f"{title}||{desc}||{fields}||{footer}||{thumb}"


_TIER_RANK = {
    AchievementTier.COMMON: 0,
    AchievementTier.RARE: 1,
    AchievementTier.EPIC: 2,
    AchievementTier.LEGENDARY: 3,
    AchievementTier.MYTHIC: 4,
}


def _achievement_spotlight(row: tuple) -> str:
    count = _fmt_int(int(row[1] or 0))
    icon = str(row[2] or "🏅")
    name = str(row[3] or "No achievement data")
    tier = str(row[4] or "common").title()
    return f"{count} unlocked  •  {icon} {name} ({tier})"


@dataclass
class _GuildState:
    tag_index: int = 0
    last_api_at: float = 0.0
    backoff_until: float = 0.0
    last_cleanup_at: float = 0.0


class LeaderboardsCog(commands.Cog):
    LEADERBOARD_CHANNEL_ID = 1464852132359569469

    AUTO_LIMIT = 10
    FOOTER_TAG_PREFIX = "lbmsg:"
    TABLE_MAP = "leaderboard_message_map"

    TAG_MONTHLY_CHAT = "monthly_chat"
    TAG_MONTHLY_VC = "monthly_vc"
    TAG_MESSAGES = "messages"
    TAG_LEVELS = "levels"
    TAG_VCTIME = "vctime"
    TAG_MONEY = "money"
    TAG_ACHIEVEMENTS = "achievements"

    STARTUP_DELAY_RANGE = (6.0, 14.0)

    # One embed edit per minute
    TICK_SECONDS = 60.0

    # If missing, you said it is fine to send them all at once
    CREATE_BURST_DELAY = 0.85

    # Keep it gentle
    MIN_SECONDS_BETWEEN_API_CALLS_PER_GUILD = 8.0

    # Backoff on 429
    BACKOFF_MIN = 8.0
    BACKOFF_MAX = 25.0

    # Periodically clean leaderboard channel so it never accumulates clutter again
    CLEANUP_INTERVAL_SECONDS = 60.0 * 60.0
    CLEANUP_HISTORY_LIMIT = 500

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self._lock = asyncio.Lock()
        self._started = False

        # (guild_id, tag) -> message_id
        self._msg_map: dict[tuple[int, str], int] = {}

        # guild_id -> scheduler state
        self._gstate: dict[int, _GuildState] = {}

        # guild_id -> loaded mapping?
        self._loaded_guilds: set[int] = set()

        if self.bot.is_ready():
            self._kickoff()

    async def cog_load(self) -> None:
        if self.bot.is_ready():
            self._kickoff()

    @commands.Cog.listener("on_ready")
    async def _on_ready(self) -> None:
        self._kickoff()

    def _kickoff(self) -> None:
        if self._started:
            return
        self._started = True
        if not self.paced_loop.is_running():
            self.paced_loop.start()

    def cog_unload(self) -> None:
        try:
            self.paced_loop.cancel()
        except Exception:
            pass

    def _tags(self) -> list[str]:
        return [
            self.TAG_MONTHLY_CHAT,
            self.TAG_MONTHLY_VC,
            self.TAG_MESSAGES,
            self.TAG_LEVELS,
            self.TAG_VCTIME,
            self.TAG_MONEY,
            self.TAG_ACHIEVEMENTS,
        ]

    async def _ensure_tables(self) -> None:
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_MAP} (
            guild_id BIGINT NOT NULL,
            tag VARCHAR(32) NOT NULL,
            channel_id BIGINT NOT NULL,
            message_id BIGINT NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id, tag),
            KEY ix_lb_map_channel (channel_id),
            KEY ix_lb_map_message (message_id)
        );
        """
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(sql))

    async def _load_map_for_guild(self, guild_id: int) -> None:
        await self._ensure_tables()
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    text(f"SELECT tag, message_id FROM {self.TABLE_MAP} WHERE guild_id=:gid"),
                    {"gid": int(guild_id)},
                )
            ).all()

        for tag, mid in rows:
            try:
                self._msg_map[(int(guild_id), str(tag))] = int(mid)
            except Exception:
                pass

        self._loaded_guilds.add(int(guild_id))

    async def _set_map(self, guild_id: int, channel_id: int, tag: str, message_id: int) -> None:
        await self._ensure_tables()
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"""
                        INSERT INTO {self.TABLE_MAP} (guild_id, tag, channel_id, message_id)
                        VALUES (:gid, :tag, :cid, :mid)
                        ON DUPLICATE KEY UPDATE
                            channel_id=VALUES(channel_id),
                            message_id=VALUES(message_id)
                        """
                    ),
                    {
                        "gid": int(guild_id),
                        "tag": str(tag),
                        "cid": int(channel_id),
                        "mid": int(message_id),
                    },
                )
        self._msg_map[(int(guild_id), str(tag))] = int(message_id)

    async def _clear_map_tag(self, guild_id: int, tag: str) -> None:
        await self._ensure_tables()
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(f"DELETE FROM {self.TABLE_MAP} WHERE guild_id=:gid AND tag=:tag"),
                    {"gid": int(guild_id), "tag": str(tag)},
                )
        self._msg_map.pop((int(guild_id), str(tag)), None)

    async def _clear_map_guild(self, guild_id: int) -> None:
        await self._ensure_tables()
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(f"DELETE FROM {self.TABLE_MAP} WHERE guild_id=:gid"),
                    {"gid": int(guild_id)},
                )

        gid = int(guild_id)
        for tag in self._tags():
            self._msg_map.pop((gid, tag), None)

    def _message_has_tag(self, message: discord.Message, tag: str) -> bool:
        if not message.embeds:
            return False
        e = message.embeds[0]
        if not e.footer or not e.footer.text:
            return False
        return f"{self.FOOTER_TAG_PREFIX}{tag}" in e.footer.text

    def _extract_footer_tag(self, message: discord.Message) -> Optional[str]:
        if not message.embeds:
            return None
        e = message.embeds[0]
        if not e.footer or not e.footer.text:
            return None
        footer = str(e.footer.text)
        if self.FOOTER_TAG_PREFIX not in footer:
            return None
        tag = footer.split(self.FOOTER_TAG_PREFIX, 1)[1].strip()
        if tag not in self._tags():
            return None
        return tag

    async def _get_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        ch = guild.get_channel(self.LEADERBOARD_CHANNEL_ID)
        if isinstance(ch, discord.TextChannel):
            return ch
        try:
            fetched = await self.bot.fetch_channel(self.LEADERBOARD_CHANNEL_ID)
            if isinstance(fetched, discord.TextChannel) and fetched.guild.id == guild.id:
                return fetched
        except Exception:
            return None
        return None

    def _can_post(self, guild: discord.Guild, ch: discord.TextChannel) -> bool:
        me = guild.get_member(self.bot.user.id) if self.bot.user else None
        if me is None:
            return False
        perms = ch.permissions_for(me)
        return bool(perms.view_channel and perms.send_messages)

    async def _safe_send(self, ch: discord.TextChannel, *, embed: discord.Embed) -> Optional[discord.Message]:
        backoff = 0.0
        for _ in range(6):
            try:
                if backoff > 0:
                    await asyncio.sleep(backoff)
                return await ch.send(embed=embed)
            except discord.HTTPException as e:
                if e.status == 429:
                    retry_after = getattr(e, "retry_after", None)
                    if retry_after is None:
                        retry_after = random.uniform(self.BACKOFF_MIN, self.BACKOFF_MAX)
                    backoff = float(retry_after) + random.uniform(0.25, 0.9)
                    continue
                return None
            except Exception:
                return None
        return None

    async def _safe_edit(self, msg: discord.Message, *, embed: discord.Embed) -> bool:
        backoff = 0.0
        for _ in range(6):
            try:
                if backoff > 0:
                    await asyncio.sleep(backoff)
                await msg.edit(embed=embed)
                return True
            except discord.HTTPException as e:
                if e.status == 429:
                    retry_after = getattr(e, "retry_after", None)
                    if retry_after is None:
                        retry_after = random.uniform(self.BACKOFF_MIN, self.BACKOFF_MAX)
                    backoff = float(retry_after) + random.uniform(0.25, 0.9)
                    continue
                return False
            except Exception:
                return False
        return False

    async def _safe_delete(self, msg: discord.Message) -> bool:
        backoff = 0.0
        for _ in range(4):
            try:
                if backoff > 0:
                    await asyncio.sleep(backoff)
                await msg.delete()
                return True
            except discord.NotFound:
                return True
            except discord.HTTPException as e:
                if e.status == 429:
                    retry_after = getattr(e, "retry_after", None)
                    if retry_after is None:
                        retry_after = random.uniform(self.BACKOFF_MIN, self.BACKOFF_MAX)
                    backoff = float(retry_after) + random.uniform(0.25, 0.9)
                    continue
                return False
            except Exception:
                return False
        return False

    async def _cleanup_channel_messages(self, guild: discord.Guild, ch: discord.TextChannel, *, full_reset: bool) -> int:
        tags = set(self._tags())
        tagged_by_tag: dict[str, list[discord.Message]] = {tag: [] for tag in tags}
        clutter: list[discord.Message] = []
        deleted = 0

        history_limit = None if full_reset else int(self.CLEANUP_HISTORY_LIMIT)
        async for msg in ch.history(limit=history_limit):
            tag = self._extract_footer_tag(msg)
            if tag and tag in tags:
                tagged_by_tag[tag].append(msg)
            else:
                clutter.append(msg)

        if full_reset:
            await self._clear_map_guild(guild.id)
            for msg in clutter:
                if await self._safe_delete(msg):
                    deleted += 1
                await asyncio.sleep(0.25)
            for dupes in tagged_by_tag.values():
                for msg in dupes:
                    if await self._safe_delete(msg):
                        deleted += 1
                    await asyncio.sleep(0.25)
            return deleted

        keep_ids: set[int] = set()
        for tag in tags:
            msgs = tagged_by_tag[tag]
            if not msgs:
                await self._clear_map_tag(guild.id, tag)
                continue

            mapped_mid = self._msg_map.get((guild.id, tag))
            keep_msg = next((m for m in msgs if mapped_mid and m.id == mapped_mid), msgs[0])
            keep_ids.add(keep_msg.id)
            await self._set_map(guild.id, ch.id, tag, keep_msg.id)

            for msg in msgs:
                if msg.id == keep_msg.id:
                    continue
                if await self._safe_delete(msg):
                    deleted += 1
                await asyncio.sleep(0.25)

        for msg in clutter:
            if msg.id in keep_ids:
                continue
            if await self._safe_delete(msg):
                deleted += 1
            await asyncio.sleep(0.25)

        return deleted

    async def _fetch_mapped_message(self, ch: discord.TextChannel, guild_id: int, tag: str) -> Optional[discord.Message]:
        mid = self._msg_map.get((int(guild_id), str(tag)))
        if not mid:
            return None
        try:
            msg = await ch.fetch_message(int(mid))
        except Exception:
            return None
        if not self._message_has_tag(msg, tag):
            return None
        return msg

    async def _ensure_all_messages_exist_burst(self, guild: discord.Guild, ch: discord.TextChannel) -> None:
        tags = self._tags()
        missing: list[str] = []

        for tag in tags:
            msg = await self._fetch_mapped_message(ch, guild.id, tag)
            if msg is None:
                missing.append(tag)

        if not missing:
            return

        for tag in missing:
            try:
                embed = await self._build_embed(guild, tag=tag, limit=self.AUTO_LIMIT)
                sent = await self._safe_send(ch, embed=embed)
                if sent is None:
                    return
                await self._set_map(guild.id, ch.id, tag, sent.id)
            except Exception:
                return
            await asyncio.sleep(self.CREATE_BURST_DELAY)

    @tasks.loop(seconds=TICK_SECONDS)
    async def paced_loop(self) -> None:
        async with self._lock:
            now = time.time()
            tags = self._tags()

            for guild in self.bot.guilds:
                st = self._gstate.get(guild.id)
                if st is None:
                    st = _GuildState(tag_index=random.randrange(0, len(tags)))
                    self._gstate[guild.id] = st

                if st.backoff_until > now:
                    continue
                if now - st.last_api_at < self.MIN_SECONDS_BETWEEN_API_CALLS_PER_GUILD:
                    continue

                ch = await self._get_channel(guild)
                if ch is None:
                    continue
                if not self._can_post(guild, ch):
                    continue

                if guild.id not in self._loaded_guilds:
                    await self._load_map_for_guild(guild.id)

                if now - st.last_cleanup_at >= self.CLEANUP_INTERVAL_SECONDS:
                    try:
                        await self._cleanup_channel_messages(guild, ch, full_reset=False)
                    except Exception:
                        pass
                    st.last_cleanup_at = time.time()

                # Create missing messages in a burst if needed
                await self._ensure_all_messages_exist_burst(guild, ch)

                # Edit exactly one per tick
                tag = tags[st.tag_index % len(tags)]
                st.tag_index = (st.tag_index + 1) % len(tags)

                msg = await self._fetch_mapped_message(ch, guild.id, tag)
                if msg is None:
                    # mapping broke or message removed, recreate this one
                    try:
                        embed = await self._build_embed(guild, tag=tag, limit=self.AUTO_LIMIT)
                        sent = await self._safe_send(ch, embed=embed)
                        if sent is not None:
                            await self._set_map(guild.id, ch.id, tag, sent.id)
                        st.last_api_at = time.time()
                    except Exception:
                        pass
                    continue

                try:
                    new_embed = await self._build_embed(guild, tag=tag, limit=self.AUTO_LIMIT)
                    new_fp = _embed_fingerprint(new_embed)
                    old_fp = _msg_embed_fingerprint(msg)

                    if new_fp == old_fp:
                        st.last_api_at = time.time()
                        continue

                    ok = await self._safe_edit(msg, embed=new_embed)
                    st.last_api_at = time.time()

                    if not ok:
                        st.backoff_until = time.time() + random.uniform(self.BACKOFF_MIN, self.BACKOFF_MAX)

                except discord.NotFound:
                    await self._clear_map_tag(guild.id, tag)
                except discord.Forbidden:
                    st.backoff_until = time.time() + random.uniform(self.BACKOFF_MIN, self.BACKOFF_MAX)
                except discord.HTTPException as e:
                    if getattr(e, "status", None) == 429:
                        st.backoff_until = time.time() + random.uniform(self.BACKOFF_MIN, self.BACKOFF_MAX)
                except Exception:
                    pass

    @paced_loop.before_loop
    async def _before_loop(self) -> None:
        await self.bot.wait_until_ready()
        await asyncio.sleep(random.uniform(*self.STARTUP_DELAY_RANGE))

    async def _build_embed(self, guild: discord.Guild, *, tag: str, limit: int) -> discord.Embed:
        n = max(1, min(int(limit or 10), 25))
        gid = guild.id
        today = _utc_today()

        month_start = _first_of_month_utc(today)
        if month_start < SEASON_START_UTC:
            month_start = SEASON_START_UTC

        month_name = _month_label(today)

        def make_embed(*, title: str, color: discord.Color, subtitle: str) -> discord.Embed:
            e = discord.Embed(title=title, description=subtitle, color=color)
            if guild.icon:
                e.set_thumbnail(url=guild.icon.url)
            return e

        async def compact_lines(rows: list[tuple], *, value_fmt) -> tuple[str, str]:
            if not rows:
                return "Nobody yet.", "No data yet."

            first = rows[0]
            rest = rows[1:]

            top_name = await _display_name(self.bot, guild, int(first[0]))
            top_value = value_fmt(first)
            spotlight = f"**🥇 {top_name}**  •  **{top_value}**"

            lines: list[str] = []
            for idx, r in enumerate(rest, start=2):
                nm = await _display_name(self.bot, guild, int(r[0]))
                lines.append(f"{_medal(idx)} {nm}  •  {value_fmt(r)}")

            return spotlight, "\n".join(lines) if lines else "No runners-up yet."

        if tag == self.TAG_MONTHLY_CHAT:
            rows = await self._query_monthly_messages(gid, month_start, today, n, exclude_users=True)
            e = make_embed(
                title="📅 Monthly Chatter",
                color=discord.Color.teal(),
                subtitle=f"**{month_name}**  •  {month_start.isoformat()} to {today.isoformat()} (UTC)",
            )
            spotlight, rest = await compact_lines(rows, value_fmt=lambda r: f"{_fmt_int(int(r[1] or 0))} msgs")
            e.add_field(name="👑 #1", value=spotlight, inline=False)
            e.add_field(name=f"Top {min(n, 10)}", value=rest, inline=False)
            e.set_footer(text=f"{self.FOOTER_TAG_PREFIX}{tag}")
            return e

        if tag == self.TAG_MONTHLY_VC:
            rows = await self._query_monthly_vctime(gid, month_start, today, n, exclude_users=True)
            e = make_embed(
                title="📅 Monthly VC",
                color=discord.Color.purple(),
                subtitle=f"**{month_name}**  •  {month_start.isoformat()} to {today.isoformat()} (UTC)",
            )
            spotlight, rest = await compact_lines(rows, value_fmt=lambda r: _fmt_duration(int(r[1] or 0)))
            e.add_field(name="👑 #1", value=spotlight, inline=False)
            e.add_field(name=f"Top {min(n, 10)}", value=rest, inline=False)
            e.set_footer(text=f"{self.FOOTER_TAG_PREFIX}{tag}")
            return e

        if tag == self.TAG_MESSAGES:
            rows = await self._query_messages_since_start(gid, SEASON_START_UTC, n)
            e = make_embed(
                title="💬 Most Messages",
                color=discord.Color.green(),
                subtitle=f"Tracked since {SEASON_START_UTC.isoformat()} (UTC)",
            )
            spotlight, rest = await compact_lines(rows, value_fmt=lambda r: f"{_fmt_int(int(r[1] or 0))} msgs")
            e.add_field(name="👑 #1", value=spotlight, inline=False)
            e.add_field(name=f"Top {min(n, 10)}", value=rest, inline=False)
            e.set_footer(text=f"{self.FOOTER_TAG_PREFIX}{tag}")
            return e

        if tag == self.TAG_LEVELS:
            rows = await self._query_levels(gid, n)
            e = make_embed(
                title="🧠 Highest Level",
                color=discord.Color.blurple(),
                subtitle="Global XP leaderboard",
            )

            if not rows:
                e.add_field(name="👑 #1", value="Nobody yet.", inline=False)
                e.add_field(name=f"Top {min(n, 10)}", value="No data yet.", inline=False)
                e.set_footer(text=f"{self.FOOTER_TAG_PREFIX}{tag}")
                return e

            top_name = await _display_name(self.bot, guild, int(rows[0][0]))
            top_val = f"Lvl {_fmt_int(int(rows[0][1] or 0))}  •  {_fmt_int(int(rows[0][2] or 0))} XP"
            spotlight = f"**🥇 {top_name}**  •  **{top_val}**"

            lines: list[str] = []
            for idx, r in enumerate(rows[1:], start=2):
                nm = await _display_name(self.bot, guild, int(r[0]))
                val = f"Lvl {_fmt_int(int(r[1] or 0))}  •  {_fmt_int(int(r[2] or 0))} XP"
                lines.append(f"{_medal(idx)} {nm}  •  {val}")

            e.add_field(name="👑 #1", value=spotlight, inline=False)
            e.add_field(name=f"Top {min(n, 10)}", value="\n".join(lines) if lines else "No runners-up yet.", inline=False)
            e.set_footer(text=f"{self.FOOTER_TAG_PREFIX}{tag}")
            return e

        if tag == self.TAG_VCTIME:
            rows = await self._query_vctime_since_start(gid, SEASON_START_UTC, n)
            e = make_embed(
                title="🎙️ Most VC Time",
                color=discord.Color.purple(),
                subtitle=f"Tracked since {SEASON_START_UTC.isoformat()} (UTC)",
            )
            spotlight, rest = await compact_lines(rows, value_fmt=lambda r: _fmt_duration(int(r[1] or 0)))
            e.add_field(name="👑 #1", value=spotlight, inline=False)
            e.add_field(name=f"Top {min(n, 10)}", value=rest, inline=False)
            e.set_footer(text=f"{self.FOOTER_TAG_PREFIX}{tag}")
            return e

        if tag == self.TAG_ACHIEVEMENTS:
            rows = await self._query_achievements(gid, n)
            e = make_embed(
                title="🏆 Most Achievements",
                color=discord.Color.fuchsia(),
                subtitle="Total unlocked achievements • ties broken by rarest unlock",
            )
            spotlight, rest = await compact_lines(rows, value_fmt=_achievement_spotlight)
            e.add_field(name="👑 #1", value=spotlight, inline=False)
            e.add_field(name=f"Top {min(n, 10)}", value=rest, inline=False)
            e.set_footer(text=f"{self.FOOTER_TAG_PREFIX}{tag}")
            return e

        rows = await self._query_money(gid, n)
        e = make_embed(
            title="💰 Most Silver",
            color=discord.Color.gold(),
            subtitle="Current wallet balance",
        )
        spotlight, rest = await compact_lines(rows, value_fmt=lambda r: f"{_fmt_int(int(r[1] or 0))} silver")
        e.add_field(name="👑 #1", value=spotlight, inline=False)
        e.add_field(name=f"Top {min(n, 10)}", value=rest, inline=False)
        e.set_footer(text=f"{self.FOOTER_TAG_PREFIX}{tag}")
        return e

    async def _query_monthly_messages(self, guild_id: int, start: date, end: date, limit: int, *, exclude_users: bool):
        async with self.sessionmaker() as session:
            q = (
                select(
                    ActivityDailyRow.user_id,
                    func.coalesce(func.sum(ActivityDailyRow.message_count), 0).label("msg_total"),
                )
                .where(ActivityDailyRow.guild_id == int(guild_id))
                .where(ActivityDailyRow.day >= start)
                .where(ActivityDailyRow.day <= end)
            )
            if exclude_users and EXCLUDED_USER_IDS:
                q = q.where(ActivityDailyRow.user_id.notin_(list(EXCLUDED_USER_IDS)))

            rows = (
                await session.execute(
                    q.group_by(ActivityDailyRow.user_id)
                    .order_by(func.sum(ActivityDailyRow.message_count).desc())
                    .limit(limit)
                )
            ).all()
        return rows

    async def _query_monthly_vctime(self, guild_id: int, start: date, end: date, limit: int, *, exclude_users: bool):
        async with self.sessionmaker() as session:
            q = (
                select(
                    ActivityDailyRow.user_id,
                    func.coalesce(func.sum(ActivityDailyRow.vc_seconds), 0).label("vc_total"),
                )
                .where(ActivityDailyRow.guild_id == int(guild_id))
                .where(ActivityDailyRow.day >= start)
                .where(ActivityDailyRow.day <= end)
            )
            if exclude_users and EXCLUDED_USER_IDS:
                q = q.where(ActivityDailyRow.user_id.notin_(list(EXCLUDED_USER_IDS)))

            rows = (
                await session.execute(
                    q.group_by(ActivityDailyRow.user_id)
                    .order_by(func.sum(ActivityDailyRow.vc_seconds).desc())
                    .limit(limit)
                )
            ).all()
        return rows

    async def _query_messages_since_start(self, guild_id: int, start: date, limit: int):
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    select(
                        ActivityDailyRow.user_id,
                        func.coalesce(func.sum(ActivityDailyRow.message_count), 0).label("msg_total"),
                    )
                    .where(ActivityDailyRow.guild_id == int(guild_id))
                    .where(ActivityDailyRow.day >= start)
                    .group_by(ActivityDailyRow.user_id)
                    .order_by(func.sum(ActivityDailyRow.message_count).desc())
                    .limit(limit)
                )
            ).all()
        return rows

    async def _query_levels(self, guild_id: int, limit: int):
        async with self.sessionmaker() as session:
            raw_rows = (
                await session.execute(
                    select(XpRow.user_id, XpRow.xp_total)
                    .where(XpRow.guild_id == int(guild_id))
                )
            ).all()

        canonical_rows: list[tuple[int, int, int]] = []
        for user_id, xp_total in raw_rows:
            xp = int(xp_total or 0)
            lvl = int(get_xp_progress(xp).level)
            canonical_rows.append((int(user_id), lvl, xp))

        canonical_rows.sort(key=lambda r: (r[1], r[2]), reverse=True)
        return canonical_rows[: max(int(limit), 0)]

    async def _query_vctime_since_start(self, guild_id: int, start: date, limit: int):
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    select(
                        ActivityDailyRow.user_id,
                        func.coalesce(func.sum(ActivityDailyRow.vc_seconds), 0).label("vc_total"),
                    )
                    .where(ActivityDailyRow.guild_id == int(guild_id))
                    .where(ActivityDailyRow.day >= start)
                    .group_by(ActivityDailyRow.user_id)
                    .order_by(func.sum(ActivityDailyRow.vc_seconds).desc())
                    .limit(limit)
                )
            ).all()
        return rows


    async def _query_achievements(self, guild_id: int, limit: int):
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    select(
                        UserAchievementRow.user_id,
                        UserAchievementRow.achievement_key,
                        func.max(UserAchievementRow.unlocked_at).label("latest_unlocked_at"),
                        func.count(UserAchievementRow.id).label("achievement_total"),
                    )
                    .where(UserAchievementRow.guild_id == int(guild_id))
                    .group_by(UserAchievementRow.user_id, UserAchievementRow.achievement_key)
                )
            ).all()

        leaderboard: list[tuple[int, int, str, str, str, int, datetime]] = []
        by_user: dict[int, dict[str, object]] = {}
        for user_id, achievement_key, latest_unlocked_at, _ in rows:
            definition = ACHIEVEMENT_CATALOG.get(str(achievement_key))
            if definition is None:
                continue

            uid = int(user_id)
            record = by_user.setdefault(
                uid,
                {
                    "count": 0,
                    "best_rank": -1,
                    "best_sort_order": 10**9,
                    "best_name": "No achievement data",
                    "best_icon": "🏅",
                    "best_tier": AchievementTier.COMMON.value,
                    "latest_unlocked_at": datetime.min.replace(tzinfo=timezone.utc),
                },
            )
            record["count"] = int(record["count"]) + 1

            tier_rank = _TIER_RANK.get(definition.tier, -1)
            best_rank = int(record["best_rank"])
            best_sort_order = int(record["best_sort_order"])
            best_latest = record["latest_unlocked_at"]
            latest = latest_unlocked_at or datetime.min.replace(tzinfo=timezone.utc)
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)

            should_replace = (
                tier_rank > best_rank
                or (tier_rank == best_rank and definition.sort_order < best_sort_order)
                or (tier_rank == best_rank and definition.sort_order == best_sort_order and latest > best_latest)
            )
            if should_replace:
                record["best_rank"] = tier_rank
                record["best_sort_order"] = definition.sort_order
                record["best_name"] = definition.name
                record["best_icon"] = definition.icon
                record["best_tier"] = definition.tier.value
                record["latest_unlocked_at"] = latest

        for uid, record in by_user.items():
            leaderboard.append(
                (
                    uid,
                    int(record["count"]),
                    str(record["best_icon"]),
                    str(record["best_name"]),
                    str(record["best_tier"]),
                    int(record["best_rank"]),
                    record["latest_unlocked_at"],
                )
            )

        leaderboard.sort(
            key=lambda row: (
                -row[1],
                -row[5],
                row[3].lower(),
                -row[6].timestamp() if isinstance(row[6], datetime) else 0.0,
                row[0],
            )
        )
        return leaderboard[:limit]

    async def _query_money(self, guild_id: int, limit: int):
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    select(WalletRow.user_id, WalletRow.silver)
                    .where(WalletRow.guild_id == int(guild_id))
                    .order_by(WalletRow.silver.desc())
                    .limit(limit)
                )
            ).all()
        return rows

    leaderboard = app_commands.Group(name="leaderboard", description="View server leaderboards.")

    @leaderboard.command(name="refresh_channel", description="Force ensure leaderboard messages exist now.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def refresh_channel(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        async with self._lock:
            ch = await self._get_channel(interaction.guild)
            if ch is None:
                await interaction.followup.send("Leaderboard channel not found.", ephemeral=True)
                return
            if not self._can_post(interaction.guild, ch):
                await interaction.followup.send("Missing permissions in leaderboard channel.", ephemeral=True)
                return

            if interaction.guild.id not in self._loaded_guilds:
                await self._load_map_for_guild(interaction.guild.id)

            await self._ensure_all_messages_exist_burst(interaction.guild, ch)

        await interaction.followup.send("✅ Leaderboards ensured. Auto-updates run one per minute.", ephemeral=True)

    @leaderboard.command(name="reset_channel", description="Delete all messages in leaderboard channel and recreate clean board.")
    @app_commands.checks.has_permissions(manage_channels=True)
    async def reset_channel(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        async with self._lock:
            ch = await self._get_channel(interaction.guild)
            if ch is None:
                await interaction.followup.send("Leaderboard channel not found.", ephemeral=True)
                return
            if not self._can_post(interaction.guild, ch):
                await interaction.followup.send("Missing permissions in leaderboard channel.", ephemeral=True)
                return

            if interaction.guild.id not in self._loaded_guilds:
                await self._load_map_for_guild(interaction.guild.id)

            deleted = await self._cleanup_channel_messages(interaction.guild, ch, full_reset=True)
            await self._ensure_all_messages_exist_burst(interaction.guild, ch)

        await interaction.followup.send(
            f"✅ Leaderboard channel reset complete. Deleted {deleted} old messages and rebuilt category posts.",
            ephemeral=True,
        )

    @leaderboard.command(name="debug_exclusions", description="Show excluded user IDs used by this cog.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def debug_exclusions(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await interaction.response.send_message(
            "Monthly exclusions:\n" + "\n".join(f"- `{uid}`" for uid in sorted(EXCLUDED_USER_IDS)),
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(LeaderboardsCog(bot))
