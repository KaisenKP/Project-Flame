from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
from sqlalchemy import func, select

from db.models import ActivityDailyRow, WalletRow, XpRow
from services.db import sessions


SEASON_START_UTC = date(2026, 1, 25)

EXCLUDED_USER_IDS: set[int] = {
    326498486335963137,  # Mavis
    537375301915901975,  # Kai
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _truncate(text: str, max_len: int) -> str:
    text = str(text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 15] + "...<truncated>"


class LeaderboardSyncCog(commands.Cog):
    TAG_MONTHLY_CHAT = "monthly_chat"
    TAG_MONTHLY_VC = "monthly_vc"
    TAG_MESSAGES = "messages"
    TAG_LEVELS = "levels"
    TAG_VCTIME = "vctime"
    TAG_MONEY = "money"

    DEFAULT_LIMIT = 10
    WRITE_INTERVAL_SECONDS = 300.0

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self._lock = asyncio.Lock()
        self._started = False

        self.output_dir = Path(os.getenv("LEADERBOARD_SNAPSHOT_DIR", "data/leaderboard_snapshots")).resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.website_sync_enabled = str(os.getenv("LEADERBOARD_SYNC_ENABLED", "false")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

        # Base44 direct entity API config
        self.base44_app_id = str(os.getenv("BASE44_APP_ID", "69acbce85ee689a96f4dd42f")).strip()
        self.base44_api_base = str(os.getenv("BASE44_API_BASE", "https://app.base44.com")).strip().rstrip("/")
        self.base44_api_key = str(os.getenv("BASE44_API_KEY", "")).strip()
        self.sync_timeout_seconds = float(os.getenv("LEADERBOARD_SYNC_TIMEOUT", "15"))

        self.http_session: Optional[aiohttp.ClientSession] = None
        self._last_post_error: Optional[str] = None

        if self.bot.is_ready():
            self._kickoff()

    async def cog_load(self) -> None:
        if self.http_session is None or self.http_session.closed:
            timeout = aiohttp.ClientTimeout(total=self.sync_timeout_seconds)
            self.http_session = aiohttp.ClientSession(timeout=timeout)

        if self.bot.is_ready():
            self._kickoff()

    def cog_unload(self) -> None:
        try:
            self.snapshot_loop.cancel()
        except Exception:
            pass

        session = self.http_session
        self.http_session = None
        if session and not session.closed:
            try:
                asyncio.create_task(session.close())
            except Exception:
                pass

    @commands.Cog.listener("on_ready")
    async def _on_ready(self) -> None:
        self._kickoff()

    def _kickoff(self) -> None:
        if self._started:
            return
        self._started = True
        if not self.snapshot_loop.is_running():
            self.snapshot_loop.start()

    def _tags(self) -> list[str]:
        return [
            self.TAG_MONTHLY_CHAT,
            self.TAG_MONTHLY_VC,
            self.TAG_MESSAGES,
            self.TAG_LEVELS,
            self.TAG_VCTIME,
            self.TAG_MONEY,
        ]

    def _snapshot_path(self, guild_id: int, tag: str) -> Path:
        guild_dir = self.output_dir / str(int(guild_id))
        guild_dir.mkdir(parents=True, exist_ok=True)
        return guild_dir / f"{tag}.json"

    def _base44_entity_collection_url(self) -> str:
        return f"{self.base44_api_base}/api/apps/{self.base44_app_id}/entities/LeaderboardSnapshot"

    def _base44_entity_item_url(self, entity_id: str) -> str:
        return f"{self._base44_entity_collection_url()}/{entity_id}"

    def _base44_headers(self) -> dict[str, str]:
        return {
            "api_key": self.base44_api_key,
            "Content-Type": "application/json",
        }

    async def _resolve_user_info(self, guild: discord.Guild, user_id: int) -> tuple[str, Optional[str]]:
        uid = int(user_id)

        member = guild.get_member(uid)
        if member is not None:
            return member.display_name, str(member.display_avatar.url)

        user = self.bot.get_user(uid)
        if user is not None:
            return user.name, str(user.display_avatar.url)

        try:
            fetched = await self.bot.fetch_user(uid)
            return fetched.name, str(fetched.display_avatar.url)
        except Exception:
            return f"User {uid}", None

    async def _rows_to_entries(
        self,
        guild: discord.Guild,
        rows: list[tuple],
        *,
        tag: str,
    ) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []

        for idx, row in enumerate(rows, start=1):
            user_id = _safe_int(row[0])
            username, avatar_url = await self._resolve_user_info(guild, user_id)

            entry: dict[str, Any] = {
                "rank": idx,
                "user_id": str(user_id),
                "username": username,
                "avatar_url": avatar_url,
            }

            if tag in {self.TAG_MONTHLY_CHAT, self.TAG_MESSAGES}:
                total = _safe_int(row[1])
                entry["primary_value"] = total
                entry["display_value"] = f"{_fmt_int(total)} msgs"

            elif tag in {self.TAG_MONTHLY_VC, self.TAG_VCTIME}:
                total = _safe_int(row[1])
                entry["primary_value"] = total
                entry["display_value"] = _fmt_duration(total)

            elif tag == self.TAG_MONEY:
                silver = _safe_int(row[1])
                entry["primary_value"] = silver
                entry["display_value"] = f"{_fmt_int(silver)} silver"

            elif tag == self.TAG_LEVELS:
                level_cached = _safe_int(row[1])
                xp_total = _safe_int(row[2])
                entry["primary_value"] = level_cached
                entry["secondary_value"] = xp_total
                entry["display_value"] = f"Lvl {_fmt_int(level_cached)} • {_fmt_int(xp_total)} XP"

            else:
                raw = _safe_int(row[1] if len(row) > 1 else 0)
                entry["primary_value"] = raw
                entry["display_value"] = str(raw)

            entries.append(entry)

        return entries

    async def _build_snapshot(self, guild: discord.Guild, *, tag: str, limit: int) -> dict[str, Any]:
        n = max(1, min(int(limit or self.DEFAULT_LIMIT), 25))
        today = _utc_today()
        guild_id = int(guild.id)

        month_start = _first_of_month_utc(today)
        if month_start < SEASON_START_UTC:
            month_start = SEASON_START_UTC

        month_name = _month_label(today)

        if tag == self.TAG_MONTHLY_CHAT:
            rows = await self._query_monthly_messages(guild_id, month_start, today, n, exclude_users=True)
            title = "Monthly Chatter"
            subtitle = f"{month_name} • {month_start.isoformat()} to {today.isoformat()} (UTC)"

        elif tag == self.TAG_MONTHLY_VC:
            rows = await self._query_monthly_vctime(guild_id, month_start, today, n, exclude_users=True)
            title = "Monthly VC"
            subtitle = f"{month_name} • {month_start.isoformat()} to {today.isoformat()} (UTC)"

        elif tag == self.TAG_MESSAGES:
            rows = await self._query_messages_since_start(guild_id, SEASON_START_UTC, n)
            title = "Most Messages"
            subtitle = f"Tracked since {SEASON_START_UTC.isoformat()} (UTC)"

        elif tag == self.TAG_LEVELS:
            rows = await self._query_levels(guild_id, n)
            title = "Highest Level"
            subtitle = "Global XP leaderboard"

        elif tag == self.TAG_VCTIME:
            rows = await self._query_vctime_since_start(guild_id, SEASON_START_UTC, n)
            title = "Most VC Time"
            subtitle = f"Tracked since {SEASON_START_UTC.isoformat()} (UTC)"

        elif tag == self.TAG_MONEY:
            rows = await self._query_money(guild_id, n)
            title = "Most Silver"
            subtitle = "Current wallet balance"

        else:
            raise ValueError(f"Unknown leaderboard tag: {tag}")

        entries = await self._rows_to_entries(guild, rows, tag=tag)

        snapshot: dict[str, Any] = {
            "guild_id": str(guild_id),
            "guild_name": guild.name,
            "category": tag,
            "title": title,
            "subtitle": subtitle,
            "generated_at": _utc_now_iso(),
            "limit": n,
            "entries": entries,
        }
        return snapshot

    async def _write_snapshot_to_disk(self, snapshot: dict[str, Any]) -> Path:
        guild_id = _safe_int(snapshot.get("guild_id"))
        tag = str(snapshot.get("category") or "unknown").strip() or "unknown"

        path = self._snapshot_path(guild_id, tag)
        tmp_path = path.with_suffix(".json.tmp")

        text = json.dumps(snapshot, indent=2, ensure_ascii=False)
        tmp_path.write_text(text, encoding="utf-8")
        tmp_path.replace(path)

        return path

    async def _base44_find_existing_snapshot_id(self, guild_id: str, category: str) -> Optional[str]:
        if self.http_session is None or self.http_session.closed:
            timeout = aiohttp.ClientTimeout(total=self.sync_timeout_seconds)
            self.http_session = aiohttp.ClientSession(timeout=timeout)

        params = {
            "guild_id": guild_id,
            "category": category,
        }

        url = f"{self._base44_entity_collection_url()}?{urlencode(params)}"

        try:
            async with self.http_session.get(url, headers=self._base44_headers()) as resp:
                body_text = await resp.text()
                if not (200 <= resp.status < 300):
                    self._last_post_error = _truncate(f"Lookup HTTP {resp.status}: {body_text}", 220)
                    return None

                data = json.loads(body_text)
                if isinstance(data, list):
                    rows = data
                elif isinstance(data, dict):
                    if isinstance(data.get("items"), list):
                        rows = data["items"]
                    elif isinstance(data.get("data"), list):
                        rows = data["data"]
                    elif isinstance(data.get("results"), list):
                        rows = data["results"]
                    else:
                        rows = []

                else:
                    rows = []

                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    if str(row.get("guild_id")) == guild_id and str(row.get("category")) == category:
                        entity_id = row.get("id")
                        if entity_id is not None:
                            return str(entity_id)

                return None

        except Exception as exc:
            self._last_post_error = _truncate(f"Lookup {type(exc).__name__}: {exc}", 220)
            return None

    async def _base44_create_snapshot(self, snapshot: dict[str, Any]) -> bool:
        if self.http_session is None or self.http_session.closed:
            timeout = aiohttp.ClientTimeout(total=self.sync_timeout_seconds)
            self.http_session = aiohttp.ClientSession(timeout=timeout)

        try:
            async with self.http_session.post(
                self._base44_entity_collection_url(),
                headers=self._base44_headers(),
                json=snapshot,
            ) as resp:
                body = await resp.text()
                if 200 <= resp.status < 300:
                    return True

                self._last_post_error = _truncate(f"Create HTTP {resp.status}: {body}", 220)
                return False

        except Exception as exc:
            self._last_post_error = _truncate(f"Create {type(exc).__name__}: {exc}", 220)
            return False

    async def _base44_update_snapshot(self, entity_id: str, snapshot: dict[str, Any]) -> bool:
        if self.http_session is None or self.http_session.closed:
            timeout = aiohttp.ClientTimeout(total=self.sync_timeout_seconds)
            self.http_session = aiohttp.ClientSession(timeout=timeout)

        try:
            async with self.http_session.put(
                self._base44_entity_item_url(entity_id),
                headers=self._base44_headers(),
                json=snapshot,
            ) as resp:
                body = await resp.text()
                if 200 <= resp.status < 300:
                    return True

                self._last_post_error = _truncate(f"Update HTTP {resp.status}: {body}", 220)
                return False

        except Exception as exc:
            self._last_post_error = _truncate(f"Update {type(exc).__name__}: {exc}", 220)
            return False

    async def _upsert_snapshot_to_base44(self, snapshot: dict[str, Any]) -> bool:
        self._last_post_error = None

        if not self.website_sync_enabled:
            self._last_post_error = "Website sync is disabled."
            return False

        if not self.base44_app_id:
            self._last_post_error = "BASE44_APP_ID is not set."
            return False

        if not self.base44_api_key:
            self._last_post_error = "BASE44_API_KEY is not set."
            return False

        guild_id = str(snapshot.get("guild_id", "")).strip()
        category = str(snapshot.get("category", "")).strip()

        if not guild_id or not category:
            self._last_post_error = "Snapshot missing guild_id or category."
            return False

        entity_id = await self._base44_find_existing_snapshot_id(guild_id, category)

        if entity_id:
            return await self._base44_update_snapshot(entity_id, snapshot)

        return await self._base44_create_snapshot(snapshot)

    async def _process_guild(self, guild: discord.Guild, *, limit: int) -> dict[str, Any]:
        results: dict[str, Any] = {
            "guild_id": guild.id,
            "guild_name": guild.name,
            "written": [],
            "posted": [],
            "failed": [],
            "post_errors": [],
        }

        for tag in self._tags():
            try:
                snapshot = await self._build_snapshot(guild, tag=tag, limit=limit)
                path = await self._write_snapshot_to_disk(snapshot)
                results["written"].append({"tag": tag, "path": str(path)})

                posted = await self._upsert_snapshot_to_base44(snapshot)
                if posted:
                    results["posted"].append(tag)
                elif self.website_sync_enabled:
                    results["post_errors"].append(
                        {
                            "tag": tag,
                            "error": self._last_post_error or "Unknown Base44 sync failure.",
                        }
                    )

            except Exception as exc:
                results["failed"].append({"tag": tag, "error": _truncate(str(exc), 220)})

        return results

    @tasks.loop(seconds=WRITE_INTERVAL_SECONDS)
    async def snapshot_loop(self) -> None:
        async with self._lock:
            for guild in self.bot.guilds:
                try:
                    await self._process_guild(guild, limit=self.DEFAULT_LIMIT)
                except Exception:
                    pass

    @snapshot_loop.before_loop
    async def _before_snapshot_loop(self) -> None:
        await self.bot.wait_until_ready()
        await asyncio.sleep(8.0)

    @app_commands.command(
        name="leaderboard_export_now",
        description="Export leaderboard snapshots to JSON files right now.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def leaderboard_export_now(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        async with self._lock:
            result = await self._process_guild(interaction.guild, limit=self.DEFAULT_LIMIT)

        written_lines = "\n".join(
            f"• `{item['tag']}` → `{item['path']}`"
            for item in result["written"]
        ) or "None"

        posted_lines = "\n".join(f"• `{tag}`" for tag in result["posted"]) or "None"

        failed_lines = "\n".join(
            f"• `{item['tag']}`: {_truncate(item['error'], 140)}"
            for item in result["failed"]
        ) or "None"

        post_error_lines = "\n".join(
            f"• `{item['tag']}`: {_truncate(item['error'], 120)}"
            for item in result["post_errors"]
        ) or "None"

        msg = (
            f"✅ Snapshot export finished for **{interaction.guild.name}**\n\n"
            f"**Written files**\n{written_lines}\n\n"
            f"**Synced to Base44**\n{posted_lines}\n\n"
            f"**Base44 sync errors**\n{post_error_lines}\n\n"
            f"**Failures**\n{failed_lines}"
        )

        msg = _truncate(msg, 1900)
        await interaction.followup.send(msg, ephemeral=True)

    @app_commands.command(
        name="leaderboard_export_status",
        description="Show leaderboard snapshot export settings.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def leaderboard_export_status(self, interaction: discord.Interaction) -> None:
        masked_api_key = "set" if self.base44_api_key else "missing"
        lines = [
            f"Snapshot dir: `{self.output_dir}`",
            f"Auto export interval: `{int(self.WRITE_INTERVAL_SECONDS)}` seconds",
            f"Website sync enabled: `{self.website_sync_enabled}`",
            f"Base44 API base: `{_truncate(self.base44_api_base or 'missing', 120)}`",
            f"Base44 app id: `{self.base44_app_id or 'missing'}`",
            f"Base44 API key: `{masked_api_key}`",
            f"Collection URL: `{_truncate(self._base44_entity_collection_url(), 160)}`",
            f"Tracked categories: `{', '.join(self._tags())}`",
            f"Last sync error: `{_truncate(self._last_post_error or 'None', 180)}`",
        ]
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

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
            rows = (
                await session.execute(
                    select(XpRow.user_id, XpRow.level_cached, XpRow.xp_total)
                    .where(XpRow.guild_id == int(guild_id))
                    .order_by(XpRow.level_cached.desc(), XpRow.xp_total.desc())
                    .limit(limit)
                )
            ).all()
        return rows

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


async def setup(bot: commands.Bot):
    await bot.add_cog(LeaderboardSyncCog(bot))