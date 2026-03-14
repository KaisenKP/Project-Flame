from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks
from sqlalchemy import func, select, text

from db.models import ActivityDailyRow, CrownsWalletRow
from services.db import sessions


SEASON_START_UTC = date(2026, 1, 25)

ANNOUNCE_CHANNEL_ID = 1460858466905362478

ROLE_TOP_CHATTER = 1464059557297782794
ROLE_TOP_VCHATTER = 1464059896591941762

CROWNS_PER_WIN = 1

EXCLUDED_USER_IDS: set[int] = {
    326498486335963137,  # Mavis
    537375301915901975,  # Kai
}

DIFF_WINNER_MESSAGES = [
    "📣 Monthly recap time.\nTop Chatter: {chat}\nTop VChatter: {vc}\nCrowns paid. Keep cooking.",
    "🏆 New month, new champs.\nMost active chatter: {chat}\nMost active VC chatter: {vc}\nCrowns secured.",
    "🔥 The numbers are in.\nChat demon: {chat}\nVC demon: {vc}\nCrowns dropped. Respect.",
    "🎉 Monthly winners!\n{chat} took Chat.\n{vc} took VC.\nCrowns delivered.",
    "📊 Stats dropped.\nChat crown: {chat}\nVC crown: {vc}\nLiteral crowns, too.",
    "👑 Two crowns this month.\nChatter champ: {chat}\nVC champ: {vc}\nW payouts.",
    "🧾 Monthly results.\nMost messages: {chat}\nMost VC time: {vc}\nCrowns are yours.",
    "🚨 Leaderboard update.\nChat MVP: {chat}\nVC MVP: {vc}\nCrowns granted.",
    "✨ Month closed.\n{chat} ran the chat.\n{vc} ran VC.\nCrowns claimed.",
    "🎯 Monthly champs selected.\nChat: {chat}\nVC: {vc}\nCrowns awarded.",
]

SAME_WINNER_MESSAGES = [
    "👑 One person, two crowns.\n{user} won Top Chatter and Top VChatter.\nDouble payout.",
    "🏆 Sweep alert.\n{user} took BOTH chat and VC.\nCrowns stacked.",
    "🔥 Absolute takeover.\n{user} won both categories.\n2x crowns, easy.",
    "🎉 Double champ!\n{user} dominated chat and VC.\nDouble crowns delivered.",
    "🚨 This month’s MVP is… {user}.\nBoth titles. Both crowns.",
    "✨ Clean sweep.\n{user} owns chat AND VC.\nCrowns secured.",
    "📈 Stats went crazy.\n{user} topped both boards.\nCrowns on crowns.",
    "🫡 Salute the grinder.\n{user} won both.\nDouble crowns. W.",
    "👑 Royal month.\n{user} took both crowns.\nCrowns paid out.",
    "🎯 Perfect month.\n{user} won both titles.\nDouble crowns awarded.",
]


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _month_name(d: date) -> str:
    return d.strftime("%B %Y")


@dataclass(frozen=True)
class RangeSpec:
    key: str
    label: str
    start: date
    end: date  # inclusive


def _first_of_month(d: date) -> date:
    return date(d.year, d.month, 1)


def _prev_month_start(today_utc: date) -> date:
    y = today_utc.year
    m = today_utc.month
    if m == 1:
        return date(y - 1, 12, 1)
    return date(y, m - 1, 1)


def _prev_month_end(today_utc: date) -> date:
    this_month_start = _first_of_month(today_utc)
    return date.fromordinal(this_month_start.toordinal() - 1)


def _prev_month_range(today_utc: date) -> RangeSpec:
    pm_start = _prev_month_start(today_utc)
    pm_end = _prev_month_end(today_utc)

    start = pm_start
    if start < SEASON_START_UTC:
        start = SEASON_START_UTC

    end = pm_end
    return RangeSpec(
        key=_month_key(pm_start),
        label=_month_name(pm_start),
        start=start,
        end=end,
    )


def _current_month_range(today_utc: date) -> RangeSpec:
    month_start = _first_of_month(today_utc)
    start = month_start
    if start < SEASON_START_UTC:
        start = SEASON_START_UTC
    return RangeSpec(
        key=_month_key(month_start),
        label=_month_name(month_start),
        start=start,
        end=today_utc,
    )


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
    return f"{h:,}h {m:02d}m {sec:02d}s"


class MonthlyActivityChampionsCog(commands.Cog):
    # Payout check cadence (auto catch-up if missed the 1st UTC)
    AWARD_CHECK_EVERY_SECONDS = 60.0 * 60.0  # 60 min

    # Live role sync cadence
    ROLE_REFRESH_EVERY_SECONDS = 60.0 * 30.0  # 30 min

    TABLE_STATE = "monthly_activity_champions_state"

    CAT_CHAT = "chat"
    CAT_VC = "vc"
    CAT_AWARDED = "awarded"  # month_key paid+announced

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self._lock = asyncio.Lock()
        self._started = False

        # guild_id -> (chat_uid, vc_uid, month_key)
        self._live_role_cache: dict[int, tuple[int, int, str]] = {}

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

        if not self.monthly_award_loop.is_running():
            self.monthly_award_loop.start()
        if not self.role_refresh_loop.is_running():
            self.role_refresh_loop.start()

    def cog_unload(self) -> None:
        for t in (self.monthly_award_loop, self.role_refresh_loop):
            try:
                t.cancel()
            except Exception:
                pass

    async def _ensure_tables(self) -> None:
        state_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_STATE} (
            guild_id BIGINT NOT NULL,
            category VARCHAR(16) NOT NULL,
            month_key VARCHAR(7) NOT NULL,
            user_id BIGINT NOT NULL,
            metric_value BIGINT NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id, category)
        );
        """
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(state_sql))

        # Ensure crowns table exists too, since we write to it from here.
        crowns_sql = """
        CREATE TABLE IF NOT EXISTS crowns_wallets (
            id INT NOT NULL AUTO_INCREMENT,
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            crowns INT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uq_crowns_wallets_guild_user (guild_id, user_id),
            KEY ix_crowns_wallets_guild_id (guild_id),
            KEY ix_crowns_wallets_user_id (user_id)
        );
        """
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(crowns_sql))

    async def _add_crowns(self, guild_id: int, user_id: int, amount: int) -> None:
        amt = int(amount)
        if amt <= 0:
            return
        if int(user_id) in EXCLUDED_USER_IDS:
            return

        # Atomic increment, no lost updates.
        sql = text(
            """
            INSERT INTO crowns_wallets (guild_id, user_id, crowns)
            VALUES (:gid, :uid, :amt)
            ON DUPLICATE KEY UPDATE
                crowns = GREATEST(crowns + VALUES(crowns), 0)
            """
        )

        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    sql,
                    {
                        "gid": int(guild_id),
                        "uid": int(user_id),
                        "amt": amt,
                    },
                )

    async def _get_state(self, guild_id: int, category: str) -> Optional[tuple[str, int, int]]:
        async with self.sessionmaker() as session:
            async with session.begin():
                row = (
                    await session.execute(
                        text(
                            f"SELECT month_key, user_id, metric_value FROM {self.TABLE_STATE} "
                            "WHERE guild_id=:gid AND category=:cat LIMIT 1"
                        ),
                        {"gid": int(guild_id), "cat": str(category)},
                    )
                ).first()
                if not row:
                    return None
                return str(row[0]), int(row[1]), int(row[2])

    async def _set_state(self, guild_id: int, category: str, month_key: str, user_id: int, metric_value: int) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"""
                        INSERT INTO {self.TABLE_STATE} (guild_id, category, month_key, user_id, metric_value)
                        VALUES (:gid, :cat, :mk, :uid, :mv)
                        ON DUPLICATE KEY UPDATE
                            month_key=VALUES(month_key),
                            user_id=VALUES(user_id),
                            metric_value=VALUES(metric_value)
                        """
                    ),
                    {
                        "gid": int(guild_id),
                        "cat": str(category),
                        "mk": str(month_key),
                        "uid": int(user_id),
                        "mv": int(metric_value),
                    },
                )

    async def _winner_chat(self, guild_id: int, rng: RangeSpec) -> Optional[tuple[int, int]]:
        if rng.end < rng.start:
            return None

        async with self.sessionmaker() as session:
            q = (
                select(
                    ActivityDailyRow.user_id,
                    func.coalesce(func.sum(ActivityDailyRow.message_count), 0).label("msg_total"),
                )
                .where(ActivityDailyRow.guild_id == int(guild_id))
                .where(ActivityDailyRow.day >= rng.start)
                .where(ActivityDailyRow.day <= rng.end)
            )
            if EXCLUDED_USER_IDS:
                q = q.where(ActivityDailyRow.user_id.notin_(list(EXCLUDED_USER_IDS)))

            row = (
                await session.execute(
                    q.group_by(ActivityDailyRow.user_id)
                    .order_by(func.sum(ActivityDailyRow.message_count).desc())
                    .limit(1)
                )
            ).first()

        if not row:
            return None
        uid = int(row[0])
        val = int(row[1] or 0)
        if val <= 0:
            return None
        return uid, val

    async def _winner_vc(self, guild_id: int, rng: RangeSpec) -> Optional[tuple[int, int]]:
        if rng.end < rng.start:
            return None

        async with self.sessionmaker() as session:
            q = (
                select(
                    ActivityDailyRow.user_id,
                    func.coalesce(func.sum(ActivityDailyRow.vc_seconds), 0).label("vc_total"),
                )
                .where(ActivityDailyRow.guild_id == int(guild_id))
                .where(ActivityDailyRow.day >= rng.start)
                .where(ActivityDailyRow.day <= rng.end)
            )
            if EXCLUDED_USER_IDS:
                q = q.where(ActivityDailyRow.user_id.notin_(list(EXCLUDED_USER_IDS)))

            row = (
                await session.execute(
                    q.group_by(ActivityDailyRow.user_id)
                    .order_by(func.sum(ActivityDailyRow.vc_seconds).desc())
                    .limit(1)
                )
            ).first()

        if not row:
            return None
        uid = int(row[0])
        val = int(row[1] or 0)
        if val <= 0:
            return None
        return uid, val

    async def _get_member(self, guild: discord.Guild, user_id: int) -> Optional[discord.Member]:
        m = guild.get_member(int(user_id))
        if m is not None:
            return m
        try:
            return await guild.fetch_member(int(user_id))
        except Exception:
            return None

    async def _apply_role_swap(
        self,
        guild: discord.Guild,
        role_id: int,
        prev_user_id: int,
        new_user_id: int,
        reason: str,
    ) -> tuple[Optional[discord.Member], Optional[str]]:
        if int(new_user_id) in EXCLUDED_USER_IDS:
            return None, "Winner is excluded from monthly challenges."

        role = guild.get_role(int(role_id))
        if role is None:
            return None, f"Role missing: `{role_id}`"

        me = guild.get_member(self.bot.user.id) if self.bot.user else None
        if me is None:
            return None, "Bot member missing"
        if not me.guild_permissions.manage_roles:
            return None, "Missing Manage Roles permission"
        if role >= me.top_role:
            return None, f"Role hierarchy issue: move bot role above {role.name}"

        # HARD ENFORCE: remove role from anyone who has it except the new winner.
        to_remove = [m for m in list(role.members) if m.id != int(new_user_id)]
        for m in to_remove:
            try:
                await m.remove_roles(role, reason=reason)
            except Exception:
                pass

        if prev_user_id and prev_user_id != new_user_id:
            prev = await self._get_member(guild, int(prev_user_id))
            if prev is not None and role in prev.roles:
                try:
                    await prev.remove_roles(role, reason=reason)
                except Exception:
                    pass

        champ = await self._get_member(guild, int(new_user_id))
        if champ is None:
            return None, "Winner not fetchable"
        if role not in champ.roles:
            try:
                await champ.add_roles(role, reason=reason)
            except Exception:
                return champ, "Could not add role (permissions/hierarchy)"
        return champ, None

    async def _announce(
        self,
        guild: discord.Guild,
        rng: RangeSpec,
        chat_member: discord.Member,
        vc_member: discord.Member,
    ) -> None:
        ch = guild.get_channel(ANNOUNCE_CHANNEL_ID)
        if not isinstance(ch, discord.TextChannel):
            try:
                fetched = await self.bot.fetch_channel(ANNOUNCE_CHANNEL_ID)
                if isinstance(fetched, discord.TextChannel) and fetched.guild.id == guild.id:
                    ch = fetched
                else:
                    return
            except Exception:
                return

        me = guild.get_member(self.bot.user.id) if self.bot.user else None
        if me is not None:
            perms = ch.permissions_for(me)
            if not perms.view_channel or not perms.send_messages:
                return

        if chat_member.id == vc_member.id:
            template = random.choice(SAME_WINNER_MESSAGES)
            content = template.format(user=chat_member.mention)
            crowns_line = f"👑 +{CROWNS_PER_WIN * 2} crowns"
        else:
            template = random.choice(DIFF_WINNER_MESSAGES)
            content = template.format(chat=chat_member.mention, vc=vc_member.mention)
            crowns_line = f"👑 +{CROWNS_PER_WIN} crowns each"

        embed = discord.Embed(
            title=f"📅 {rng.label} Winners",
            description=content,
            color=discord.Color.gold(),
        )
        embed.add_field(name="Top Chatter", value=chat_member.mention, inline=True)
        embed.add_field(name="Top VChatter", value=vc_member.mention, inline=True)
        embed.add_field(name="Crowns", value=crowns_line, inline=False)
        embed.set_footer(text="Monthly challenges exclude server owners.")

        try:
            await ch.send(embed=embed)
        except Exception:
            pass

    async def _award_prev_month_once(self, guild: discord.Guild) -> list[str]:
        await self._ensure_tables()

        today = _utc_today()
        rng = _prev_month_range(today)

        if rng.end < rng.start:
            return [f"{guild.name}: Nothing to award for {rng.label} (season starts {SEASON_START_UTC.isoformat()})."]

        awarded_state = await self._get_state(guild.id, self.CAT_AWARDED)
        if awarded_state and awarded_state[0] == rng.key:
            return [f"{guild.name}: Already awarded {rng.label} ({rng.key})."]

        chat_winner = await self._winner_chat(guild.id, rng)
        vc_winner = await self._winner_vc(guild.id, rng)

        if not chat_winner:
            return [f"{guild.name}: No eligible chat data for {rng.label}."]
        if not vc_winner:
            return [f"{guild.name}: No eligible VC data for {rng.label}."]

        chat_uid, chat_val = chat_winner
        vc_uid, vc_val = vc_winner

        chat_state = await self._get_state(guild.id, self.CAT_CHAT)
        vc_state = await self._get_state(guild.id, self.CAT_VC)

        prev_chat_uid = chat_state[1] if chat_state else 0
        prev_vc_uid = vc_state[1] if vc_state else 0

        out: list[str] = []

        chat_member, chat_err = await self._apply_role_swap(
            guild,
            ROLE_TOP_CHATTER,
            prev_chat_uid,
            chat_uid,
            reason=f"Top Chatter ({rng.key})",
        )
        vc_member, vc_err = await self._apply_role_swap(
            guild,
            ROLE_TOP_VCHATTER,
            prev_vc_uid,
            vc_uid,
            reason=f"Top VChatter ({rng.key})",
        )

        if chat_err:
            out.append(f"{guild.name}: Top Chatter role issue: {chat_err}")
        if vc_err:
            out.append(f"{guild.name}: Top VChatter role issue: {vc_err}")

        if chat_member is None or vc_member is None:
            out.append(f"{guild.name}: Could not fetch winners as members.")
            return out

        await self._add_crowns(guild.id, chat_uid, CROWNS_PER_WIN)
        await self._add_crowns(guild.id, vc_uid, CROWNS_PER_WIN)

        await self._set_state(guild.id, self.CAT_CHAT, rng.key, chat_uid, chat_val)
        await self._set_state(guild.id, self.CAT_VC, rng.key, vc_uid, vc_val)
        await self._set_state(guild.id, self.CAT_AWARDED, rng.key, 1, 1)

        out.append(f"{guild.name}: Awarded {rng.label}.")
        out.append(f"{guild.name}: Top Chatter is {chat_member.mention} with {_fmt_int(chat_val)} messages.")
        out.append(f"{guild.name}: Top VChatter is {vc_member.mention} with {_fmt_duration(vc_val)} VC.")

        await self._announce(guild, rng, chat_member, vc_member)

        return out

    async def _assign_roles_for_current_month(self, guild: discord.Guild) -> None:
        await self._ensure_tables()

        today = _utc_today()
        rng = _current_month_range(today)

        chat_winner = await self._winner_chat(guild.id, rng)
        vc_winner = await self._winner_vc(guild.id, rng)

        if not chat_winner or not vc_winner:
            return

        chat_uid, _chat_val = chat_winner
        vc_uid, _vc_val = vc_winner

        cached = self._live_role_cache.get(guild.id)
        if cached and cached[0] == chat_uid and cached[1] == vc_uid and cached[2] == rng.key:
            return

        prev_chat_uid = cached[0] if cached and cached[2] == rng.key else 0
        prev_vc_uid = cached[1] if cached and cached[2] == rng.key else 0

        await self._apply_role_swap(
            guild,
            ROLE_TOP_CHATTER,
            prev_chat_uid,
            chat_uid,
            reason=f"Top Chatter (live {rng.key})",
        )
        await self._apply_role_swap(
            guild,
            ROLE_TOP_VCHATTER,
            prev_vc_uid,
            vc_uid,
            reason=f"Top VChatter (live {rng.key})",
        )

        self._live_role_cache[guild.id] = (chat_uid, vc_uid, rng.key)

    @tasks.loop(seconds=AWARD_CHECK_EVERY_SECONDS)
    async def monthly_award_loop(self) -> None:
        async with self._lock:
            for guild in self.bot.guilds:
                try:
                    await self._award_prev_month_once(guild)
                except Exception:
                    pass

    @monthly_award_loop.before_loop
    async def _before_award_loop(self) -> None:
        await self.bot.wait_until_ready()

    @tasks.loop(seconds=ROLE_REFRESH_EVERY_SECONDS)
    async def role_refresh_loop(self) -> None:
        async with self._lock:
            for guild in self.bot.guilds:
                try:
                    await self._assign_roles_for_current_month(guild)
                except Exception:
                    pass

    @role_refresh_loop.before_loop
    async def _before_role_loop(self) -> None:
        await self.bot.wait_until_ready()

    monthly = app_commands.Group(name="monthly", description="Monthly chatter + VC roles.")

    @monthly.command(name="status", description="Show current + previous month windows and exclusions (admin).")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def status(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        today = _utc_today()
        curr = _current_month_range(today)
        prev = _prev_month_range(today)

        awarded_state = await self._get_state(interaction.guild.id, self.CAT_AWARDED)
        awarded_key = awarded_state[0] if awarded_state else "none"

        embed = discord.Embed(title="📅 Monthly Activity Status", color=discord.Color.gold())
        embed.add_field(name="Season start (UTC)", value=SEASON_START_UTC.isoformat(), inline=False)
        embed.add_field(
            name="Current month-to-date window",
            value=f"{curr.label}: {curr.start.isoformat()} -> {curr.end.isoformat()}",
            inline=False,
        )
        embed.add_field(
            name="Next payout window (auto, previous month)",
            value=f"{prev.label}: {prev.start.isoformat()} -> {prev.end.isoformat()}",
            inline=False,
        )
        embed.add_field(name="Last awarded month_key", value=awarded_key, inline=False)
        embed.add_field(name="Crowns per win", value=f"👑 {CROWNS_PER_WIN}", inline=False)
        embed.add_field(
            name="Excluded from monthly",
            value="\n".join(f"`{x}`" for x in sorted(EXCLUDED_USER_IDS)) or "None",
            inline=False,
        )
        embed.add_field(
            name="Live role refresh cadence",
            value=f"Every {int(self.ROLE_REFRESH_EVERY_SECONDS // 60)} minutes (no crowns).",
            inline=False,
        )
        embed.add_field(
            name="Payout check cadence",
            value=f"Every {int(self.AWARD_CHECK_EVERY_SECONDS // 60)} minutes (auto catch-up).",
            inline=False,
        )

        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(MonthlyActivityChampionsCog(bot))
