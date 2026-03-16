from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import discord
from discord import app_commands
from discord.app_commands import checks
from discord.ext import commands, tasks
from sqlalchemy import select, text

from db.models import LootboxInventoryRow, WalletRow
from services.db import sessions
from services.users import ensure_user_rows
from services.xp_award import award_xp


class LootboxRarity(str, Enum):
    COMMON = "common"
    RARE = "rare"
    EPIC = "epic"
    LEGENDARY = "legendary"


class DropKind(str, Enum):
    SILVER = "silver"
    XP = "xp"
    LOOTBOX = "lootbox"


@dataclass(frozen=True)
class DropRoll:
    kind: DropKind
    silver_amount: int = 0
    xp_amount: int = 0
    lootbox_rarity: Optional[LootboxRarity] = None
    lootbox_amount: int = 0


def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _safe_int(x) -> int:
    try:
        return int(x)
    except Exception:
        return 0


def _roll_bp(chance_bp: int) -> bool:
    bp = max(int(chance_bp), 0)
    if bp <= 0:
        return False
    if bp >= 10000:
        return True
    return random.randint(1, 10000) <= bp


def _pick_weighted_str(table: dict[str, int]) -> str:
    total = 0
    for w in table.values():
        total += max(int(w), 0)
    if total <= 0:
        return next(iter(table.keys()))
    r = random.randint(1, total)
    acc = 0
    for k, w in table.items():
        acc += max(int(w), 0)
        if r <= acc:
            return k
    return next(iter(table.keys()))


def _pick_weighted_rarity(table: dict[LootboxRarity, int]) -> LootboxRarity:
    total = 0
    for w in table.values():
        total += max(int(w), 0)
    if total <= 0:
        return LootboxRarity.COMMON
    r = random.randint(1, total)
    acc = 0
    for rar, w in table.items():
        acc += max(int(w), 0)
        if r <= acc:
            return rar
    return LootboxRarity.COMMON


def _rarity_color(r: LootboxRarity) -> discord.Color:
    if r == LootboxRarity.COMMON:
        return discord.Color.light_grey()
    if r == LootboxRarity.RARE:
        return discord.Color.blue()
    if r == LootboxRarity.EPIC:
        return discord.Color.purple()
    return discord.Color.gold()


def _rarity_emoji(r: LootboxRarity) -> str:
    if r == LootboxRarity.COMMON:
        return "📦"
    if r == LootboxRarity.RARE:
        return "🎁"
    if r == LootboxRarity.EPIC:
        return "🧰"
    return "👑"


def _kind_emoji(k: DropKind) -> str:
    if k == DropKind.SILVER:
        return "💰"
    if k == DropKind.XP:
        return "🧠"
    return "🎁"


class DropPartyDropView(discord.ui.View):
    def __init__(
        self,
        *,
        cog: "DropPartyCog",
        guild_id: int,
        channel_id: int,
        roll: DropRoll,
        expires_in: float,
    ):
        super().__init__(timeout=expires_in)
        self.cog = cog
        self.guild_id = int(guild_id)
        self.channel_id = int(channel_id)
        self.roll = roll

        self.claimed: set[int] = set()
        self.message: Optional[discord.Message] = None

        self._last_edit_at: float = 0.0
        self._edit_lock = asyncio.Lock()

    async def on_timeout(self) -> None:
        try:
            if self.message:
                await self.message.delete()
        except Exception:
            pass

        self.cog._active_channel_drops.pop(self.channel_id, None)

        # If we built up backlog while this drop was active, spawn next.
        try:
            await self.cog._maybe_spawn_backlog(self.guild_id, self.channel_id)
        except Exception:
            pass

    async def _throttled_edit(self) -> None:
        if not self.message:
            return

        async with self._edit_lock:
            now = time.time()
            if now - self._last_edit_at < float(self.cog.DROP_EDIT_MIN_INTERVAL_SECONDS):
                return
            self._last_edit_at = now

            try:
                embed = self.message.embeds[0] if self.message.embeds else None
                if embed:
                    embed.description = self.cog._drop_description(self.roll, claimed=len(self.claimed))
                await self.message.edit(embed=embed, view=self)
            except Exception:
                return

    @discord.ui.button(label="Claim Drop", style=discord.ButtonStyle.success, emoji="🖱️")
    async def claim(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        if interaction.channel is None or int(interaction.channel.id) != self.channel_id:
            await interaction.response.send_message("Wrong channel for this drop.", ephemeral=True)
            return

        uid = int(interaction.user.id)
        if uid in self.claimed:
            await interaction.response.send_message("You already claimed this one. Chillllll.", ephemeral=True)
            return

        ok = await self.cog._apply_drop_claim(
            guild_id=self.guild_id,
            user_id=uid,
            roll=self.roll,
        )
        if not ok:
            await interaction.response.send_message("Could not apply reward. Try again.", ephemeral=True)
            return

        self.claimed.add(uid)

        try:
            if self.roll.kind == DropKind.SILVER:
                await interaction.response.send_message(
                    f"✅ You claimed **+{_fmt_int(self.roll.silver_amount)}** silver.",
                    ephemeral=True,
                )
            elif self.roll.kind == DropKind.XP:
                await interaction.response.send_message(
                    f"✅ You claimed **+{_fmt_int(self.roll.xp_amount)}** XP.",
                    ephemeral=True,
                )
            else:
                rar = self.roll.lootbox_rarity or LootboxRarity.COMMON
                amt = int(self.roll.lootbox_amount or 1)
                await interaction.response.send_message(
                    f"✅ You claimed **{_fmt_int(amt)}x {rar.value}** lootbox(es).",
                    ephemeral=True,
                )
        except Exception:
            pass

        await self._throttled_edit()


class DropPartyCog(commands.Cog):
    TABLE_DROP_PARTY_STATE_SQL = """
    CREATE TABLE IF NOT EXISTS dropparty_state (
        guild_id BIGINT NOT NULL,
        channel_id BIGINT NOT NULL DEFAULT 0,
        enabled TINYINT(1) NOT NULL DEFAULT 0,
        ends_at_ts BIGINT NOT NULL DEFAULT 0,
        messages_per_drop INT NOT NULL DEFAULT 5,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (guild_id),
        KEY ix_dropparty_state_channel_id (channel_id),
        KEY ix_dropparty_state_enabled (enabled),
        KEY ix_dropparty_state_ends_at (ends_at_ts)
    );
    """

    TABLE_LOOTBOX_INV_SQL = """
    CREATE TABLE IF NOT EXISTS lootbox_inventory (
        id INT NOT NULL AUTO_INCREMENT,
        guild_id BIGINT NOT NULL,
        user_id BIGINT NOT NULL,
        rarity VARCHAR(32) NOT NULL,
        amount INT NOT NULL DEFAULT 0,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uq_lootbox_inventory_guild_user_rarity (guild_id, user_id, rarity),
        KEY ix_lootbox_inventory_guild_id (guild_id),
        KEY ix_lootbox_inventory_user_id (user_id)
    );
    """

    DROP_EXPIRES_SECONDS = 20.0
    DROP_EDIT_MIN_INTERVAL_SECONDS = 1.8

    DEFAULT_MESSAGES_PER_DROP = 5
    MIN_MESSAGES_PER_DROP = 2
    MAX_MESSAGES_PER_DROP = 50

    KIND_WEIGHTS: dict[str, int] = {
        "silver": 45,
        "xp": 35,
        "lootbox": 20,
    }

    # Nerfed a bit (roughly 25% down from the earlier "significant" values)
    SILVER_MIN = 1800
    SILVER_MAX = 9000

    XP_MIN = 180
    XP_MAX = 800

    LOOTBOX_DOUBLE_BP = 600  # 6%

    LOOTBOX_RARITY_WEIGHTS: dict[LootboxRarity, int] = {
        LootboxRarity.COMMON: 70,
        LootboxRarity.RARE: 22,
        LootboxRarity.EPIC: 7,
        LootboxRarity.LEGENDARY: 1,
    }

    CLEANUP_TICK_SECONDS = 8.0

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

        self._state_cache: dict[int, tuple[int, bool, int, int]] = {}  # guild_id -> (channel_id, enabled, ends_at_ts, messages_per_drop)

        # count messages always
        self._counters: dict[tuple[int, int], int] = {}  # (guild_id, channel_id) -> counter

        # backlog drops when threshold hit during active drop
        self._backlog: dict[tuple[int, int], int] = {}  # (guild_id, channel_id) -> pending_drops

        # channel_id -> expires_at (seconds)
        self._active_channel_drops: dict[int, float] = {}

        self._spawn_lock = asyncio.Lock()

        if self.bot.is_ready():
            self._kickoff()

    def _kickoff(self) -> None:
        if not self.cleanup_task.is_running():
            self.cleanup_task.start()

    async def cog_load(self) -> None:
        await self._ensure_tables()
        await self._prime_cache()
        self._kickoff()

    def cog_unload(self) -> None:
        try:
            self.cleanup_task.cancel()
        except Exception:
            pass

    async def _ensure_tables(self) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(self.TABLE_DROP_PARTY_STATE_SQL))
                await session.execute(text(self.TABLE_LOOTBOX_INV_SQL))

    async def _prime_cache(self) -> None:
        await self._ensure_tables()
        async with self.sessionmaker() as session:
            async with session.begin():
                rows = (await session.execute(
                    text("SELECT guild_id, channel_id, enabled, ends_at_ts, messages_per_drop FROM dropparty_state")
                )).all()

        tmp: dict[int, tuple[int, bool, int, int]] = {}
        for r in rows:
            gid = _safe_int(r[0])
            ch = _safe_int(r[1])
            en = bool(_safe_int(r[2]))
            ends = _safe_int(r[3])
            mpd = _safe_int(r[4]) or self.DEFAULT_MESSAGES_PER_DROP
            mpd = max(self.MIN_MESSAGES_PER_DROP, min(self.MAX_MESSAGES_PER_DROP, int(mpd)))
            tmp[gid] = (ch, en, ends, mpd)
        self._state_cache = tmp

    async def _get_state(self, guild_id: int) -> tuple[int, bool, int, int]:
        s = self._state_cache.get(int(guild_id))
        if s is not None:
            return s

        await self._ensure_tables()
        async with self.sessionmaker() as session:
            async with session.begin():
                row = (await session.execute(
                    text("SELECT channel_id, enabled, ends_at_ts, messages_per_drop FROM dropparty_state WHERE guild_id=:gid"),
                    {"gid": int(guild_id)},
                )).first()

        if row is None:
            ch = 0
            en = False
            ends = 0
            mpd = self.DEFAULT_MESSAGES_PER_DROP
            self._state_cache[int(guild_id)] = (ch, en, ends, mpd)
            return (ch, en, ends, mpd)

        ch = _safe_int(row[0])
        en = bool(_safe_int(row[1]))
        ends = _safe_int(row[2])
        mpd = _safe_int(row[3]) or self.DEFAULT_MESSAGES_PER_DROP
        mpd = max(self.MIN_MESSAGES_PER_DROP, min(self.MAX_MESSAGES_PER_DROP, int(mpd)))

        self._state_cache[int(guild_id)] = (ch, en, ends, mpd)
        return (ch, en, ends, mpd)

    async def _set_state(self, guild_id: int, *, channel_id: int, enabled: bool, ends_at_ts: int, messages_per_drop: int) -> None:
        await self._ensure_tables()

        mpd = max(self.MIN_MESSAGES_PER_DROP, min(self.MAX_MESSAGES_PER_DROP, int(messages_per_drop or self.DEFAULT_MESSAGES_PER_DROP)))

        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        INSERT INTO dropparty_state (guild_id, channel_id, enabled, ends_at_ts, messages_per_drop)
                        VALUES (:gid, :cid, :en, :ends, :mpd)
                        ON DUPLICATE KEY UPDATE
                            channel_id = VALUES(channel_id),
                            enabled = VALUES(enabled),
                            ends_at_ts = VALUES(ends_at_ts),
                            messages_per_drop = VALUES(messages_per_drop)
                        """
                    ),
                    {
                        "gid": int(guild_id),
                        "cid": int(channel_id),
                        "en": 1 if enabled else 0,
                        "ends": int(ends_at_ts),
                        "mpd": int(mpd),
                    },
                )

        self._state_cache[int(guild_id)] = (int(channel_id), bool(enabled), int(ends_at_ts), int(mpd))

    def _roll_drop(self) -> DropRoll:
        kind_key = _pick_weighted_str(self.KIND_WEIGHTS)

        if kind_key == "silver":
            amt = random.randint(int(self.SILVER_MIN), int(self.SILVER_MAX))
            return DropRoll(kind=DropKind.SILVER, silver_amount=max(int(amt), 0))

        if kind_key == "xp":
            amt = random.randint(int(self.XP_MIN), int(self.XP_MAX))
            return DropRoll(kind=DropKind.XP, xp_amount=max(int(amt), 0))

        rar = _pick_weighted_rarity(self.LOOTBOX_RARITY_WEIGHTS)
        lb_amt = 2 if _roll_bp(self.LOOTBOX_DOUBLE_BP) else 1
        return DropRoll(kind=DropKind.LOOTBOX, lootbox_rarity=rar, lootbox_amount=int(lb_amt))

    def _drop_title(self, roll: DropRoll) -> str:
        if roll.kind == DropKind.SILVER:
            return "💰 Silver Drop!"
        if roll.kind == DropKind.XP:
            return "🧠 XP Drop!"
        rar = roll.lootbox_rarity or LootboxRarity.COMMON
        return f"{_rarity_emoji(rar)} Lootbox Drop!"

    def _drop_color(self, roll: DropRoll) -> discord.Color:
        if roll.kind == DropKind.SILVER:
            return discord.Color.gold()
        if roll.kind == DropKind.XP:
            return discord.Color.blurple()
        rar = roll.lootbox_rarity or LootboxRarity.COMMON
        return _rarity_color(rar)

    def _drop_reward_line(self, roll: DropRoll) -> str:
        if roll.kind == DropKind.SILVER:
            return f"Reward: **+{_fmt_int(roll.silver_amount)}** silver"
        if roll.kind == DropKind.XP:
            return f"Reward: **+{_fmt_int(roll.xp_amount)}** XP"
        rar = roll.lootbox_rarity or LootboxRarity.COMMON
        amt = int(roll.lootbox_amount or 1)
        return f"Reward: **{_fmt_int(amt)}x {rar.value}** lootbox(es)"

    def _drop_description(self, roll: DropRoll, *, claimed: int) -> str:
        return (
            f"**{_kind_emoji(roll.kind)} DROP PARTY DROP**\n"
            f"{self._drop_reward_line(roll)}\n\n"
            f"Click the button to claim. One claim per user.\n"
            f"Expires in **{int(self.DROP_EXPIRES_SECONDS)}s**.\n\n"
            f"Claimed so far: **{_fmt_int(claimed)}**"
        )

    async def _spawn_drop(self, *, channel: discord.TextChannel, roll: DropRoll) -> None:
        now = time.time()
        expires_at = now + float(self.DROP_EXPIRES_SECONDS)
        self._active_channel_drops[channel.id] = expires_at

        embed = discord.Embed(
            title=self._drop_title(roll),
            description=self._drop_description(roll, claimed=0),
            color=self._drop_color(roll),
        )
        embed.set_footer(text="DropParty is live. Spam responsibly.")

        view = DropPartyDropView(
            cog=self,
            guild_id=channel.guild.id,
            channel_id=channel.id,
            roll=roll,
            expires_in=self.DROP_EXPIRES_SECONDS,
        )

        msg = await channel.send(embed=embed, view=view)
        view.message = msg

    async def _add_lootbox(self, *, session, guild_id: int, user_id: int, rarity: LootboxRarity, amount: int) -> None:
        amt = max(int(amount), 0)
        if amt <= 0:
            return

        row = await session.scalar(
            select(LootboxInventoryRow).where(
                LootboxInventoryRow.guild_id == int(guild_id),
                LootboxInventoryRow.user_id == int(user_id),
                LootboxInventoryRow.rarity == rarity.value,
            )
        )
        if row is None:
            row = LootboxInventoryRow(
                guild_id=int(guild_id),
                user_id=int(user_id),
                rarity=rarity.value,
                amount=amt,
            )
            session.add(row)
            await session.flush()
        else:
            row.amount = int(row.amount) + amt

    async def _apply_drop_claim(self, *, guild_id: int, user_id: int, roll: DropRoll) -> bool:
        await self._ensure_tables()
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    await ensure_user_rows(session, guild_id=int(guild_id), user_id=int(user_id))

                    if roll.kind == DropKind.SILVER:
                        wallet = await session.scalar(
                            select(WalletRow).where(
                                WalletRow.guild_id == int(guild_id),
                                WalletRow.user_id == int(user_id),
                            )
                        )
                        if wallet is None:
                            wallet = WalletRow(guild_id=int(guild_id), user_id=int(user_id), silver=0, diamonds=0)
                            session.add(wallet)
                            await session.flush()

                        add = max(int(roll.silver_amount), 0)
                        wallet.silver += int(add)
                        if hasattr(wallet, "silver_earned"):
                            wallet.silver_earned += int(add)
                        return True

                    if roll.kind == DropKind.XP:
                        add = max(int(roll.xp_amount), 0)
                        if add > 0:
                            await award_xp(session, guild_id=int(guild_id), user_id=int(user_id), amount=int(add))
                        return True

                    rar = roll.lootbox_rarity or LootboxRarity.COMMON
                    amt = max(int(roll.lootbox_amount or 1), 1)
                    await self._add_lootbox(session=session, guild_id=int(guild_id), user_id=int(user_id), rarity=rar, amount=amt)
                    return True
        except Exception:
            return False

    def _is_drop_active(self, channel_id: int) -> bool:
        active_until = self._active_channel_drops.get(int(channel_id))
        if active_until is None:
            return False
        if time.time() < float(active_until):
            return True
        self._active_channel_drops.pop(int(channel_id), None)
        return False

    async def _maybe_spawn_backlog(self, guild_id: int, channel_id: int) -> None:
        k = (int(guild_id), int(channel_id))
        pending = int(self._backlog.get(k, 0))
        if pending <= 0:
            return
        if self._is_drop_active(int(channel_id)):
            return

        channel = None
        g = self.bot.get_guild(int(guild_id))
        if g is not None:
            ch = g.get_channel(int(channel_id))
            if isinstance(ch, discord.TextChannel):
                channel = ch
        if channel is None:
            self._backlog[k] = 0
            return

        async with self._spawn_lock:
            if self._is_drop_active(int(channel_id)):
                return

            pending2 = int(self._backlog.get(k, 0))
            if pending2 <= 0:
                return

            self._backlog[k] = max(pending2 - 1, 0)
            roll = self._roll_drop()
            try:
                await self._spawn_drop(channel=channel, roll=roll)
            except Exception:
                self._active_channel_drops.pop(int(channel_id), None)

    @tasks.loop(seconds=CLEANUP_TICK_SECONDS)
    async def cleanup_task(self) -> None:
        now = time.time()
        expired = [cid for cid, until in self._active_channel_drops.items() if now >= float(until)]
        for cid in expired:
            self._active_channel_drops.pop(cid, None)

        # If anything is queued and channel is now free, spawn one.
        for (gid, cid), pending in list(self._backlog.items()):
            if int(pending) <= 0:
                continue
            if self._is_drop_active(int(cid)):
                continue
            try:
                await self._maybe_spawn_backlog(int(gid), int(cid))
            except Exception:
                pass

    @cleanup_task.before_loop
    async def _before_cleanup(self) -> None:
        await self.bot.wait_until_ready()

    @commands.Cog.listener("on_ready")
    async def _on_ready(self) -> None:
        self._kickoff()

    @commands.Cog.listener("on_message")
    async def on_message_dropparty(self, message: discord.Message):
        if message.guild is None:
            return
        if message.author.bot:
            return
        if not message.content:
            return
        if not isinstance(message.channel, discord.TextChannel):
            return

        gid = int(message.guild.id)
        cid = int(message.channel.id)

        channel_id, enabled, ends_at_ts, mpd = await self._get_state(gid)
        if not enabled:
            return
        if channel_id and cid != int(channel_id):
            return

        now_ts = int(time.time())
        if ends_at_ts and now_ts >= int(ends_at_ts):
            async with self._spawn_lock:
                ch2, en2, ends2, mpd2 = await self._get_state(gid)
                if en2 and ends2 and now_ts >= int(ends2):
                    await self._set_state(gid, channel_id=int(ch2), enabled=False, ends_at_ts=0, messages_per_drop=int(mpd2))
            return

        k = (gid, cid)

        # Always count messages, even if a drop is active.
        cur = int(self._counters.get(k, 0))
        cur += 1
        self._counters[k] = cur

        # Convert messages to "drops to spawn"
        if cur < int(mpd):
            return

        drops_to_queue = cur // int(mpd)
        remainder = cur % int(mpd)
        self._counters[k] = remainder

        # If a drop is active, queue backlog instead of spawning immediately.
        if self._is_drop_active(cid):
            self._backlog[k] = int(self._backlog.get(k, 0)) + int(drops_to_queue)
            return

        async with self._spawn_lock:
            if self._is_drop_active(cid):
                self._backlog[k] = int(self._backlog.get(k, 0)) + int(drops_to_queue)
                return

            # Spawn exactly one now
            roll = self._roll_drop()
            try:
                await self._spawn_drop(channel=message.channel, roll=roll)
            except Exception:
                self._active_channel_drops.pop(cid, None)
                self._backlog[k] = int(self._backlog.get(k, 0)) + int(drops_to_queue)
                return

            # Any extra drops become backlog
            extra = int(drops_to_queue) - 1
            if extra > 0:
                self._backlog[k] = int(self._backlog.get(k, 0)) + int(extra)

    dropparty = app_commands.Group(name="dropparty", description="DropParty controls.")

    @dropparty.command(name="start", description="Start DropParty in this channel.")
    @app_commands.describe(duration_minutes="0 means until stopped", messages_per_drop="Drops every N messages (default 5)")
    @checks.has_permissions(manage_guild=True)
    async def dropparty_start(self, interaction: discord.Interaction, duration_minutes: Optional[int] = 30, messages_per_drop: Optional[int] = 5):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("Run this in a text channel.", ephemeral=True)
            return

        gid = int(interaction.guild.id)
        cid = int(interaction.channel.id)

        mins = max(int(duration_minutes or 0), 0)
        ends = 0 if mins <= 0 else int(time.time()) + (mins * 60)

        mpd2 = max(self.MIN_MESSAGES_PER_DROP, min(self.MAX_MESSAGES_PER_DROP, int(messages_per_drop or self.DEFAULT_MESSAGES_PER_DROP)))

        await self._set_state(
            gid,
            channel_id=int(cid),
            enabled=True,
            ends_at_ts=int(ends),
            messages_per_drop=int(mpd2),
        )

        self._counters[(gid, cid)] = 0
        self._backlog[(gid, cid)] = 0

        if ends:
            await interaction.response.send_message(
                f"🎉 DropParty ON in {interaction.channel.mention}. Drops every **{mpd2}** messages for **{mins}** minutes.",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                f"🎉 DropParty ON in {interaction.channel.mention}. Drops every **{mpd2}** messages until stopped.",
                ephemeral=True,
            )

    @dropparty.command(name="stop", description="Stop DropParty.")
    @checks.has_permissions(manage_guild=True)
    async def dropparty_stop(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        gid = int(interaction.guild.id)
        channel_id, enabled, ends_at_ts, mpd = await self._get_state(gid)

        await self._set_state(
            gid,
            channel_id=int(channel_id),
            enabled=False,
            ends_at_ts=0,
            messages_per_drop=int(mpd),
        )

        await interaction.response.send_message("🛑 DropParty OFF.", ephemeral=True)

    @dropparty.command(name="status", description="Show DropParty status.")
    @checks.has_permissions(manage_guild=True)
    async def dropparty_status(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        gid = int(interaction.guild.id)
        channel_id, enabled, ends_at_ts, mpd = await self._get_state(gid)

        ch = interaction.guild.get_channel(int(channel_id)) if channel_id else None
        ch_label = ch.mention if isinstance(ch, discord.TextChannel) else (f"`{channel_id}`" if channel_id else "Not set")

        now_ts = int(time.time())
        if enabled and ends_at_ts and ends_at_ts > now_ts:
            remaining = ends_at_ts - now_ts
            desc = f"✅ **ON**\nChannel: {ch_label}\nDrops every: **{mpd}** messages\nEnds in: **{remaining}s**"
        elif enabled and ends_at_ts == 0:
            desc = f"✅ **ON**\nChannel: {ch_label}\nDrops every: **{mpd}** messages\nEnds: **manual stop**"
        else:
            desc = f"🛑 **OFF**\nChannel: {ch_label}\nDrops every: **{mpd}** messages"

        e = discord.Embed(title="🎉 DropParty Status", description=desc, color=discord.Color.blurple())
        e.add_field(name="Silver Drop", value=f"{self.SILVER_MIN:,} to {self.SILVER_MAX:,}", inline=True)
        e.add_field(name="XP Drop", value=f"{self.XP_MIN:,} to {self.XP_MAX:,}", inline=True)
        e.add_field(name="Lootbox Drop", value="Common/Rare/Epic/Legendary", inline=True)

        k = (gid, int(channel_id)) if channel_id else None
        if k:
            e.add_field(name="Backlog", value=f"Pending drops: **{_fmt_int(int(self._backlog.get(k, 0)))}**", inline=False)

        await interaction.response.send_message(embed=e, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(DropPartyCog(bot))
