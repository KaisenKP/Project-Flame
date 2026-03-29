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
from discord.ext import commands
from sqlalchemy import select, text

from db.models import (
    LootboxGrantPermRow,
    LootboxGrantUserPermRow,
    LootboxInventoryRow,
    WalletRow,
)
from services.db import sessions
from services.users import ensure_user_rows
from services.xp_award import award_xp


class LootboxRarity(str, Enum):
    COMMON = "common"
    RARE = "rare"
    EPIC = "epic"
    LEGENDARY = "legendary"
    MYTHICAL = "mythical"


@dataclass(frozen=True)
class LootboxReward:
    silver: int
    xp: int


@dataclass(frozen=True)
class RarityConfig:
    weight: int
    min_silver: int
    max_silver: int
    min_xp: int
    max_xp: int


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


def _pick_weighted_rarity(table: dict[LootboxRarity, RarityConfig]) -> LootboxRarity:
    total = 0
    for cfg in table.values():
        total += max(int(cfg.weight), 0)
    if total <= 0:
        return LootboxRarity.COMMON

    r = random.randint(1, total)
    acc = 0
    for rarity, cfg in table.items():
        acc += max(int(cfg.weight), 0)
        if r <= acc:
            return rarity
    return LootboxRarity.COMMON


def _rarity_color(r: LootboxRarity) -> discord.Color:
    if r == LootboxRarity.COMMON:
        return discord.Color.light_grey()
    if r == LootboxRarity.RARE:
        return discord.Color.blue()
    if r == LootboxRarity.EPIC:
        return discord.Color.purple()
    if r == LootboxRarity.LEGENDARY:
        return discord.Color.gold()
    return discord.Color.fuchsia()


def _rarity_emoji(r: LootboxRarity) -> str:
    if r == LootboxRarity.COMMON:
        return "📦"
    if r == LootboxRarity.RARE:
        return "🎁"
    if r == LootboxRarity.EPIC:
        return "🧰"
    if r == LootboxRarity.LEGENDARY:
        return "👑"
    return "🌌"


def _rarity_label(r: LootboxRarity) -> str:
    return r.value.upper()


class LootboxDropView(discord.ui.View):
    def __init__(
        self,
        *,
        cog: "LootboxCog",
        guild_id: int,
        channel_id: int,
        rarity: LootboxRarity,
        expires_in: float = 60.0,
    ):
        super().__init__(timeout=expires_in)
        self.cog = cog
        self.guild_id = int(guild_id)
        self.channel_id = int(channel_id)
        self.rarity = rarity
        self.claimed: set[int] = set()
        self.message: Optional[discord.Message] = None

    async def on_timeout(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True

        try:
            if self.message:
                embed = self.message.embeds[0] if self.message.embeds else None
                if embed:
                    embed.set_footer(text="This drop expired.")
                await self.message.edit(embed=embed, view=self)
        except Exception:
            pass

        self.cog._active_channel_drops.pop(self.channel_id, None)

    @discord.ui.button(label="Claim Lootbox", style=discord.ButtonStyle.success, emoji="🖱️")
    async def claim(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        if interaction.channel is None or int(interaction.channel.id) != self.channel_id:
            await interaction.response.send_message("Wrong channel for this drop.", ephemeral=True)
            return

        uid = int(interaction.user.id)

        if uid in self.claimed:
            await interaction.response.send_message("You already claimed this drop. Greedy lil goblin.", ephemeral=True)
            return

        self.claimed.add(uid)

        await self.cog._add_lootbox(
            guild_id=self.guild_id,
            user_id=uid,
            rarity=self.rarity,
            amount=1,
        )

        try:
            await interaction.response.send_message(
                f"✅ Claimed: **{self.rarity.value}** lootbox.",
                ephemeral=True,
            )
        except discord.NotFound:
            return

        try:
            if self.message:
                embed = self.message.embeds[0] if self.message.embeds else None
                if embed:
                    embed.description = self.cog._drop_description(self.rarity, claimed=len(self.claimed))
                await self.message.edit(embed=embed, view=self)
        except Exception:
            pass


class LootboxCog(commands.Cog):
    DROP_CHANCE_BP = 100  # 1%
    DROP_COOLDOWN_SECONDS_PER_USER = 12.0

    DROP_EXPIRES_SECONDS = 60.0
    MAX_ACTIVE_DROPS_PER_CHANNEL = 1

    # Your exact silver ranges:
    # common: 2500-5000
    # rare: 10000-25000
    # epic: 55000-165000
    #
    # legendary kept as a jackpot tier.
    RARITY_TABLE: dict[LootboxRarity, RarityConfig] = {
        LootboxRarity.COMMON: RarityConfig(weight=75, min_silver=2500, max_silver=5000, min_xp=8, max_xp=22),
        LootboxRarity.RARE: RarityConfig(weight=18, min_silver=10000, max_silver=25000, min_xp=22, max_xp=60),
        # Epic and Legendary buffed by 10x.
        LootboxRarity.EPIC: RarityConfig(weight=6, min_silver=550000, max_silver=1650000, min_xp=660, max_xp=1760),
        LootboxRarity.LEGENDARY: RarityConfig(weight=1, min_silver=2200000, max_silver=6600000, min_xp=1760, max_xp=4400),
        LootboxRarity.MYTHICAL: RarityConfig(weight=1, min_silver=8000000, max_silver=16000000, min_xp=5000, max_xp=9500),
    }

    # "Animation" tuning
    OPEN_ANIM_STEPS = 4
    OPEN_ANIM_STEP_DELAY = 0.75
    OPEN_FAST_MODE_THRESHOLD = 12  # if opening more than this, skip suspense
    OPEN_HARD_CAP = 50

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

    TABLE_GRANT_ROLE_SQL = """
    CREATE TABLE IF NOT EXISTS lootbox_grant_perms (
        id INT NOT NULL AUTO_INCREMENT,
        guild_id BIGINT NOT NULL,
        role_id BIGINT NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uq_lootbox_grant_perms_guild_role (guild_id, role_id),
        KEY ix_lootbox_grant_perms_guild_id (guild_id),
        KEY ix_lootbox_grant_perms_role_id (role_id)
    );
    """

    TABLE_GRANT_USER_SQL = """
    CREATE TABLE IF NOT EXISTS lootbox_grant_user_perms (
        id INT NOT NULL AUTO_INCREMENT,
        guild_id BIGINT NOT NULL,
        user_id BIGINT NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uq_lootbox_grant_user_perms_guild_user (guild_id, user_id),
        KEY ix_lootbox_grant_user_perms_guild_id (guild_id),
        KEY ix_lootbox_grant_user_perms_user_id (user_id)
    );
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self._last_drop_try: dict[tuple[int, int], float] = {}
        self._active_channel_drops: dict[int, float] = {}  # channel_id -> expires_at

    # -------------------------
    # DB bootstrap
    # -------------------------
    async def _ensure_tables(self) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(self.TABLE_LOOTBOX_INV_SQL))
                await session.execute(text(self.TABLE_GRANT_ROLE_SQL))
                await session.execute(text(self.TABLE_GRANT_USER_SQL))

    # -------------------------
    # DB helpers
    # -------------------------
    async def _add_lootbox(self, *, guild_id: int, user_id: int, rarity: LootboxRarity, amount: int) -> None:
        await self._ensure_tables()

        amt = max(int(amount), 0)
        if amt <= 0:
            return

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                row = await session.scalar(
                    select(LootboxInventoryRow).where(
                        LootboxInventoryRow.guild_id == guild_id,
                        LootboxInventoryRow.user_id == user_id,
                        LootboxInventoryRow.rarity == rarity.value,
                    )
                )
                if row is None:
                    row = LootboxInventoryRow(
                        guild_id=guild_id,
                        user_id=user_id,
                        rarity=rarity.value,
                        amount=amt,
                    )
                    session.add(row)
                    await session.flush()
                else:
                    row.amount = int(row.amount) + amt

    async def _get_inventory_amount(self, *, guild_id: int, user_id: int, rarity: LootboxRarity) -> int:
        await self._ensure_tables()

        async with self.sessionmaker() as session:
            async with session.begin():
                row = await session.scalar(
                    select(LootboxInventoryRow).where(
                        LootboxInventoryRow.guild_id == guild_id,
                        LootboxInventoryRow.user_id == user_id,
                        LootboxInventoryRow.rarity == rarity.value,
                    )
                )
                if row is None:
                    return 0
                return max(int(row.amount), 0)

    async def _consume_inventory(
        self,
        *,
        session,
        guild_id: int,
        user_id: int,
        rarity: LootboxRarity,
        amount: int,
    ) -> bool:
        amt = max(int(amount), 0)
        if amt <= 0:
            return False

        row = await session.scalar(
            select(LootboxInventoryRow).where(
                LootboxInventoryRow.guild_id == guild_id,
                LootboxInventoryRow.user_id == user_id,
                LootboxInventoryRow.rarity == rarity.value,
            )
        )
        if row is None:
            return False

        have = int(row.amount)
        if have < amt:
            return False

        row.amount = have - amt
        return True

    def _roll_reward(self, rarity: LootboxRarity) -> LootboxReward:
        cfg = self.RARITY_TABLE[rarity]
        silver = random.randint(int(cfg.min_silver), int(cfg.max_silver))
        xp = random.randint(int(cfg.min_xp), int(cfg.max_xp))
        return LootboxReward(silver=max(_safe_int(silver), 0), xp=max(_safe_int(xp), 0))

    # -------------------------
    # Drop embed
    # -------------------------
    def _drop_description(self, rarity: LootboxRarity, *, claimed: int) -> str:
        return (
            f"**{_rarity_emoji(rarity)} {_rarity_label(rarity)} LOOTBOX DROP**\n"
            f"Click the button to claim **1** lootbox.\n"
            f"Expires in **60s**.\n\n"
            f"Claimed so far: **{_fmt_int(claimed)}**"
        )

    async def _spawn_drop(self, *, channel: discord.TextChannel, rarity: LootboxRarity) -> None:
        now = time.time()
        expires_at = now + float(self.DROP_EXPIRES_SECONDS)

        self._active_channel_drops[channel.id] = expires_at

        embed = discord.Embed(
            title="🎁 Lootbox Drop!",
            description=self._drop_description(rarity, claimed=0),
            color=_rarity_color(rarity),
        )
        embed.set_footer(text="First come, first served. One claim per user.")

        view = LootboxDropView(
            cog=self,
            guild_id=channel.guild.id,
            channel_id=channel.id,
            rarity=rarity,
            expires_in=self.DROP_EXPIRES_SECONDS,
        )

        msg = await channel.send(embed=embed, view=view)
        view.message = msg

    # -------------------------
    # Message listener drop
    # -------------------------
    @commands.Cog.listener("on_message")
    async def on_message_lootbox_drop(self, message: discord.Message):
        if message.guild is None:
            return
        if message.author.bot:
            return
        if not message.content:
            return
        if not isinstance(message.channel, discord.TextChannel):
            return

        guild_id = message.guild.id
        user_id = message.author.id
        channel_id = message.channel.id

        now = time.time()

        active_until = self._active_channel_drops.get(channel_id)
        if active_until is not None:
            if now < active_until:
                return
            self._active_channel_drops.pop(channel_id, None)

        k = (guild_id, user_id)
        last = self._last_drop_try.get(k, 0.0)
        if now - last < self.DROP_COOLDOWN_SECONDS_PER_USER:
            return
        self._last_drop_try[k] = now

        if not _roll_bp(self.DROP_CHANCE_BP):
            return

        rarity = _pick_weighted_rarity(self.RARITY_TABLE)
        try:
            await self._spawn_drop(channel=message.channel, rarity=rarity)
        except Exception:
            self._active_channel_drops.pop(channel_id, None)

    # -------------------------
    # "Animation" helpers for opening
    # -------------------------
    def _open_anim_frames(self, rarity: LootboxRarity, amt: int) -> list[tuple[str, str]]:
        emoji = _rarity_emoji(rarity)
        label = _rarity_label(rarity)
        plural = "es" if amt != 1 else ""

        frames = [
            (f"{emoji} Opening {label} Lootbox{plural}", "Shaking the box..."),
            (f"{emoji} Opening {label} Lootbox{plural}", "Listening for loot noises..."),
            (f"{emoji} Opening {label} Lootbox{plural}", "This better not be pocket lint..."),
            (f"{emoji} Opening {label} Lootbox{plural}", "Reveal incoming..."),
        ]
        return frames

    async def _animate_open(self, msg: Optional[discord.Message], rarity: LootboxRarity, amt: int) -> None:
        if msg is None:
            return

        frames = self._open_anim_frames(rarity, amt)
        steps = min(self.OPEN_ANIM_STEPS, len(frames))

        for i in range(steps):
            title, desc = frames[i]
            e = discord.Embed(
                title=title,
                description=desc,
                color=_rarity_color(rarity),
            )
            e.add_field(name="Boxes", value=f"🎲 **{_fmt_int(amt)}**", inline=True)
            e.add_field(name="Status", value=f"⏳ Step **{i + 1}** / **{steps}**", inline=True)
            e.set_footer(text="Hold up...")

            try:
                await msg.edit(embed=e)
            except Exception:
                return

            await asyncio.sleep(float(self.OPEN_ANIM_STEP_DELAY))

    # -------------------------
    # /lootbox group
    # -------------------------
    lootbox = app_commands.Group(name="lootbox", description="Open and manage your lootboxes.")

    @lootbox.command(name="open", description="Open lootboxes from your inventory.")
    @app_commands.describe(rarity="Which rarity to open", amount="How many to open (default 1)")
    async def lootbox_open(
        self,
        interaction: discord.Interaction,
        rarity: str,
        amount: Optional[int] = 1,
    ):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await self._ensure_tables()

        guild_id = interaction.guild.id
        user_id = interaction.user.id

        rkey = (rarity or "").strip().lower()
        if rkey not in {r.value for r in LootboxRarity}:
            await interaction.response.send_message("Invalid rarity. Use: common, rare, epic, legendary, mythical", ephemeral=True)
            return

        rarity_enum = LootboxRarity(rkey)
        amt_req = max(int(amount or 1), 1)
        amt = min(amt_req, self.OPEN_HARD_CAP)

        await interaction.response.defer(thinking=True)

        have = await self._get_inventory_amount(guild_id=guild_id, user_id=user_id, rarity=rarity_enum)
        if have < amt:
            await interaction.followup.send(
                f"You don’t have enough **{rarity_enum.value}** lootboxes. You have **{_fmt_int(have)}**.",
                ephemeral=True,
            )
            return

        init_embed = discord.Embed(
            title=f"{_rarity_emoji(rarity_enum)} Opening {_rarity_label(rarity_enum)} Lootboxes",
            description="Starting...",
            color=_rarity_color(rarity_enum),
        )
        init_embed.add_field(name="Boxes", value=f"🎲 **{_fmt_int(amt)}**", inline=True)
        init_embed.add_field(name="Status", value="⏳ Preparing...", inline=True)

        msg: Optional[discord.Message] = None
        try:
            msg = await interaction.followup.send(embed=init_embed, wait=True)
        except TypeError:
            try:
                await interaction.followup.send(embed=init_embed)
            except Exception:
                msg = None
        except Exception:
            msg = None

        fast_mode = amt > self.OPEN_FAST_MODE_THRESHOLD
        if not fast_mode:
            await self._animate_open(msg, rarity_enum, amt)

        total_silver = 0
        total_xp = 0

        rolls: list[LootboxReward] = []
        best_silver = 0
        best_xp = 0

        for _ in range(amt):
            rw = self._roll_reward(rarity_enum)
            rolls.append(rw)
            total_silver += int(rw.silver)
            total_xp += int(rw.xp)
            best_silver = max(best_silver, int(rw.silver))
            best_xp = max(best_xp, int(rw.xp))

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                ok = await self._consume_inventory(
                    session=session,
                    guild_id=guild_id,
                    user_id=user_id,
                    rarity=rarity_enum,
                    amount=amt,
                )
                if not ok:
                    have2 = await self._get_inventory_amount(guild_id=guild_id, user_id=user_id, rarity=rarity_enum)
                    try:
                        await interaction.followup.send(
                            f"Not enough **{rarity_enum.value}** lootboxes anymore. You now have **{_fmt_int(have2)}**.",
                            ephemeral=True,
                        )
                    except Exception:
                        pass
                    return

                wallet = await session.scalar(
                    select(WalletRow).where(
                        WalletRow.guild_id == guild_id,
                        WalletRow.user_id == user_id,
                    )
                )
                if wallet is None:
                    wallet = WalletRow(guild_id=guild_id, user_id=user_id, silver=0, diamonds=0)
                    session.add(wallet)
                    await session.flush()

                wallet.silver += int(total_silver)
                if hasattr(wallet, "silver_earned"):
                    wallet.silver_earned += int(max(total_silver, 0))

                if total_xp > 0:
                    await award_xp(session, guild_id=guild_id, user_id=user_id, amount=int(total_xp))

        e = discord.Embed(
            title=f"{_rarity_emoji(rarity_enum)} Lootbox Results",
            description=f"Opened **{_fmt_int(amt)}x {rarity_enum.value}** lootbox(es).",
            color=_rarity_color(rarity_enum),
        )
        e.add_field(name="Silver", value=f"💰 **+{_fmt_int(total_silver)}**", inline=True)
        e.add_field(name="XP", value=f"🧠 **+{_fmt_int(total_xp)}**", inline=True)
        e.add_field(name="Best Pull", value=f"💰 **{_fmt_int(best_silver)}** | 🧠 **{_fmt_int(best_xp)}**", inline=False)

        if fast_mode:
            e.set_footer(text="Fast reveal mode (opening lots of boxes).")
        else:
            e.set_footer(text="Respectfully, your wallet just did a backflip.")

        if msg is not None:
            try:
                await msg.edit(embed=e)
                return
            except Exception:
                pass

        await interaction.followup.send(embed=e)

    @lootbox.command(name="inventory", description="See how many lootboxes you have.")
    async def lootbox_inventory(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await self._ensure_tables()

        guild_id = interaction.guild.id
        user_id = interaction.user.id

        async with self.sessionmaker() as session:
            async with session.begin():
                rows = (await session.scalars(
                    select(LootboxInventoryRow).where(
                        LootboxInventoryRow.guild_id == guild_id,
                        LootboxInventoryRow.user_id == user_id,
                    )
                )).all()

        by = {r.rarity: int(r.amount) for r in rows}
        lines = []
        for rar in LootboxRarity:
            lines.append(f"• **{rar.value}**: **{_fmt_int(by.get(rar.value, 0))}**")

        embed = discord.Embed(
            title="🎒 Your Lootboxes",
            description="\n".join(lines),
            color=discord.Color.blurple(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # -------------------------
    # Grant permissions
    # -------------------------
    async def _can_grant(self, interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            return False

        member = interaction.user
        if not isinstance(member, discord.Member):
            return False

        if member.guild_permissions.manage_guild or member.guild_permissions.administrator:
            return True

        await self._ensure_tables()

        guild_id = interaction.guild.id
        user_id = member.id

        async with self.sessionmaker() as session:
            async with session.begin():
                u = await session.scalar(
                    select(LootboxGrantUserPermRow).where(
                        LootboxGrantUserPermRow.guild_id == guild_id,
                        LootboxGrantUserPermRow.user_id == user_id,
                    )
                )
                if u is not None:
                    return True

                role_ids = [r.id for r in member.roles]
                if not role_ids:
                    return False

                rows = (await session.scalars(
                    select(LootboxGrantPermRow).where(
                        LootboxGrantPermRow.guild_id == guild_id,
                        LootboxGrantPermRow.role_id.in_(role_ids),
                    )
                )).all()

                return bool(rows)

    lootbox_admin = app_commands.Group(name="lootbox_admin", description="Admin tools for lootboxes.")

    @lootbox_admin.command(name="perms", description="Show who can give lootboxes (roles + users).")
    @checks.has_permissions(manage_guild=True)
    async def lootbox_admin_perms(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await self._ensure_tables()

        guild_id = interaction.guild.id

        async with self.sessionmaker() as session:
            async with session.begin():
                role_rows = (await session.scalars(
                    select(LootboxGrantPermRow).where(LootboxGrantPermRow.guild_id == guild_id)
                )).all()
                user_rows = (await session.scalars(
                    select(LootboxGrantUserPermRow).where(LootboxGrantUserPermRow.guild_id == guild_id)
                )).all()

        role_lines = []
        for rr in role_rows:
            role = interaction.guild.get_role(int(rr.role_id))
            role_lines.append(role.mention if role else f"`{rr.role_id}` (missing role)")

        user_lines = []
        for ur in user_rows:
            member = interaction.guild.get_member(int(ur.user_id))
            user_lines.append(member.mention if member else f"`{ur.user_id}` (missing user)")

        embed = discord.Embed(
            title="🎁 Lootbox Grant Permissions",
            description="These roles/users are allowed to use `/lootbox_admin give` even without Manage Server.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Roles", value="\n".join(role_lines) if role_lines else "None", inline=False)
        embed.add_field(name="Users", value="\n".join(user_lines) if user_lines else "None", inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @lootbox_admin.command(name="grant_role", description="Allow a role to give lootboxes.")
    @app_commands.describe(role="Role to allow")
    @checks.has_permissions(manage_guild=True)
    async def lootbox_admin_grant_role(self, interaction: discord.Interaction, role: discord.Role):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await self._ensure_tables()

        guild_id = interaction.guild.id
        role_id = role.id

        async with self.sessionmaker() as session:
            async with session.begin():
                row = await session.scalar(
                    select(LootboxGrantPermRow).where(
                        LootboxGrantPermRow.guild_id == guild_id,
                        LootboxGrantPermRow.role_id == role_id,
                    )
                )
                if row is None:
                    session.add(LootboxGrantPermRow(guild_id=guild_id, role_id=role_id))

        await interaction.response.send_message(f"✅ {role.mention} can now give lootboxes.", ephemeral=True)

    @lootbox_admin.command(name="revoke_role", description="Remove a role from lootbox grant permissions.")
    @app_commands.describe(role="Role to revoke")
    @checks.has_permissions(manage_guild=True)
    async def lootbox_admin_revoke_role(self, interaction: discord.Interaction, role: discord.Role):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await self._ensure_tables()

        guild_id = interaction.guild.id
        role_id = role.id

        async with self.sessionmaker() as session:
            async with session.begin():
                row = await session.scalar(
                    select(LootboxGrantPermRow).where(
                        LootboxGrantPermRow.guild_id == guild_id,
                        LootboxGrantPermRow.role_id == role_id,
                    )
                )
                if row is not None:
                    await session.delete(row)

        await interaction.response.send_message(f"✅ Removed {role.mention} from grant perms.", ephemeral=True)

    @lootbox_admin.command(name="grant_user", description="Allow a specific user to give lootboxes.")
    @app_commands.describe(user="User to allow")
    @checks.has_permissions(manage_guild=True)
    async def lootbox_admin_grant_user(self, interaction: discord.Interaction, user: discord.Member):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await self._ensure_tables()

        guild_id = interaction.guild.id
        user_id = user.id

        async with self.sessionmaker() as session:
            async with session.begin():
                row = await session.scalar(
                    select(LootboxGrantUserPermRow).where(
                        LootboxGrantUserPermRow.guild_id == guild_id,
                        LootboxGrantUserPermRow.user_id == user_id,
                    )
                )
                if row is None:
                    session.add(LootboxGrantUserPermRow(guild_id=guild_id, user_id=user_id))

        await interaction.response.send_message(f"✅ {user.mention} can now give lootboxes.", ephemeral=True)

    @lootbox_admin.command(name="revoke_user", description="Remove a user from grant permissions.")
    @app_commands.describe(user="User to revoke")
    @checks.has_permissions(manage_guild=True)
    async def lootbox_admin_revoke_user(self, interaction: discord.Interaction, user: discord.Member):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await self._ensure_tables()

        guild_id = interaction.guild.id
        user_id = user.id

        async with self.sessionmaker() as session:
            async with session.begin():
                row = await session.scalar(
                    select(LootboxGrantUserPermRow).where(
                        LootboxGrantUserPermRow.guild_id == guild_id,
                        LootboxGrantUserPermRow.user_id == user_id,
                    )
                )
                if row is not None:
                    await session.delete(row)

        await interaction.response.send_message(f"✅ Removed {user.mention} from grant perms.", ephemeral=True)

    @lootbox_admin.command(name="give", description="Give a lootbox to a user (authorized staff).")
    @app_commands.describe(user="Who gets it", rarity="common/rare/epic/legendary", amount="How many (default 1)")
    async def lootbox_admin_give(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        rarity: str,
        amount: Optional[int] = 1
    ):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await self._ensure_tables()

        if not (await self._can_grant(interaction)):
            await interaction.response.send_message("You don’t have permission to give lootboxes.", ephemeral=True)
            return

        rkey = (rarity or "").strip().lower()
        if rkey not in {r.value for r in LootboxRarity}:
            await interaction.response.send_message("Invalid rarity. Use: common, rare, epic, legendary, mythical", ephemeral=True)
            return

        amt = max(int(amount or 1), 1)
        amt = min(amt, self.OPEN_HARD_CAP)

        await self._add_lootbox(
            guild_id=interaction.guild.id,
            user_id=user.id,
            rarity=LootboxRarity(rkey),
            amount=amt,
        )

        embed = discord.Embed(
            title="🎁 Lootbox Granted",
            description=f"Gave {user.mention} **{_fmt_int(amt)}x {rkey}** lootbox(es).",
            color=discord.Color.green(),
        )
        await interaction.response.send_message(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(LootboxCog(bot))
