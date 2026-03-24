from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ItemInventoryRow, WalletRow
from services.db import sessions
from services.items_inventory import remove_item
from services.users import ensure_user_rows
from services.vip import is_vip_member

UNO_REVERSE_WALLET_KEY = "uno_reverse_wallet"
REVENGE_VIEW_TIMEOUT_SECONDS = 60


SUCCESS_FLAVOR_LINES = [
    "Bro got looted in 4K.",
    "Wallet audited successfully.",
    "That pocket got inspected.",
    "Silver changed ownership instantly.",
    "Financial disrespect completed.",
    "Pocket tax processed successfully.",
]

LOW_BALANCE_FAIL_LINES = [
    "They have under 10 Silver. That wallet is running on fumes.",
    "You checked the pockets and found pure economic sadness.",
    "Their balance is under 10 Silver. There is nothing to finesse here.",
    "You tried to rob them, but that wallet is basically decorative.",
    "Not enough Silver to steal. The pockets are on life support.",
]

FAIL_LINES_GENERAL = [
    "You fumble the bag and trip over your own confidence.",
    "You go for the swipe and hit… absolutely nothing.",
    "They look at you. You look away. Cringe detected.",
    "Your stealth stat is fake news.",
    "You get caught and act like you were just fixing their pocket. Sure.",
    "You miss the pocket and steal… their respect for you.",
    "They dodge like they have ultra instinct.",
    "You try to pickpocket but your hands lag IRL.",
    "You reach in and pull out shame. Just shame.",
    "You nearly had it… then you remembered you don’t got it like that.",
]

REVENGE_SUCCESS_LINES = [
    "Immediate get-back. Cinema.",
    "Pocket karma is real.",
    "Reverse uno from the trenches.",
    "Justice arrived fast.",
    "That comeback actually landed.",
]

REVENGE_FAIL_LINES = [
    "Bro grabbed air.",
    "Revenge was attempted. Heavy on attempted.",
    "Timing awful. Hands colder.",
    "Public embarrassment unlocked.",
    "The comeback was not comebacking.",
]

UNO_REVERSE_LINES = [
    "Trap card activated.",
    "Wallet said not today.",
    "Instant karma hit hard.",
    "Pocket physics just folded you.",
    "Reverse successful. Pride not included.",
]

_COOLDOWNS: Dict[Tuple[int, int], float] = {}


@dataclass(frozen=True)
class RobberyOutcome:
    amount: int
    percent: int


async def _get_or_create_wallet(session: AsyncSession, *, guild_id: int, user_id: int) -> WalletRow:
    wallet = await session.scalar(
        select(WalletRow)
        .where(WalletRow.guild_id == int(guild_id), WalletRow.user_id == int(user_id))
        .with_for_update()
    )
    if wallet is None:
        wallet = WalletRow(guild_id=int(guild_id), user_id=int(user_id), silver=0, diamonds=0)
        session.add(wallet)
        await session.flush()
    return wallet


def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _cooldown_text(seconds_left: int) -> str:
    s = max(int(seconds_left), 0)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{(s + 59) // 60}m"
    return f"{(s + 3599) // 3600}h"


def _clean_discord_kwargs(**kwargs):
    return {k: v for k, v in kwargs.items() if v is not None}

def _compute_percent_steal(balance: int) -> Optional[RobberyOutcome]:
    current_balance = max(int(balance), 0)
    percent = random.randint(2, 25)
    steal_amount = math.floor(current_balance * (percent / 100))
    steal_amount = max(10, steal_amount)
    steal_amount = min(50_000, steal_amount)
    steal_amount = min(steal_amount, current_balance)
    if steal_amount <= 0:
        return None
    return RobberyOutcome(amount=int(steal_amount), percent=int(percent))


async def _transfer_silver(*, payer: WalletRow, payee: WalletRow, amount: int) -> int:
    current_payer = max(int(getattr(payer, "silver", 0) or 0), 0)
    applied = min(max(int(amount), 0), current_payer)
    if applied <= 0:
        return 0

    payer.silver = current_payer - applied
    payee.silver = max(int(getattr(payee, "silver", 0) or 0), 0) + applied

    if hasattr(payer, "silver_spent"):
        payer.silver_spent = int(getattr(payer, "silver_spent", 0) or 0) + applied
    if hasattr(payee, "silver_earned"):
        payee.silver_earned = int(getattr(payee, "silver_earned", 0) or 0) + applied
    return applied


async def _get_locked_inventory_row(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    item_key: str,
) -> Optional[ItemInventoryRow]:
    return await session.scalar(
        select(ItemInventoryRow)
        .where(
            ItemInventoryRow.guild_id == int(guild_id),
            ItemInventoryRow.user_id == int(user_id),
            ItemInventoryRow.item_key == str(item_key),
        )
        .with_for_update()
    )


class RevengeView(discord.ui.View):
    def __init__(
        self,
        *,
        cog: "PickpocketCog",
        guild_id: int,
        victim_id: int,
        thief_id: int,
        original_amount: int,
    ):
        super().__init__(timeout=REVENGE_VIEW_TIMEOUT_SECONDS)
        self.cog = cog
        self.guild_id = int(guild_id)
        self.victim_id = int(victim_id)
        self.thief_id = int(thief_id)
        self.original_amount = int(original_amount)
        self.message: Optional[discord.Message] = None
        self.resolved = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.victim_id:
            await interaction.response.send_message("That button is not for you. Mind your own pockets.", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        self._disable_all()
        await self._safe_edit_message()

    def _disable_all(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

    async def _safe_edit_message(self) -> None:
        if self.message is None:
            return
        try:
            await self.message.edit(view=self)
        except Exception:
            pass

    @discord.ui.button(label="Caught You Lacking", style=discord.ButtonStyle.danger)
    async def revenge_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if self.resolved:
            await interaction.response.send_message("That revenge window is already closed.", ephemeral=True)
            return

        self.resolved = True
        self._disable_all()
        await interaction.response.edit_message(view=self)

        embed = await self.cog.resolve_revenge(
            guild_id=self.guild_id,
            victim=interaction.user,
            thief_id=self.thief_id,
            original_amount=self.original_amount,
        )
        await interaction.followup.send(embed=embed)


class PickpocketCog(commands.Cog):
    COOLDOWN_REGULAR = 3 * 60 * 60  # 3 hours
    COOLDOWN_VIP = 3 * 60  # 3 minutes

    FAIL_CHANCE_BP = 7800  # 78% fail chance

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    def _roll_bp(self, chance_bp: int) -> bool:
        bp = max(int(chance_bp), 0)
        if bp <= 0:
            return False
        if bp >= 10000:
            return True
        return random.randint(1, 10000) <= bp

    def _failure_embed(self, *, thief: discord.Member, target: discord.Member, line: str) -> discord.Embed:
        return discord.Embed(
            title="Pickpocket Result",
            description=f"{thief.mention} tried to pickpocket {target.mention}.\n\n**FAILED** ❌\n{line}",
            color=discord.Color.red(),
        )

    def _success_embed(
        self,
        *,
        thief: discord.Member,
        target: discord.Member,
        amount: int,
        percent: int,
    ) -> discord.Embed:
        return discord.Embed(
            title="SUCCESS ✅",
            description=(
                f"{thief.mention} finessed **{_fmt_int(amount)} Silver** from {target.mention}.\n"
                f"That was a **{percent}%** pocket tax.\n\n"
                f"_{random.choice(SUCCESS_FLAVOR_LINES)}_"
            ),
            color=discord.Color.green(),
        )

    def _reverse_embed(
        self,
        *,
        victim: discord.Member,
        thief: discord.Member,
        amount: int,
        percent: int,
    ) -> discord.Embed:
        return discord.Embed(
            title="UNO REVERSE 🔁",
            description=(
                f"{victim.mention}’s **Uno Reverse Wallet** snapped shut on {thief.mention} and stole **{_fmt_int(amount)} Silver** back.\n"
                f"That was a **{percent}%** bozo tax.\n\n"
                f"_{random.choice(UNO_REVERSE_LINES)}_"
            ),
            color=discord.Color.purple(),
        )

    def _revenge_success_embed(self, *, victim: discord.abc.User, thief: discord.abc.User, amount: int) -> discord.Embed:
        return discord.Embed(
            title="CAUGHT YOU LACKING 🔥",
            description=(
                f"{victim.mention} reached into {thief.mention}’s pockets and stole back **{_fmt_int(amount)} Silver**.\n"
                f"_{random.choice(REVENGE_SUCCESS_LINES)}_"
            ),
            color=discord.Color.orange(),
        )

    def _revenge_fail_embed(self, *, victim: discord.abc.User, thief: discord.abc.User) -> discord.Embed:
        return discord.Embed(
            title="REVENGE FAILED 💀",
            description=(
                f"{victim.mention} tried to get even with {thief.mention} and absolutely did not.\n"
                f"_{random.choice(REVENGE_FAIL_LINES)}_"
            ),
            color=discord.Color.dark_red(),
        )

    async def resolve_revenge(
        self,
        *,
        guild_id: int,
        victim: discord.abc.User,
        thief_id: int,
        original_amount: int,
    ) -> discord.Embed:
        guild = self.bot.get_guild(int(guild_id))
        thief_obj = guild.get_member(int(thief_id)) if guild is not None else None
        thief_user: discord.abc.User = thief_obj or self.bot.get_user(int(thief_id)) or victim

        success = not self._roll_bp(self.FAIL_CHANCE_BP)
        if not success:
            return self._revenge_fail_embed(victim=victim, thief=thief_user)

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=int(victim.id))
                await ensure_user_rows(session, guild_id=guild_id, user_id=int(thief_id))

                victim_wallet = await _get_or_create_wallet(session, guild_id=guild_id, user_id=int(victim.id))
                thief_wallet = await _get_or_create_wallet(session, guild_id=guild_id, user_id=int(thief_id))

                minimum = max(1, math.floor(int(original_amount) * 0.5))
                maximum = max(1, int(original_amount))
                revenge_amount = random.randint(minimum, maximum)
                revenge_amount = min(revenge_amount, max(int(thief_wallet.silver or 0), 0))

                applied = await _transfer_silver(payer=thief_wallet, payee=victim_wallet, amount=revenge_amount)

        if applied <= 0:
            return self._revenge_fail_embed(victim=victim, thief=thief_user)
        return self._revenge_success_embed(victim=victim, thief=thief_user, amount=applied)

    @app_commands.command(name="pickpocket", description="Try to steal some silver from someone. Risky.")
    @app_commands.describe(target="The person you want to attempt to pickpocket")
    async def pickpocket_cmd(self, interaction: discord.Interaction, target: discord.Member):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        thief = interaction.user
        if not isinstance(thief, discord.Member):
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        if target.bot:
            await interaction.response.send_message("Pickpocketing bots is crazy work.", ephemeral=True)
            return

        if target.id == thief.id:
            await interaction.response.send_message("You can’t pickpocket yourself. Nice try though.", ephemeral=True)
            return

        guild_id = interaction.guild.id
        thief_id = thief.id
        target_id = target.id

        vip = is_vip_member(thief)  # type: ignore[arg-type]
        cooldown = self.COOLDOWN_VIP if vip else self.COOLDOWN_REGULAR

        now = time.time()
        cd_key = (guild_id, thief_id)
        ready_at = _COOLDOWNS.get(cd_key, 0.0)
        if ready_at > now:
            left = int(max(ready_at - now, 0))
            await interaction.response.send_message(
                f"Cooldown active. Try again in **{_cooldown_text(left)}**.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(thinking=True)

        followup_embed: Optional[discord.Embed] = None
        followup_view: Optional[RevengeView] = None
        reverse_embed: Optional[discord.Embed] = None

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=thief_id)
                await ensure_user_rows(session, guild_id=guild_id, user_id=target_id)

                thief_wallet = await _get_or_create_wallet(session, guild_id=guild_id, user_id=thief_id)
                target_wallet = await _get_or_create_wallet(session, guild_id=guild_id, user_id=target_id)

                target_silver = max(int(getattr(target_wallet, "silver", 0) or 0), 0)
                if target_silver < 10:
                    _COOLDOWNS[cd_key] = now + float(cooldown)
                    followup_embed = self._failure_embed(
                        thief=thief,
                        target=target,
                        line=random.choice(LOW_BALANCE_FAIL_LINES),
                    )
                else:
                    failed = self._roll_bp(self.FAIL_CHANCE_BP)
                    if failed:
                        _COOLDOWNS[cd_key] = now + float(cooldown)
                        followup_embed = self._failure_embed(
                            thief=thief,
                            target=target,
                            line=random.choice(FAIL_LINES_GENERAL),
                        )
                    else:
                        robbery = _compute_percent_steal(target_silver)
                        if robbery is None or robbery.amount <= 0:
                            _COOLDOWNS[cd_key] = now + float(cooldown)
                            followup_embed = self._failure_embed(
                                thief=thief,
                                target=target,
                                line=random.choice(LOW_BALANCE_FAIL_LINES),
                            )
                        else:
                            applied = await _transfer_silver(
                                payer=target_wallet,
                                payee=thief_wallet,
                                amount=robbery.amount,
                            )
                            _COOLDOWNS[cd_key] = now + float(cooldown)

                            if applied <= 0:
                                followup_embed = self._failure_embed(
                                    thief=thief,
                                    target=target,
                                    line=random.choice(LOW_BALANCE_FAIL_LINES),
                                )
                            else:
                                followup_embed = self._success_embed(
                                    thief=thief,
                                    target=target,
                                    amount=applied,
                                    percent=robbery.percent,
                                )

                                reverse_row = await _get_locked_inventory_row(
                                    session,
                                    guild_id=guild_id,
                                    user_id=target_id,
                                    item_key=UNO_REVERSE_WALLET_KEY,
                                )
                                reverse_qty = max(int(getattr(reverse_row, "qty", 0) or 0), 0) if reverse_row else 0
                                if reverse_qty > 0:
                                    thief_current_silver = max(int(getattr(thief_wallet, "silver", 0) or 0), 0)
                                    reverse = _compute_percent_steal(thief_current_silver)
                                    await remove_item(
                                        session,
                                        guild_id=guild_id,
                                        user_id=target_id,
                                        item_key=UNO_REVERSE_WALLET_KEY,
                                        qty=1,
                                    )
                                    if reverse is not None and reverse.amount > 0:
                                        reversed_amount = await _transfer_silver(
                                            payer=thief_wallet,
                                            payee=target_wallet,
                                            amount=reverse.amount,
                                        )
                                        if reversed_amount > 0:
                                            reverse_embed = self._reverse_embed(
                                                victim=target,
                                                thief=thief,
                                                amount=reversed_amount,
                                                percent=reverse.percent,
                                            )
                                else:
                                    followup_view = RevengeView(
                                        cog=self,
                                        guild_id=guild_id,
                                        victim_id=target_id,
                                        thief_id=thief_id,
                                        original_amount=applied,
                                    )

        if followup_embed is None:
            followup_embed = self._failure_embed(thief=thief, target=target, line="The wallet math broke. Somehow.")

        sent = await interaction.followup.send(**_clean_discord_kwargs(embed=followup_embed, view=followup_view))
        if followup_view is not None:
            followup_view.message = sent
        if reverse_embed is not None:
            await interaction.followup.send(embed=reverse_embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(PickpocketCog(bot))
