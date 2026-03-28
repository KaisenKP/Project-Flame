from __future__ import annotations

import asyncio
import logging
from collections import defaultdict

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select

from services.achievements import check_and_grant_achievements, queue_achievement_announcements
from services.db import sessions
from services.users import ensure_user_rows

from .catalog import ASSET_CATALOG
from .embeds import (
    build_assets_embed,
    build_buy_confirmation_embed,
    build_loan_capacity_embed,
    build_loan_confirmation_embed,
    build_loan_status_embed,
    build_showcase_embed,
    build_store_embed,
)
from .repo import (
    clear_showcase_slot,
    count_owned_asset_key,
    evaluate_loan_state,
    get_active_loan,
    get_loan_capacity,
    get_loan_snapshot,
    get_or_create_wallet,
    get_overview_snapshot,
    get_user_asset_value,
    get_user_net_worth,
    issue_loan,
    list_owned_assets,
    purchase_asset,
    repay_loan,
    set_showcase_slot,
)
from .ui import ConfirmCancelView, LuxuryHubData, LuxuryHubView, build_shop_confirmation_embed
from .util import BASE_INTEREST_RATE, LOAN_DURATION_DAYS, SHOWCASE_SLOTS_MAX, due_date_from_now, fmt_int

log = logging.getLogger(__name__)


class LuxuryHubController:
    def __init__(self, *, cog: "LuxuryAssetsCog"):
        self.cog = cog
        self._asset_cache: dict[tuple[int, int], list] = {}
        self._slot_state: dict[tuple[int, int], int] = {}

    def _key(self, interaction: discord.Interaction) -> tuple[int, int]:
        return (int(interaction.guild_id), int(interaction.user.id))

    async def get_data(self, interaction: discord.Interaction) -> LuxuryHubData:
        guild_id = int(interaction.guild_id)
        user_id = int(interaction.user.id)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)
                await evaluate_loan_state(session, guild_id=guild_id, user_id=user_id)
                wallet = await get_or_create_wallet(session, guild_id=guild_id, user_id=user_id)
                assets = await list_owned_assets(session, guild_id=guild_id, user_id=user_id)
                capacity = await get_loan_capacity(session, guild_id=guild_id, user_id=user_id)
                loan = await get_loan_snapshot(session, guild_id=guild_id, user_id=user_id)
                total_asset_value = await get_user_asset_value(session, guild_id=guild_id, user_id=user_id)
                net_worth = await get_user_net_worth(session, guild_id=guild_id, user_id=user_id)

        self._asset_cache[self._key(interaction)] = assets
        return LuxuryHubData(
            balance=int(wallet.silver),
            assets=assets,
            total_asset_value=total_asset_value,
            net_worth=net_worth,
            capacity=capacity,
            loan=loan,
        )

    async def get_overview(self, interaction: discord.Interaction):
        guild_id = int(interaction.guild_id)
        user_id = int(interaction.user.id)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)
                await evaluate_loan_state(session, guild_id=guild_id, user_id=user_id)
                return await get_overview_snapshot(session, guild_id=guild_id, user_id=user_id)

    def peek_assets(self, interaction: discord.Interaction):
        return self._asset_cache.get(self._key(interaction), [])

    async def find_asset(self, interaction: discord.Interaction, asset_id: int):
        assets = self.peek_assets(interaction)
        if not assets:
            await self.get_data(interaction)
            assets = self.peek_assets(interaction)
        for row in assets:
            if int(row.id) == int(asset_id):
                return row
        return None

    async def get_owned_count(self, interaction: discord.Interaction, asset_key: str | None) -> int:
        if not asset_key:
            return 0
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                return await count_owned_asset_key(
                    session,
                    guild_id=int(interaction.guild_id),
                    user_id=int(interaction.user.id),
                    asset_key=asset_key,
                )

    async def get_shop_confirmation(self, interaction: discord.Interaction, asset_key: str):
        if asset_key not in ASSET_CATALOG or not ASSET_CATALOG[asset_key].is_active:
            return None
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                wallet = await get_or_create_wallet(session, guild_id=int(interaction.guild_id), user_id=int(interaction.user.id))
        return await build_shop_confirmation_embed(user=interaction.user, asset_key=asset_key, balance=int(wallet.silver))

    async def buy(self, interaction: discord.Interaction, asset_key: str) -> tuple[bool, str]:
        if asset_key not in ASSET_CATALOG or not ASSET_CATALOG[asset_key].is_active:
            return False, "That item is not available in the active catalog."

        lock = self.cog._user_locks[self._key(interaction)]
        async with lock:
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    try:
                        row = await purchase_asset(
                            session,
                            guild_id=int(interaction.guild_id),
                            user_id=int(interaction.user.id),
                            asset_key=asset_key,
                        )
                    except ValueError as exc:
                        if str(exc) == "insufficient_silver":
                            return False, "Insufficient silver for this purchase."
                        return False, f"Purchase blocked: {exc}"

                    unlocked_achievements = await check_and_grant_achievements(
                        session,
                        guild_id=int(interaction.guild_id),
                        user_id=int(interaction.user.id),
                    )

            if unlocked_achievements:
                queue_achievement_announcements(
                    bot=self.cog.bot,
                    guild_id=int(interaction.guild_id),
                    user_id=int(interaction.user.id),
                    unlocks=unlocked_achievements,
                )

        asset = ASSET_CATALOG[asset_key]
        await self.get_data(interaction)
        return True, f"Purchased {asset.emoji} **{asset.name}** (`#{row.id}`)."

    def set_selected_slot(self, interaction: discord.Interaction, slot: int) -> None:
        self._slot_state[self._key(interaction)] = int(slot)

    def get_selected_slot(self, interaction: discord.Interaction) -> int | None:
        return self._slot_state.get(self._key(interaction))

    async def assign_showcase(self, interaction: discord.Interaction, *, asset_id: int, slot: int) -> tuple[bool, str]:
        lock = self.cog._user_locks[self._key(interaction)]
        async with lock:
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    try:
                        row = await set_showcase_slot(
                            session,
                            guild_id=int(interaction.guild_id),
                            user_id=int(interaction.user.id),
                            asset_id=int(asset_id),
                            slot=int(slot),
                        )
                    except ValueError as exc:
                        text = str(exc)
                        if text == "invalid_slot":
                            return False, "Invalid showcase slot."
                        if text == "asset_not_found":
                            return False, "Asset not found or it is seized."
                        return False, f"Showcase update failed: {text}"

        await self.get_data(interaction)
        asset = ASSET_CATALOG.get(row.asset_key)
        name = asset.name if asset else row.asset_key
        return True, f"Assigned **{name}** (`#{row.id}`) to slot **{slot}**."

    async def clear_showcase(self, interaction: discord.Interaction, *, slot: int) -> tuple[bool, str]:
        lock = self.cog._user_locks[self._key(interaction)]
        async with lock:
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    changed = await clear_showcase_slot(
                        session,
                        guild_id=int(interaction.guild_id),
                        user_id=int(interaction.user.id),
                        slot=int(slot),
                    )

        await self.get_data(interaction)
        if not changed:
            return False, "That slot was already empty or invalid."
        return True, f"Cleared showcase slot **{slot}**."

    async def borrow(self, interaction: discord.Interaction, amount: int) -> tuple[bool, str]:
        lock = self.cog._user_locks[self._key(interaction)]
        async with lock:
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    await evaluate_loan_state(session, guild_id=int(interaction.guild_id), user_id=int(interaction.user.id))
                    cap = await get_loan_capacity(session, guild_id=int(interaction.guild_id), user_id=int(interaction.user.id))
                    active = await get_active_loan(session, guild_id=int(interaction.guild_id), user_id=int(interaction.user.id))
                    if active is not None:
                        return False, "You already have an active/overdue/defaulted loan."
                    if amount > cap.available_to_borrow:
                        return False, f"Amount exceeds available capacity ({fmt_int(cap.available_to_borrow)})."
                    try:
                        await issue_loan(
                            session,
                            guild_id=int(interaction.guild_id),
                            user_id=int(interaction.user.id),
                            principal_amount=int(amount),
                            duration_days=LOAN_DURATION_DAYS,
                        )
                    except ValueError as exc:
                        return False, f"Loan failed: {exc}"

        await self.get_data(interaction)
        return True, f"Borrowed **{fmt_int(amount)}** silver."

    async def repay(self, interaction: discord.Interaction, amount: int) -> tuple[bool, str]:
        lock = self.cog._user_locks[self._key(interaction)]
        async with lock:
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    try:
                        loan = await repay_loan(
                            session,
                            guild_id=int(interaction.guild_id),
                            user_id=int(interaction.user.id),
                            amount=int(amount),
                        )
                    except ValueError as exc:
                        return False, f"Repayment failed: {exc}"

        await self.get_data(interaction)
        return True, f"Remaining loan balance: **{fmt_int(loan.remaining_balance)}**."


class LuxuryAssetsCog(commands.Cog):
    luxury = app_commands.Group(name="luxury", description="Luxury asset hub and compatibility commands.")
    loan = app_commands.Group(name="loan", description="Asset-backed loans and repayment.")

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self._loan_task: asyncio.Task | None = None
        self._user_locks = defaultdict(asyncio.Lock)
        self.hub_controller = LuxuryHubController(cog=self)

    async def cog_load(self):
        self._loan_task = asyncio.create_task(self._loan_lifecycle_loop(), name="luxury.loan.lifecycle")

    async def cog_unload(self):
        if self._loan_task is not None:
            self._loan_task.cancel()

    async def _loan_lifecycle_loop(self):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                async with self.sessionmaker() as session:
                    async with session.begin():
                        from db.models import LuxuryLoanRow

                        rows = (
                            await session.scalars(
                                select(LuxuryLoanRow).where(
                                    LuxuryLoanRow.status.in_(["active", "overdue", "defaulted"])
                                )
                            )
                        ).all()
                        for row in rows:
                            await evaluate_loan_state(session, guild_id=int(row.guild_id), user_id=int(row.user_id))
            except Exception:
                log.exception("loan lifecycle loop tick failed")
            await asyncio.sleep(300)

    async def open_hub(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)

        view = LuxuryHubView(owner_id=interaction.user.id, controller=self.hub_controller)
        data = await self.hub_controller.get_data(interaction)
        _ = data
        embed = await view.build_embed(interaction)
        view._rebuild_dynamic_controls(interaction)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @luxury.command(name="hub", description="Open the Luxury Hub.")
    async def luxury_hub(self, interaction: discord.Interaction):
        await self.open_hub(interaction)

    @luxury.command(name="store", description="Browse luxury assets and prices. (legacy compatibility)")
    async def luxury_store(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                wallet = await get_or_create_wallet(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                em = build_store_embed(user=interaction.user, balance=int(wallet.silver))
        await interaction.followup.send(embed=em, ephemeral=True)

    @luxury.command(name="buy", description="Buy a luxury asset. (legacy compatibility)")
    @app_commands.describe(asset_key="Catalog key, e.g. supercar")
    async def luxury_buy(self, interaction: discord.Interaction, asset_key: str):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)

        asset_key = asset_key.strip().lower()
        if asset_key not in ASSET_CATALOG or not ASSET_CATALOG[asset_key].is_active:
            return await interaction.response.send_message("Unknown asset key. Use `/luxury store` first.", ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                wallet = await get_or_create_wallet(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                owned_count = await count_owned_asset_key(
                    session,
                    guild_id=interaction.guild.id,
                    user_id=interaction.user.id,
                    asset_key=asset_key,
                )

        em = build_buy_confirmation_embed(user=interaction.user, asset_key=asset_key, balance=int(wallet.silver))
        em.add_field(name="You Own", value=f"{owned_count}x", inline=True)

        view = ConfirmCancelView(owner_id=interaction.user.id)
        await interaction.response.send_message(embed=em, view=view, ephemeral=True)
        await view.wait()
        if not view.confirmed:
            return

        ok, message = await self.hub_controller.buy(interaction, asset_key)
        if ok:
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.followup.send(message, ephemeral=True)

    @luxury.command(name="showcase", description="View your 3 luxury showcase slots. (legacy compatibility)")
    async def luxury_showcase(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                assets = await list_owned_assets(session, guild_id=interaction.guild.id, user_id=interaction.user.id)

        await interaction.followup.send(embed=build_showcase_embed(user=interaction.user, assets=assets), ephemeral=True)

    @luxury.command(name="equip", description="Equip an owned asset to a showcase slot. (legacy compatibility)")
    @app_commands.describe(asset_id="Asset inventory ID", slot="Showcase slot 1-3")
    async def luxury_equip(self, interaction: discord.Interaction, asset_id: int, slot: int):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        if slot < 1 or slot > SHOWCASE_SLOTS_MAX:
            return await interaction.response.send_message("Slot must be 1-3.", ephemeral=True)

        ok, msg = await self.hub_controller.assign_showcase(interaction, asset_id=asset_id, slot=slot)
        await interaction.response.send_message(("✅ " if ok else "❌ ") + msg, ephemeral=True)

    @luxury.command(name="unequip", description="Clear a showcase slot. (legacy compatibility)")
    @app_commands.describe(slot="Showcase slot 1-3")
    async def luxury_unequip(self, interaction: discord.Interaction, slot: int):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        if slot < 1 or slot > SHOWCASE_SLOTS_MAX:
            return await interaction.response.send_message("Slot must be 1-3.", ephemeral=True)

        ok, msg = await self.hub_controller.clear_showcase(interaction, slot=slot)
        await interaction.response.send_message(("✅ " if ok else "❌ ") + msg, ephemeral=True)

    @luxury.command(name="assets", description="View your luxury inventory and net worth. (legacy compatibility)")
    async def luxury_assets(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                assets = await list_owned_assets(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                total_value = await get_user_asset_value(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                net_worth = await get_user_net_worth(session, guild_id=interaction.guild.id, user_id=interaction.user.id)

        await interaction.followup.send(
            embed=build_assets_embed(user=interaction.user, assets=assets, total_value=total_value, net_worth=net_worth),
            ephemeral=True,
        )

    @loan.command(name="capacity", description="See your collateralized borrowing limit. (legacy compatibility)")
    async def loan_capacity(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                await evaluate_loan_state(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                snap = await get_loan_capacity(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
        await interaction.followup.send(embed=build_loan_capacity_embed(user=interaction.user, snap=snap), ephemeral=True)

    @loan.command(name="borrow", description="Borrow silver against your luxury assets. (legacy compatibility)")
    @app_commands.describe(amount="Amount of silver to borrow")
    async def loan_borrow(self, interaction: discord.Interaction, amount: int):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        if amount <= 0:
            return await interaction.response.send_message("Amount must be > 0.", ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                await evaluate_loan_state(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                cap = await get_loan_capacity(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                active = await get_active_loan(session, guild_id=interaction.guild.id, user_id=interaction.user.id)

        if active is not None:
            return await interaction.response.send_message("You already have an active/overdue/defaulted loan.", ephemeral=True)
        if amount > cap.available_to_borrow:
            return await interaction.response.send_message(
                f"Amount exceeds available capacity ({fmt_int(cap.available_to_borrow)}).",
                ephemeral=True,
            )

        total_repay = int(round(amount * (1 + BASE_INTEREST_RATE)))
        confirm_embed = build_loan_confirmation_embed(
            amount=amount,
            interest_rate=BASE_INTEREST_RATE,
            total_repay=total_repay,
            due_at=due_date_from_now(days=LOAN_DURATION_DAYS),
        )
        view = ConfirmCancelView(owner_id=interaction.user.id)
        await interaction.response.send_message(embed=confirm_embed, view=view, ephemeral=True)
        await view.wait()
        if not view.confirmed:
            return

        ok, msg = await self.hub_controller.borrow(interaction, amount)
        await interaction.followup.send(("✅ " if ok else "❌ ") + msg, ephemeral=True)

    @loan.command(name="repay", description="Repay your active loan. (legacy compatibility)")
    @app_commands.describe(amount="Amount to repay")
    async def loan_repay(self, interaction: discord.Interaction, amount: int):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        if amount <= 0:
            return await interaction.response.send_message("Amount must be > 0.", ephemeral=True)

        ok, msg = await self.hub_controller.repay(interaction, amount)
        await interaction.response.send_message(("✅ " if ok else "❌ ") + msg, ephemeral=True)

    @loan.command(name="status", description="View your current loan status. (legacy compatibility)")
    async def loan_status(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                loan = await evaluate_loan_state(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                if loan is None:
                    loan = await get_loan_snapshot(session, guild_id=interaction.guild.id, user_id=interaction.user.id)

        await interaction.followup.send(embed=build_loan_status_embed(user=interaction.user, loan=loan), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(LuxuryAssetsCog(bot))
