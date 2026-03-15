from __future__ import annotations

import asyncio
import logging

import discord
from sqlalchemy import select
from discord import app_commands
from discord.ext import commands

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
    count_owned_asset_key,
    evaluate_loan_state,
    get_active_loan,
    get_loan_capacity,
    get_loan_snapshot,
    get_or_create_wallet,
    get_user_asset_value,
    get_user_net_worth,
    issue_loan,
    list_owned_assets,
    purchase_asset,
    repay_loan,
    set_showcase_slot,
    clear_showcase_slot,
)
from .ui import ConfirmCancelView
from .util import BASE_INTEREST_RATE, LOAN_DURATION_DAYS, SHOWCASE_SLOTS_MAX, fmt_int

log = logging.getLogger(__name__)


class LuxuryAssetsCog(commands.Cog):
    luxury = app_commands.Group(name="luxury", description="Luxury assets and showcase.")
    loan = app_commands.Group(name="loan", description="Asset-backed loans and repayment.")

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self._loan_task: asyncio.Task | None = None

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

    @luxury.command(name="store", description="Browse luxury assets and prices.")
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

    @luxury.command(name="buy", description="Buy a luxury asset.")
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

        async with self.sessionmaker() as session:
            async with session.begin():
                try:
                    row = await purchase_asset(
                        session,
                        guild_id=interaction.guild.id,
                        user_id=interaction.user.id,
                        asset_key=asset_key,
                    )
                except ValueError:
                    await interaction.followup.send("Not enough silver for that purchase.", ephemeral=True)
                    return

        asset = ASSET_CATALOG[asset_key]
        done = discord.Embed(
            title="Purchase Complete",
            description=f"✅ You purchased {asset.emoji} **{asset.name}** (`#{row.id}`).",
            color=discord.Color.green(),
        )
        done.add_field(name="Price", value=f"{fmt_int(asset.price)} Silver")
        await interaction.followup.send(embed=done, ephemeral=True)

    @luxury.command(name="showcase", description="View your 3 luxury showcase slots.")
    async def luxury_showcase(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                assets = await list_owned_assets(session, guild_id=interaction.guild.id, user_id=interaction.user.id)

        await interaction.followup.send(embed=build_showcase_embed(user=interaction.user, assets=assets), ephemeral=True)

    @luxury.command(name="equip", description="Equip an owned asset to a showcase slot.")
    @app_commands.describe(asset_id="Asset inventory ID", slot="Showcase slot 1-3")
    async def luxury_equip(self, interaction: discord.Interaction, asset_id: int, slot: int):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        if slot < 1 or slot > SHOWCASE_SLOTS_MAX:
            return await interaction.response.send_message("Slot must be 1-3.", ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                try:
                    row = await set_showcase_slot(
                        session,
                        guild_id=interaction.guild.id,
                        user_id=interaction.user.id,
                        asset_id=asset_id,
                        slot=slot,
                    )
                except ValueError:
                    return await interaction.response.send_message("Asset not found or seized.", ephemeral=True)

        asset = ASSET_CATALOG.get(row.asset_key)
        await interaction.response.send_message(
            f"✅ Equipped {asset.emoji if asset else '🏆'} **{asset.name if asset else row.asset_key}** to slot **{slot}**.",
            ephemeral=True,
        )

    @luxury.command(name="unequip", description="Clear a showcase slot.")
    @app_commands.describe(slot="Showcase slot 1-3")
    async def luxury_unequip(self, interaction: discord.Interaction, slot: int):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        if slot < 1 or slot > SHOWCASE_SLOTS_MAX:
            return await interaction.response.send_message("Slot must be 1-3.", ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                changed = await clear_showcase_slot(
                    session,
                    guild_id=interaction.guild.id,
                    user_id=interaction.user.id,
                    slot=slot,
                )

        await interaction.response.send_message(
            "✅ Slot cleared." if changed else "That slot is already empty.", ephemeral=True
        )

    @luxury.command(name="assets", description="View your luxury inventory and net worth.")
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

    @loan.command(name="capacity", description="See your collateralized borrowing limit.")
    async def loan_capacity(self, interaction: discord.Interaction):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                await evaluate_loan_state(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
                snap = await get_loan_capacity(session, guild_id=interaction.guild.id, user_id=interaction.user.id)
        await interaction.followup.send(embed=build_loan_capacity_embed(user=interaction.user, snap=snap), ephemeral=True)

    @loan.command(name="borrow", description="Borrow silver against your luxury assets.")
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
        from .util import due_date_from_now

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

        async with self.sessionmaker() as session:
            async with session.begin():
                try:
                    loan = await issue_loan(
                        session,
                        guild_id=interaction.guild.id,
                        user_id=interaction.user.id,
                        principal_amount=int(amount),
                        duration_days=LOAN_DURATION_DAYS,
                    )
                except ValueError as e:
                    return await interaction.followup.send(f"Loan failed: {e}", ephemeral=True)

        done = build_loan_status_embed(user=interaction.user, loan=loan)
        done.title = "✅ Loan Issued"
        await interaction.followup.send(embed=done, ephemeral=True)

    @loan.command(name="repay", description="Repay your active loan.")
    @app_commands.describe(amount="Amount to repay")
    async def loan_repay(self, interaction: discord.Interaction, amount: int):
        if interaction.guild is None:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        if amount <= 0:
            return await interaction.response.send_message("Amount must be > 0.", ephemeral=True)

        async with self.sessionmaker() as session:
            async with session.begin():
                try:
                    loan = await repay_loan(
                        session,
                        guild_id=interaction.guild.id,
                        user_id=interaction.user.id,
                        amount=int(amount),
                    )
                except ValueError as e:
                    return await interaction.response.send_message(f"Repayment failed: {e}", ephemeral=True)

        em = build_loan_status_embed(user=interaction.user, loan=loan)
        em.title = "💸 Repayment Applied"
        await interaction.response.send_message(embed=em, ephemeral=True)

    @loan.command(name="status", description="View your current loan status.")
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
