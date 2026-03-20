from __future__ import annotations

from datetime import datetime

import discord

from .catalog import ASSET_CATALOG, iter_active_assets
from .domain import LoanCapacitySnapshot, LoanSnapshot, OwnedAssetView
from .util import fmt_int, fmt_percent


def build_store_embed(*, user: discord.abc.User, balance: int) -> discord.Embed:
    em = discord.Embed(
        title="💎 Luxury Asset Store",
        description="Buy prestige assets to boost net worth and unlock collateral-backed loans.",
        color=discord.Color.gold(),
    )
    em.set_author(name=str(user))
    em.add_field(name="Wallet", value=f"💰 **{fmt_int(balance)} Silver**", inline=False)

    lines: list[str] = []
    for asset in iter_active_assets():
        lines.append(
            f"`{asset.asset_key}` {asset.emoji} **{asset.name}** · {asset.category.value.title()}\n"
            f"Price: **{fmt_int(asset.price)}** | Value: **{fmt_int(asset.value)}**"
        )
    em.add_field(name="Catalog", value="\n\n".join(lines[:12]) or "No active assets.", inline=False)
    if len(lines) > 12:
        em.set_footer(text=f"Showing first 12 of {len(lines)} assets.")
    return em


def build_buy_confirmation_embed(*, user: discord.abc.User, asset_key: str, balance: int) -> discord.Embed:
    asset = ASSET_CATALOG[asset_key]
    em = discord.Embed(title="Confirm Purchase", color=discord.Color.orange())
    em.description = f"Buy {asset.emoji} **{asset.name}**?"
    em.set_author(name=str(user))
    em.add_field(name="Price", value=f"💰 {fmt_int(asset.price)} Silver")
    em.add_field(name="Asset Value", value=f"📈 {fmt_int(asset.value)}")
    em.add_field(name="Wallet After", value=f"💼 {fmt_int(max(0, balance - asset.price))}", inline=False)
    em.add_field(name="Flavor", value=asset.flavor_text, inline=False)
    em.set_footer(text="Silver is only deducted when you press Confirm.")
    return em


def build_assets_embed(*, user: discord.abc.User, assets: list[OwnedAssetView], total_value: int, net_worth: int) -> discord.Embed:
    em = discord.Embed(title="🏆 Luxury Inventory", color=discord.Color.blurple())
    em.set_author(name=str(user))

    if not assets:
        em.description = "You don't own any luxury assets yet. Use `/luxury store` and `/luxury buy`."
        return em

    lines: list[str] = []
    for a in assets:
        d = ASSET_CATALOG.get(a.asset_key)
        if d is None:
            continue
        showcase = f" | Slot {a.showcase_slot}" if a.is_showcased and a.showcase_slot else ""
        seized = " | 🚫 Seized" if a.is_seized else ""
        lines.append(f"`#{a.id}` {d.emoji} **{d.name}** ({fmt_int(d.value)}){showcase}{seized}")

    em.add_field(name="Owned Assets", value="\n".join(lines[:20]), inline=False)
    em.add_field(name="Total Asset Value", value=f"📈 {fmt_int(total_value)}", inline=True)
    em.add_field(name="Net Worth", value=f"👑 {fmt_int(net_worth)}", inline=True)
    return em


def build_showcase_embed(*, user: discord.abc.User, assets: list[OwnedAssetView]) -> discord.Embed:
    em = discord.Embed(title="✨ Showcase Slots", color=discord.Color.purple())
    em.set_author(name=str(user))

    by_slot = {a.showcase_slot: a for a in assets if a.is_showcased and a.showcase_slot}
    for slot in (1, 2, 3):
        a = by_slot.get(slot)
        if not a:
            em.add_field(name=f"Slot {slot}", value="(empty)", inline=False)
            continue
        d = ASSET_CATALOG.get(a.asset_key)
        if not d:
            em.add_field(name=f"Slot {slot}", value="(unknown asset)", inline=False)
            continue
        em.add_field(name=f"Slot {slot}", value=f"{d.emoji} **{d.name}** (`#{a.id}`)", inline=False)
    em.set_footer(text="Use /luxury equip and /luxury unequip to manage slots.")
    return em


def build_loan_capacity_embed(*, user: discord.abc.User, snap: LoanCapacitySnapshot) -> discord.Embed:
    em = discord.Embed(title="🏦 Loan Capacity", color=discord.Color.teal())
    em.set_author(name=str(user))
    em.add_field(name="Collateral Ratio", value=fmt_percent(snap.collateral_ratio), inline=True)
    em.add_field(name="Total Asset Value", value=f"📈 {fmt_int(snap.total_asset_value)}", inline=True)
    em.add_field(name="Borrow Limit", value=f"💳 {fmt_int(snap.borrow_limit)}", inline=True)
    em.add_field(name="Active Balance", value=f"🧾 {fmt_int(snap.active_loan_balance)}", inline=True)
    em.add_field(name="Available", value=f"✅ {fmt_int(snap.available_to_borrow)}", inline=True)
    em.set_footer(text="Business ownership value now counts toward asset-backed borrowing.")
    return em


def build_loan_confirmation_embed(*, amount: int, interest_rate: float, total_repay: int, due_at: datetime) -> discord.Embed:
    em = discord.Embed(title="Confirm Loan", color=discord.Color.orange())
    em.add_field(name="Loan Amount", value=f"💰 {fmt_int(amount)}", inline=True)
    em.add_field(name="Interest Rate", value=fmt_percent(interest_rate), inline=True)
    em.add_field(name="Total Repayment", value=f"🧾 {fmt_int(total_repay)}", inline=False)
    em.add_field(name="Due Date", value=discord.utils.format_dt(due_at, style="F"), inline=False)
    em.set_footer(text="Funds are only issued after you confirm.")
    return em


def build_loan_status_embed(*, user: discord.abc.User, loan: LoanSnapshot | None) -> discord.Embed:
    em = discord.Embed(title="📑 Loan Status", color=discord.Color.dark_teal())
    em.set_author(name=str(user))
    if loan is None:
        em.description = "No active/defaulted loan found."
        return em

    em.add_field(name="Loan ID", value=str(loan.loan_id), inline=True)
    em.add_field(name="Status", value=loan.status.value.upper(), inline=True)
    em.add_field(name="Principal", value=fmt_int(loan.principal_amount), inline=True)
    em.add_field(name="Interest", value=fmt_percent(loan.interest_rate), inline=True)
    em.add_field(name="Remaining", value=f"🧾 {fmt_int(loan.remaining_balance)}", inline=True)
    em.add_field(name="Total Due", value=fmt_int(loan.total_due), inline=True)
    em.add_field(name="Issued", value=discord.utils.format_dt(loan.issued_at, style="R"), inline=True)
    em.add_field(name="Due", value=discord.utils.format_dt(loan.due_at, style="R"), inline=True)
    if loan.debt_recovery_mode:
        em.add_field(name="Debt Recovery", value=f"Enabled ({loan.recovery_rate_bp/100:.2f}% income redirect)", inline=False)
    return em
