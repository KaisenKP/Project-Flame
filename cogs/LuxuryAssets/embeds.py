from __future__ import annotations

from datetime import datetime

import discord

from .catalog import ASSET_CATALOG, iter_active_assets
from .domain import LoanCapacitySnapshot, LoanSnapshot, LuxuryOverviewSnapshot, OwnedAssetView
from .util import fmt_int, fmt_percent, safe_asset_label


def _resolve_asset_display(asset_key: str, fallback_id: int | None = None) -> tuple[str, str, int, str]:
    definition = ASSET_CATALOG.get(asset_key)
    if definition is None:
        return "🏷️", safe_asset_label(key=asset_key, fallback_id=fallback_id), 0, "Legacy catalog key"
    return definition.emoji, definition.name, int(definition.value), definition.category.value.title()


def build_hub_overview_embed(
    *,
    user: discord.abc.User,
    snap: LuxuryOverviewSnapshot,
    showcased_assets: list[OwnedAssetView],
) -> discord.Embed:
    em = discord.Embed(
        title="💎 Luxury Hub",
        description="Welcome to your premium luxury command center.",
        color=discord.Color.gold(),
    )
    em.set_author(name=str(user))

    em.add_field(name="Wallet", value=f"💰 **{fmt_int(snap.wallet_silver)} Silver**", inline=True)
    em.add_field(name="Luxury Asset Value", value=f"📈 **{fmt_int(snap.total_asset_value)}**", inline=True)
    em.add_field(name="Net Worth", value=f"👑 **{fmt_int(snap.net_worth)}**", inline=True)

    em.add_field(name="Owned Assets", value=str(snap.owned_assets_count), inline=True)
    em.add_field(name="Showcased", value=f"{snap.showcased_assets_count}/3", inline=True)

    if snap.active_loan is None:
        em.add_field(name="Loan", value="No active loan", inline=True)
    else:
        em.add_field(
            name="Loan",
            value=(
                f"{snap.active_loan.status.value.upper()}\n"
                f"Remaining: **{fmt_int(snap.active_loan.remaining_balance)}**"
            ),
            inline=True,
        )

    showcase_lines: list[str] = []
    by_slot = {a.showcase_slot: a for a in showcased_assets if a.showcase_slot in (1, 2, 3)}
    for slot in (1, 2, 3):
        row = by_slot.get(slot)
        if row is None:
            showcase_lines.append(f"`Slot {slot}` (empty)")
            continue
        emoji, name, _, _ = _resolve_asset_display(row.asset_key, fallback_id=row.id)
        showcase_lines.append(f"`Slot {slot}` {emoji} **{name}** (`#{row.id}`)")
    em.add_field(name="Showcase Preview", value="\n".join(showcase_lines), inline=False)

    em.set_footer(text="Use the buttons below to open Shop, Collection, Showcase, and Bank.")
    return em


def build_shop_embed(
    *,
    user: discord.abc.User,
    balance: int,
    selected_key: str | None = None,
    owned_count: int = 0,
) -> discord.Embed:
    em = discord.Embed(
        title="🛍️ Luxury Shop",
        description="Browse elite assets and purchase with confidence.",
        color=discord.Color.purple(),
    )
    em.set_author(name=str(user))
    em.add_field(name="Wallet", value=f"💰 {fmt_int(balance)} Silver", inline=True)

    active_assets = iter_active_assets()
    if not active_assets:
        em.add_field(name="Catalog", value="No active assets in catalog.", inline=False)
        return em

    if selected_key is None:
        selected_key = active_assets[0].asset_key

    selected = ASSET_CATALOG.get(selected_key)
    if selected is None:
        em.add_field(name="Selected Asset", value="Unknown catalog key. Please pick another asset.", inline=False)
    else:
        affordable = "✅ Affordable" if balance >= selected.price else "❌ Not enough silver"
        em.add_field(
            name="Selected",
            value=(
                f"{selected.emoji} **{selected.name}**\n"
                f"Category: **{selected.category.value.title()}**\n"
                f"Price: **{fmt_int(selected.price)}**\n"
                f"Value: **{fmt_int(selected.value)}**\n"
                f"Owned: **{owned_count}x**\n"
                f"Status: {affordable}"
            ),
            inline=False,
        )
        em.add_field(name="Flavor", value=selected.flavor_text, inline=False)

    top_lines = []
    for idx, asset in enumerate(active_assets[:8], start=1):
        top_lines.append(f"`{idx:>2}` {asset.emoji} **{asset.name}** · {fmt_int(asset.price)}")
    em.add_field(name="Featured", value="\n".join(top_lines), inline=False)
    return em


def build_collection_embed(*, user: discord.abc.User, assets: list[OwnedAssetView], page: int = 0, page_size: int = 10) -> discord.Embed:
    em = discord.Embed(title="📦 Collection", color=discord.Color.blurple())
    em.set_author(name=str(user))

    if not assets:
        em.description = "You do not own any luxury assets yet. Visit the **Shop** section to get started."
        return em

    start = page * page_size
    end = start + page_size
    chunk = assets[start:end]

    lines: list[str] = []
    for row in chunk:
        emoji, name, value, _ = _resolve_asset_display(row.asset_key, fallback_id=row.id)
        showcased = f"Slot {row.showcase_slot}" if row.is_showcased and row.showcase_slot in (1, 2, 3) else "—"
        seized = "🚫 Seized" if row.is_seized else "✅ Active"
        fallback_value = value if value > 0 else max(0, int(row.purchase_price))
        lines.append(
            f"`#{row.id}` {emoji} **{name}** · Value: **{fmt_int(fallback_value)}** · Showcase: **{showcased}** · {seized}"
        )

    em.add_field(name="Owned Assets", value="\n".join(lines), inline=False)
    pages = (len(assets) + page_size - 1) // page_size
    em.set_footer(text=f"Page {page + 1}/{max(1, pages)} · Select an asset below for actions.")
    return em


def build_asset_detail_embed(*, user: discord.abc.User, row: OwnedAssetView) -> discord.Embed:
    emoji, name, value, category = _resolve_asset_display(row.asset_key, fallback_id=row.id)
    em = discord.Embed(title="🔎 Asset Detail", color=discord.Color.dark_gold())
    em.set_author(name=str(user))
    em.add_field(name="Asset", value=f"{emoji} **{name}** (`#{row.id}`)", inline=False)
    em.add_field(name="Category", value=category, inline=True)
    em.add_field(name="Current Value", value=fmt_int(value if value > 0 else row.purchase_price), inline=True)
    em.add_field(name="Purchase Price", value=fmt_int(row.purchase_price), inline=True)
    em.add_field(name="Status", value="🚫 Seized" if row.is_seized else "✅ Active", inline=True)
    showcased = f"Slot {row.showcase_slot}" if row.is_showcased and row.showcase_slot in (1, 2, 3) else "Not showcased"
    em.add_field(name="Showcase", value=showcased, inline=True)
    em.add_field(name="Purchased", value=discord.utils.format_dt(row.purchased_at, style="F"), inline=False)
    return em


def build_showcase_embed(*, user: discord.abc.User, assets: list[OwnedAssetView]) -> discord.Embed:
    em = discord.Embed(title="✨ Showcase", color=discord.Color.fuchsia())
    em.set_author(name=str(user))

    by_slot = {a.showcase_slot: a for a in assets if a.is_showcased and a.showcase_slot in (1, 2, 3)}
    for slot in (1, 2, 3):
        row = by_slot.get(slot)
        if not row:
            em.add_field(name=f"Slot {slot}", value="(empty)", inline=False)
            continue
        emoji, name, _, _ = _resolve_asset_display(row.asset_key, fallback_id=row.id)
        seized = "\n🚫 Seized" if row.is_seized else ""
        em.add_field(name=f"Slot {slot}", value=f"{emoji} **{name}** (`#{row.id}`){seized}", inline=False)
    em.set_footer(text="Assign or clear slots using the controls below.")
    return em


def build_bank_embed(*, user: discord.abc.User, snap: LoanCapacitySnapshot, loan: LoanSnapshot | None) -> discord.Embed:
    em = discord.Embed(title="🏦 Luxury Bank", color=discord.Color.teal())
    em.set_author(name=str(user))
    em.add_field(name="Collateral Ratio", value=fmt_percent(snap.collateral_ratio), inline=True)
    em.add_field(name="Total Asset Value", value=f"📈 {fmt_int(snap.total_asset_value)}", inline=True)
    em.add_field(name="Borrow Limit", value=f"💳 {fmt_int(snap.borrow_limit)}", inline=True)
    em.add_field(name="Active Balance", value=f"🧾 {fmt_int(snap.active_loan_balance)}", inline=True)
    em.add_field(name="Available To Borrow", value=f"✅ {fmt_int(snap.available_to_borrow)}", inline=True)

    if loan is None:
        em.add_field(name="Loan Status", value="No active loan.", inline=False)
    else:
        em.add_field(
            name="Loan Status",
            value=(
                f"ID: **{loan.loan_id}** · **{loan.status.value.upper()}**\n"
                f"Principal: **{fmt_int(loan.principal_amount)}**\n"
                f"Remaining: **{fmt_int(loan.remaining_balance)}**\n"
                f"Due: {discord.utils.format_dt(loan.due_at, style='R')}"
            ),
            inline=False,
        )
        if loan.debt_recovery_mode:
            em.add_field(name="Debt Recovery", value=f"Enabled ({loan.recovery_rate_bp / 100:.2f}% income redirect)", inline=False)

    em.set_footer(text="Borrowing and repayment only apply when confirmed.")
    return em


def build_store_embed(*, user: discord.abc.User, balance: int) -> discord.Embed:
    return build_shop_embed(user=user, balance=balance)


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
    em = build_collection_embed(user=user, assets=assets, page=0, page_size=20)
    em.title = "🏆 Luxury Inventory"
    em.add_field(name="Total Asset Value", value=f"📈 {fmt_int(total_value)}", inline=True)
    em.add_field(name="Net Worth", value=f"👑 {fmt_int(net_worth)}", inline=True)
    return em


def build_loan_capacity_embed(*, user: discord.abc.User, snap: LoanCapacitySnapshot) -> discord.Embed:
    return build_bank_embed(user=user, snap=snap, loan=None)


def build_loan_confirmation_embed(*, amount: int, interest_rate: float, total_repay: int, due_at: datetime) -> discord.Embed:
    em = discord.Embed(title="Confirm Loan", color=discord.Color.orange())
    em.add_field(name="Loan Amount", value=f"💰 {fmt_int(amount)}", inline=True)
    em.add_field(name="Interest Rate", value=fmt_percent(interest_rate), inline=True)
    em.add_field(name="Total Repayment", value=f"🧾 {fmt_int(total_repay)}", inline=False)
    em.add_field(name="Due Date", value=discord.utils.format_dt(due_at, style="F"), inline=False)
    em.set_footer(text="Funds are only issued after you confirm.")
    return em


def build_loan_status_embed(*, user: discord.abc.User, loan: LoanSnapshot | None) -> discord.Embed:
    if loan is None:
        em = discord.Embed(title="📑 Loan Status", description="No active/defaulted loan found.", color=discord.Color.dark_teal())
        em.set_author(name=str(user))
        return em
    em = discord.Embed(title="📑 Loan Status", color=discord.Color.dark_teal())
    em.set_author(name=str(user))
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


def build_success_embed(*, title: str, description: str) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=discord.Color.green())


def build_error_embed(*, title: str, description: str) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=discord.Color.red())
