from __future__ import annotations

from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import BusinessOwnershipRow, LuxuryLoanRow, UserAssetRow, WalletRow
from services.users import ensure_user_rows

from .catalog import ASSET_CATALOG
from .domain import LoanCapacitySnapshot, LoanSnapshot, LoanStatus, OwnedAssetView, SeizureResult
from .util import (
    BASE_INTEREST_RATE,
    COLLATERAL_RATIO,
    DEBT_RECOVERY_RATE_BP,
    LOAN_DURATION_DAYS,
    OVERDUE_GRACE_DAYS,
    PENALTY_INTEREST_RATE,
    due_date_from_now,
    now_utc,
)


async def get_or_create_wallet(session: AsyncSession, *, guild_id: int, user_id: int) -> WalletRow:
    wallet = await session.scalar(
        select(WalletRow).where(WalletRow.guild_id == int(guild_id), WalletRow.user_id == int(user_id))
    )
    if wallet is None:
        wallet = WalletRow(guild_id=int(guild_id), user_id=int(user_id), silver=0, diamonds=0)
        session.add(wallet)
        await session.flush()
    return wallet


async def list_owned_assets(session: AsyncSession, *, guild_id: int, user_id: int) -> list[OwnedAssetView]:
    rows = (
        await session.scalars(
            select(UserAssetRow)
            .where(UserAssetRow.guild_id == guild_id, UserAssetRow.user_id == user_id)
            .order_by(UserAssetRow.purchased_at.asc(), UserAssetRow.id.asc())
        )
    ).all()
    return [
        OwnedAssetView(
            id=r.id,
            asset_key=r.asset_key,
            purchased_at=r.purchased_at,
            purchase_price=r.purchase_price,
            showcase_slot=r.showcase_slot,
            is_showcased=r.is_showcased,
            is_seized=r.is_seized,
        )
        for r in rows
    ]


async def count_owned_asset_key(session: AsyncSession, *, guild_id: int, user_id: int, asset_key: str) -> int:
    q = await session.scalar(
        select(func.count(UserAssetRow.id)).where(
            UserAssetRow.guild_id == guild_id,
            UserAssetRow.user_id == user_id,
            UserAssetRow.asset_key == asset_key,
            UserAssetRow.is_seized.is_(False),
        )
    )
    return int(q or 0)


async def purchase_asset(session: AsyncSession, *, guild_id: int, user_id: int, asset_key: str) -> UserAssetRow:
    await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)
    definition = ASSET_CATALOG[asset_key]
    wallet = await get_or_create_wallet(session, guild_id=guild_id, user_id=user_id)
    if int(wallet.silver) < int(definition.price):
        raise ValueError("insufficient_silver")

    wallet.silver -= int(definition.price)
    wallet.silver_spent += int(definition.price)

    row = UserAssetRow(
        guild_id=guild_id,
        user_id=user_id,
        asset_key=asset_key,
        purchase_price=int(definition.price),
        is_showcased=False,
    )
    session.add(row)
    await session.flush()
    return row


async def set_showcase_slot(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    asset_id: int,
    slot: int,
) -> UserAssetRow:
    row = await session.scalar(
        select(UserAssetRow).where(
            UserAssetRow.id == asset_id,
            UserAssetRow.guild_id == guild_id,
            UserAssetRow.user_id == user_id,
            UserAssetRow.is_seized.is_(False),
        )
    )
    if row is None:
        raise ValueError("asset_not_found")

    occupied = await session.scalar(
        select(UserAssetRow).where(
            UserAssetRow.guild_id == guild_id,
            UserAssetRow.user_id == user_id,
            UserAssetRow.showcase_slot == slot,
            UserAssetRow.is_showcased.is_(True),
            UserAssetRow.id != row.id,
        )
    )
    if occupied is not None:
        occupied.showcase_slot = None
        occupied.is_showcased = False

    row.showcase_slot = int(slot)
    row.is_showcased = True
    await session.flush()
    return row


async def clear_showcase_slot(session: AsyncSession, *, guild_id: int, user_id: int, slot: int) -> bool:
    row = await session.scalar(
        select(UserAssetRow).where(
            UserAssetRow.guild_id == guild_id,
            UserAssetRow.user_id == user_id,
            UserAssetRow.showcase_slot == int(slot),
            UserAssetRow.is_showcased.is_(True),
        )
    )
    if row is None:
        return False
    row.showcase_slot = None
    row.is_showcased = False
    await session.flush()
    return True


async def get_total_asset_value(session: AsyncSession, *, guild_id: int, user_id: int) -> int:
    luxury_rows = (
        await session.scalars(
            select(UserAssetRow.asset_key).where(
                UserAssetRow.guild_id == guild_id,
                UserAssetRow.user_id == user_id,
                UserAssetRow.is_seized.is_(False),
            )
        )
    ).all()
    luxury_value = int(sum(ASSET_CATALOG.get(k).value for k in luxury_rows if k in ASSET_CATALOG))
    business_value = int(
        await session.scalar(
            select(func.coalesce(func.sum(BusinessOwnershipRow.total_spent), 0)).where(
                BusinessOwnershipRow.guild_id == guild_id,
                BusinessOwnershipRow.user_id == user_id,
            )
        )
        or 0
    )
    return luxury_value + business_value


async def get_user_asset_value(session: AsyncSession, *, guild_id: int, user_id: int) -> int:
    return await get_total_asset_value(session, guild_id=guild_id, user_id=user_id)


async def get_user_net_worth(session: AsyncSession, *, guild_id: int, user_id: int) -> int:
    wallet = await get_or_create_wallet(session, guild_id=guild_id, user_id=user_id)
    return int(wallet.silver) + int(await get_total_asset_value(session, guild_id=guild_id, user_id=user_id))


async def get_active_loan(session: AsyncSession, *, guild_id: int, user_id: int) -> LuxuryLoanRow | None:
    return await session.scalar(
        select(LuxuryLoanRow).where(
            LuxuryLoanRow.guild_id == guild_id,
            LuxuryLoanRow.user_id == user_id,
            LuxuryLoanRow.status.in_([LoanStatus.ACTIVE.value, LoanStatus.OVERDUE.value, LoanStatus.DEFAULTED.value]),
        )
    )


def _loan_to_snapshot(loan: LuxuryLoanRow) -> LoanSnapshot:
    return LoanSnapshot(
        loan_id=loan.id,
        principal_amount=loan.principal_amount,
        interest_rate=float(loan.interest_rate),
        total_due=loan.total_due,
        remaining_balance=loan.remaining_balance,
        issued_at=loan.issued_at,
        due_at=loan.due_at,
        status=LoanStatus(loan.status),
        last_interest_applied_at=loan.last_interest_applied_at,
        debt_recovery_mode=loan.debt_recovery_mode,
        recovery_rate_bp=loan.recovery_rate_bp,
    )


async def get_loan_capacity(session: AsyncSession, *, guild_id: int, user_id: int) -> LoanCapacitySnapshot:
    total_assets = await get_total_asset_value(session, guild_id=guild_id, user_id=user_id)
    borrow_limit = int(total_assets * COLLATERAL_RATIO)
    active_loan = await get_active_loan(session, guild_id=guild_id, user_id=user_id)
    active_balance = int(active_loan.remaining_balance) if active_loan is not None else 0
    return LoanCapacitySnapshot(
        total_asset_value=total_assets,
        collateral_ratio=COLLATERAL_RATIO,
        borrow_limit=borrow_limit,
        active_loan_balance=active_balance,
        available_to_borrow=max(0, borrow_limit - active_balance),
    )


async def issue_loan(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    principal_amount: int,
    duration_days: int = LOAN_DURATION_DAYS,
) -> LoanSnapshot:
    await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)
    capacity = await get_loan_capacity(session, guild_id=guild_id, user_id=user_id)
    active = await get_active_loan(session, guild_id=guild_id, user_id=user_id)

    if active is not None:
        raise ValueError("active_loan_exists")
    if principal_amount <= 0:
        raise ValueError("invalid_amount")
    if principal_amount > capacity.available_to_borrow:
        raise ValueError("exceeds_capacity")

    total_due = int(round(principal_amount * (1 + BASE_INTEREST_RATE)))
    due_at = due_date_from_now(days=duration_days)
    now = now_utc()

    loan = LuxuryLoanRow(
        guild_id=guild_id,
        user_id=user_id,
        principal_amount=int(principal_amount),
        interest_rate=float(BASE_INTEREST_RATE),
        total_due=total_due,
        remaining_balance=total_due,
        issued_at=now,
        due_at=due_at,
        status=LoanStatus.ACTIVE.value,
        last_interest_applied_at=now,
        debt_recovery_mode=False,
        recovery_rate_bp=DEBT_RECOVERY_RATE_BP,
    )
    session.add(loan)

    wallet = await get_or_create_wallet(session, guild_id=guild_id, user_id=user_id)
    wallet.silver += int(principal_amount)
    wallet.silver_earned += int(principal_amount)

    await session.flush()
    return _loan_to_snapshot(loan)


async def repay_loan(session: AsyncSession, *, guild_id: int, user_id: int, amount: int) -> LoanSnapshot:
    loan = await get_active_loan(session, guild_id=guild_id, user_id=user_id)
    if loan is None:
        raise ValueError("no_active_loan")
    if amount <= 0:
        raise ValueError("invalid_amount")

    wallet = await get_or_create_wallet(session, guild_id=guild_id, user_id=user_id)
    if wallet.silver < amount:
        raise ValueError("insufficient_silver")

    pay = min(int(amount), int(loan.remaining_balance))
    wallet.silver -= pay
    wallet.silver_spent += pay
    loan.remaining_balance -= pay

    if int(loan.remaining_balance) <= 0:
        loan.remaining_balance = 0
        loan.status = LoanStatus.REPAID.value
        loan.debt_recovery_mode = False

    await session.flush()
    return _loan_to_snapshot(loan)


async def evaluate_loan_state(session: AsyncSession, *, guild_id: int, user_id: int) -> LoanSnapshot | None:
    loan = await get_active_loan(session, guild_id=guild_id, user_id=user_id)
    if loan is None:
        return None

    now = now_utc()
    if loan.status == LoanStatus.ACTIVE.value and now > loan.due_at:
        loan.status = LoanStatus.OVERDUE.value
        penalty = int(round(loan.remaining_balance * PENALTY_INTEREST_RATE))
        loan.remaining_balance += penalty
        loan.total_due += penalty
        loan.last_interest_applied_at = now

    if loan.status in (LoanStatus.OVERDUE.value, LoanStatus.ACTIVE.value):
        if now > loan.due_at:
            default_cutoff = loan.due_at + timedelta(days=OVERDUE_GRACE_DAYS)
            if now >= default_cutoff:
                loan.status = LoanStatus.DEFAULTED.value
                seizure = await seize_assets_for_default(session, guild_id=guild_id, user_id=user_id, loan=loan)
                if loan.remaining_balance > 0 and not seizure.seized_asset_ids:
                    loan.debt_recovery_mode = True

    await session.flush()
    return _loan_to_snapshot(loan)


async def seize_assets_for_default(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    loan: LuxuryLoanRow,
) -> SeizureResult:
    rows = (
        await session.scalars(
            select(UserAssetRow)
            .where(
                UserAssetRow.guild_id == guild_id,
                UserAssetRow.user_id == user_id,
                UserAssetRow.is_seized.is_(False),
            )
            .order_by(UserAssetRow.purchased_at.desc(), UserAssetRow.id.desc())
        )
    ).all()

    ranked = sorted(rows, key=lambda r: ASSET_CATALOG.get(r.asset_key).value if r.asset_key in ASSET_CATALOG else 0, reverse=True)

    seized_ids: list[int] = []
    seized_value = 0
    for row in ranked:
        if loan.remaining_balance <= 0:
            break
        value = ASSET_CATALOG[row.asset_key].value if row.asset_key in ASSET_CATALOG else 0
        row.is_seized = True
        row.seized_at = now_utc()
        row.is_showcased = False
        row.showcase_slot = None
        seized_ids.append(row.id)
        seized_value += int(value)
        loan.remaining_balance = max(0, int(loan.remaining_balance) - int(value))

    if loan.remaining_balance <= 0:
        loan.status = LoanStatus.REPAID.value
        loan.debt_recovery_mode = False

    await session.flush()
    return SeizureResult(
        seized_asset_ids=seized_ids,
        seized_value_total=seized_value,
        remaining_balance_after_seizure=int(loan.remaining_balance),
    )


async def apply_income_with_debt_recovery(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    gross_income: int,
) -> tuple[int, int]:
    wallet = await get_or_create_wallet(session, guild_id=guild_id, user_id=user_id)
    loan = await get_active_loan(session, guild_id=guild_id, user_id=user_id)

    if loan is None or not loan.debt_recovery_mode or loan.status not in {
        LoanStatus.DEFAULTED.value,
        LoanStatus.OVERDUE.value,
        LoanStatus.ACTIVE.value,
    }:
        wallet.silver += int(gross_income)
        wallet.silver_earned += int(gross_income)
        await session.flush()
        return int(gross_income), 0

    garnish = int(gross_income * (loan.recovery_rate_bp / 10_000.0))
    net = int(gross_income) - garnish
    wallet.silver += net
    wallet.silver_earned += net

    loan.remaining_balance = max(0, int(loan.remaining_balance) - garnish)
    if loan.remaining_balance == 0:
        loan.status = LoanStatus.REPAID.value
        loan.debt_recovery_mode = False

    await session.flush()
    return net, garnish


async def get_loan_snapshot(session: AsyncSession, *, guild_id: int, user_id: int) -> LoanSnapshot | None:
    loan = await get_active_loan(session, guild_id=guild_id, user_id=user_id)
    if loan is None:
        return None
    return _loan_to_snapshot(loan)
