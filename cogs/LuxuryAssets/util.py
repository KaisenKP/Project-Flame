from __future__ import annotations

from datetime import datetime, timedelta, timezone

COLLATERAL_RATIO = 0.40
BASE_INTEREST_RATE = 0.12
PENALTY_INTEREST_RATE = 0.06
LOAN_DURATION_DAYS = 7
OVERDUE_GRACE_DAYS = 3
DEBT_RECOVERY_RATE_BP = 3_000  # 30.00%
SHOWCASE_SLOTS_MAX = 3


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def fmt_int(value: int) -> str:
    return f"{int(value):,}"


def fmt_percent(rate: float) -> str:
    return f"{rate * 100:.2f}%"


def due_date_from_now(days: int = LOAN_DURATION_DAYS) -> datetime:
    return now_utc() + timedelta(days=int(days))


def clamp_positive(value: int) -> int:
    return max(0, int(value))
