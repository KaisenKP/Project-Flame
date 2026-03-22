from __future__ import annotations

from datetime import datetime

BASE_XP_MULTIPLIER = 1.75
WEEKEND_XP_MULTIPLIER = 2.0


def utc_now(*, now: datetime | None = None) -> datetime:
    return now or datetime.utcnow()


def is_weekend(*, now: datetime | None = None) -> bool:
    return utc_now(now=now).weekday() >= 5


def apply_work_xp_multipliers(base_xp: int, *, now: datetime | None = None) -> int:
    xp = max(int(base_xp), 0) * BASE_XP_MULTIPLIER
    if is_weekend(now=now):
        xp *= WEEKEND_XP_MULTIPLIER
    return int(xp)


def current_work_xp_multiplier(*, now: datetime | None = None) -> float:
    multiplier = BASE_XP_MULTIPLIER
    if is_weekend(now=now):
        multiplier *= WEEKEND_XP_MULTIPLIER
    return multiplier


def weekend_id(*, now: datetime | None = None) -> str:
    year, week, _ = utc_now(now=now).isocalendar()
    return f"{year}-W{week:02d}"
