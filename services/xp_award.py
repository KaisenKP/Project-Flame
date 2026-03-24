# services/xp_award.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ActivityDailyRow, XpRow
from services.xp import get_xp_progress

# Weekend logic should follow your server timezone (America/New_York)
try:
    from zoneinfo import ZoneInfo

    _TZ = ZoneInfo("America/New_York")
except Exception:
    _TZ = None


@dataclass(frozen=True)
class XpAwardResult:
    user_id: int
    guild_id: int
    amount: int
    old_level: int
    new_level: int
    new_xp_total: int


def xp_multiplier_for_now(*, now: datetime | None = None) -> int:
    dt = now or datetime.now(tz=_TZ) if _TZ is not None else datetime.now()
    # Saturday(5), Sunday(6)
    return 2 if dt.weekday() >= 5 else 1


async def get_or_create_xp_row(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
) -> XpRow:
    row = await session.scalar(
        select(XpRow).where(
            XpRow.guild_id == guild_id,
            XpRow.user_id == user_id,
        )
    )
    if row is not None:
        # Self-heal stale/corrupt cached level values from legacy migrations/manual edits.
        # Canonical truth is xp_total -> computed level.
        prog = get_xp_progress(int(row.xp_total or 0))
        computed_level = int(prog.level)
        if int(row.level_cached or 1) != computed_level:
            row.level_cached = computed_level
        return row

    row = XpRow(guild_id=guild_id, user_id=user_id, xp_total=0, level_cached=1)
    session.add(row)
    await session.flush()
    return row


async def get_or_create_activity_daily(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    day: date,
) -> ActivityDailyRow:
    row = await session.scalar(
        select(ActivityDailyRow).where(
            ActivityDailyRow.guild_id == guild_id,
            ActivityDailyRow.user_id == user_id,
            ActivityDailyRow.day == day,
        )
    )
    if row is not None:
        return row

    row = ActivityDailyRow(
        guild_id=guild_id,
        user_id=user_id,
        day=day,
        message_count=0,
        vc_seconds=0,
        activity_score=0,
    )
    session.add(row)
    await session.flush()
    return row


async def award_xp(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    amount: int,
    apply_weekend_multiplier: bool = True,
) -> Optional[XpAwardResult]:
    base = int(amount)
    if base <= 0:
        return None

    mult = xp_multiplier_for_now() if apply_weekend_multiplier else 1
    amt = base * mult
    if amt <= 0:
        return None

    xp_row = await get_or_create_xp_row(session, guild_id=guild_id, user_id=user_id)

    old_level = int(xp_row.level_cached)
    new_total = int(xp_row.xp_total) + amt

    prog = get_xp_progress(new_total)

    xp_row.xp_total = int(prog.xp_total)
    xp_row.level_cached = int(prog.level)

    return XpAwardResult(
        user_id=user_id,
        guild_id=guild_id,
        amount=amt,
        old_level=old_level,
        new_level=int(prog.level),
        new_xp_total=int(prog.xp_total),
    )
