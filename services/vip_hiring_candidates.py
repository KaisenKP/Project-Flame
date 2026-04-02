from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cogs.Business.core import get_staff_grant_catalog
from db.models import BusinessManagerAssignmentRow, BusinessWorkerAssignmentRow


@dataclass(slots=True)
class CandidatePool:
    mode: str
    candidates: list[dict[str, Any]]


def _normalize_rarities(values: set[str]) -> set[str]:
    out: set[str] = set()
    for v in values:
        key = str(v or "").strip().lower()
        if not key:
            continue
        if key == "mythical":
            key = "mythic"
        out.add(key)
    return out


async def build_candidate_pool(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
    mode: str,
    allowed_rarities: set[str],
    disallow_duplicates: bool,
) -> CandidatePool:
    normalized_mode = str(mode).strip().lower()
    rarity_filter = _normalize_rarities(allowed_rarities)

    entries = get_staff_grant_catalog(
        staff_kind=normalized_mode,
        business_key=business_key,
        rarity_filter=rarity_filter or None,
    )

    used_names: set[str] = set()
    if disallow_duplicates:
        if normalized_mode == "worker":
            rows = (await session.scalars(select(BusinessWorkerAssignmentRow).where(
                BusinessWorkerAssignmentRow.guild_id == int(guild_id),
                BusinessWorkerAssignmentRow.user_id == int(user_id),
                BusinessWorkerAssignmentRow.business_key == str(business_key),
                BusinessWorkerAssignmentRow.is_active.is_(True),
            ))).all()
            used_names = {str(r.worker_name).strip().lower() for r in rows if r.worker_name}
        else:
            rows = (await session.scalars(select(BusinessManagerAssignmentRow).where(
                BusinessManagerAssignmentRow.guild_id == int(guild_id),
                BusinessManagerAssignmentRow.user_id == int(user_id),
                BusinessManagerAssignmentRow.business_key == str(business_key),
                BusinessManagerAssignmentRow.is_active.is_(True),
            ))).all()
            used_names = {str(r.manager_name).strip().lower() for r in rows if r.manager_name}

    pool: list[dict[str, Any]] = []
    for entry in entries:
        name = str(entry.display_name or "").strip()
        if not name:
            continue
        if disallow_duplicates and name.lower() in used_names:
            continue
        pool.append({
            "name": name,
            "rarity": str(entry.rarity),
            "worker_type": entry.worker_type,
            "flat_profit_bonus": int(entry.flat_profit_bonus or 0),
            "percent_profit_bonus_bp": int(entry.percent_profit_bonus_bp or 0),
            "runtime_bonus_hours": int(entry.runtime_bonus_hours or 0),
            "profit_bonus_bp": int(entry.profit_bonus_bp or 0),
            "auto_restart_charges": int(entry.auto_restart_charges or 0),
            "key": str(entry.key),
        })
    return CandidatePool(mode=normalized_mode, candidates=pool)
