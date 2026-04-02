from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import uuid

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import VipHiringJobRow


RUNNING_STATUSES = {"queued", "running"}
FINAL_STATUSES = {"completed", "partially_completed", "interrupted", "failed", "cancelled"}


@dataclass(slots=True)
class CreateVipHiringJobParams:
    guild_id: int
    user_id: int
    started_by_user_id: int
    business_key: str
    mode: str
    requested_count: int
    filters_json: dict
    batch_settings_json: dict


async def find_active_job(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    business_key: str,
    mode: str,
) -> VipHiringJobRow | None:
    q: Select[tuple[VipHiringJobRow]] = select(VipHiringJobRow).where(
        VipHiringJobRow.guild_id == int(guild_id),
        VipHiringJobRow.user_id == int(user_id),
        VipHiringJobRow.business_key == str(business_key),
        VipHiringJobRow.mode == str(mode),
        VipHiringJobRow.status.in_(RUNNING_STATUSES),
    ).order_by(VipHiringJobRow.id.desc())
    return await session.scalar(q)


async def create_job(session: AsyncSession, *, params: CreateVipHiringJobParams) -> VipHiringJobRow:
    row = VipHiringJobRow(
        job_id=f"vh-{uuid.uuid4().hex[:16]}",
        guild_id=int(params.guild_id),
        user_id=int(params.user_id),
        started_by_user_id=int(params.started_by_user_id),
        business_key=str(params.business_key),
        mode=str(params.mode),
        status="queued",
        requested_count=max(int(params.requested_count), 0),
        processed_count=0,
        success_count=0,
        skipped_count=0,
        failed_count=0,
        duplicate_blocked_count=0,
        filters_json=dict(params.filters_json or {}),
        batch_settings_json=dict(params.batch_settings_json or {}),
        started_at=datetime.now(timezone.utc),
        last_heartbeat_at=datetime.now(timezone.utc),
    )
    session.add(row)
    await session.flush()
    return row


def is_stale_running_job(job: VipHiringJobRow, *, now: datetime | None = None, max_age_minutes: int = 10) -> bool:
    now_utc = now or datetime.now(timezone.utc)
    heartbeat = job.last_heartbeat_at or job.updated_at or job.started_at
    if heartbeat is None:
        return True
    if heartbeat.tzinfo is None:
        heartbeat = heartbeat.replace(tzinfo=timezone.utc)
    return now_utc - heartbeat > timedelta(minutes=max(int(max_age_minutes), 1))
