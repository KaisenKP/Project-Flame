from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker

from cogs.Business.core import (
    get_manager_assignment_slots,
    get_worker_assignment_slots,
    hire_manager_manual,
    hire_worker_manual,
)
from db.models import VipHiringJobRow, VipHiringJobStepRow
from services.vip_hiring_candidates import build_candidate_pool
from services.vip_hiring_jobs import CreateVipHiringJobParams, create_job, find_active_job
from services.vip_hiring_progress import upsert_progress_message

log = logging.getLogger(__name__)


class VipHiringService:
    def __init__(self, *, sessionmaker: async_sessionmaker, bot) -> None:
        self.sessionmaker = sessionmaker
        self.bot = bot

    async def start_job(self, *, guild_id: int, user_id: int, started_by_user_id: int, business_key: str, mode: str, requested_count: int, allowed_rarities: set[str]) -> tuple[VipHiringJobRow | None, str | None]:
        mode_key = str(mode).strip().lower()
        async with self.sessionmaker() as session:
            async with session.begin():
                active = await find_active_job(session, guild_id=guild_id, user_id=user_id, business_key=business_key, mode=mode_key)
                if active is not None:
                    return None, f"A {mode_key} hiring job is already running (`{active.job_id}`)."
                pool = await build_candidate_pool(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    business_key=business_key,
                    mode=mode_key,
                    allowed_rarities=allowed_rarities,
                    disallow_duplicates=True,
                )
                if not pool.candidates:
                    return None, "No valid candidates match your filters and duplicate restrictions."
                row = await create_job(
                    session,
                    params=CreateVipHiringJobParams(
                        guild_id=guild_id,
                        user_id=user_id,
                        started_by_user_id=started_by_user_id,
                        business_key=business_key,
                        mode=mode_key,
                        requested_count=requested_count,
                        filters_json={"allowed_rarities": sorted(allowed_rarities), "disallow_duplicates": True},
                        batch_settings_json={"chunk_size": 10},
                    ),
                )
                row.status = "running"
                row.progress_message_channel_id = int(getattr(getattr(self.bot, "_connection", None), "_last_message_channel_id", 0) or 0)
                return row, None

    async def attach_progress_message(self, *, job_id: int, channel_id: int) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                job = await session.get(VipHiringJobRow, int(job_id))
                if job is None:
                    return
                job.progress_message_channel_id = int(channel_id)
                await upsert_progress_message(bot=self.bot, job=job)

    async def run_job(self, *, job_id: int) -> VipHiringJobRow | None:
        chunk_size = 10
        while True:
            async with self.sessionmaker() as session:
                async with session.begin():
                    job = await session.get(VipHiringJobRow, int(job_id), with_for_update=True)
                    if job is None:
                        return None
                    if job.status not in {"running", "queued"}:
                        return job
                    job.status = "running"
                    job.last_heartbeat_at = datetime.now(timezone.utc)

                    if int(job.processed_count or 0) >= int(job.requested_count or 0):
                        job.status = "completed"
                        job.finished_at = datetime.now(timezone.utc)
                        await upsert_progress_message(bot=self.bot, job=job)
                        return job

                    pool = await build_candidate_pool(
                        session,
                        guild_id=int(job.guild_id),
                        user_id=int(job.user_id),
                        business_key=str(job.business_key),
                        mode=str(job.mode),
                        allowed_rarities=set((job.filters_json or {}).get("allowed_rarities") or []),
                        disallow_duplicates=bool((job.filters_json or {}).get("disallow_duplicates", True)),
                    )
                    if not pool.candidates:
                        job.status = "partially_completed" if int(job.success_count or 0) > 0 else "failed"
                        job.error_summary = "candidate pool exhausted"
                        job.finished_at = datetime.now(timezone.utc)
                        await upsert_progress_message(bot=self.bot, job=job)
                        return job

                    iterations = min(chunk_size, int(job.requested_count or 0) - int(job.processed_count or 0))
                    for _ in range(iterations):
                        candidate = random.choice(pool.candidates)
                        step_no = int(job.processed_count or 0) + 1
                        step = VipHiringJobStepRow(
                            job_id=int(job.id),
                            step_number=step_no,
                            entity_kind=str(job.mode),
                            entity_key=str(candidate.get("key", "")),
                            entity_name=str(candidate.get("name", ""))[:64],
                            action_type="hire",
                            result_status="started",
                            metadata_json={"rarity": candidate.get("rarity")},
                        )
                        try:
                            session.add(step)
                            await session.flush()
                        except IntegrityError:
                            job.duplicate_blocked_count = int(job.duplicate_blocked_count or 0) + 1
                            continue

                        if str(job.mode) == "worker":
                            res = await hire_worker_manual(
                                session,
                                guild_id=int(job.guild_id),
                                user_id=int(job.user_id),
                                business_key=str(job.business_key),
                                worker_name=str(candidate.get("name", "Worker")),
                                worker_type=str(candidate.get("worker_type", "efficient")),
                                rarity=str(candidate.get("rarity", "common")),
                                flat_profit_bonus=int(candidate.get("flat_profit_bonus", 0) or 0),
                                percent_profit_bonus_bp=int(candidate.get("percent_profit_bonus_bp", 0) or 0),
                                charge_silver=False,
                            )
                        else:
                            res = await hire_manager_manual(
                                session,
                                guild_id=int(job.guild_id),
                                user_id=int(job.user_id),
                                business_key=str(job.business_key),
                                manager_name=str(candidate.get("name", "Manager")),
                                rarity=str(candidate.get("rarity", "common")),
                                runtime_bonus_hours=int(candidate.get("runtime_bonus_hours", 0) or 0),
                                profit_bonus_bp=int(candidate.get("profit_bonus_bp", 0) or 0),
                                auto_restart_charges=int(candidate.get("auto_restart_charges", 0) or 0),
                                charge_silver=False,
                            )
                        job.processed_count = int(job.processed_count or 0) + 1
                        if res.ok:
                            job.success_count = int(job.success_count or 0) + 1
                            step.result_status = "committed"
                        else:
                            job.failed_count = int(job.failed_count or 0) + 1
                            step.result_status = "failed"
                            step.skip_reason = str(res.message)[:255]
                            if "full" in str(res.message).lower():
                                job.error_summary = "slot restrictions prevented further progress"
                                job.status = "partially_completed" if int(job.success_count or 0) > 0 else "failed"
                                job.finished_at = datetime.now(timezone.utc)
                                break
                    if int(job.processed_count or 0) >= int(job.requested_count or 0) and job.status == "running":
                        job.status = "completed"
                        job.finished_at = datetime.now(timezone.utc)
                    await upsert_progress_message(bot=self.bot, job=job)
                    if job.status in {"completed", "failed", "partially_completed"}:
                        return job
            await asyncio.sleep(0)

    async def recover_stale_jobs(self) -> int:
        now = datetime.now(timezone.utc)
        updated = 0
        async with self.sessionmaker() as session:
            async with session.begin():
                rows = (await session.scalars(select(VipHiringJobRow).where(VipHiringJobRow.status.in_(["queued", "running"])))).all()
                for row in rows:
                    hb = row.last_heartbeat_at or row.updated_at or row.started_at
                    if hb is None:
                        continue
                    if hb.tzinfo is None:
                        hb = hb.replace(tzinfo=timezone.utc)
                    if (now - hb).total_seconds() > 600:
                        row.status = "interrupted"
                        row.interrupted_at = now
                        row.error_summary = "interrupted by restart/recovery"
                        updated += 1
        if updated:
            log.warning("vip_hiring_recovery_interrupted_jobs=%s", updated)
        return updated
