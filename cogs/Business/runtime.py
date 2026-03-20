# cogs/Business/runtime.py
from __future__ import annotations

"""
Business Runtime

What this file does:
- Processes active business runs over time
- Pays silver into WalletRow for completed elapsed business hours
- Updates ownership totals as income is earned
- Marks runs completed when their timer ends
- Builds end-of-run reports
- Exposes a reusable runtime engine for future background tasks / loops

What this file does NOT do:
- It does not register Discord commands
- It does not build embeds
- It does not own the business catalog
- It does not start business runs (core.py does that)
- It does not yet handle:
    - events
    - mythical auto-reopen chains
    - taxes
    - worker morale/fatigue systems
    - complex business incidents

What this file requires:
- db.models must expose:
    WalletRow
    BusinessOwnershipRow
    BusinessRunRow

- cogs/Business/core.py must expose:
    RUN_STATUS_RUNNING
    RUN_STATUS_COMPLETED
    RUN_STATUS_CANCELLED
    get_active_runs_for_processing(...)

- services.db must expose:
    sessions()

How it is intended to be used:
- core.py starts a run by creating BusinessRunRow
- this file periodically checks all active rows
- for each elapsed full hour, it adds silver to the wallet
- when the run ends, it marks the row completed and stores a report

Recommended future usage:
- instantiate BusinessRuntimeEngine()
- call tick_once() on a timer
- or attach start_loop()/stop_loop() to bot startup / shutdown

Design choices in this version:
- payout happens in whole-hour chunks only
- partial hours are never paid early
- if the bot was offline for 7 hours, the next tick catches up all 7 owed hours
- each run uses hourly_profit_snapshot from the time the run started
- current staffing/levels do NOT retroactively change an active run
- this keeps sessions deterministic and way less cursed
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import random
from typing import Awaitable, Callable, Optional, Sequence

from sqlalchemy import select

from db.models import BusinessOwnershipRow, BusinessRunRow, WalletRow
from services.db import sessions

from .core import (
    RUN_STATUS_CANCELLED,
    RUN_STATUS_COMPLETED,
    RUN_STATUS_RUNNING,
    get_active_runs_for_processing,
)

log = logging.getLogger(__name__)


# =========================================================
# CONSTANTS
# =========================================================

DEFAULT_TICK_INTERVAL_SECONDS = 60
SECONDS_PER_HOUR = 3600
DEFAULT_EVENT_CHANCE_PER_HOUR = 0.22


# =========================================================
# RUNTIME RESULT DATACLASSES
# =========================================================

@dataclass(slots=True)
class ProcessRunResult:
    run_id: int
    business_key: str
    user_id: int
    guild_id: int
    hours_paid: int
    silver_paid: int
    completed: bool
    skipped: bool
    note: str = ""


@dataclass(slots=True)
class RuntimeTickResult:
    scanned_runs: int
    processed_runs: int
    completed_runs: int
    paid_hours: int
    paid_silver: int
    errored_runs: int
    completed_notices: list["CompletedRunNotice"]


@dataclass(slots=True)
class RuntimeEventOutcome:
    event_key: str
    title: str
    description: str
    multiplier_bp: int
    silver_delta: int


@dataclass(slots=True)
class CompletedRunNotice:
    run_id: int
    guild_id: int
    user_id: int
    business_key: str
    hours_paid_total: int
    silver_paid_total: int
    event_outcomes: list[RuntimeEventOutcome]


@dataclass(frozen=True, slots=True)
class BusinessEventDef:
    key: str
    title: str
    description: str
    multiplier_bp: int


_BUSINESS_EVENT_POOLS: dict[str, tuple[BusinessEventDef, ...]] = {
    "farm": (
        BusinessEventDef("blight", "Crop Blight", "A plant outbreak damaged part of the fields.", -2000),
        BusinessEventDef("perfect_rain", "Perfect Rain", "Weather conditions were ideal for growth.", 2000),
    ),
    "restaurant": (
        BusinessEventDef("health_scare", "Health Inspection Delay", "Kitchen operations slowed after a strict inspection.", -1200),
        BusinessEventDef("viral_review", "Viral Review", "A food post exploded online and bookings surged.", 1800),
    ),
    "nightclub": (
        BusinessEventDef("permit_noise", "Noise Complaint", "A permit warning forced an early close window.", -1700),
        BusinessEventDef("headline_dj", "Headline DJ Night", "A surprise guest set packed the club.", 2200),
    ),
    "factory": (
        BusinessEventDef("machine_jam", "Machine Jam", "A production line stalled and throughput dipped.", -1500),
        BusinessEventDef("bulk_order", "Bulk Contract", "A large order boosted line utilization.", 1700),
    ),
    "casino": (
        BusinessEventDef("compliance_audit", "Compliance Audit", "Table access tightened during a compliance review.", -1800),
        BusinessEventDef("high_roller", "High Roller Rush", "VIP traffic and table volume spiked.", 2100),
    ),
    "tech_company": (
        BusinessEventDef("service_outage", "Service Outage", "A core platform incident reduced billable activity.", -1600),
        BusinessEventDef("enterprise_deal", "Enterprise Deal", "A major contract landed unexpectedly.", 2000),
    ),
    "shipping_company": (
        BusinessEventDef("port_delay", "Port Delay", "Congestion delayed key routes.", -1500),
        BusinessEventDef("fuel_efficiency", "Routing Optimization", "Fleet routing improvements lowered burn and waste.", 1600),
    ),
    "hotel": (
        BusinessEventDef("maintenance", "Maintenance Incident", "Unexpected repairs closed premium rooms.", -1400),
        BusinessEventDef("conference_week", "Conference Week", "A large event drove occupancy above forecast.", 1800),
    ),
    "movie_studio": (
        BusinessEventDef("reshoot", "Reshoot Overrun", "A production reset slowed release commitments.", -1700),
        BusinessEventDef("box_office_buzz", "Box Office Buzz", "Strong previews drove licensing demand.", 2200),
    ),
    "space_mining": (
        BusinessEventDef("solar_storm", "Solar Storm", "Radiation spikes forced reduced extraction windows.", -2000),
        BusinessEventDef("rich_vein", "Rich Vein", "A dense ore pocket was discovered on route.", 2400),
    ),
}

_DEFAULT_EVENT_POOL: tuple[BusinessEventDef, ...] = (
    BusinessEventDef("supply_disruption", "Supply Disruption", "A vendor issue reduced output for the hour.", -1200),
    BusinessEventDef("efficiency_boost", "Efficiency Boost", "Ops worked smoothly with better than expected throughput.", 1200),
)


# =========================================================
# TIME HELPERS
# =========================================================

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _min_dt(a: datetime, b: datetime) -> datetime:
    utc_a = _as_utc(a)
    utc_b = _as_utc(b)
    return utc_a if utc_a <= utc_b else utc_b


def _safe_run_anchor(run: BusinessRunRow) -> datetime:
    """
    Payout anchor:
    - use last_payout_at if present
    - otherwise fall back to started_at
    """
    if run.last_payout_at is not None:
        return run.last_payout_at
    return run.started_at


def _whole_hours_between(start: datetime, end: datetime) -> int:
    """
    Returns whole elapsed hours only.
    """
    seconds = int((_as_utc(end) - _as_utc(start)).total_seconds())
    if seconds <= 0:
        return 0
    return seconds // SECONDS_PER_HOUR


def _run_has_ended(run: BusinessRunRow, *, now: Optional[datetime] = None) -> bool:
    if now is None:
        now = _utc_now()
    return _as_utc(now) >= _as_utc(run.ends_at)


# =========================================================
# DB HELPERS
# =========================================================

async def _get_wallet(session, *, guild_id: int, user_id: int) -> WalletRow:
    wallet = await session.scalar(
        select(WalletRow).where(
            WalletRow.guild_id == int(guild_id),
            WalletRow.user_id == int(user_id),
        )
    )
    if wallet is None:
        wallet = WalletRow(
            guild_id=int(guild_id),
            user_id=int(user_id),
            silver=0,
            diamonds=0,
        )
        session.add(wallet)
        await session.flush()
    return wallet


async def _get_ownership_for_run(session, run: BusinessRunRow) -> Optional[BusinessOwnershipRow]:
    ownership = await session.scalar(
        select(BusinessOwnershipRow).where(
            BusinessOwnershipRow.id == int(run.ownership_id)
        )
    )
    return ownership


# =========================================================
# REPORT HELPERS
# =========================================================

def _build_run_report_json(run: BusinessRunRow, *, completed_at: datetime) -> dict:
    return {
        "run_id": int(run.id),
        "business_key": str(run.business_key),
        "status": str(run.status),
        "started_at_iso": run.started_at.isoformat() if run.started_at else None,
        "ended_at_iso": run.ends_at.isoformat() if run.ends_at else None,
        "completed_at_iso": completed_at.isoformat(),
        "runtime_hours_snapshot": int(run.runtime_hours_snapshot or 0),
        "hourly_profit_snapshot": int(run.hourly_profit_snapshot or 0),
        "silver_paid_total": int(run.silver_paid_total or 0),
        "hours_paid_total": int(run.hours_paid_total or 0),
        "auto_restart_remaining": int(run.auto_restart_remaining or 0),
        "runtime_events": list((run.report_json or {}).get("runtime_events", [])),
    }


def _spawn_auto_restart_run(session, *, run: BusinessRunRow) -> BusinessRunRow:
    restart_at = _as_utc(run.ends_at)
    runtime_hours = max(int(run.runtime_hours_snapshot or 0), 1)
    restarted = BusinessRunRow(
        ownership_id=int(run.ownership_id),
        guild_id=int(run.guild_id),
        user_id=int(run.user_id),
        business_key=str(run.business_key),
        status=RUN_STATUS_RUNNING,
        started_at=restart_at,
        ends_at=restart_at + timedelta(hours=runtime_hours),
        last_payout_at=restart_at,
        completed_at=None,
        runtime_hours_snapshot=runtime_hours,
        hourly_profit_snapshot=max(int(run.hourly_profit_snapshot or 0), 0),
        auto_restart_remaining=max(int(run.auto_restart_remaining or 0) - 1, 0),
        snapshot_json={
            **dict(run.snapshot_json or {}),
            "auto_restarted_from_run_id": int(run.id),
            "auto_restart_remaining": max(int(run.auto_restart_remaining or 0) - 1, 0),
            "started_at_iso": restart_at.isoformat(),
            "ends_at_iso": (restart_at + timedelta(hours=runtime_hours)).isoformat(),
        },
        report_json=None,
        silver_paid_total=0,
        hours_paid_total=0,
    )
    session.add(restarted)
    return restarted


def _event_pool_for_business_key(business_key: str) -> tuple[BusinessEventDef, ...]:
    return _BUSINESS_EVENT_POOLS.get(str(business_key).strip().lower(), _DEFAULT_EVENT_POOL)


def _roll_hourly_event(*, business_key: str) -> Optional[BusinessEventDef]:
    if random.random() >= DEFAULT_EVENT_CHANCE_PER_HOUR:
        return None
    pool = _event_pool_for_business_key(business_key)
    if not pool:
        return None
    return random.choice(pool)


# =========================================================
# RUN FINALIZATION
# =========================================================

def _finalize_run_in_place(run: BusinessRunRow, *, now: Optional[datetime] = None) -> None:
    if now is None:
        now = _utc_now()

    run.status = RUN_STATUS_COMPLETED
    run.completed_at = now
    run.last_payout_at = run.ends_at
    run.report_json = _build_run_report_json(run, completed_at=now)


# =========================================================
# SINGLE RUN PROCESSOR
# =========================================================

async def process_single_run(
    session,
    *,
    run: BusinessRunRow,
    now: Optional[datetime] = None,
) -> ProcessRunResult:
    """
    Processes one active business run.

    Rules:
    - pay only for whole elapsed hours
    - pay up to ends_at, never past it
    - mark completed once ended and all whole-hour payouts are applied
    """
    if now is None:
        now = _utc_now()

    if str(run.status) != RUN_STATUS_RUNNING:
        return ProcessRunResult(
            run_id=int(run.id),
            business_key=str(run.business_key),
            user_id=int(run.user_id),
            guild_id=int(run.guild_id),
            hours_paid=0,
            silver_paid=0,
            completed=False,
            skipped=True,
            note="Run is not in running state.",
        )

    ownership = await _get_ownership_for_run(session, run)
    if ownership is None:
        # Orphaned row. Mark cancelled so it stops clogging the pipe.
        run.status = RUN_STATUS_CANCELLED
        run.completed_at = now
        run.report_json = {
            "run_id": int(run.id),
            "business_key": str(run.business_key),
            "status": RUN_STATUS_CANCELLED,
            "reason": "Missing ownership row.",
            "completed_at_iso": now.isoformat(),
        }
        return ProcessRunResult(
            run_id=int(run.id),
            business_key=str(run.business_key),
            user_id=int(run.user_id),
            guild_id=int(run.guild_id),
            hours_paid=0,
            silver_paid=0,
            completed=True,
            skipped=False,
            note="Ownership row missing. Run cancelled.",
        )

    anchor = _safe_run_anchor(run)
    effective_end = _min_dt(now, run.ends_at)
    whole_hours_due = _whole_hours_between(anchor, effective_end)

    hours_paid = 0
    silver_paid = 0

    if whole_hours_due > 0:
        hourly_profit = max(int(run.hourly_profit_snapshot or 0), 0)
        event_outcomes: list[RuntimeEventOutcome] = []
        for _ in range(whole_hours_due):
            event = _roll_hourly_event(business_key=str(run.business_key))
            multiplier_bp = int(event.multiplier_bp) if event is not None else 0
            hour_profit = max(int(round(hourly_profit * (10_000 + multiplier_bp) / 10_000)), 0)
            silver_paid += hour_profit
            if event is not None:
                event_outcomes.append(
                    RuntimeEventOutcome(
                        event_key=str(event.key),
                        title=str(event.title),
                        description=str(event.description),
                        multiplier_bp=multiplier_bp,
                        silver_delta=hour_profit - hourly_profit,
                    )
                )
        hours_paid = whole_hours_due

        wallet = await _get_wallet(
            session,
            guild_id=int(run.guild_id),
            user_id=int(run.user_id),
        )

        wallet.silver += silver_paid
        if hasattr(wallet, "silver_earned"):
            wallet.silver_earned += silver_paid

        ownership.total_earned = int(ownership.total_earned or 0) + silver_paid

        run.silver_paid_total = int(run.silver_paid_total or 0) + silver_paid
        run.hours_paid_total = int(run.hours_paid_total or 0) + hours_paid
        run.last_payout_at = anchor + timedelta(hours=whole_hours_due)
        run_report = dict(run.report_json or {})
        existing_events = list(run_report.get("runtime_events", []))
        existing_events.extend(
            {
                "event_key": e.event_key,
                "title": e.title,
                "description": e.description,
                "multiplier_bp": int(e.multiplier_bp),
                "silver_delta": int(e.silver_delta),
            }
            for e in event_outcomes
        )
        run_report["runtime_events"] = existing_events
        run.report_json = run_report

    completed = False

    # If the run has ended and there are no remaining whole unpaid hours,
    # finalize it cleanly or consume one auto-restart charge.
    if _run_has_ended(run, now=now):
        post_anchor = _safe_run_anchor(run)
        remaining_due_after_payment = _whole_hours_between(post_anchor, run.ends_at)

        if remaining_due_after_payment <= 0:
            auto_restart_remaining = max(int(run.auto_restart_remaining or 0), 0)
            if auto_restart_remaining > 0:
                restarted = _spawn_auto_restart_run(session, run=run)
                await session.flush()
                run_report = dict(run.report_json or {})
                run_report["auto_restarted"] = True
                run_report["auto_restart_spawned_run_id"] = int(restarted.id)
                run_report["auto_restart_charges_before_completion"] = auto_restart_remaining
                run_report["auto_restart_charges_after_completion"] = int(restarted.auto_restart_remaining or 0)
                run.report_json = run_report
            _finalize_run_in_place(run, now=now)
            completed = True

    note = ""
    if hours_paid > 0:
        note = f"Paid {hours_paid}h / {silver_paid} silver."
    elif completed:
        note = "Run completed with no additional payout due this tick."
    else:
        note = "No whole hours due yet."

    return ProcessRunResult(
        run_id=int(run.id),
        business_key=str(run.business_key),
        user_id=int(run.user_id),
        guild_id=int(run.guild_id),
        hours_paid=hours_paid,
        silver_paid=silver_paid,
        completed=completed,
        skipped=False,
        note=note,
    )


# =========================================================
# BULK TICK HELPERS
# =========================================================

async def tick_active_runs_in_session(
    session,
    *,
    guild_id: Optional[int] = None,
    now: Optional[datetime] = None,
) -> RuntimeTickResult:
    """
    Processes all currently running business runs in a single DB session/transaction.

    This is the core workhorse.
    """
    if now is None:
        now = _utc_now()

    runs = await get_active_runs_for_processing(session, guild_id=guild_id)

    scanned_runs = len(runs)
    processed_runs = 0
    completed_runs = 0
    paid_hours = 0
    paid_silver = 0
    errored_runs = 0
    completed_notices: list[CompletedRunNotice] = []

    for run in runs:
        try:
            result = await process_single_run(session, run=run, now=now)
            if not result.skipped:
                processed_runs += 1
            if result.completed:
                completed_runs += 1
                report_json = dict(run.report_json or {})
                raw_events = list(report_json.get("runtime_events", []))
                completed_notices.append(
                    CompletedRunNotice(
                        run_id=int(run.id),
                        guild_id=int(run.guild_id),
                        user_id=int(run.user_id),
                        business_key=str(run.business_key),
                        hours_paid_total=int(run.hours_paid_total or 0),
                        silver_paid_total=int(run.silver_paid_total or 0),
                        event_outcomes=[
                            RuntimeEventOutcome(
                                event_key=str(evt.get("event_key", "event")),
                                title=str(evt.get("title", "Business Event")),
                                description=str(evt.get("description", "")),
                                multiplier_bp=int(evt.get("multiplier_bp", 0)),
                                silver_delta=int(evt.get("silver_delta", 0)),
                            )
                            for evt in raw_events
                        ],
                    )
                )
            paid_hours += int(result.hours_paid)
            paid_silver += int(result.silver_paid)
        except Exception:
            errored_runs += 1
            log.exception(
                "Failed processing business run id=%s guild=%s user=%s business=%s",
                getattr(run, "id", "?"),
                getattr(run, "guild_id", "?"),
                getattr(run, "user_id", "?"),
                getattr(run, "business_key", "?"),
            )

    return RuntimeTickResult(
        scanned_runs=scanned_runs,
        processed_runs=processed_runs,
        completed_runs=completed_runs,
        paid_hours=paid_hours,
        paid_silver=paid_silver,
        errored_runs=errored_runs,
        completed_notices=completed_notices,
    )


async def tick_active_runs(
    *,
    guild_id: Optional[int] = None,
    now: Optional[datetime] = None,
) -> RuntimeTickResult:
    """
    Convenience wrapper that opens its own session and transaction.
    """
    sessionmaker = sessions()

    async with sessionmaker() as session:
        async with session.begin():
            return await tick_active_runs_in_session(
                session,
                guild_id=guild_id,
                now=now,
            )


# =========================================================
# RUNTIME ENGINE
# =========================================================

class BusinessRuntimeEngine:
    """
    Reusable runtime engine.

    Current best usage:
        engine = BusinessRuntimeEngine()
        await engine.tick_once()

    Future best usage:
        await engine.start_loop()
        ...
        await engine.stop_loop()
    """

    def __init__(
        self,
        *,
        tick_interval_seconds: int = DEFAULT_TICK_INTERVAL_SECONDS,
        on_run_completed: Optional[Callable[[CompletedRunNotice], Awaitable[None]]] = None,
    ):
        self.sessionmaker = sessions()
        self.tick_interval_seconds = max(int(tick_interval_seconds), 5)
        self.on_run_completed = on_run_completed
        self._task: Optional[asyncio.Task] = None
        self._stopping = False

    @property
    def running(self) -> bool:
        task = self._task
        return task is not None and not task.done()

    async def tick_once(
        self,
        *,
        guild_id: Optional[int] = None,
        now: Optional[datetime] = None,
    ) -> RuntimeTickResult:
        async with self.sessionmaker() as session:
            async with session.begin():
                result = await tick_active_runs_in_session(
                    session,
                    guild_id=guild_id,
                    now=now,
                )
            completed_notices = list(result.completed_notices)
        for notice in completed_notices:
            if self.on_run_completed is None:
                continue
            try:
                await self.on_run_completed(notice)
            except Exception:
                log.exception(
                    "Failed sending business completion notice | run_id=%s guild_id=%s user_id=%s",
                    notice.run_id,
                    notice.guild_id,
                    notice.user_id,
                )
        return result

    async def _loop(self, *, guild_id: Optional[int] = None) -> None:
        log.info(
            "Business runtime loop started. interval=%ss guild_id=%s",
            self.tick_interval_seconds,
            guild_id,
        )

        try:
            while not self._stopping:
                try:
                    result = await self.tick_once(guild_id=guild_id)
                    if (
                        result.scanned_runs > 0
                        or result.paid_silver > 0
                        or result.completed_runs > 0
                        or result.errored_runs > 0
                    ):
                        log.info(
                            "Business runtime tick | scanned=%s processed=%s completed=%s hours=%s silver=%s errors=%s guild_id=%s",
                            result.scanned_runs,
                            result.processed_runs,
                            result.completed_runs,
                            result.paid_hours,
                            result.paid_silver,
                            result.errored_runs,
                            guild_id,
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("Unhandled exception in business runtime loop.")

                await asyncio.sleep(self.tick_interval_seconds)
        finally:
            log.info("Business runtime loop stopped. guild_id=%s", guild_id)

    async def start_loop(self, *, guild_id: Optional[int] = None) -> None:
        if self.running:
            return
        self._stopping = False
        self._task = asyncio.create_task(self._loop(guild_id=guild_id))

    async def stop_loop(self) -> None:
        self._stopping = True
        task = self._task
        if task is None:
            return

        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self._task = None


# =========================================================
# OPTIONAL UTILITIES
# =========================================================

async def finalize_expired_runs_once(
    *,
    guild_id: Optional[int] = None,
) -> RuntimeTickResult:
    """
    Friendly alias for manual admin/testing use.
    """
    return await tick_active_runs(guild_id=guild_id)


async def process_specific_run_by_id(
    *,
    run_id: int,
    now: Optional[datetime] = None,
) -> Optional[ProcessRunResult]:
    """
    Useful for debugging/testing a single run.
    """
    if now is None:
        now = _utc_now()

    sessionmaker = sessions()

    async with sessionmaker() as session:
        async with session.begin():
            run = await session.scalar(
                select(BusinessRunRow).where(BusinessRunRow.id == int(run_id))
            )
            if run is None:
                return None

            return await process_single_run(session, run=run, now=now)
