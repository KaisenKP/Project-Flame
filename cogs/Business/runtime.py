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
from contextlib import asynccontextmanager
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Optional, Sequence

from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from db.models import BusinessOwnershipRow, BusinessRunRow, WalletRow
from services.db import sessions

from .core import (
    RUN_STATUS_CANCELLED,
    RUN_STATUS_COMPLETED,
    RUN_STATUS_RUNNING,
    get_active_runs_for_processing,
)
from .systems import as_utc, summarize_active_events

log = logging.getLogger(__name__)


# =========================================================
# CONSTANTS
# =========================================================

DEFAULT_TICK_INTERVAL_SECONDS = 60
SECONDS_PER_HOUR = 3600
DEFAULT_DB_RETRY_ATTEMPTS = 3
DEFAULT_DB_RETRY_BASE_DELAY_SECONDS = 0.35


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
    event_type: str
    rarity: str
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
    summary: dict
    event_outcomes: list[RuntimeEventOutcome]



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


def _is_retryable_operational_error(exc: Exception) -> bool:
    if not isinstance(exc, OperationalError):
        return False
    raw = str(exc).lower()
    if "lock wait timeout exceeded" in raw:
        return True
    if "deadlock found when trying to get lock" in raw:
        return True
    orig = getattr(exc, "orig", None)
    code = None
    if orig is not None:
        args = getattr(orig, "args", ())
        if args:
            code = args[0]
    return code in {1205, 1213}


@asynccontextmanager
async def _run_processing_scope(session, *, owner: str, phase: str):
    """
    Isolate per-run failures so one bad run never poisons the outer startup/runtime transaction.
    """
    rollback_occurred = False
    tx_active_before = bool(session.in_transaction())
    nested_before = bool(session.in_nested_transaction())
    log.debug(
        "Business transaction scope enter | owner=%s phase=%s tx_active=%s nested_active=%s",
        owner,
        phase,
        tx_active_before,
        nested_before,
    )
    try:
        cm = session.begin_nested() if tx_active_before else session.begin()
        async with cm:
            yield
    except Exception:
        rollback_occurred = True
        log.exception(
            "Business transaction scope failed | owner=%s phase=%s rollback=%s tx_active_after=%s nested_active_after=%s",
            owner,
            phase,
            rollback_occurred,
            bool(session.in_transaction()),
            bool(session.in_nested_transaction()),
        )
        raise
    finally:
        log.debug(
            "Business transaction scope exit | owner=%s phase=%s rollback=%s tx_active=%s nested_active=%s",
            owner,
            phase,
            rollback_occurred,
            bool(session.in_transaction()),
            bool(session.in_nested_transaction()),
        )


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
        "summary_tracking": dict((run.report_json or {}).get("summary_tracking", {})),
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


def _premium_hour_adjustment(*, run: BusinessRunRow, premium: dict, hour_base_profit: int) -> tuple[int, str]:
    key = str(run.business_key)
    if key == "liquor_store":
        stock = int(premium.get("stock", 100) or 0)
        stock_mode = str(premium.get("stock_mode", "balanced"))
        hype = int(premium.get("hype_boost", 0) or 0)
        drain = random.randint(8, 14) + (2 if stock_mode == "premium" else -1 if stock_mode == "cheap" else 0)
        stock = max(stock - drain, 0)
        premium["stock"] = stock
        bonus_bp = 0
        if stock_mode == "premium":
            bonus_bp += 1900
        elif stock_mode == "cheap":
            bonus_bp -= 700
        if stock < 30:
            bonus_bp -= 1600
        rush_chance = 0.16 + (hype / 10_000)
        if random.random() < rush_chance:
            bonus_bp += 2600
            premium["last_moment"] = "Rush Night popped off"
        if random.random() < (0.08 if stock_mode == "premium" else 0.04):
            return int(round(hour_base_profit * (bonus_bp / 10_000))) + int(hour_base_profit * 0.35), "Rare bottles sold out fast"
        return int(round(hour_base_profit * (bonus_bp / 10_000))), "Shelf traffic"
    if key == "underground_market":
        risk = str(premium.get("risk", "mixed"))
        hot_push = int(premium.get("hot_push", 0) or 0)
        if risk == "safe":
            low, high = -900, 1600
        elif risk == "risky":
            low, high = -2800, 4200
        else:
            low, high = -1600, 2500
        roll_bp = random.randint(low, high)
        if random.random() < (0.14 + hot_push / 20_000):
            roll_bp += 3200
            premium["last_moment"] = "Hot Deal carried the run"
        return int(round(hour_base_profit * (roll_bp / 10_000))), "Deal swings"
    if key == "cartel":
        control = int(premium.get("control", 70) or 70)
        pressure = int(premium.get("pressure", 20) or 20)
        control = max(min(control + random.randint(-6, 5), 100), 20)
        pressure = max(min(pressure + random.randint(-4, 9), 100), 0)
        premium["control"] = control
        premium["pressure"] = pressure
        profit_bp = (control - 55) * 80 + pressure * 20
        if control >= 80 and random.random() < 0.18:
            profit_bp += 2200
            premium["last_moment"] = "Pressure stayed high"
        return int(round(hour_base_profit * (profit_bp / 10_000))), "Control pressure"
    if key == "shadow_government":
        focus = str(premium.get("focus", "power"))
        power = int(premium.get("power", 35) or 35)
        power = max(min(power + random.randint(2, 9), 150), 0)
        premium["power"] = power
        if focus == "cashout":
            profit_bp = 1800 + power * 25
        elif focus == "network":
            profit_bp = 900 + power * 15
        else:
            profit_bp = 200 + power * 10
            premium["power_bank"] = min(int(premium.get("power_bank", 0) or 0) + 2, 120)
        if random.random() < 0.12:
            premium["last_moment"] = "Favors paid off"
            profit_bp += 2600
        return int(round(hour_base_profit * (profit_bp / 10_000))), "Power shift"
    return 0, "No premium effect"


# =========================================================
# SINGLE RUN PROCESSOR
# =========================================================

async def process_single_run(
    session,
    *,
    run: BusinessRunRow,
    now: Optional[datetime] = None,
) -> ProcessRunResult:
    if now is None:
        now = _utc_now()

    if str(run.status) != RUN_STATUS_RUNNING:
        return ProcessRunResult(int(run.id), str(run.business_key), int(run.user_id), int(run.guild_id), 0, 0, False, True, "Run is not in running state.")

    ownership = await _get_ownership_for_run(session, run)
    if ownership is None:
        run.status = RUN_STATUS_CANCELLED
        run.completed_at = now
        run.report_json = {"run_id": int(run.id), "business_key": str(run.business_key), "status": RUN_STATUS_CANCELLED, "reason": "Missing ownership row.", "completed_at_iso": now.isoformat()}
        return ProcessRunResult(int(run.id), str(run.business_key), int(run.user_id), int(run.guild_id), 0, 0, True, False, "Ownership row missing. Run cancelled.")

    anchor = _safe_run_anchor(run)
    effective_end = _min_dt(now, run.ends_at)
    whole_hours_due = _whole_hours_between(anchor, effective_end)
    hours_paid = 0
    silver_paid = 0
    event_outcomes: list[RuntimeEventOutcome] = []

    if whole_hours_due > 0:
        hourly_profit = max(int(run.hourly_profit_snapshot or 0), 0)
        snapshot = dict(run.snapshot_json or {})
        components = dict(snapshot.get("summary_components", {}))
        report = dict(run.report_json or {})
        tracking = dict(report.get("summary_tracking", {}))
        hourly_breakdown = list(tracking.get("hourly_breakdown", []))
        event_log = list(tracking.get("event_log", []))
        plan = list(snapshot.get("event_plan", []))
        triggered = list(report.get("runtime_events", []))
        event_income_positive = int(tracking.get("event_income_positive", 0) or 0)
        event_income_negative = int(tracking.get("event_income_negative", 0) or 0)
        positive_events = int(tracking.get("positive_events", 0) or 0)
        negative_events = int(tracking.get("negative_events", 0) or 0)
        highest_rarity = str(tracking.get("highest_rarity", "none"))
        premium_tracking = dict(tracking.get("premium", {}))
        premium_state = dict(snapshot.get("premium_run") or {})
        rarity_order = {"none": 0, "common": 1, "uncommon": 2, "rare": 3, "epic": 4, "legendary": 5, "mythical": 6}
        for hour_index in range(whole_hours_due):
            hour_start = as_utc(anchor + timedelta(hours=hour_index))
            hour_end = as_utc(hour_start + timedelta(hours=1))
            active_bp = 0
            pause_minutes = 0
            instant_bonus = 0
            for evt in plan:
                evt_start = as_utc(datetime.fromisoformat(evt["starts_at_iso"]))
                evt_end_raw = evt.get("ends_at_iso")
                evt_end = as_utc(datetime.fromisoformat(evt_end_raw)) if evt_end_raw else None
                if evt_start >= hour_end or (evt_end is not None and evt_end <= hour_start):
                    continue
                if not evt.get("resolved") and evt_start >= hour_start and evt_start < hour_end:
                    evt["resolved"] = True
                    estimated_delta = int(round(hourly_profit * (int(evt.get("multiplier_bp", 0) or 0) / 10_000)))
                    evt_report = {
                        **evt,
                        "title": str(evt.get("name", "Business Event")),
                        "silver_delta": int(estimated_delta + instant_bonus),
                    }
                    triggered.append(evt_report)
                    instant_bonus += int(round(hourly_profit * float(evt.get("instant_bonus_hours", 0.0))))
                    pause_minutes = max(pause_minutes, int(evt.get("pause_minutes", 0) or 0))
                    evt_type = str(evt.get("event_type", "neutral"))
                    evt_rarity = str(evt.get("rarity", "common"))
                    if evt_type == "positive":
                        positive_events += 1
                    elif evt_type == "negative":
                        negative_events += 1
                    if rarity_order.get(evt_rarity, 0) > rarity_order.get(highest_rarity, 0):
                        highest_rarity = evt_rarity
                    event_outcomes.append(RuntimeEventOutcome(str(evt.get("event_key","event")), str(evt.get("name","Business Event")), evt_type, evt_rarity, str(evt.get("description","")), int(evt.get("multiplier_bp",0) or 0), int(evt_report["silver_delta"])))
                if evt_start < hour_end and (evt_end is None or evt_end > hour_start):
                    active_bp += int(evt.get("multiplier_bp", 0) or 0)
            effective_hour_profit = max(int(round(hourly_profit * (10_000 + active_bp) / 10_000)), 0)
            if pause_minutes > 0:
                effective_hour_profit = int(round(effective_hour_profit * max(0.15, (60 - pause_minutes) / 60)))
            payout = effective_hour_profit + instant_bonus
            premium_delta = 0
            premium_reason = ""
            if premium_state:
                premium_delta, premium_reason = _premium_hour_adjustment(
                    run=run,
                    premium=premium_state,
                    hour_base_profit=effective_hour_profit,
                )
                payout = max(payout + premium_delta, 0)
                premium_tracking["hours_with_premium"] = int(premium_tracking.get("hours_with_premium", 0) or 0) + 1
                premium_tracking["premium_income_delta"] = int(premium_tracking.get("premium_income_delta", 0) or 0) + int(premium_delta)
                if premium_reason:
                    premium_tracking["last_reason"] = premium_reason
            silver_paid += payout
            event_delta = payout - hourly_profit
            if event_delta >= 0:
                event_income_positive += event_delta
            else:
                event_income_negative += abs(event_delta)
            hourly_breakdown.append(
                {
                    "hour_index": int(run.hours_paid_total or 0) + hour_index + 1,
                    "starts_at_iso": hour_start.isoformat(),
                    "ends_at_iso": hour_end.isoformat(),
                    "base_payout": int(hourly_profit),
                    "event_delta": int(event_delta),
                    "premium_delta": int(premium_delta),
                    "total_payout": int(payout),
                }
            )
            if event_delta != 0:
                event_log.append(
                    {
                        "hour_index": int(hourly_breakdown[-1]["hour_index"]),
                        "delta": int(event_delta),
                        "active_bp": int(active_bp),
                    }
                )
        hours_paid = whole_hours_due
        wallet = await _get_wallet(session, guild_id=int(run.guild_id), user_id=int(run.user_id))
        wallet.silver += silver_paid
        if hasattr(wallet, "silver_earned"):
            wallet.silver_earned += silver_paid
        ownership.total_earned = int(ownership.total_earned or 0) + silver_paid
        run.silver_paid_total = int(run.silver_paid_total or 0) + silver_paid
        run.hours_paid_total = int(run.hours_paid_total or 0) + hours_paid
        run.last_payout_at = anchor + timedelta(hours=whole_hours_due)
        report["runtime_events"] = triggered
        per_hour_values = [int(item.get("total_payout", 0)) for item in hourly_breakdown]
        tracking = {
            "total_income_generated": int(run.silver_paid_total or 0),
            "income_per_hour_tick": per_hour_values[-240:],
            "hourly_breakdown": hourly_breakdown[-240:],
            "event_log": event_log[-240:],
            "event_income_positive": int(event_income_positive),
            "event_income_negative": int(event_income_negative),
            "worker_contribution": int(components.get("worker_hourly_bonus", 0)) * int(run.hours_paid_total or 0),
            "manager_contribution": int(components.get("manager_hourly_bonus", 0)) * int(run.hours_paid_total or 0),
            "base_contribution": int(components.get("base_hourly_income", max(hourly_profit - int(components.get("worker_hourly_bonus", 0)) - int(components.get("manager_hourly_bonus", 0)), 0))) * int(run.hours_paid_total or 0),
            "events_triggered_total": int(positive_events + negative_events),
            "positive_events": int(positive_events),
            "negative_events": int(negative_events),
            "highest_single_hour_payout": max(per_hour_values) if per_hour_values else 0,
            "lowest_single_hour_payout": min(per_hour_values) if per_hour_values else 0,
            "highest_rarity": highest_rarity,
            "premium": {
                **premium_tracking,
                "start_action": str(premium_state.get("start_action", "Standard")),
                "last_moment": str(premium_state.get("last_moment", premium_tracking.get("last_reason", "No special moment"))),
                "stock_left": int(premium_state.get("stock", 0) or 0),
                "control_end": int(premium_state.get("control", 0) or 0),
                "power_end": int(premium_state.get("power", 0) or 0),
                "power_bank": int(premium_state.get("power_bank", 0) or 0),
                "control_kept": bool(int(premium_state.get("control", 0) or 0) >= 65),
            },
        }
        report["summary_tracking"] = tracking
        run.report_json = report
        snapshot["event_plan"] = plan
        snapshot["premium_run"] = premium_state
        run.snapshot_json = snapshot

    completed = False
    if _run_has_ended(run, now=now):
        post_anchor = _safe_run_anchor(run)
        if _whole_hours_between(post_anchor, run.ends_at) <= 0:
            auto_restart_remaining = max(int(run.auto_restart_remaining or 0), 0)
            if auto_restart_remaining > 0:
                restarted = _spawn_auto_restart_run(session, run=run)
                await session.flush()
                run_report = dict(run.report_json or {})
                run_report["auto_restarted"] = True
                run_report["auto_restart_spawned_run_id"] = int(restarted.id)
                run.report_json = run_report
            _finalize_run_in_place(run, now=now)
            completed = True

    note = f"Paid {hours_paid}h / {silver_paid} silver." if hours_paid > 0 else ("Run completed with no additional payout due this tick." if completed else "No whole hours due yet.")
    return ProcessRunResult(int(run.id), str(run.business_key), int(run.user_id), int(run.guild_id), hours_paid, silver_paid, completed, False, note)


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
        phase = f"run:{getattr(run, 'id', '?')}"
        try:
            async with _run_processing_scope(session, owner="tick_active_runs_in_session", phase=phase):
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
                        summary=dict(report_json.get("summary_tracking", {})),
                        event_outcomes=[
                            RuntimeEventOutcome(
                                event_key=str(evt.get("event_key", "event")),
                                title=str(evt.get("title", "Business Event")),
                                event_type=str(evt.get("event_type", "neutral")),
                                rarity=str(evt.get("rarity", "common")),
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
                "Failed processing business run id=%s guild=%s user=%s business=%s owner=%s phase=%s tx_active=%s nested_active=%s",
                getattr(run, "id", "?"),
                getattr(run, "guild_id", "?"),
                getattr(run, "user_id", "?"),
                getattr(run, "business_key", "?"),
                "tick_active_runs_in_session",
                phase,
                bool(session.in_transaction()),
                bool(session.in_nested_transaction()),
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
        db_retry_attempts: int = DEFAULT_DB_RETRY_ATTEMPTS,
        db_retry_base_delay_seconds: float = DEFAULT_DB_RETRY_BASE_DELAY_SECONDS,
        on_run_completed: Optional[Callable[[CompletedRunNotice], Awaitable[None]]] = None,
    ):
        self.sessionmaker = sessions()
        self.tick_interval_seconds = max(int(tick_interval_seconds), 5)
        self.db_retry_attempts = max(int(db_retry_attempts), 1)
        self.db_retry_base_delay_seconds = max(float(db_retry_base_delay_seconds), 0.05)
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
        attempts = self.db_retry_attempts
        for attempt in range(1, attempts + 1):
            try:
                async with self.sessionmaker() as session:
                    async with session.begin():
                        result = await tick_active_runs_in_session(
                            session,
                            guild_id=guild_id,
                            now=now,
                        )
                    completed_notices = list(result.completed_notices)
                break
            except Exception as exc:
                if not _is_retryable_operational_error(exc) or attempt >= attempts:
                    raise
                backoff = self.db_retry_base_delay_seconds * attempt
                log.warning(
                    "Retryable DB error during business runtime tick; retrying. attempt=%s/%s delay=%.2fs guild_id=%s error=%s",
                    attempt,
                    attempts,
                    backoff,
                    guild_id,
                    exc,
                )
                await asyncio.sleep(backoff)
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
