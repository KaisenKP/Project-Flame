from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Awaitable, Callable, Literal

from sqlalchemy import func, select

from db.models import BusinessRunRow, SundayAnnouncementStateRow, UserRow, WalletRow
from services.config import VIP_ROLE_ID
from services.db import sessions
from services.items_catalog import ITEMS
from services.startup_diagnostics import STATUS_FAIL, STATUS_PASS, STATUS_WARN, StartupDiagnostics

log = logging.getLogger("startup_manager")

RoutineKind = Literal["cache_warmup", "custom_boot"]
RoutineCallable = Callable[[Any, "BotStartupCache"], Any | Awaitable[Any]]


@dataclass(slots=True)
class RoutineSpec:
    name: str
    kind: RoutineKind
    handler: RoutineCallable
    required: bool = False
    description: str = ""


@dataclass(slots=True)
class RoutineResult:
    name: str
    required: bool
    ok: bool
    duration_ms: int
    details: str
    error: str | None = None


@dataclass(slots=True)
class CategoryReport:
    kind: RoutineKind
    results: list[RoutineResult] = field(default_factory=list)
    duration_ms: int = 0

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def failed_required(self) -> int:
        return sum(1 for r in self.results if not r.ok and r.required)

    @property
    def failed_optional(self) -> int:
        return sum(1 for r in self.results if not r.ok and not r.required)

    @property
    def status(self) -> str:
        if self.failed_required:
            return STATUS_FAIL
        if self.failed_optional:
            return STATUS_WARN
        return STATUS_PASS

    @property
    def summary(self) -> str:
        label = "Cache warmup" if self.kind == "cache_warmup" else "Custom boot routines"
        if self.total == 0:
            return f"No {self.kind.replace('_', ' ')} tasks registered; skipping"
        if self.status == STATUS_PASS:
            return f"{label} completed: {self.total} tasks, {self.duration_ms} ms"
        if self.status == STATUS_WARN:
            return (
                f"{label} completed with warnings: {self.total} tasks, "
                f"{self.failed_optional} optional failed, {self.duration_ms} ms"
            )
        return (
            f"{label} failed: {self.total} tasks, "
            f"{self.failed_required} required failed, {self.duration_ms} ms"
        )


@dataclass(slots=True)
class BotStartupCache:
    guild_ids: set[int] = field(default_factory=set)
    feature_flags: dict[str, Any] = field(default_factory=dict)
    lookup_maps: dict[str, dict[str, Any]] = field(default_factory=dict)
    counters: dict[str, int] = field(default_factory=dict)


class StartupManager:
    def __init__(self) -> None:
        self._routines: dict[RoutineKind, dict[str, RoutineSpec]] = {
            "cache_warmup": {},
            "custom_boot": {},
        }
        self.cache = BotStartupCache()
        self.last_reports: dict[RoutineKind, CategoryReport] = {}

    def clear(self) -> None:
        for bucket in self._routines.values():
            bucket.clear()

    def register(self, spec: RoutineSpec) -> None:
        self._routines[spec.kind][spec.name] = spec

    def register_cache_warmup(self, name: str, handler: RoutineCallable, *, required: bool, description: str = "") -> None:
        self.register(RoutineSpec(name=name, kind="cache_warmup", handler=handler, required=required, description=description))

    def register_boot_routine(self, name: str, handler: RoutineCallable, *, required: bool, description: str = "") -> None:
        self.register(RoutineSpec(name=name, kind="custom_boot", handler=handler, required=required, description=description))

    def configure_defaults(self) -> None:
        self.clear()

        self.register_cache_warmup(
            "guild_state_snapshot",
            _warm_guild_state_snapshot,
            required=False,
            description="Preload guild-id and active-business counts for early runtime checks.",
        )
        self.register_cache_warmup(
            "feature_flag_snapshot",
            _warm_feature_flags,
            required=False,
            description="Preload Sunday shop state flags used by recurring announcer logic.",
        )
        self.register_cache_warmup(
            "static_lookup_indexes",
            _warm_static_lookup_indexes,
            required=True,
            description="Build in-memory indexes for item/shop and category lookups.",
        )

        self.register_boot_routine(
            "validate_configured_discord_targets",
            _boot_validate_discord_targets,
            required=False,
            description="Validate configured role/channel IDs against connected guild cache.",
        )
        self.register_boot_routine(
            "sanitize_sunday_state",
            _boot_sanitize_sunday_state,
            required=False,
            description="Fix impossible Sunday announcement state rows to keep rotation sane.",
        )
        self.register_boot_routine(
            "verify_background_services",
            _boot_verify_background_services,
            required=True,
            description="Verify key long-running services started by loaded cogs are alive.",
        )

    async def run_category(self, kind: RoutineKind, *, bot: Any, diagnostics: StartupDiagnostics | None) -> CategoryReport:
        specs = list(self._routines[kind].values())
        started = time.perf_counter()
        results: list[RoutineResult] = []

        for spec in specs:
            r_started = time.perf_counter()
            try:
                maybe = spec.handler(bot, self.cache)
                if asyncio.iscoroutine(maybe):
                    maybe = await maybe
                details = str(maybe or "ok")
                ok = True
                err_text = None
            except Exception as exc:
                ok = False
                details = str(exc)
                err_text = f"{type(exc).__name__}: {exc}"
                if diagnostics is not None:
                    if spec.required:
                        diagnostics.capture_exception(
                            exc,
                            category="startup",
                            subsystem="startup_manager",
                            source=f"{kind}.{spec.name}",
                            summary=f"Startup routine failed: {spec.name}",
                            fatal=True,
                            extra_context={"kind": kind, "required": True},
                        )
                    else:
                        diagnostics.record_entry(
                            phase="startup",
                            status=STATUS_WARN,
                            fatal=False,
                            category="startup",
                            subsystem="startup_manager",
                            source=f"{kind}.{spec.name}",
                            summary=f"Optional startup routine failed: {spec.name} ({exc})",
                            exception_type=type(exc).__name__,
                            exception_message=str(exc),
                            extra_context={"kind": kind, "required": False},
                        )
                log.exception("Startup routine failed [%s/%s]", kind, spec.name)

            results.append(
                RoutineResult(
                    name=spec.name,
                    required=spec.required,
                    ok=ok,
                    duration_ms=int((time.perf_counter() - r_started) * 1000),
                    details=details,
                    error=err_text,
                )
            )

        report = CategoryReport(kind=kind, results=results, duration_ms=int((time.perf_counter() - started) * 1000))
        self.last_reports[kind] = report
        return report


async def _warm_guild_state_snapshot(bot: Any, cache: BotStartupCache) -> str:
    sessionmaker = sessions()
    guild_ids: set[int] = set()

    async with sessionmaker() as session:
        user_guilds = (await session.execute(select(UserRow.guild_id).group_by(UserRow.guild_id).limit(2000))).scalars().all()
        wallet_guilds = (await session.execute(select(WalletRow.guild_id).group_by(WalletRow.guild_id).limit(2000))).scalars().all()
        active_runs = (
            await session.execute(
                select(BusinessRunRow.guild_id, func.count(BusinessRunRow.id))
                .where(BusinessRunRow.status == "running")
                .group_by(BusinessRunRow.guild_id)
                .limit(1000)
            )
        ).all()

    guild_ids.update(int(g) for g in user_guilds)
    guild_ids.update(int(g) for g in wallet_guilds)
    guild_ids.update(int(gid) for gid, _ in active_runs)

    cache.guild_ids = guild_ids
    cache.counters["active_business_runs"] = int(sum(int(cnt) for _, cnt in active_runs))
    cache.lookup_maps["active_business_runs_by_guild"] = {str(int(gid)): int(cnt) for gid, cnt in active_runs}
    return f"guilds={len(guild_ids)} active_runs={cache.counters['active_business_runs']}"


async def _warm_feature_flags(bot: Any, cache: BotStartupCache) -> str:
    sessionmaker = sessions()
    async with sessionmaker() as session:
        rows = (
            await session.execute(
                select(
                    SundayAnnouncementStateRow.guild_id,
                    SundayAnnouncementStateRow.launch_sent,
                    SundayAnnouncementStateRow.midday_sent,
                    SundayAnnouncementStateRow.final_sent,
                    SundayAnnouncementStateRow.last_event_date,
                ).limit(2000)
            )
        ).all()

    flags: dict[str, Any] = {}
    for gid, launch, midday, final, last_event_date in rows:
        flags[str(int(gid))] = {
            "launch_sent": bool(launch),
            "midday_sent": bool(midday),
            "final_sent": bool(final),
            "last_event_date": last_event_date.isoformat() if last_event_date else None,
        }

    cache.feature_flags["sunday_announcement"] = flags
    return f"sunday_state_rows={len(flags)}"


def _warm_static_lookup_indexes(bot: Any, cache: BotStartupCache) -> str:
    rarity_counts: dict[str, int] = {}
    for item in ITEMS.values():
        rarity = str(item.rarity.value)
        rarity_counts[rarity] = rarity_counts.get(rarity, 0) + 1

    cache.lookup_maps["item_rarity_counts"] = rarity_counts
    cache.lookup_maps["item_daily_limits"] = {k: int(v.daily_limit) for k, v in ITEMS.items()}
    cache.counters["catalog_items"] = len(ITEMS)
    return f"items={len(ITEMS)} rarities={len(rarity_counts)}"


def _boot_validate_discord_targets(bot: Any, cache: BotStartupCache) -> str:
    from cogs.shop import SHOP_SUNDAY_ANNOUNCE_CHANNEL_ID

    checked = 0
    missing: list[str] = []

    if int(VIP_ROLE_ID) > 0:
        checked += 1
        role_found = any(guild.get_role(int(VIP_ROLE_ID)) is not None for guild in bot.guilds)
        if not role_found:
            missing.append(f"VIP_ROLE_ID={VIP_ROLE_ID}")

    if int(SHOP_SUNDAY_ANNOUNCE_CHANNEL_ID) > 0:
        checked += 1
        channel = bot.get_channel(int(SHOP_SUNDAY_ANNOUNCE_CHANNEL_ID))
        if channel is None:
            missing.append(f"SHOP_SUNDAY_ANNOUNCE_CHANNEL_ID={SHOP_SUNDAY_ANNOUNCE_CHANNEL_ID}")

    if missing:
        raise RuntimeError("Missing configured Discord targets: " + ", ".join(missing))

    return f"checked={checked}"


async def _boot_sanitize_sunday_state(bot: Any, cache: BotStartupCache) -> str:
    today = date.today()
    fixed = 0
    sessionmaker = sessions()

    async with sessionmaker() as session:
        async with session.begin():
            rows = list((await session.execute(select(SundayAnnouncementStateRow))).scalars())
            for row in rows:
                if row.last_event_date and row.last_event_date > today:
                    row.last_event_date = today
                    row.launch_sent = False
                    row.midday_sent = False
                    row.final_sent = False
                    fixed += 1

    return f"rows_scanned={len(rows)} fixed={fixed}"


def _boot_verify_background_services(bot: Any, cache: BotStartupCache) -> str:
    issues: list[str] = []

    if not any(t.get_name() == "pulse.heartbeat" for t in getattr(bot, "_bg_tasks", set())):
        issues.append("pulse.heartbeat task is missing")
    if not any(t.get_name() == "pulse.scheduled_restart" for t in getattr(bot, "_bg_tasks", set())):
        issues.append("pulse.scheduled_restart task is missing")

    activity_cog = bot.get_cog("ActivityListenerCog")
    if activity_cog is not None:
        svc = getattr(activity_cog, "msg_counter", None)
        running = bool(getattr(svc, "_running", False))
        flush_task = getattr(svc, "_flush_task", None)
        if not running or flush_task is None or flush_task.done():
            issues.append("ActivityListenerCog message counter loop is not healthy")

    if issues:
        raise RuntimeError("; ".join(issues))

    return "background services healthy"
