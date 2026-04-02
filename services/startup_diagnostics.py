from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import platform
import sys
import textwrap
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import discord

STATUS_PASS = "PASS"
STATUS_WARN = "WARN"
STATUS_FAIL = "FAIL"
STATUS_SKIP = "SKIP"

PHASE_STARTUP = "startup"
PHASE_RUNTIME = "runtime"

EMBED_FIELD_CHAR_LIMIT = 1024
DEFAULT_FALLBACK_CHANNEL_ID = 1460862634143256803
DISCORD_MESSAGE_LIMIT = 2000


@dataclass(slots=True)
class DiagnosticEntry:
    id: str
    timestamp: datetime
    phase: str
    status: str
    fatal: bool
    category: str
    subsystem: str
    stage: str | None
    source: str
    summary: str
    exception_type: str | None = None
    exception_message: str | None = None
    traceback_text: str | None = None
    guild_id: int | None = None
    channel_id: int | None = None
    user_id: int | None = None
    command_name: str | None = None
    interaction_type: str | None = None
    extension_name: str | None = None
    task_name: str | None = None
    extra_context: dict[str, Any] = field(default_factory=dict)
    fingerprint: str = ""


@dataclass(slots=True)
class StartupStageResult:
    stage_name: str
    status: str
    summary: str
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    exception_type: str | None = None
    exception_message: str | None = None
    traceback_text: str | None = None
    fatal: bool = False


class DiagnosticsSettings:
    def __init__(self, *, path: Path | None = None):
        self.path = path or Path("data/diagnostics_settings.json")
        self.discord_notifications_enabled = True
        self._loaded = False

    def load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            if self.path.exists():
                data = json.loads(self.path.read_text(encoding="utf-8"))
                self.discord_notifications_enabled = bool(data.get("discord_notifications_enabled", True))
            else:
                self.save()
        except Exception:
            self.discord_notifications_enabled = True

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "discord_notifications_enabled": bool(self.discord_notifications_enabled),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def toggle(self) -> bool:
        self.discord_notifications_enabled = not self.discord_notifications_enabled
        self.save()
        return self.discord_notifications_enabled


class StartupDiagnostics:
    def __init__(self, *, fallback_channel_id: int = DEFAULT_FALLBACK_CHANNEL_ID, settings: DiagnosticsSettings | None = None):
        self.boot_started_at = datetime.now(timezone.utc)
        self._boot_perf_started = time.perf_counter()
        self._startup_complete = False
        self._entry_seq = 0
        self._dedupe_window_seconds = 2.0
        self._seen_fingerprints: dict[str, datetime] = {}
        self.entries: list[DiagnosticEntry] = []
        self.stages: list[StartupStageResult] = []
        self.fallback_channel_id = fallback_channel_id
        self.settings = settings or DiagnosticsSettings()
        self.settings.load()
        self.owner_id_hint: int | None = None
        self._bot_ref: discord.Client | None = None
        self._report_sent = False
        self._runtime_report_task: asyncio.Task[Any] | None = None
        self._last_runtime_delivery_at: float = 0.0
        self._runtime_delivery_cooldown_seconds: float = 30.0
        self._loop_handler_installed = False
        self._sys_hook_installed = False
        self._old_sys_hook = sys.excepthook
        self._startup_task_names: set[str] = set()
        self.logger = self._build_logger()
        self._install_logging_handler()

        self.logger.info("Diagnostics initialized at %s", self.boot_started_at.isoformat())

    def _build_logger(self) -> logging.Logger:
        logger = logging.getLogger("diagnostics")
        logger.setLevel(logging.INFO)
        log_path = Path("logs/diagnostics.log")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        has_file = any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "").endswith(str(log_path)) for h in logger.handlers)
        if not has_file:
            fh = logging.FileHandler(log_path, encoding="utf-8")
            fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            logger.addHandler(fh)
        return logger

    def _install_logging_handler(self) -> None:
        if getattr(logging, "_pulse_diagnostics_hook_installed", False):
            return

        recorder = self

        class _DiagnosticsHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                if record.levelno < logging.ERROR or not record.exc_info:
                    return
                exc_type, exc, exc_tb = record.exc_info
                if not isinstance(exc, BaseException):
                    return
                tb = "".join(traceback.format_exception(exc_type, exc, exc_tb))
                recorder.capture_exception(
                    exc,
                    phase=recorder.current_phase,
                    status=STATUS_FAIL,
                    category="logger",
                    subsystem=record.name,
                    source="logger.exception",
                    summary=record.getMessage(),
                    traceback_text=tb,
                    fatal=False,
                    extra_context={"logger": record.name},
                )

        root = logging.getLogger()
        root.addHandler(_DiagnosticsHandler())
        setattr(logging, "_pulse_diagnostics_hook_installed", True)

    @property
    def current_phase(self) -> str:
        return PHASE_RUNTIME if self._startup_complete else PHASE_STARTUP

    def mark_startup_complete(self) -> None:
        self._startup_complete = True

    def _next_id(self) -> str:
        self._entry_seq += 1
        return f"diag-{self._entry_seq:06d}"

    def _fingerprint_for(
        self,
        *,
        phase: str,
        subsystem: str,
        source: str,
        exception_type: str | None,
        exception_message: str | None,
        traceback_text: str | None,
    ) -> str:
        trace_hash = hashlib.sha1((traceback_text or "").encode("utf-8", errors="ignore")).hexdigest()[:16]
        raw = "|".join([phase, subsystem, source, exception_type or "", exception_message or "", trace_hash])
        return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()

    def _is_duplicate(self, fingerprint: str, now: datetime) -> bool:
        last = self._seen_fingerprints.get(fingerprint)
        self._seen_fingerprints[fingerprint] = now
        if last is None:
            return False
        return (now - last).total_seconds() <= self._dedupe_window_seconds

    def record_entry(
        self,
        *,
        phase: str,
        status: str,
        fatal: bool,
        category: str,
        subsystem: str,
        source: str,
        summary: str,
        exception_type: str | None = None,
        exception_message: str | None = None,
        traceback_text: str | None = None,
        stage: str | None = None,
        guild_id: int | None = None,
        channel_id: int | None = None,
        user_id: int | None = None,
        command_name: str | None = None,
        interaction_type: str | None = None,
        extension_name: str | None = None,
        task_name: str | None = None,
        extra_context: dict[str, Any] | None = None,
    ) -> DiagnosticEntry | None:
        now = datetime.now(timezone.utc)
        fp = self._fingerprint_for(
            phase=phase,
            subsystem=subsystem,
            source=source,
            exception_type=exception_type,
            exception_message=exception_message,
            traceback_text=traceback_text,
        )
        if self._is_duplicate(fp, now):
            return None

        entry = DiagnosticEntry(
            id=self._next_id(),
            timestamp=now,
            phase=phase,
            status=status,
            fatal=fatal,
            category=category,
            subsystem=subsystem,
            stage=stage,
            source=source,
            summary=summary,
            exception_type=exception_type,
            exception_message=exception_message,
            traceback_text=traceback_text,
            guild_id=guild_id,
            channel_id=channel_id,
            user_id=user_id,
            command_name=command_name,
            interaction_type=interaction_type,
            extension_name=extension_name,
            task_name=task_name,
            extra_context=extra_context or {},
            fingerprint=fp,
        )
        self.entries.append(entry)
        self._log_entry(entry)
        self._schedule_automatic_runtime_delivery(entry)
        return entry

    def _schedule_automatic_runtime_delivery(self, entry: DiagnosticEntry) -> None:
        if entry.phase != PHASE_RUNTIME or entry.status not in {STATUS_FAIL, STATUS_WARN}:
            return
        if self._bot_ref is None or self._runtime_report_task is not None:
            return
        now = time.monotonic()
        if now - self._last_runtime_delivery_at < self._runtime_delivery_cooldown_seconds:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._last_runtime_delivery_at = now
        self._runtime_report_task = loop.create_task(
            self.deliver_report(self._bot_ref, reason="runtime", force_channel=True),
            name="diagnostics.runtime.delivery",
        )

        def _done(task: asyncio.Task[Any]) -> None:
            self._runtime_report_task = None
            try:
                task.result()
            except Exception:
                self.logger.exception("Automatic runtime diagnostics delivery crashed")

        self._runtime_report_task.add_done_callback(_done)

    def capture_exception(
        self,
        error: BaseException,
        *,
        phase: str | None = None,
        status: str = STATUS_FAIL,
        fatal: bool = False,
        category: str = "exception",
        subsystem: str = "general",
        source: str = "unknown",
        summary: str | None = None,
        traceback_text: str | None = None,
        stage: str | None = None,
        guild_id: int | None = None,
        channel_id: int | None = None,
        user_id: int | None = None,
        command_name: str | None = None,
        interaction_type: str | None = None,
        extension_name: str | None = None,
        task_name: str | None = None,
        extra_context: dict[str, Any] | None = None,
    ) -> DiagnosticEntry | None:
        tb = traceback_text or "".join(traceback.format_exception(type(error), error, error.__traceback__))
        return self.record_entry(
            phase=phase or self.current_phase,
            status=status,
            fatal=fatal,
            category=category,
            subsystem=subsystem,
            source=source,
            summary=summary or str(error),
            exception_type=type(error).__name__,
            exception_message=str(error),
            traceback_text=tb,
            stage=stage,
            guild_id=guild_id,
            channel_id=channel_id,
            user_id=user_id,
            command_name=command_name,
            interaction_type=interaction_type,
            extension_name=extension_name,
            task_name=task_name,
            extra_context=extra_context,
        )

    def add_warning(self, message: str, *, stage_name: str | None = None, subsystem: str = "startup") -> None:
        self.record_entry(
            phase=self.current_phase,
            status=STATUS_WARN,
            fatal=False,
            category="warning",
            subsystem=subsystem,
            source=stage_name or subsystem,
            summary=message,
            stage=stage_name,
        )

    def record_failure(self, *, stage_name: str, summary: str, exception: BaseException, traceback_text: str, fatal: bool) -> None:
        self.capture_exception(
            exception,
            phase=PHASE_STARTUP,
            status=STATUS_FAIL,
            fatal=fatal,
            category="startup",
            subsystem="startup",
            source=stage_name,
            summary=summary,
            traceback_text=traceback_text,
            stage=stage_name,
        )

    def _log_entry(self, entry: DiagnosticEntry) -> None:
        msg = f"[{entry.phase}] [{entry.status}] subsystem={entry.subsystem} source={entry.source} summary={entry.summary}"
        if entry.status == STATUS_WARN:
            self.logger.warning(msg)
        else:
            self.logger.error(msg)
        if entry.traceback_text:
            self.logger.error(entry.traceback_text.rstrip())

    def total_duration_ms(self) -> int:
        return int((time.perf_counter() - self._boot_perf_started) * 1000)

    def install_global_exception_hooks(self, loop: asyncio.AbstractEventLoop) -> None:
        if not self._loop_handler_installed:
            previous = loop.get_exception_handler()

            def handle_loop_exception(loop_: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
                try:
                    exception = context.get("exception")
                    message = context.get("message", "Asyncio exception")
                    task = context.get("task")
                    task_name = task.get_name() if task and hasattr(task, "get_name") else "unknown"
                    if isinstance(exception, BaseException):
                        self.capture_exception(
                            exception,
                            category="asyncio",
                            subsystem="tasks",
                            source="asyncio.loop",
                            summary=message,
                            task_name=task_name,
                            extra_context={k: repr(v) for k, v in context.items() if k != "exception"},
                        )
                    else:
                        self.record_entry(
                            phase=self.current_phase,
                            status=STATUS_WARN,
                            fatal=False,
                            category="asyncio",
                            subsystem="tasks",
                            source="asyncio.loop",
                            summary=message,
                            task_name=task_name,
                            extra_context={k: repr(v) for k, v in context.items()},
                        )
                except Exception:
                    self.logger.exception("Failed in custom asyncio loop exception handler")
                if previous is not None:
                    previous(loop_, context)
                else:
                    loop_.default_exception_handler(context)

            loop.set_exception_handler(handle_loop_exception)
            self._loop_handler_installed = True

        if not self._sys_hook_installed:

            def handle_sys_exception(exc_type: type[BaseException], exc_value: BaseException, exc_traceback: Any) -> None:
                tb = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
                self.capture_exception(
                    exc_value,
                    category="unhandled",
                    subsystem="process",
                    source="sys.excepthook",
                    summary="Unhandled top-level exception",
                    traceback_text=tb,
                    fatal=True,
                )
                self._old_sys_hook(exc_type, exc_value, exc_traceback)

            sys.excepthook = handle_sys_exception
            self._sys_hook_installed = True

    async def run_stage(
        self,
        stage_name: str,
        func: Callable[[], Any] | Callable[[], Awaitable[Any]],
        *,
        fatal: bool = False,
        summary_on_pass: str = "Completed",
        summary_on_skip: str | None = None,
    ) -> Any:
        started = datetime.now(timezone.utc)
        perf = time.perf_counter()
        try:
            value = func()
            if asyncio.iscoroutine(value):
                value = await value
            finished = datetime.now(timezone.utc)
            status = STATUS_SKIP if summary_on_skip else STATUS_PASS
            summary = summary_on_skip or summary_on_pass
            self.stages.append(
                StartupStageResult(
                    stage_name=stage_name,
                    status=status,
                    summary=summary,
                    started_at=started,
                    finished_at=finished,
                    duration_ms=int((time.perf_counter() - perf) * 1000),
                    fatal=False,
                )
            )
            if status == STATUS_SKIP:
                self.record_entry(
                    phase=PHASE_STARTUP,
                    status=STATUS_WARN,
                    fatal=False,
                    category="startup",
                    subsystem="startup",
                    source=stage_name,
                    summary=summary,
                    stage=stage_name,
                )
            return value
        except Exception as exc:
            tb = traceback.format_exc()
            finished = datetime.now(timezone.utc)
            self.stages.append(
                StartupStageResult(
                    stage_name=stage_name,
                    status=STATUS_FAIL,
                    summary=str(exc),
                    started_at=started,
                    finished_at=finished,
                    duration_ms=int((time.perf_counter() - perf) * 1000),
                    exception_type=type(exc).__name__,
                    exception_message=str(exc),
                    traceback_text=tb,
                    fatal=fatal,
                )
            )
            self.capture_exception(
                exc,
                phase=PHASE_STARTUP,
                fatal=fatal,
                category="startup",
                subsystem="startup",
                source=stage_name,
                summary=str(exc),
                traceback_text=tb,
                stage=stage_name,
            )
            if fatal:
                raise
            return None

    def add_startup_task(self, task: asyncio.Task[Any]) -> None:
        self._startup_task_names.add(task.get_name())

        def _done(t: asyncio.Task[Any]) -> None:
            try:
                t.result()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.capture_exception(
                    exc,
                    phase=PHASE_STARTUP,
                    category="task",
                    subsystem="tasks",
                    source="startup-task",
                    summary="Startup-created task failed",
                    task_name=t.get_name(),
                )

        task.add_done_callback(_done)

    def attach_task(self, task: asyncio.Task[Any], *, subsystem: str, source: str, recurring: bool = False) -> None:
        def _done(t: asyncio.Task[Any]) -> None:
            try:
                t.result()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.capture_exception(
                    exc,
                    category="task",
                    subsystem=subsystem,
                    source=source,
                    summary="Background task failed",
                    task_name=t.get_name(),
                    extra_context={"recurring": recurring},
                )

        task.add_done_callback(_done)

    def _entries(self, *, phase: str | None = None, status: str | None = None) -> list[DiagnosticEntry]:
        values = self.entries
        if phase:
            values = [e for e in values if e.phase == phase]
        if status:
            values = [e for e in values if e.status == status]
        return values

    def counts(self) -> dict[str, int]:
        fail = len([e for e in self.entries if e.status == STATUS_FAIL])
        warn = len([e for e in self.entries if e.status == STATUS_WARN])
        startup_fail = len([e for e in self.entries if e.phase == PHASE_STARTUP and e.status == STATUS_FAIL])
        runtime_fail = len([e for e in self.entries if e.phase == PHASE_RUNTIME and e.status == STATUS_FAIL])
        fatal = len([e for e in self.entries if e.fatal])
        return {
            "errors": fail,
            "warnings": warn,
            "startup_errors": startup_fail,
            "runtime_errors": runtime_fail,
            "fatal": fatal,
            "passed_stages": len([s for s in self.stages if s.status == STATUS_PASS]),
        }

    def environment_summary(self, bot: discord.Client | None = None) -> dict[str, str]:
        user_tag = str(bot.user) if bot and bot.user else "unavailable"
        guild_count = str(len(bot.guilds)) if bot else "unavailable"
        return {
            "python": platform.python_version(),
            "discord_py": getattr(discord, "__version__", "unknown"),
            "platform": platform.platform(),
            "pid": str(os.getpid()),
            "bot_user": user_tag,
            "guilds": guild_count,
        }

    def overall_status(self) -> str:
        c = self.counts()
        if c["fatal"] > 0:
            return "FAILED"
        if c["errors"] > 0:
            return "DEGRADED"
        if c["warnings"] > 0:
            return "WARN"
        return "OK"

    def render_summary_embed(self, bot: discord.Client | None = None) -> discord.Embed:
        counts = self.counts()
        status = self.overall_status()
        if counts["errors"] == 0 and counts["warnings"] == 0:
            desc = "Bot started cleanly. No issues detected."
        else:
            desc = f"Diagnostics recorded {counts['runtime_errors']} runtime errors and {counts['warnings']} warnings."
        if counts["startup_errors"] > 0:
            desc = f"Startup completed with {counts['startup_errors']} error(s)."

        e = discord.Embed(title="Bot Diagnostics Report", description=desc, color=discord.Color.blurple())
        e.add_field(name="Overall Status", value=status, inline=True)
        e.add_field(name="Startup Errors", value=str(counts["startup_errors"]), inline=True)
        e.add_field(name="Runtime Errors", value=str(counts["runtime_errors"]), inline=True)
        e.add_field(name="Warnings", value=str(counts["warnings"]), inline=True)
        e.add_field(name="Fatal", value=str(counts["fatal"]), inline=True)
        e.add_field(name="Passed Startup Stages", value=str(counts["passed_stages"]), inline=True)
        env = self.environment_summary(bot)
        e.add_field(name="Bot", value=f"{env['bot_user']} | guilds={env['guilds']}", inline=False)
        subsystems = sorted({x.subsystem for x in self.entries if x.status == STATUS_FAIL})
        e.add_field(name="Affected Subsystems", value=", ".join(subsystems[:8]) or "None", inline=False)
        if self.entries:
            e.add_field(name="Last Error", value=max(self.entries, key=lambda x: x.timestamp).timestamp.isoformat(), inline=False)
        e.timestamp = datetime.now(timezone.utc)
        return e

    def _chunk(self, title: str, lines: list[str], color: discord.Color) -> list[discord.Embed]:
        if not lines:
            return [discord.Embed(title=title, description="None", color=color)]
        embeds: list[discord.Embed] = []
        buf: list[str] = []
        size = 0
        for line in lines:
            if size + len(line) + 1 > 3500 and buf:
                embeds.append(discord.Embed(title=title, description="\n".join(buf), color=color))
                buf = [line]
                size = len(line)
            else:
                buf.append(line)
                size += len(line) + 1
        if buf:
            embeds.append(discord.Embed(title=title, description="\n".join(buf), color=color))
        return embeds

    def _entry_line(self, e: DiagnosticEntry) -> str:
        ctx = []
        if e.command_name:
            ctx.append(f"cmd={e.command_name}")
        if e.task_name:
            ctx.append(f"task={e.task_name}")
        if e.extension_name:
            ctx.append(f"ext={e.extension_name}")
        return (
            f"• `{e.timestamp.isoformat()}` | `{e.subsystem}/{e.source}` | `{e.exception_type or e.category}` | "
            f"fatal={e.fatal} | {textwrap.shorten(e.summary, width=120)}"
            + (f" | {' '.join(ctx)}" if ctx else "")
        )

    def render_entries_embeds(self, *, phase: str | None = None, status: str | None = None, title: str = "Diagnostics") -> list[discord.Embed]:
        entries = self._entries(phase=phase, status=status)
        lines = [self._entry_line(e) for e in entries]
        color = discord.Color.red() if status == STATUS_FAIL else discord.Color.gold() if status == STATUS_WARN else discord.Color.blurple()
        return self._chunk(title, lines, color)

    def render_traceback_embeds(self, *, phase: str | None = None) -> list[discord.Embed]:
        entries = [e for e in self._entries(phase=phase) if e.traceback_text]
        lines = [f"• `{e.id}` {e.subsystem}/{e.source} `{e.exception_type}`\n```py\n{e.traceback_text[-700:]}\n```" for e in entries]
        return self._chunk("Tracebacks", lines, discord.Color.dark_red())

    def render_subsystems_embeds(self) -> list[discord.Embed]:
        buckets: dict[str, int] = {}
        for e in self.entries:
            buckets[e.subsystem] = buckets.get(e.subsystem, 0) + 1
        lines = [f"• `{name}`: {count}" for name, count in sorted(buckets.items(), key=lambda kv: kv[1], reverse=True)]
        return self._chunk("Subsystems", lines, discord.Color.blue())

    def build_report_text(self, bot: discord.Client | None = None) -> str:
        env = self.environment_summary(bot)
        c = self.counts()
        lines = [
            "Bot Diagnostics Report",
            "=" * 80,
            f"Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}",
            f"Boot started (UTC): {self.boot_started_at.isoformat()}",
            f"Duration ms: {self.total_duration_ms()}",
            f"Status: {self.overall_status()}",
            f"Startup errors={c['startup_errors']} Runtime errors={c['runtime_errors']} Warnings={c['warnings']} Fatal={c['fatal']}",
            "",
            "Environment",
            "-" * 80,
        ]
        lines.extend(f"{k}: {v}" for k, v in env.items())
        lines.extend(["", "Entries", "-" * 80])
        for e in self.entries:
            lines.append(f"[{e.phase}][{e.status}] {e.subsystem}/{e.source} | {e.exception_type}: {e.exception_message}")
            lines.append(f"summary: {e.summary}")
            if e.command_name or e.task_name:
                lines.append(f"command={e.command_name} task={e.task_name}")
            if e.traceback_text:
                lines.append("traceback:")
                lines.append(e.traceback_text.rstrip())
            lines.append("")
        return "\n".join(lines)

    def write_local_report_file(self, bot: discord.Client | None = None) -> Path:
        path = Path("logs/diagnostics_report.txt")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.build_report_text(bot), encoding="utf-8")
        return path

    async def resolve_owner_user(self, bot: discord.Client) -> discord.User | None:
        owner_ids = list(getattr(bot, "owner_ids", set()) or set())
        if self.owner_id_hint and self.owner_id_hint not in owner_ids:
            owner_ids.insert(0, self.owner_id_hint)
        for owner_id in owner_ids:
            user = bot.get_user(owner_id)
            if user is None:
                try:
                    user = await bot.fetch_user(owner_id)
                except Exception:
                    user = None
            if user is not None:
                return user
        try:
            app_info = await bot.application_info()
            if app_info.owner:
                return app_info.owner
        except Exception:
            pass
        return None

    async def is_authorized_actor(self, user: discord.abc.User | discord.Member, guild: discord.Guild | None) -> bool:
        user_id = getattr(user, "id", None)
        if user_id is None:
            return False
        if user_id in (getattr(guild, "owner_id", None), self.owner_id_hint):
            return True
        bot = self._bot_ref
        if bot is not None and user_id in (getattr(bot, "owner_ids", set()) or set()):
            return True
        return isinstance(user, discord.Member) and user.guild_permissions.administrator

    def _entry_to_copy_text(self, entry: DiagnosticEntry) -> str:
        lines = [
            f"timestamp: {entry.timestamp.isoformat()}",
            f"phase: {entry.phase}",
            f"status: {entry.status}",
            f"subsystem: {entry.subsystem}",
            f"source: {entry.source}",
            f"exception: {entry.exception_type or 'n/a'}",
            f"message: {entry.exception_message or entry.summary}",
            f"summary: {entry.summary}",
            f"fatal: {entry.fatal}",
            f"entry_id: {entry.id}",
        ]
        if entry.command_name:
            lines.append(f"command: {entry.command_name}")
        if entry.task_name:
            lines.append(f"task: {entry.task_name}")
        if entry.channel_id is not None:
            lines.append(f"channel_id: {entry.channel_id}")
        if entry.guild_id is not None:
            lines.append(f"guild_id: {entry.guild_id}")
        return "\n".join(lines)

    def _code_block_chunks(self, content: str, *, lang: str = "text") -> list[str]:
        safe = content or "No details available."
        fence = f"```{lang}\n"
        suffix = "\n```"
        max_payload = DISCORD_MESSAGE_LIMIT - len(fence) - len(suffix)
        chunks: list[str] = []
        for index in range(0, len(safe), max_payload):
            chunk = safe[index:index + max_payload]
            chunks.append(f"{fence}{chunk}\n```")
        return chunks or [f"{fence}No details available.\n```"]

    def render_entries_text_blocks(self, *, phase: str | None = None, status: str | None = None) -> list[str]:
        entries = self._entries(phase=phase, status=status)
        blocks: list[str] = []
        for entry in entries:
            blocks.extend(self._code_block_chunks(self._entry_to_copy_text(entry), lang="text"))
        return blocks

    def render_traceback_text_blocks(self, *, phase: str | None = None) -> list[str]:
        entries = [e for e in self._entries(phase=phase) if e.traceback_text]
        blocks: list[str] = []
        for entry in entries:
            header = f"entry_id: {entry.id}\nsource: {entry.subsystem}/{entry.source}\ntraceback:"
            blocks.extend(self._code_block_chunks(f"{header}\n{entry.traceback_text.rstrip()}", lang="py"))
        return blocks

    async def _send_text_blocks(self, destination: discord.abc.Messageable, blocks: list[str]) -> None:
        for block in blocks:
            await destination.send(block)

    async def send_report_to_owner_dm(
        self,
        bot: discord.Client,
        *,
        summary: discord.Embed,
        view: discord.ui.View,
        report_path: Path | None,
    ) -> tuple[bool, str | None]:
        owner = await self.resolve_owner_user(bot)
        if owner is None:
            return False, "owner_not_resolved"
        try:
            files = [discord.File(report_path, filename="diagnostics_report.txt")] if report_path else []
            await owner.send(embed=summary, view=view, files=files)
            await self._send_text_blocks(owner, self.render_entries_text_blocks(status=STATUS_FAIL)[-15:])
            await self._send_text_blocks(owner, self.render_traceback_text_blocks()[-8:])
            return True, None
        except discord.Forbidden as exc:
            return False, f"owner_dm_forbidden: {exc}"
        except discord.HTTPException as exc:
            return False, f"owner_dm_http_exception: {exc}"
        except Exception as exc:
            return False, f"owner_dm_failed: {exc}"

    async def send_report_to_channel(
        self,
        bot: discord.Client,
        *,
        summary: discord.Embed,
        view: discord.ui.View,
        report_path: Path | None,
    ) -> tuple[bool, str]:
        channel_id = self.fallback_channel_id
        self.logger.info("Diagnostics channel delivery step: bot.get_channel(%s)", channel_id)
        channel = bot.get_channel(channel_id)
        if channel is None:
            self.logger.warning("Diagnostics channel delivery step failed: get_channel returned None")
            try:
                self.logger.info("Diagnostics channel delivery step: bot.fetch_channel(%s)", channel_id)
                channel = await bot.fetch_channel(channel_id)
            except discord.NotFound as exc:
                return False, f"fetch_channel_not_found: {exc}"
            except discord.Forbidden as exc:
                return False, f"fetch_channel_forbidden_missing_guild_or_permissions: {exc}"
            except discord.HTTPException as exc:
                return False, f"fetch_channel_http_exception: {exc}"
            except Exception as exc:
                return False, f"fetch_channel_failed: {exc}"

        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            return False, f"wrong_channel_type: {type(channel).__name__}"

        permissions = channel.permissions_for(channel.guild.me) if isinstance(channel, discord.TextChannel) and channel.guild else None
        if permissions is not None and not permissions.send_messages:
            return False, "missing_permissions: send_messages"

        try:
            files = [discord.File(report_path, filename="diagnostics_report.txt")] if report_path else []
            await channel.send(embed=summary, view=view, files=files)
            await self._send_text_blocks(channel, self.render_entries_text_blocks(status=STATUS_FAIL)[-20:])
            await self._send_text_blocks(channel, self.render_traceback_text_blocks()[-10:])
            return True, "channel_send_success"
        except discord.NotFound as exc:
            return False, f"channel_send_not_found: {exc}"
        except discord.Forbidden as exc:
            return False, f"channel_send_forbidden_missing_permissions: {exc}"
        except discord.HTTPException as exc:
            return False, f"channel_send_http_exception: {exc}"
        except Exception as exc:
            return False, f"channel_send_failed: {exc}"

    async def deliver_report(
        self,
        bot: discord.Client,
        *,
        reason: str,
        force_channel: bool = False,
    ) -> None:
        summary = self.render_summary_embed(bot)
        view = DiagnosticsReportView(self)
        report_path = self.write_local_report_file(bot)
        counts = self.counts()
        attachment_path = report_path if (counts["errors"] > 0 or counts["warnings"] > 0) else None

        send_errors: list[str] = []
        dm_ok = False
        if not force_channel:
            dm_ok, dm_error = await self.send_report_to_owner_dm(bot, summary=summary, view=view, report_path=attachment_path)
            if dm_error:
                send_errors.append(dm_error)

        channel_ok, channel_status = await self.send_report_to_channel(bot, summary=summary, view=view, report_path=attachment_path)
        if not channel_ok:
            send_errors.append(channel_status)
        self.logger.info(
            "Diagnostics delivery result reason=%s dm_ok=%s channel_ok=%s",
            reason,
            dm_ok,
            channel_ok,
        )
        if send_errors and not channel_ok:
            self.logger.error("Diagnostics delivery failure reason=%s details=%s", reason, " | ".join(send_errors))
            self.write_local_report_file(bot)

    async def send_report(self, bot: discord.Client, *, include_success_message: bool = True) -> None:
        if self._report_sent:
            return
        self._report_sent = True
        self._bot_ref = bot
        self.mark_startup_complete()

        counts = self.counts()
        if counts["errors"] == 0 and counts["warnings"] == 0 and include_success_message:
            pass

        await self.deliver_report(bot, reason="startup")


class DiagnosticsReportView(discord.ui.View):
    def __init__(self, diagnostics: StartupDiagnostics):
        super().__init__(timeout=None)
        self.diagnostics = diagnostics
        self._refresh_toggle_label()

    def _refresh_toggle_label(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button) and child.custom_id == "diag:toggle":
                child.label = f"Diagnostics: {'ON' if self.diagnostics.settings.discord_notifications_enabled else 'OFF'}"

    async def _send_text_blocks(self, interaction: discord.Interaction, blocks: list[str]) -> None:
        if not blocks:
            await interaction.response.send_message("No data available.", ephemeral=True)
            return
        await interaction.response.send_message(blocks[0], ephemeral=True)
        for block in blocks[1:]:
            await interaction.followup.send(block, ephemeral=True)

    @discord.ui.button(label="Summary", style=discord.ButtonStyle.primary, custom_id="diag:summary")
    async def summary_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_message(embed=self.diagnostics.render_summary_embed(self.diagnostics._bot_ref), ephemeral=True)

    @discord.ui.button(label="Startup Errors", style=discord.ButtonStyle.danger, custom_id="diag:startup_errors")
    async def startup_errors_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._send_text_blocks(interaction, self.diagnostics.render_entries_text_blocks(phase=PHASE_STARTUP, status=STATUS_FAIL))

    @discord.ui.button(label="Runtime Errors", style=discord.ButtonStyle.danger, custom_id="diag:runtime_errors")
    async def runtime_errors_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._send_text_blocks(interaction, self.diagnostics.render_entries_text_blocks(phase=PHASE_RUNTIME, status=STATUS_FAIL))

    @discord.ui.button(label="Warnings", style=discord.ButtonStyle.secondary, custom_id="diag:warnings")
    async def warnings_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._send_text_blocks(interaction, self.diagnostics.render_entries_text_blocks(status=STATUS_WARN))

    @discord.ui.button(label="Tracebacks", style=discord.ButtonStyle.secondary, custom_id="diag:tracebacks")
    async def tracebacks_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._send_text_blocks(interaction, self.diagnostics.render_traceback_text_blocks())

    @discord.ui.button(label="Subsystems", style=discord.ButtonStyle.secondary, custom_id="diag:subsystems")
    async def subsystems_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        buckets: dict[str, int] = {}
        for entry in self.diagnostics.entries:
            buckets[entry.subsystem] = buckets.get(entry.subsystem, 0) + 1
        lines = [f"{name}: {count}" for name, count in sorted(buckets.items(), key=lambda kv: kv[1], reverse=True)]
        blocks = self.diagnostics._code_block_chunks("\n".join(lines) if lines else "No data available.")
        await self._send_text_blocks(interaction, blocks)

    @discord.ui.button(label="Recent Errors", style=discord.ButtonStyle.secondary, custom_id="diag:recent")
    async def recent_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        recent = self.diagnostics.entries[-25:]
        lines = [self.diagnostics._entry_to_copy_text(e) for e in reversed(recent)]
        blocks = self.diagnostics._code_block_chunks("\n\n".join(lines) if lines else "No data available.")
        await self._send_text_blocks(interaction, blocks)

    @discord.ui.button(label="Diagnostics: ON", style=discord.ButtonStyle.success, custom_id="diag:toggle")
    async def toggle_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self.diagnostics.is_authorized_actor(interaction.user, interaction.guild):
            await interaction.response.send_message("You are not allowed to change diagnostics settings.", ephemeral=True)
            return
        enabled = self.diagnostics.settings.toggle()
        self._refresh_toggle_label()
        await interaction.response.edit_message(view=self)
        await interaction.followup.send(
            "Diagnostics notifications enabled." if enabled else "Diagnostics notifications disabled.",
            ephemeral=True,
        )


def format_exception_brief(exc: BaseException) -> str:
    return textwrap.shorten(f"{type(exc).__name__}: {exc}", width=240, placeholder="...")
