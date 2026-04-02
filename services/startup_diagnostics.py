from __future__ import annotations

import asyncio
import json
import logging
import os
import platform
import sys
import textwrap
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import discord

STATUS_PASS = "PASS"
STATUS_WARN = "WARN"
STATUS_FAIL = "FAIL"
STATUS_SKIP = "SKIP"

EMBED_FIELD_CHAR_LIMIT = 1024
MAX_TRACEBACK_PREVIEW = 900
DEFAULT_FALLBACK_CHANNEL_ID = 1460862634143256803


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


@dataclass(slots=True)
class StartupWarning:
    message: str
    timestamp: datetime
    stage_name: str | None = None


class StartupDiagnosticsSettings:
    def __init__(self, *, path: Path | None = None):
        self.path = path or Path("data/startup_diagnostics_settings.json")
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


class StartupReportView(discord.ui.View):
    def __init__(self, diagnostics: "StartupDiagnostics"):
        super().__init__(timeout=None)
        self.diagnostics = diagnostics
        self._refresh_toggle_label()

    def _refresh_toggle_label(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button) and child.custom_id == "startup:toggle_diagnostics":
                child.label = f"Diagnostics: {'ON' if self.diagnostics.settings.discord_notifications_enabled else 'OFF'}"

    async def _send_embeds(self, interaction: discord.Interaction, embeds: list[discord.Embed]) -> None:
        if not embeds:
            await interaction.response.send_message("No data available.", ephemeral=True)
            return
        await interaction.response.send_message(embed=embeds[0], ephemeral=True)
        for embed in embeds[1:]:
            await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="Summary", style=discord.ButtonStyle.primary, custom_id="startup:summary")
    async def summary_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_message(embed=self.diagnostics.render_summary_embed(), ephemeral=True)

    @discord.ui.button(label="Errors", style=discord.ButtonStyle.danger, custom_id="startup:errors")
    async def errors_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._send_embeds(interaction, self.diagnostics.render_errors_embeds())

    @discord.ui.button(label="Warnings", style=discord.ButtonStyle.secondary, custom_id="startup:warnings")
    async def warnings_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._send_embeds(interaction, self.diagnostics.render_warnings_embeds())

    @discord.ui.button(label="Passed Stages", style=discord.ButtonStyle.success, custom_id="startup:passed")
    async def passed_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._send_embeds(interaction, self.diagnostics.render_passed_embeds())

    @discord.ui.button(label="Environment", style=discord.ButtonStyle.secondary, custom_id="startup:environment")
    async def environment_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_message(embed=self.diagnostics.render_environment_embed(), ephemeral=True)

    @discord.ui.button(label="Tracebacks", style=discord.ButtonStyle.danger, custom_id="startup:tracebacks")
    async def tracebacks_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._send_embeds(interaction, self.diagnostics.render_traceback_embeds())

    @discord.ui.button(label="Diagnostics: ON", style=discord.ButtonStyle.secondary, custom_id="startup:toggle_diagnostics")
    async def toggle_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self.diagnostics.is_authorized_actor(interaction.user, interaction.guild):
            await interaction.response.send_message("You are not allowed to change startup diagnostics settings.", ephemeral=True)
            return

        enabled = self.diagnostics.settings.toggle()
        self._refresh_toggle_label()
        await interaction.response.edit_message(view=self)
        msg = "Startup diagnostics notifications enabled" if enabled else "Startup diagnostics notifications disabled"
        await interaction.followup.send(msg, ephemeral=True)


class StartupDiagnostics:
    def __init__(
        self,
        *,
        fallback_channel_id: int = DEFAULT_FALLBACK_CHANNEL_ID,
        settings: StartupDiagnosticsSettings | None = None,
    ):
        self.boot_started_at = datetime.now(timezone.utc)
        self._boot_perf_started = time.perf_counter()
        self.stages: list[StartupStageResult] = []
        self.warnings: list[StartupWarning] = []
        self.unhandled_exceptions: list[str] = []
        self.fallback_channel_id = fallback_channel_id
        self.settings = settings or StartupDiagnosticsSettings()
        self.settings.load()
        self.logger = self._build_logger()
        self._report_sent = False
        self._loop_handler_installed = False
        self._sys_hook_installed = False
        self._old_sys_hook = sys.excepthook
        self._startup_task_names: set[str] = set()
        self.owner_id_hint: int | None = None
        self._bot_ref: discord.Client | None = None

        self.logger.info("Startup diagnostics initialized at %s", self.boot_started_at.isoformat())

    def _build_logger(self) -> logging.Logger:
        logger = logging.getLogger("startup_diagnostics")
        logger.setLevel(logging.INFO)
        log_path = Path("logs/startup.log")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        has_file = any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "").endswith(str(log_path)) for h in logger.handlers)
        if not has_file:
            fh = logging.FileHandler(log_path, encoding="utf-8")
            fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            logger.addHandler(fh)
        return logger

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
                        tb = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
                        self.record_failure(
                            stage_name=f"asyncio-loop:{task_name}",
                            summary=message,
                            exception=exception,
                            traceback_text=tb,
                            fatal=False,
                        )
                    else:
                        self.add_warning(f"Asyncio loop warning: {message}", stage_name=f"asyncio-loop:{task_name}")
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
                self.record_failure(
                    stage_name="sys.excepthook",
                    summary="Unhandled top-level exception",
                    exception=exc_value,
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
            self.stages.append(
                StartupStageResult(
                    stage_name=stage_name,
                    status=STATUS_SKIP if summary_on_skip else STATUS_PASS,
                    summary=summary_on_skip or summary_on_pass,
                    started_at=started,
                    finished_at=finished,
                    duration_ms=int((time.perf_counter() - perf) * 1000),
                    fatal=False,
                )
            )
            self.logger.info("[%s] %s", stage_name, summary_on_skip or summary_on_pass)
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
            self.logger.exception("[%s] failed", stage_name)
            if fatal:
                raise
            return None

    def add_warning(self, message: str, *, stage_name: str | None = None) -> None:
        entry = StartupWarning(message=message, timestamp=datetime.now(timezone.utc), stage_name=stage_name)
        self.warnings.append(entry)
        self.logger.warning("[%s] %s", stage_name or "startup", message)

    def record_failure(
        self,
        *,
        stage_name: str,
        summary: str,
        exception: BaseException,
        traceback_text: str,
        fatal: bool,
    ) -> None:
        now = datetime.now(timezone.utc)
        self.stages.append(
            StartupStageResult(
                stage_name=stage_name,
                status=STATUS_FAIL,
                summary=summary,
                started_at=now,
                finished_at=now,
                duration_ms=0,
                exception_type=type(exception).__name__,
                exception_message=str(exception),
                traceback_text=traceback_text,
                fatal=fatal,
            )
        )
        self.logger.error("[%s] %s | %s", stage_name, type(exception).__name__, summary)

    def add_startup_task(self, task: asyncio.Task[Any]) -> None:
        self._startup_task_names.add(task.get_name())

        def _done(t: asyncio.Task[Any]) -> None:
            try:
                t.result()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
                self.record_failure(
                    stage_name=f"startup-task:{t.get_name()}",
                    summary="Startup-created task failed",
                    exception=exc,
                    traceback_text=tb,
                    fatal=False,
                )

        task.add_done_callback(_done)

    def _counts(self) -> dict[str, int]:
        return {
            "pass": len([s for s in self.stages if s.status == STATUS_PASS]),
            "warn_stage": len([s for s in self.stages if s.status == STATUS_WARN]),
            "fail": len([s for s in self.stages if s.status == STATUS_FAIL]),
            "skip": len([s for s in self.stages if s.status == STATUS_SKIP]),
            "fatal": len([s for s in self.stages if s.status == STATUS_FAIL and s.fatal]),
            "warnings": len(self.warnings),
        }

    def total_duration_ms(self) -> int:
        return int((time.perf_counter() - self._boot_perf_started) * 1000)

    def overall_status(self) -> str:
        counts = self._counts()
        if counts["fatal"] > 0:
            return "FAILED"
        if counts["fail"] > 0:
            return "DEGRADED"
        if counts["warnings"] > 0:
            return "WARN"
        return "OK"

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

    def render_summary_embed(self, bot: discord.Client | None = None) -> discord.Embed:
        counts = self._counts()
        env = self.environment_summary(bot)
        status = self.overall_status()
        description = (
            "Bot started successfully. No startup issues detected."
            if counts["fail"] == 0 and counts["warnings"] == 0
            else f"Bot started with {counts['fail']} startup errors and {counts['warnings']} warnings."
        )
        if status == "FAILED":
            description = "Startup failed before full readiness."

        e = discord.Embed(title="Bot Startup Report", description=description, color=discord.Color.blurple())
        e.add_field(name="Overall Status", value=status, inline=True)
        e.add_field(name="Passed Stages", value=str(counts["pass"]), inline=True)
        e.add_field(name="Warnings", value=str(counts["warnings"]), inline=True)
        e.add_field(name="Errors", value=str(counts["fail"]), inline=True)
        e.add_field(name="Fatal Errors", value=str(counts["fatal"]), inline=True)
        e.add_field(name="Startup Duration", value=f"{self.total_duration_ms()} ms", inline=True)
        e.add_field(name="Environment", value=f"Python {env['python']} | discord.py {env['discord_py']}", inline=False)
        e.add_field(name="Bot", value=f"{env['bot_user']} | guilds={env['guilds']}", inline=False)
        e.timestamp = datetime.now(timezone.utc)
        return e

    def _chunk_lines_embed(self, title: str, lines: list[str], *, color: discord.Color) -> list[discord.Embed]:
        if not lines:
            return [discord.Embed(title=title, description="None", color=color)]
        embeds: list[discord.Embed] = []
        chunk: list[str] = []
        size = 0
        for line in lines:
            if size + len(line) + 1 > 3500 and chunk:
                embeds.append(discord.Embed(title=title, description="\n".join(chunk), color=color))
                chunk = [line]
                size = len(line)
            else:
                chunk.append(line)
                size += len(line) + 1
        if chunk:
            embeds.append(discord.Embed(title=title, description="\n".join(chunk), color=color))
        return embeds

    def render_errors_embeds(self) -> list[discord.Embed]:
        lines = []
        for s in self.stages:
            if s.status != STATUS_FAIL:
                continue
            lines.append(f"• `{s.stage_name}` | `{s.exception_type or 'Error'}` | fatal={s.fatal} | {s.summary[:180]}")
        return self._chunk_lines_embed("Startup Errors", lines, color=discord.Color.red())

    def render_warnings_embeds(self) -> list[discord.Embed]:
        lines = [f"• `{w.stage_name or 'startup'}` | {w.message[:220]}" for w in self.warnings]
        return self._chunk_lines_embed("Startup Warnings", lines, color=discord.Color.gold())

    def render_passed_embeds(self) -> list[discord.Embed]:
        lines = [f"• `{s.stage_name}` | {s.duration_ms}ms | {s.summary}" for s in self.stages if s.status == STATUS_PASS]
        return self._chunk_lines_embed("Passed Startup Stages", lines, color=discord.Color.green())

    def render_environment_embed(self, bot: discord.Client | None = None) -> discord.Embed:
        env = self.environment_summary(bot)
        e = discord.Embed(title="Startup Environment", color=discord.Color.blue())
        for k, v in env.items():
            e.add_field(name=k.replace("_", " ").title(), value=str(v), inline=False)
        return e

    def render_traceback_embeds(self) -> list[discord.Embed]:
        embeds: list[discord.Embed] = []
        for s in self.stages:
            if s.status != STATUS_FAIL or not s.traceback_text:
                continue
            preview = s.traceback_text[-MAX_TRACEBACK_PREVIEW:]
            preview = preview if len(preview) <= EMBED_FIELD_CHAR_LIMIT else preview[-EMBED_FIELD_CHAR_LIMIT:]
            e = discord.Embed(title=f"Traceback: {s.stage_name}", color=discord.Color.dark_red())
            e.add_field(name="Exception", value=f"{s.exception_type}: {s.exception_message}"[:EMBED_FIELD_CHAR_LIMIT], inline=False)
            e.add_field(name="Preview", value=f"```py\n{preview}\n```"[:EMBED_FIELD_CHAR_LIMIT], inline=False)
            embeds.append(e)
        return embeds or [discord.Embed(title="Tracebacks", description="No traceback data", color=discord.Color.dark_red())]

    def build_report_text(self, bot: discord.Client | None = None) -> str:
        env = self.environment_summary(bot)
        counts = self._counts()
        lines = [
            "Bot Startup Diagnostics Report",
            "=" * 80,
            f"Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}",
            f"Boot started (UTC): {self.boot_started_at.isoformat()}",
            f"Duration ms: {self.total_duration_ms()}",
            f"Overall status: {self.overall_status()}",
            f"Pass={counts['pass']} Warn={counts['warnings']} Fail={counts['fail']} Fatal={counts['fatal']} Skip={counts['skip']}",
            "",
            "Environment",
            "-" * 80,
        ]
        lines.extend(f"{k}: {v}" for k, v in env.items())
        lines.extend(["", "Stages", "-" * 80])
        for s in self.stages:
            lines.append(
                f"[{s.status}] {s.stage_name} | duration={s.duration_ms}ms | started={s.started_at.isoformat()} | finished={s.finished_at.isoformat()}"
            )
            lines.append(f"summary: {s.summary}")
            if s.exception_type:
                lines.append(f"exception: {s.exception_type}: {s.exception_message}")
            if s.traceback_text:
                lines.append("traceback:")
                lines.append(s.traceback_text.rstrip())
            lines.append("")
        lines.extend(["Warnings", "-" * 80])
        if not self.warnings:
            lines.append("None")
        else:
            for w in self.warnings:
                lines.append(f"[{w.timestamp.isoformat()}] {w.stage_name or 'startup'}: {w.message}")
        return "\n".join(lines)

    def write_local_report_file(self, bot: discord.Client | None = None) -> Path:
        path = Path("logs/startup_report.txt")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.build_report_text(bot), encoding="utf-8")
        self.logger.info("Wrote startup report file to %s", path)
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

    async def send_startup_report(self, bot: discord.Client) -> None:
        if self._report_sent:
            return
        self._report_sent = True
        self._bot_ref = bot

        report_path = self.write_local_report_file(bot)

        if not self.settings.discord_notifications_enabled:
            self.logger.info("Discord delivery disabled by diagnostics settings; local logs only.")
            return

        summary = self.render_summary_embed(bot)
        view = StartupReportView(self)
        attachments: list[discord.File] = []
        if self._counts()["fail"] > 0 or self._counts()["warnings"] > 0:
            attachments.append(discord.File(report_path, filename="startup_report.txt"))

        send_errors: list[str] = []

        owner = await self.resolve_owner_user(bot)
        if owner is not None:
            try:
                await owner.send(embed=summary, view=view, files=attachments)
                for embed in self.render_errors_embeds()[:4]:
                    await owner.send(embed=embed)
                for embed in self.render_warnings_embeds()[:4]:
                    await owner.send(embed=embed)
                self.logger.info("Startup report sent to owner DM: %s", owner.id)
                return
            except Exception as exc:
                send_errors.append(f"owner_dm_failed: {exc}")
                self.logger.exception("Failed to DM startup report to owner")

        channel = bot.get_channel(self.fallback_channel_id)
        if channel is None:
            try:
                channel = await bot.fetch_channel(self.fallback_channel_id)
            except Exception as exc:
                send_errors.append(f"fetch_fallback_channel_failed: {exc}")
                channel = None

        if isinstance(channel, (discord.TextChannel, discord.Thread, discord.VoiceChannel)):
            try:
                await channel.send(embed=summary, view=view, files=attachments)
                for embed in self.render_errors_embeds()[:4]:
                    await channel.send(embed=embed)
                for embed in self.render_warnings_embeds()[:4]:
                    await channel.send(embed=embed)
                self.logger.info("Startup report sent to fallback channel: %s", self.fallback_channel_id)
                return
            except Exception as exc:
                send_errors.append(f"fallback_channel_send_failed: {exc}")
                self.logger.exception("Failed to send startup report to fallback channel")

        if send_errors:
            self.logger.error("All Discord startup report delivery paths failed: %s", " | ".join(send_errors))


def format_exception_brief(exc: BaseException) -> str:
    return textwrap.shorten(f"{type(exc).__name__}: {exc}", width=240, placeholder="...")
