from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Iterable

import discord
from discord import app_commands
from discord.ext import commands

from db.migrations import run_migrations
from flamebot.config import BotSettings
from flamebot.extensions import configured_extensions
from services.error_logging import build_context_from_command, build_context_from_interaction
from services.startup_diagnostics import StartupDiagnostics
from services.task_supervisor import TaskSupervisor

log = logging.getLogger("bot")

class FlameCommandTree(app_commands.CommandTree["FlameBot"]):
    async def on_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        diagnostics = getattr(self.client, "startup_diagnostics", None)
        if diagnostics is not None:
            info = build_context_from_interaction(interaction)
            diagnostics.capture_exception(
                error,
                category="app_command",
                subsystem="interactions",
                source="tree.on_error",
                summary=str(error),
                guild_id=getattr(info.get("guild"), "id", None),
                channel_id=getattr(info.get("channel"), "id", None),
                user_id=getattr(info.get("user"), "id", None),
                command_name=info.get("command_name"),
                interaction_type=info.get("interaction_type"),
                extra_context=info.get("extras", {}),
            )
        await super().on_error(interaction, error)


class FlameBot(commands.Bot):
    def __init__(
        self,
        *,
        prefix: str = "!",
        intents_message_content: bool = True,
        cogs_dir: Path | None = None,
        cogs_package: str = "cogs",
        sync_commands: bool = True,
        dev_guild_id: int | None = None,
        owner_ids: set[int] | None = None,
        active_extension_patterns: list[str] | None = None,
        inactive_extension_patterns: list[str] | None = None,
        startup_diagnostics: StartupDiagnostics | None = None,
        settings: BotSettings | None = None,
    ):
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.messages = True
        intents.voice_states = True
        intents.message_content = intents_message_content

        super().__init__(
            command_prefix=commands.when_mentioned_or(prefix),
            intents=intents,
            help_command=None,
            tree_cls=FlameCommandTree,
        )

        self.flame_prefix = prefix
        self.pulse_prefix = prefix  # legacy alias for older cogs/helpers
        self.sync_commands = sync_commands
        self.dev_guild_id = dev_guild_id
        self.owner_ids = owner_ids or set()
        self.startup_diagnostics = startup_diagnostics
        self.settings = settings
        self.active_extension_patterns = list(active_extension_patterns or ())
        self.inactive_extension_patterns = list(inactive_extension_patterns or ())

        self.cogs_dir = (cogs_dir or Path("cogs")).resolve()
        self.cogs_package = cogs_package

        self._task_supervisor = TaskSupervisor(logger=log, on_failure=self._on_background_task_failure)
        self._ready_once = asyncio.Event()
        self._startup_report_sent = False
        self._persistent_views_registered = 0
        self.shutdown_reason: str | None = None
        self.shutdown_source: str = "unknown"
        self.shutdown_intentional: bool = False

    @property
    def ready_once(self) -> bool:
        return self._ready_once.is_set()

    def note_shutdown(self, *, reason: str, intentional: bool, source: str) -> None:
        self.shutdown_reason = reason
        self.shutdown_intentional = intentional
        self.shutdown_source = source
        level = logging.INFO if intentional else logging.ERROR
        log.log(
            level,
            "Shutdown reason recorded | intentional=%s reason=%s source=%s",
            intentional,
            reason,
            source,
        )

    async def setup_hook(self) -> None:
        diag = self.startup_diagnostics
        if diag is not None:
            diag.logger.info("phase=startup status=PASS subsystem=bot source=FlameBot.setup_hook detail='setup_hook entered'")
        if diag is not None:
            await diag.run_stage(
                "setup_hook",
                lambda: None,
                summary_on_pass="setup_hook started",
            )
            await diag.run_stage("database_migrations", self._ensure_db_schema, fatal=True, summary_on_pass="Database migrations completed")
            await diag.run_stage("extension_loading", self.load_all_extensions, fatal=True, summary_on_pass="Extensions load completed")
        else:
            await self._ensure_db_schema()
            await self.load_all_extensions()

        cmds = list(self.tree.get_commands())
        log.info("App commands discovered: %d", len(cmds))
        for cmd in cmds:
            log.info(" - /%s", cmd.name)

        if self.sync_commands:
            if diag is not None:
                await diag.run_stage("command_tree_sync", self._sync_app_commands, fatal=True, summary_on_pass="Command tree sync completed")
            else:
                await self._sync_app_commands()
        elif diag is not None:
            await diag.run_stage("command_tree_sync", lambda: None, summary_on_pass="Command sync disabled", summary_on_skip="Command sync disabled by config")

        if diag is not None:
            await diag.run_stage("background_task_startup", lambda: self.start_background_tasks(), summary_on_pass="Background tasks started")
            await diag.run_stage(
                "persistent_view_registration",
                lambda: None,
                summary_on_pass=f"Persistent views registered: {self._persistent_views_registered}",
            )
        else:
            self.start_background_tasks()

    async def _ensure_db_schema(self) -> None:
        from db.engine import get_engine

        try:
            applied = await run_migrations(get_engine())
            log.info("Database schema is ready; migrations applied=%s", applied or "none")
        except Exception as exc:
            if self.startup_diagnostics is not None:
                self.startup_diagnostics.capture_exception(
                    exc,
                    category="database",
                    subsystem="database",
                    source="db_migrations",
                    summary="Database migrations failed",
                )
            log.exception("Database migrations failed; refusing to start with an unknown schema")
            raise

    async def _sync_app_commands(self) -> None:
        try:
            if self.dev_guild_id:
                guild = discord.Object(id=self.dev_guild_id)
                self.tree.clear_commands(guild=guild)
                self.tree.copy_global_to(guild=guild)
                synced = await self.tree.sync(guild=guild)
                log.info("Guild-synced %d app command(s) to guild_id=%s", len(synced), self.dev_guild_id)
                for c in synced:
                    log.info("Synced: /%s", c.name)
            else:
                synced = await self.tree.sync()
                log.info("Globally synced %d app command(s).", len(synced))
        except Exception as exc:
            if self.startup_diagnostics is not None:
                self.startup_diagnostics.capture_exception(
                    exc,
                    category="sync",
                    subsystem="sync",
                    source="command_tree_sync",
                    summary="App command sync failed",
                )
            log.exception("App command sync failed")
            raise

    async def on_ready(self) -> None:
        if not self._ready_once.is_set():
            self._ready_once.set()

        assert self.user is not None
        log.info("Ready as %s (id=%s)", self.user, self.user.id)
        log.info("Guilds: %d", len(self.guilds))
        if self.startup_diagnostics is not None:
            await self.startup_diagnostics.run_stage("on_ready", lambda: None, summary_on_pass="on_ready fired")
            self.startup_diagnostics.logger.info("phase=startup status=PASS subsystem=discord source=FlameBot.on_ready detail='on_ready reached'")
            if not self._startup_report_sent:
                self._startup_report_sent = True
                self.startup_diagnostics.mark_startup_complete()
                self.startup_diagnostics.logger.info("phase=startup status=PASS subsystem=startup source=FlameBot.on_ready detail='startup fully complete'")

    async def close(self) -> None:
        if self.shutdown_reason is None:
            self.note_shutdown(
                reason="close() invoked without explicit shutdown request",
                intentional=False,
                source="FlameBot.close",
            )
        await self.stop_background_tasks()
        await super().close()

    async def load_all_extensions(self) -> None:
        exts = configured_extensions(
            package=self.cogs_package,
            allow_patterns=self.active_extension_patterns,
            deny_patterns=self.inactive_extension_patterns,
        )

        if not exts:
            log.warning(
                "No active extensions configured (package=%s allow=%s deny=%s).",
                self.cogs_package,
                self.active_extension_patterns,
                self.inactive_extension_patterns,
            )
            return

        registered = configured_extensions(package=self.cogs_package)
        skipped = sorted(set(registered) - set(exts))
        if skipped:
            log.info("Registered extensions disabled by deployment filters:")
            for ext in skipped:
                log.info(" - %s (filtered)", ext)

        log.info("Loading %d registered extension(s) from %s ...", len(exts), self.cogs_dir)

        loaded = 0
        failed = 0
        failed_exts: list[str] = []

        for ext in exts:
            if self.startup_diagnostics is not None:
                self.startup_diagnostics.logger.info(
                    "phase=startup status=PASS subsystem=extensions source=FlameBot.load_all_extensions detail='extension load start' extension=%s",
                    ext,
                )
            try:
                log.info("Loading extension: %s", ext)
                await self.load_extension(ext)
                loaded += 1
                log.info("Loaded extension: %s", ext)
                if self.startup_diagnostics is not None:
                    self.startup_diagnostics.logger.info(
                        "phase=startup status=PASS subsystem=extensions source=FlameBot.load_all_extensions detail='extension load success' extension=%s",
                        ext,
                    )
            except Exception:
                failed += 1
                failed_exts.append(ext)
                log.error("Did NOT load extension: %s", ext)
                if self.startup_diagnostics is not None:
                    self.startup_diagnostics.capture_exception(
                        sys.exc_info()[1] or RuntimeError(f"Failed to load extension {ext}"),
                        category="extension",
                        subsystem="cogs",
                        source="extension_load",
                        summary=f"Failed to load extension: {ext}",
                        extension_name=ext,
                    )
                log.exception("Failed to load: %s", ext)

        log.info("Extension load summary: %d/%d loaded, %d failed.", loaded, len(exts), failed)
        if failed_exts:
            raise RuntimeError(f"Extension load failure(s): {', '.join(failed_exts)}")

    async def reload_extensions(self, exts: Iterable[str]) -> dict[str, bool]:
        results: dict[str, bool] = {}
        for ext in exts:
            try:
                await self.reload_extension(ext)
                results[ext] = True
            except Exception:
                results[ext] = False
                if self.startup_diagnostics is not None:
                    self.startup_diagnostics.capture_exception(
                        sys.exc_info()[1] or RuntimeError(f"Failed to reload extension {ext}"),
                        category="extension",
                        subsystem="cogs",
                        source="extension_reload",
                        summary=f"Failed to reload extension: {ext}",
                        extension_name=ext,
                    )
                log.exception("Failed to reload: %s", ext)
        return results

    def start_background_tasks(self) -> None:
        self._spawn_task(self._heartbeat_loop(), name="flame.heartbeat")

    async def stop_background_tasks(self) -> None:
        await self._task_supervisor.cancel_all()

    def _spawn_task(self, coro, *, name: str) -> None:
        task = self._task_supervisor.spawn(coro, name=name)
        if self.startup_diagnostics is not None:
            self.startup_diagnostics.attach_task(task, subsystem="tasks", source="background_task", recurring=True)

    def spawn_background_task(self, coro, *, name: str) -> asyncio.Task:
        """Public task boundary for cogs that need tracked delayed work."""

        task = self._task_supervisor.spawn(coro, name=name)
        if self.startup_diagnostics is not None:
            self.startup_diagnostics.attach_task(task, subsystem="tasks", source="cog_background_task", recurring=False)
        return task

    def _on_background_task_failure(self, task: asyncio.Task, exc: BaseException) -> None:
        if self.startup_diagnostics is not None:
            self.startup_diagnostics.capture_exception(
                exc,
                category="task",
                subsystem="tasks",
                source="background_task.done_callback",
                summary="Background task crashed",
                task_name=task.get_name(),
            )
        log.error("Background task crashed: %s", task.get_name(), exc_info=(type(exc), exc, exc.__traceback__))

    async def _heartbeat_loop(self) -> None:
        await self._ready_once.wait()
        while not self.is_closed():
            await asyncio.sleep(60)
            log.debug("Pulse heartbeat tick")

    def add_view(self, view: discord.ui.View, *, message_id: int | None = None) -> None:
        super().add_view(view, message_id=message_id)
        if self.startup_diagnostics is not None:
            self._persistent_views_registered += 1
            self.startup_diagnostics.logger.info("Persistent view registered: %s", type(view).__name__)

    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError) -> None:
        if self.startup_diagnostics is not None:
            info = build_context_from_command(ctx)
            self.startup_diagnostics.capture_exception(
                error,
                category="command",
                subsystem="commands",
                source="on_command_error",
                summary=str(error),
                guild_id=getattr(info.get("guild"), "id", None),
                channel_id=getattr(info.get("channel"), "id", None),
                user_id=getattr(info.get("user"), "id", None),
                command_name=info.get("command_name"),
                extra_context=info.get("extras", {}),
            )

    async def on_error(self, event_method: str, *args, **kwargs) -> None:
        err = sys.exc_info()[1]
        if err is None:
            err = RuntimeError(f"Unhandled event error in {event_method}")
        if self.startup_diagnostics is not None:
            self.startup_diagnostics.capture_exception(
                err,
                category="event",
                subsystem="events",
                source=event_method,
                summary=f"Unhandled listener error in {event_method}",
                extra_context={"arg_count": len(args), "kwarg_keys": list(kwargs.keys())},
            )
        await super().on_error(event_method, *args, **kwargs)


async def build_bot_from_env(
    startup_diagnostics: StartupDiagnostics | None = None,
    *,
    settings: BotSettings | None = None,
) -> FlameBot:
    settings = settings or BotSettings.from_env()
    if startup_diagnostics is not None and settings.owner_ids and startup_diagnostics.owner_id_hint is None:
        startup_diagnostics.owner_id_hint = next(iter(settings.owner_ids))

    return FlameBot(
        prefix=settings.prefix,
        intents_message_content=settings.intents_message_content,
        cogs_dir=settings.cogs_dir,
        cogs_package=settings.cogs_package,
        sync_commands=settings.sync_commands,
        dev_guild_id=settings.dev_guild_id,
        owner_ids=set(settings.owner_ids),
        active_extension_patterns=list(settings.active_extension_patterns),
        inactive_extension_patterns=list(settings.inactive_extension_patterns),
        startup_diagnostics=startup_diagnostics,
        settings=settings,
    )


# Backward compatibility for any in-repo legacy imports while transitioning to FlameBot naming.
PulseCommandTree = FlameCommandTree
PulseBot = FlameBot
