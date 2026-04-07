from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import sys
from pathlib import Path
from typing import Iterable

import discord
from discord import app_commands
from discord.ext import commands

from services.error_logging import build_context_from_command, build_context_from_interaction
from services.startup_diagnostics import StartupDiagnostics
from services.startup_manager import StartupManager

log = logging.getLogger("bot")

RESTART_BLOCK_MESSAGE = "The bot is having a scheduled restart please come back in a couple minutes"
EST = dt.timezone(dt.timedelta(hours=-5), name="EST")


def _truthy(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _looks_like_extension(py_file: Path) -> bool:
    try:
        text = py_file.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False

    if "def setup(" in text:
        return True
    if "async def setup(" in text:
        return True
    return False


def _iter_extension_modules(cogs_dir: Path, cogs_package: str) -> list[str]:
    if not cogs_dir.exists():
        return []

    exts: list[str] = []

    package_extension_dirs: set[Path] = set()
    for init_py in cogs_dir.rglob("__init__.py"):
        if _looks_like_extension(init_py):
            package_extension_dirs.add(init_py.parent.resolve())

    for py in cogs_dir.rglob("*.py"):
        if py.name.startswith("_") and py.name != "__init__.py":
            continue
        py_resolved = py.resolve()
        if py.name != "__init__.py" and any(parent in package_extension_dirs for parent in py_resolved.parents):
            continue
        if not _looks_like_extension(py):
            continue

        rel = py.relative_to(cogs_dir).with_suffix("")
        rel_parts = rel.parts[:-1] if rel.name == "__init__" else rel.parts
        exts.append(".".join((cogs_package, *rel_parts)))

    exts.sort()
    return exts




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
        startup_diagnostics: StartupDiagnostics | None = None,
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
        self.pulse_prefix = prefix  # legacy compatibility alias
        self.sync_commands = sync_commands
        self.dev_guild_id = dev_guild_id
        self.owner_ids = owner_ids or set()
        self.startup_diagnostics = startup_diagnostics

        self.cogs_dir = (cogs_dir or Path("cogs")).resolve()
        self.cogs_package = cogs_package

        self._bg_tasks: set[asyncio.Task] = set()
        self._ready_once = asyncio.Event()
        self._startup_report_sent = False
        self._post_ready_boot_completed = False
        self._persistent_views_registered = 0
        self.startup_manager = StartupManager()

    async def setup_hook(self) -> None:
        diag = self.startup_diagnostics
        if diag is not None:
            await diag.run_stage(
                "setup_hook",
                lambda: self.tree.interaction_check(self._interaction_restart_guard),
                summary_on_pass="setup_hook started",
            )
            await diag.run_stage("cog_discovery", lambda: _iter_extension_modules(self.cogs_dir, self.cogs_package), summary_on_pass="Cog discovery completed")
            await diag.run_stage("extension_loading", self.load_all_extensions, summary_on_pass="Extensions load completed")
            await diag.run_stage("database_engine_or_session_init", self._ensure_db_schema, summary_on_pass="DB schema check completed")
        else:
            self.tree.interaction_check(self._interaction_restart_guard)
            await self.load_all_extensions()
            await self._ensure_db_schema()

        cmds = list(self.tree.get_commands())
        log.info("App commands discovered: %d", len(cmds))
        for cmd in cmds:
            log.info(" - /%s", cmd.name)

        if self.sync_commands:
            if diag is not None:
                await diag.run_stage("command_tree_sync", self._sync_app_commands, summary_on_pass="Command tree sync completed")
            else:
                await self._sync_app_commands()
        elif diag is not None:
            await diag.run_stage("command_tree_sync", lambda: None, summary_on_pass="Command sync disabled", summary_on_skip="Command sync disabled by config")

        self.startup_manager.configure_defaults()

        if diag is not None:
            await diag.run_stage("background_task_startup", lambda: self.start_background_tasks(), summary_on_pass="Background tasks started")
            await diag.run_stage(
                "persistent_view_registration",
                lambda: None,
                summary_on_pass=f"Persistent views registered: {self._persistent_views_registered}",
            )
            await diag.run_stage("cache_warmup", self._run_cache_warmup, fatal=True, summary_on_pass="Cache warmup completed")
        else:
            self.start_background_tasks()
            await self._run_cache_warmup()

    async def _run_cache_warmup(self) -> str:
        report = await self.startup_manager.run_category("cache_warmup", bot=self, diagnostics=self.startup_diagnostics)
        if report.failed_required:
            raise RuntimeError(report.summary)
        return report.summary

    async def _run_custom_boot_routines(self) -> str:
        report = await self.startup_manager.run_category("custom_boot", bot=self, diagnostics=self.startup_diagnostics)
        if report.failed_required:
            raise RuntimeError(report.summary)
        return report.summary

    async def _ensure_db_schema(self) -> None:
        try:
            from db import Base
            from db.engine import get_engine

            engine = get_engine()
            async with engine.begin() as conn:
                await conn.run_sync(lambda sync_conn: Base.metadata.create_all(sync_conn, checkfirst=True))
            log.info("DB schema ensured (checkfirst=True)")
        except Exception as exc:
            if self.startup_diagnostics is not None:
                self.startup_diagnostics.capture_exception(
                    exc,
                    category="database",
                    subsystem="database",
                    source="db_schema_init",
                    summary="DB schema ensure failed",
                )
            log.exception("DB schema ensure failed, continuing without crash")

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

    async def on_ready(self) -> None:
        if not self._ready_once.is_set():
            self._ready_once.set()

        if not self._post_ready_boot_completed:
            self._post_ready_boot_completed = True
            diag = self.startup_diagnostics
            if diag is not None:
                await diag.run_stage(
                    "custom_boot_routines",
                    self._run_custom_boot_routines,
                    fatal=True,
                    summary_on_pass="Custom boot routines completed",
                )
            else:
                await self._run_custom_boot_routines()

        assert self.user is not None
        log.info("Ready as %s (id=%s)", self.user, self.user.id)
        log.info("Guilds: %d", len(self.guilds))
        if self.startup_diagnostics is not None:
            await self.startup_diagnostics.run_stage("on_ready", lambda: None, summary_on_pass="on_ready fired")
            if not self._startup_report_sent:
                self._startup_report_sent = True
                send_task = asyncio.create_task(self.startup_diagnostics.send_report(self), name="startup.report.delivery")
                self.startup_diagnostics.add_startup_task(send_task)
                self.startup_diagnostics.mark_startup_complete()

    async def close(self) -> None:
        backup_cog = self.get_cog("EconomyBackupsCog")
        if backup_cog is not None and hasattr(backup_cog, "run_pre_restart_backup"):
            try:
                await backup_cog.run_pre_restart_backup(reason="shutdown")
            except Exception as exc:
                if self.startup_diagnostics is not None:
                    self.startup_diagnostics.capture_exception(
                        exc,
                        category="economy",
                        subsystem="economy",
                        source="pre_shutdown_backup",
                        summary="Pre-shutdown economy backup failed",
                    )
                log.exception("Pre-shutdown economy backup failed")
        await self.stop_background_tasks()
        await super().close()

    async def load_all_extensions(self) -> None:
        exts = _iter_extension_modules(self.cogs_dir, self.cogs_package)

        if not exts:
            log.warning("No extensions found (dir=%s package=%s).", self.cogs_dir, self.cogs_package)
            return

        log.info("Loading %d extension(s) from %s ...", len(exts), self.cogs_dir)

        loaded = 0
        failed = 0

        for ext in exts:
            try:
                await self.load_extension(ext)
                loaded += 1
            except Exception:
                failed += 1
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
        self._spawn_task(self._heartbeat_loop(), name="flamebot.heartbeat")
        self._spawn_task(self._scheduled_restart_loop(), name="flamebot.scheduled_restart")

    def _is_restart_block_window(self, now: dt.datetime | None = None) -> bool:
        now_est = (now or dt.datetime.now(tz=EST)).astimezone(EST)
        return now_est.hour == 0 and now_est.minute == 59

    async def _interaction_restart_guard(self, interaction: discord.Interaction) -> bool:
        if not self._is_restart_block_window():
            return True

        if interaction.response.is_done():
            await interaction.followup.send(RESTART_BLOCK_MESSAGE, ephemeral=True)
        else:
            await interaction.response.send_message(RESTART_BLOCK_MESSAGE, ephemeral=True)
        return False

    async def process_commands(self, message: discord.Message) -> None:
        if message.author.bot:
            return

        ctx = await self.get_context(message)
        if ctx.command and self._is_restart_block_window():
            await message.channel.send(RESTART_BLOCK_MESSAGE)
            return

        await super().process_commands(message)

    async def stop_background_tasks(self) -> None:
        if not self._bg_tasks:
            return

        for t in list(self._bg_tasks):
            t.cancel()

        await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()

    def _spawn_task(self, coro, *, name: str) -> None:
        task = asyncio.create_task(coro, name=name)
        self._bg_tasks.add(task)
        if self.startup_diagnostics is not None:
            self.startup_diagnostics.attach_task(task, subsystem="tasks", source="background_task", recurring=True)

        def _done(_t: asyncio.Task) -> None:
            self._bg_tasks.discard(_t)
            try:
                _t.result()
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                if self.startup_diagnostics is not None:
                    self.startup_diagnostics.capture_exception(
                        exc,
                        category="task",
                        subsystem="tasks",
                        source="background_task.done_callback",
                        summary="Background task crashed",
                        task_name=_t.get_name(),
                    )
                log.exception("Background task crashed: %s", _t.get_name())

        task.add_done_callback(_done)

    async def _heartbeat_loop(self) -> None:
        await self._ready_once.wait()
        while not self.is_closed():
            await asyncio.sleep(60)
            log.debug("Pulse heartbeat tick")

    async def _scheduled_restart_loop(self) -> None:
        await self._ready_once.wait()

        while not self.is_closed():
            now_est = dt.datetime.now(tz=EST)
            target_est = now_est.replace(hour=1, minute=0, second=0, microsecond=0)
            if now_est >= target_est:
                target_est += dt.timedelta(days=1)

            sleep_for = max((target_est - now_est).total_seconds(), 0)
            log.info("Scheduled restart task sleeping for %.0f seconds until %s", sleep_for, target_est.isoformat())
            await asyncio.sleep(sleep_for)

            if self.is_closed():
                return

            log.warning("Executing scheduled restart at 1:00 AM EST")
            await self.close()
            os.execv(sys.executable, [sys.executable, *sys.argv])

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


async def build_bot_from_env(startup_diagnostics: StartupDiagnostics | None = None) -> FlameBot:
    prefix = (os.getenv("BOT_PREFIX") or "!").strip()

    intents_message_content = _truthy(os.getenv("INTENTS_MESSAGE_CONTENT"), default=True)
    sync_commands = _truthy(os.getenv("SYNC_COMMANDS"), default=True)

    cogs_package = (os.getenv("COGS_PACKAGE") or "cogs").strip()
    cogs_dir = Path(os.getenv("COGS_DIR") or "cogs").resolve()

    dev_guild_id: int | None = None
    dev_guild_raw = (os.getenv("DEV_GUILD_ID") or "").strip()
    if dev_guild_raw.isdigit():
        dev_guild_id = int(dev_guild_raw)

    owner_ids: set[int] = set()
    raw_owner_id = (os.getenv("BOT_OWNER_ID") or "").strip()
    if raw_owner_id.isdigit():
        owner_ids.add(int(raw_owner_id))
        if startup_diagnostics is not None:
            startup_diagnostics.owner_id_hint = int(raw_owner_id)
    raw_owner_ids = (os.getenv("BOT_OWNER_IDS") or "").strip()
    if raw_owner_ids:
        for part in raw_owner_ids.replace(",", " ").split():
            if part.isdigit():
                owner_ids.add(int(part))
                if startup_diagnostics is not None and startup_diagnostics.owner_id_hint is None:
                    startup_diagnostics.owner_id_hint = int(part)

    return FlameBot(
        prefix=prefix,
        intents_message_content=intents_message_content,
        cogs_dir=cogs_dir,
        cogs_package=cogs_package,
        sync_commands=sync_commands,
        dev_guild_id=dev_guild_id,
        owner_ids=owner_ids,
        startup_diagnostics=startup_diagnostics,
    )


# Legacy compatibility aliases for pre-FlameBot imports
PulseCommandTree = FlameCommandTree
PulseBot = FlameBot
