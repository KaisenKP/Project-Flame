from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Iterable

import discord
from discord.ext import commands

log = logging.getLogger("bot")


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

    for py in cogs_dir.rglob("*.py"):
        if py.name.startswith("_") and py.name != "__init__.py":
            continue
        if not _looks_like_extension(py):
            continue

        rel = py.relative_to(cogs_dir).with_suffix("")
        rel_parts = rel.parts[:-1] if rel.name == "__init__" else rel.parts
        exts.append(".".join((cogs_package, *rel_parts)))

    exts.sort()
    return exts


class PulseBot(commands.Bot):
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
        )

        self.pulse_prefix = prefix
        self.sync_commands = sync_commands
        self.dev_guild_id = dev_guild_id
        self.owner_ids = owner_ids or set()

        self.cogs_dir = (cogs_dir or Path("cogs")).resolve()
        self.cogs_package = cogs_package

        self._bg_tasks: set[asyncio.Task] = set()
        self._ready_once = asyncio.Event()

    async def setup_hook(self) -> None:
        await self.load_all_extensions()
        await self._ensure_db_schema()

        cmds = list(self.tree.get_commands())
        log.info("App commands discovered: %d", len(cmds))
        for cmd in cmds:
            log.info(" - /%s", cmd.name)

        if self.sync_commands:
            await self._sync_app_commands()

        self.start_background_tasks()

    async def _ensure_db_schema(self) -> None:
        try:
            from db import Base
            from db.engine import get_engine

            engine = get_engine()
            async with engine.begin() as conn:
                await conn.run_sync(lambda sync_conn: Base.metadata.create_all(sync_conn, checkfirst=True))
            log.info("DB schema ensured (checkfirst=True)")
        except Exception:
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
        except Exception:
            log.exception("App command sync failed")

    async def on_ready(self) -> None:
        if not self._ready_once.is_set():
            self._ready_once.set()

        assert self.user is not None
        log.info("Ready as %s (id=%s)", self.user, self.user.id)
        log.info("Guilds: %d", len(self.guilds))

    async def close(self) -> None:
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
                log.exception("Failed to reload: %s", ext)
        return results

    def start_background_tasks(self) -> None:
        self._spawn_task(self._heartbeat_loop(), name="pulse.heartbeat")

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

        def _done(_t: asyncio.Task) -> None:
            self._bg_tasks.discard(_t)
            try:
                _t.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("Background task crashed: %s", _t.get_name())

        task.add_done_callback(_done)

    async def _heartbeat_loop(self) -> None:
        await self._ready_once.wait()
        while not self.is_closed():
            await asyncio.sleep(60)
            log.debug("Pulse heartbeat tick")


async def build_bot_from_env() -> PulseBot:
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
    raw_owner_ids = (os.getenv("BOT_OWNER_IDS") or "").strip()
    if raw_owner_ids:
        for part in raw_owner_ids.replace(",", " ").split():
            if part.isdigit():
                owner_ids.add(int(part))

    return PulseBot(
        prefix=prefix,
        intents_message_content=intents_message_content,
        cogs_dir=cogs_dir,
        cogs_package=cogs_package,
        sync_commands=sync_commands,
        dev_guild_id=dev_guild_id,
        owner_ids=owner_ids,
    )
