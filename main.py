from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

import discord
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.traceback import install as rich_traceback_install

# ------------------------------------------------------------
# Path bootstrap
# ------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot import build_bot_from_env  # noqa: E402
from services.startup_diagnostics import StartupDiagnostics, format_exception_brief  # noqa: E402


# ------------------------------------------------------------
# Pretty logging (hardcoded defaults)
# ------------------------------------------------------------

def setup_logging() -> None:
    # Hardcoded vibe:
    # - INFO overall
    # - hide file paths
    # - suppress voice warning
    # - reduce discord + sqlalchemy noise
    rich_traceback_install(show_locals=False)

    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s | %(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(
                rich_tracebacks=True,
                show_time=True,
                show_level=True,
                show_path=False,
                markup=True,
            )
        ],
    )

    logging.getLogger("discord").setLevel(logging.WARNING)
    logging.getLogger("discord.http").setLevel(logging.WARNING)
    logging.getLogger("discord.client").setLevel(logging.ERROR)  # kills PyNaCl warning
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


def print_boot_banner() -> None:
    console = Console()
    console.print(
        Panel.fit(
            "[bold cyan]FlameBot[/bold cyan]\n"
            "[green]Boot sequence engaged[/green]\n"
            "[dim]All Services Run[/dim]",
            border_style="cyan",
        )
    )


log = logging.getLogger("boot")


# ------------------------------------------------------------
# Optional DB table safety
# ------------------------------------------------------------

async def _maybe_create_tables() -> None:
    try:
        from tables import create_tables_if_missing  # type: ignore

        log.info("Ensuring DB tables exist (tables.py)...")
        await create_tables_if_missing()
        log.info("DB tables ensured.")
    except ModuleNotFoundError:
        log.warning("tables.py not found; skipping auto table creation.")
    except Exception:
        log.exception("Failed to ensure DB tables on startup")
        raise


# ------------------------------------------------------------
# Graceful shutdown
# ------------------------------------------------------------

async def shutdown(bot: discord.Client, sig: str) -> None:
    log.warning("Shutdown requested (%s)", sig)
    try:
        await bot.close()
    except Exception:
        log.exception("Error while closing bot")


def install_signal_handlers(loop: asyncio.AbstractEventLoop, bot: discord.Client) -> None:
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(shutdown(bot, "SIGINT")))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(shutdown(bot, "SIGTERM")))
    except NotImplementedError:
        log.warning("Signal handlers not supported on this platform")


# ------------------------------------------------------------
# Main entry
# ------------------------------------------------------------

async def main() -> None:
    diagnostics = StartupDiagnostics()
    diagnostics.record_entry(
        phase="startup",
        status="PASS",
        fatal=False,
        category="startup",
        subsystem="startup",
        source="process_start",
        summary="Process boot sequence started",
        stage="process_start",
    )

    await diagnostics.run_stage("logging_init", setup_logging, summary_on_pass="Rich logging initialized")
    await diagnostics.run_stage("boot_banner", print_boot_banner, summary_on_pass="Boot banner rendered")

    bot: discord.Client | None = None
    try:
        log.info("Booting FlameBot")
        token = (os.getenv("BOT_TOKEN") or "").strip()
        if not token:
            raise RuntimeError("BOT_TOKEN is missing")
        await diagnostics.run_stage("environment_or_config_load", lambda: None, summary_on_pass="Environment and config loaded")

        await diagnostics.run_stage("database_engine_or_session_init", _maybe_create_tables, summary_on_pass="Optional table bootstrap completed")
        bot = await diagnostics.run_stage(
            "bot_build",
            lambda: build_bot_from_env(startup_diagnostics=diagnostics),
            fatal=True,
            summary_on_pass="Bot instance constructed",
        )
        assert bot is not None

        loop = asyncio.get_running_loop()
        diagnostics.install_global_exception_hooks(loop)
        install_signal_handlers(loop, bot)

        await diagnostics.run_stage("bot_login_and_start", lambda: bot.start(token), fatal=True, summary_on_pass="bot.start completed")
    except discord.LoginFailure as exc:
        diagnostics.capture_exception(exc, phase="startup", fatal=True, category="discord", subsystem="startup", source="bot_login_and_start", summary="Discord rejected BOT_TOKEN")
        diagnostics.logger.error("Invalid BOT_TOKEN (Discord rejected it).")
    except Exception as exc:
        diagnostics.capture_exception(exc, phase="startup", fatal=True, category="startup", subsystem="startup", source="main_runtime", summary=format_exception_brief(exc))
        log.exception("Bot crashed")
    finally:
        diagnostics.write_local_report_file(bot)
        if bot is not None:
            try:
                if not bot.is_closed():
                    await bot.close()
            except Exception:
                log.exception("Failed during final bot.close()")


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    asyncio.run(main())
