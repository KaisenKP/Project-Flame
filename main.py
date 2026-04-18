from __future__ import annotations

import asyncio
import importlib
import importlib.util
import logging
import os
import signal
import sys
import traceback
from pathlib import Path
from typing import Any

import discord

# ------------------------------------------------------------
# Path bootstrap
# ------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.startup_diagnostics import StartupDiagnostics, format_exception_brief  # noqa: E402


# ------------------------------------------------------------
# Optional Rich helpers + pretty logging (hardcoded defaults)
# ------------------------------------------------------------


def _has_rich() -> bool:
    if importlib.util.find_spec("rich") is None:
        return False
    return (
        importlib.util.find_spec("rich.console") is not None
        and importlib.util.find_spec("rich.logging") is not None
        and importlib.util.find_spec("rich.panel") is not None
        and importlib.util.find_spec("rich.traceback") is not None
    )


def _rich_install_traceback() -> None:
    rich_traceback_install = importlib.import_module("rich.traceback").install
    rich_traceback_install(show_locals=False)


def _build_rich_handler() -> logging.Handler:
    rich_handler = importlib.import_module("rich.logging").RichHandler
    return rich_handler(
        rich_tracebacks=True,
        show_time=True,
        show_level=True,
        show_path=False,
        markup=True,
    )


def _print_rich_banner() -> None:
    console = importlib.import_module("rich.console").Console()
    panel = importlib.import_module("rich.panel").Panel
    console.print(
        panel.fit(
            "[bold cyan]FlameBot[/bold cyan]\n"
            "[green]Boot sequence engaged[/green]\n"
            "[dim]All Services Run[/dim]",
            border_style="cyan",
        )
    )


def setup_logging() -> None:
    # Hardcoded vibe:
    # - INFO overall
    # - hide file paths
    # - suppress voice warning
    # - reduce discord + sqlalchemy noise
    has_rich = _has_rich()
    if has_rich:
        try:
            _rich_install_traceback()
        except Exception as exc:
            print(f"[boot] Rich traceback install failed, continuing without it: {exc}", file=sys.stderr)
            has_rich = False

    console_handler: logging.Handler
    if has_rich:
        try:
            console_handler = _build_rich_handler()
        except Exception as exc:
            print(f"[boot] Rich console handler failed, using plain StreamHandler: {exc}", file=sys.stderr)
            console_handler = logging.StreamHandler()
    else:
        print("[boot] Optional dependency 'rich' is not installed; using plain logging output.", file=sys.stderr)
        console_handler = logging.StreamHandler()

    if isinstance(console_handler, logging.StreamHandler) and not hasattr(console_handler, "console"):
        console_handler.setFormatter(logging.Formatter("%(name)s | %(message)s"))

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    for existing in list(root_logger.handlers):
        if isinstance(existing, logging.StreamHandler) and not isinstance(existing, logging.FileHandler):
            root_logger.removeHandler(existing)
    root_logger.addHandler(console_handler)

    logging.getLogger("discord").setLevel(logging.WARNING)
    logging.getLogger("discord.http").setLevel(logging.WARNING)
    logging.getLogger("discord.client").setLevel(logging.ERROR)  # kills PyNaCl warning
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


def print_boot_banner() -> None:
    if _has_rich():
        try:
            _print_rich_banner()
            return
        except Exception as exc:
            log.warning("Boot banner failed to render in rich console: %s", exc)
    print("FlameBot | Boot sequence engaged | All Services Run")


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
    if hasattr(bot, "note_shutdown"):
        try:
            bot.note_shutdown(  # type: ignore[attr-defined]
                reason=f"signal:{sig}",
                intentional=False,
                source="main.shutdown",
            )
        except Exception:
            log.exception("Failed to mark shutdown reason for signal=%s", sig)
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
    diagnostics.logger.info("phase=startup status=PASS subsystem=entrypoint source=main.main detail='entering main runtime path'")

    bot: discord.Client | None = None
    start_completed = False
    exit_code = 0
    exit_path = "natural_return"
    try:
        log.info("Booting FlameBot")
        diagnostics.logger.info("phase=startup status=PASS subsystem=config source=main.main detail='config load started'")
        token = (os.getenv("BOT_TOKEN") or "").strip()
        if not token:
            raise RuntimeError("BOT_TOKEN is missing")
        await diagnostics.run_stage("environment_or_config_load", lambda: None, summary_on_pass="Environment and config loaded")
        diagnostics.logger.info("phase=startup status=PASS subsystem=config source=main.main detail='config load completed'")

        diagnostics.logger.info("phase=startup status=PASS subsystem=database source=main.main detail='db init started'")
        await diagnostics.run_stage("database_engine_or_session_init", _maybe_create_tables, summary_on_pass="Optional table bootstrap completed")
        diagnostics.logger.info("phase=startup status=PASS subsystem=database source=main.main detail='db init completed'")

        diagnostics.logger.info("phase=startup status=PASS subsystem=bot source=main.main detail='bot object creation started'")
        bot = await diagnostics.run_stage(
            "bot_build",
            lambda: importlib.import_module("bot").build_bot_from_env(startup_diagnostics=diagnostics),
            fatal=True,
            summary_on_pass="Bot instance constructed",
        )
        diagnostics.logger.info("phase=startup status=PASS subsystem=bot source=main.main detail='bot object created'")
        assert bot is not None

        loop = asyncio.get_running_loop()
        diagnostics.install_global_exception_hooks(loop)
        install_signal_handlers(loop, bot)

        diagnostics.logger.info("phase=startup status=PASS subsystem=discord source=main.main detail='login/connect start'")
        await diagnostics.run_stage("bot_login_and_start", lambda: bot.start(token), fatal=True, summary_on_pass="bot.start completed")
        start_completed = True
        shutdown_reason = getattr(bot, "shutdown_reason", None)
        shutdown_intentional = bool(getattr(bot, "shutdown_intentional", False))
        shutdown_source = getattr(bot, "shutdown_source", "unknown")

        diagnostics.logger.error(
            "phase=runtime status=FAIL subsystem=process source=main.main "
            "detail='bot.start returned' shutdown_intentional=%s shutdown_reason=%s shutdown_source=%s",
            shutdown_intentional,
            shutdown_reason,
            shutdown_source,
        )
        if not shutdown_intentional:
            raise RuntimeError(
                "Process ended without explicit shutdown request: "
                f"reason={shutdown_reason or 'unknown'} source={shutdown_source}"
            )
        exit_path = "intentional_shutdown"
    except discord.LoginFailure as exc:
        exit_code = 1
        exit_path = "discord_login_failure"
        diagnostics.capture_exception(exc, phase="startup", fatal=True, category="discord", subsystem="startup", source="bot_login_and_start", summary="Discord rejected BOT_TOKEN")
        diagnostics.logger.error("Invalid BOT_TOKEN (Discord rejected it).")
        raise
    except SystemExit as exc:
        exit_code = exc.code if isinstance(exc.code, int) else 1
        exit_path = "system_exit"
        diagnostics.logger.error("phase=runtime status=FAIL subsystem=process source=main.main detail='SystemExit raised' code=%s", exc.code)
        raise
    except Exception as exc:
        exit_code = 1
        exit_path = "exception"
        diagnostics.capture_exception(exc, phase="startup", fatal=True, category="startup", subsystem="startup", source="main_runtime", summary=format_exception_brief(exc))
        log.exception("Bot crashed")
        raise
    finally:
        if bot is not None and not start_completed and not getattr(bot, "ready_once", False):
            diagnostics.logger.error(
                "phase=startup status=FAIL subsystem=startup source=main.finally detail='startup aborted before bot became ready'"
            )
        diagnostics.logger.error(
            "phase=runtime status=%s subsystem=process source=main.finally detail='final shutdown path' intentional=%s exit_path=%s",
            "PASS" if exit_code == 0 else "FAIL",
            bool(getattr(bot, "shutdown_intentional", False)) if bot is not None else False,
            exit_path,
        )
        diagnostics.write_local_report_file(bot)
        if bot is not None:
            try:
                if not bot.is_closed():
                    await bot.close()
            except Exception:
                log.exception("Failed during final bot.close()")


def _run() -> int:
    try:
        asyncio.run(main())
        return 0
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
        if code == 0:
            code = 1
            print("[fatal] SystemExit(0) intercepted at process boundary; converting to exit code 1", file=sys.stderr)
        return code
    except KeyboardInterrupt:
        print("[fatal] KeyboardInterrupt reached process boundary", file=sys.stderr)
        return 130
    except BaseException as exc:
        print(f"[fatal] Unhandled process exception: {type(exc).__name__}: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    raise SystemExit(_run())
