from __future__ import annotations

import asyncio
import logging
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
            "[bold cyan]CatBot[/bold cyan]\n"
            "[green]Boot sequence engaged[/green]\n"
            "[dim]All Services Run[/dim]",
            border_style="cyan",
        )
    )


setup_logging()
print_boot_banner()

log = logging.getLogger("boot")


# ------------------------------------------------------------
# Token (keep using env for secret, this should NOT be hardcoded)
# ------------------------------------------------------------

import os  # keep token env, do not hardcode secrets

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
if not BOT_TOKEN:
    raise SystemExit(
        "BOT_TOKEN is missing.\n"
        "Set it in your panel environment variables.\n"
        "Example: BOT_TOKEN=xxxxxxxxxxxxxxxx"
    )


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
    log.info("Booting Project Pulse")

    await _maybe_create_tables()

    bot = await build_bot_from_env()

    loop = asyncio.get_running_loop()
    install_signal_handlers(loop, bot)

    try:
        await bot.start(BOT_TOKEN)
    except discord.LoginFailure:
        log.error("Invalid BOT_TOKEN (Discord rejected it).")
    except Exception:
        log.exception("Bot crashed")
        raise
    finally:
        try:
            if not bot.is_closed():
                await bot.close()
        except Exception:
            log.exception("Failed during final bot.close()")


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    asyncio.run(main())