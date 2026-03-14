from __future__ import annotations

import logging
import os
from rich.console import Console
from rich.logging import RichHandler
from rich.traceback import install as rich_traceback_install

def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    rich_traceback_install(
        show_locals=False,   # keep clean
        suppress=[logging],  # reduce noise
    )

    logging.basicConfig(
        level=level,
        format="%(name)s | %(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(
                console=Console(),
                rich_tracebacks=True,
                show_time=True,
                show_level=True,
                show_path=False,   # set True if you want file paths
                markup=True,
            )
        ],
    )

    # Optional: reduce spam from common libs
    logging.getLogger("discord").setLevel(os.getenv("DISCORD_LOG_LEVEL", "WARNING").upper())
    logging.getLogger("sqlalchemy.engine").setLevel(os.getenv("SQL_LOG_LEVEL", "WARNING").upper())