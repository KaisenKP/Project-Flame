from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _clean(value: str | None) -> str:
    return (value or "").strip()


def _truthy(value: str | None, *, default: bool) -> bool:
    cleaned = _clean(value).lower()
    if not cleaned:
        return default
    return cleaned in {"1", "true", "yes", "y", "on"}


def _optional_int(value: str | None) -> int | None:
    cleaned = _clean(value)
    return int(cleaned) if cleaned.isdigit() else None


def _parse_ids(*values: str | None) -> frozenset[int]:
    ids: set[int] = set()
    for value in values:
        for token in _clean(value).replace(",", " ").split():
            if token.isdigit():
                ids.add(int(token))
    return frozenset(ids)


def _parse_patterns(value: str | None) -> tuple[str, ...]:
    return tuple(token for token in _clean(value).replace(",", " ").split() if token)


def _load_local_dotenv() -> None:
    """Load local development values without overriding SparkedHost variables."""

    environment = _clean(os.getenv("ENV") or os.getenv("APP_ENV") or os.getenv("PY_ENV")).lower()
    if environment in {"prod", "production"}:
        return
    if Path(".env").exists():
        load_dotenv(override=False)


@dataclass(frozen=True, slots=True)
class BotSettings:
    """Validated process configuration shared by the entrypoint and bot."""

    token: str
    prefix: str
    intents_message_content: bool
    sync_commands: bool
    cogs_dir: Path
    cogs_package: str
    dev_guild_id: int | None
    owner_ids: frozenset[int]
    active_extension_patterns: tuple[str, ...]
    inactive_extension_patterns: tuple[str, ...]
    environment: str

    @classmethod
    def from_env(cls) -> "BotSettings":
        _load_local_dotenv()

        token = _clean(os.getenv("BOT_TOKEN") or os.getenv("TOKEN"))
        if not token:
            raise RuntimeError("BOT_TOKEN is missing. Configure it in SparkedHost Apollo environment variables.")

        return cls(
            token=token,
            prefix=_clean(os.getenv("BOT_PREFIX")) or "!",
            intents_message_content=_truthy(os.getenv("INTENTS_MESSAGE_CONTENT"), default=True),
            sync_commands=_truthy(os.getenv("SYNC_COMMANDS"), default=True),
            cogs_dir=Path(_clean(os.getenv("COGS_DIR")) or "cogs").resolve(),
            cogs_package=_clean(os.getenv("COGS_PACKAGE")) or "cogs",
            dev_guild_id=_optional_int(os.getenv("DEV_GUILD_ID")),
            owner_ids=_parse_ids(os.getenv("BOT_OWNER_ID"), os.getenv("BOT_OWNER_IDS")),
            active_extension_patterns=_parse_patterns(os.getenv("ACTIVE_EXTENSIONS")),
            inactive_extension_patterns=_parse_patterns(os.getenv("INACTIVE_EXTENSIONS")),
            environment=_clean(os.getenv("ENV") or os.getenv("APP_ENV") or "production").lower(),
        )
