from __future__ import annotations

import json
import logging
import os
import re
import sys
import traceback
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Iterable, Mapping

import discord
from discord.ext import commands

LOGGER_NAME = "error_monitor"
DEFAULT_LOG_DIR = Path("data/error_dumps")
MAX_RECENT_ERRORS = 100
_SECRET_KEY_PATTERN = re.compile(
    r"(?P<key>(?:token|secret|password|passwd|api[_-]?key|auth(?:orization)?|session|cookie)[^\n:=]{0,40})"
    r"(?P<sep>\s*(?:=|:|=>)\s*)"
    r"(?P<value>[^\s,;]+)",
    re.IGNORECASE,
)
_DISCORD_TOKEN_PATTERN = re.compile(r"[A-Za-z\d_-]{23,28}\.[A-Za-z\d_-]{6,8}\.[A-Za-z\d_-]{27,}")
_BEARER_PATTERN = re.compile(r"Bearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE)


@dataclass(slots=True)
class LoggedError:
    timestamp: str
    error_type: str
    error_message: str
    event_name: str | None
    command_name: str | None
    task_name: str | None
    source: str | None
    file_path: Path
    payload: dict[str, Any]


class ErrorDumpWriter:
    def __init__(self, log_dir: Path = DEFAULT_LOG_DIR, logger: logging.Logger | None = None) -> None:
        self.log_dir = log_dir
        self.logger = logger or logging.getLogger(LOGGER_NAME)
        self._lock = Lock()
        self._recent: deque[LoggedError] = deque(maxlen=MAX_RECENT_ERRORS)
        self.ensure_storage()

    def ensure_storage(self) -> None:
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            self._path_for(datetime.now(timezone.utc)).touch(exist_ok=True)
        except Exception:
            self.logger.exception("Failed to initialize error dump storage at %s", self.log_dir)

    def _path_for(self, now: datetime) -> Path:
        return self.log_dir / f"errors-{now:%Y-%m-%d}.jsonl"

    def recent_errors(self, limit: int = 5) -> list[LoggedError]:
        if limit <= 0:
            return []
        memory_entries = list(self._recent)[-limit:]
        if len(memory_entries) >= limit:
            return memory_entries
        return self.read_recent_errors(limit)

    def read_recent_errors(self, limit: int = 5) -> list[LoggedError]:
        if limit <= 0:
            return []
        entries: list[LoggedError] = []
        for path in sorted(self.log_dir.glob("errors-*.jsonl"), reverse=True):
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except Exception:
                self.logger.exception("Failed reading error dump file %s", path)
                continue
            for line in reversed(lines):
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    self.logger.exception("Failed decoding error dump line from %s", path)
                    continue
                entries.append(LoggedError(
                    timestamp=payload.get("timestamp_utc", "unknown"),
                    error_type=payload.get("error_type", "UnknownError"),
                    error_message=payload.get("error_message", ""),
                    event_name=payload.get("event_name"),
                    command_name=payload.get("command_name"),
                    task_name=payload.get("task_name"),
                    source=payload.get("source"),
                    file_path=path,
                    payload=payload,
                ))
                if len(entries) >= limit:
                    return list(reversed(entries))
        return list(reversed(entries))

    def latest_log_path(self) -> Path:
        return self._path_for(datetime.now(timezone.utc))

    def log_error(
        self,
        error: BaseException,
        *,
        source: str,
        event_name: str | None = None,
        command_name: str | None = None,
        cog_name: str | None = None,
        guild: discord.abc.Snowflake | None = None,
        guild_name: str | None = None,
        channel: discord.abc.Snowflake | None = None,
        channel_name: str | None = None,
        user: discord.abc.Snowflake | None = None,
        username: str | None = None,
        interaction_type: str | None = None,
        task_name: str | None = None,
        extras: dict[str, Any] | None = None,
    ) -> LoggedError | None:
        timestamp = datetime.now(timezone.utc)
        payload = {
            "timestamp_utc": timestamp.isoformat(),
            "source": source,
            "error_type": type(error).__name__,
            "error_message": self._sanitize_text(str(error)),
            "traceback": self._sanitize_text("".join(traceback.format_exception(type(error), error, error.__traceback__))),
            "command_name": command_name,
            "cog_name": cog_name,
            "guild_id": getattr(guild, "id", None),
            "guild_name": self._sanitize_text(guild_name or getattr(guild, "name", None)),
            "channel_id": getattr(channel, "id", None),
            "channel_name": self._sanitize_text(channel_name or getattr(channel, "name", None)),
            "user_id": getattr(user, "id", None),
            "username": self._sanitize_text(username or getattr(user, "name", None) or getattr(user, "display_name", None)),
            "interaction_type": interaction_type,
            "event_name": event_name,
            "task_name": task_name,
            "python_version": sys.version.split()[0],
        }
        if extras:
            payload["extras"] = self._sanitize_value(extras)

        path = self._path_for(timestamp)
        entry = LoggedError(
            timestamp=payload["timestamp_utc"],
            error_type=payload["error_type"],
            error_message=payload["error_message"],
            event_name=event_name,
            command_name=command_name,
            task_name=task_name,
            source=source,
            file_path=path,
            payload=payload,
        )

        try:
            self.logger.error(
                "[%s] %s | command=%s event=%s task=%s guild=%s user=%s",
                source,
                payload["error_message"],
                command_name,
                event_name,
                task_name,
                payload["guild_id"],
                payload["user_id"],
                exc_info=(type(error), error, error.__traceback__),
            )
        except Exception:
            print("[error_monitor] failed to emit error to logger", file=sys.stderr)

        try:
            self.ensure_storage()
            line = json.dumps(payload, ensure_ascii=False)
            with self._lock:
                with path.open("a", encoding="utf-8") as handle:
                    handle.write(line)
                    handle.write("\n")
                self._recent.append(entry)
            return entry
        except Exception as dump_exc:
            try:
                self.logger.exception("Failed to write error dump: %s", dump_exc)
            except Exception:
                print(f"[error_monitor] failed to persist error dump: {dump_exc}", file=sys.stderr)
                print(line if 'line' in locals() else payload, file=sys.stderr)
            return None

    def _sanitize_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            return self._sanitize_text(value)
        if isinstance(value, dict):
            return {self._sanitize_text(str(key)): self._sanitize_value(inner) for key, inner in value.items()}
        if isinstance(value, (list, tuple, set, frozenset)):
            return [self._sanitize_value(item) for item in value]
        return self._sanitize_text(str(value))

    def _sanitize_text(self, value: str | None) -> str | None:
        if value is None:
            return None
        redacted = _DISCORD_TOKEN_PATTERN.sub("[REDACTED_TOKEN]", value)
        redacted = _BEARER_PATTERN.sub("Bearer [REDACTED]", redacted)
        redacted = _SECRET_KEY_PATTERN.sub(lambda match: f"{match.group('key')}{match.group('sep')}[REDACTED]", redacted)
        for env_key in self._sensitive_env_keys():
            secret = os.getenv(env_key)
            if secret:
                redacted = redacted.replace(secret, "[REDACTED]")
        return redacted

    @staticmethod
    def _sensitive_env_keys() -> Iterable[str]:
        for key in os.environ:
            upper = key.upper()
            if any(marker in upper for marker in ("TOKEN", "SECRET", "PASSWORD", "PASS", "KEY", "AUTH", "DATABASE_URL")):
                yield key


def build_context_from_interaction(interaction: discord.Interaction) -> dict[str, Any]:
    channel = interaction.channel
    guild = interaction.guild
    user = interaction.user
    command = interaction.command
    return {
        "command_name": getattr(command, "qualified_name", None) or getattr(command, "name", None),
        "cog_name": getattr(getattr(command, "binding", None), "qualified_name", None),
        "guild": guild,
        "guild_name": getattr(guild, "name", None),
        "channel": channel,
        "channel_name": getattr(channel, "name", None),
        "user": user,
        "username": str(user) if user else None,
        "interaction_type": getattr(interaction.type, "name", str(interaction.type)),
        "extras": {
            "interaction_id": interaction.id,
            "command_failed": getattr(interaction, "command_failed", None),
            "message_id": getattr(interaction.message, "id", None),
            "custom_id": getattr(getattr(interaction, "data", None), "get", lambda _key, _default=None: None)("custom_id"),
        },
    }


def build_context_from_command(ctx: commands.Context[Any]) -> dict[str, Any]:
    command = ctx.command
    cog = ctx.cog
    channel = ctx.channel
    guild = ctx.guild
    author = ctx.author
    return {
        "command_name": getattr(command, "qualified_name", None),
        "cog_name": getattr(cog, "qualified_name", None),
        "guild": guild,
        "guild_name": getattr(guild, "name", None),
        "channel": channel,
        "channel_name": getattr(channel, "name", None),
        "user": author,
        "username": str(author) if author else None,
        "extras": {
            "message_id": getattr(ctx.message, "id", None),
            "invoked_with": ctx.invoked_with,
        },
    }


def merge_logging_context(
    base: Mapping[str, Any] | None = None,
    *,
    extras: Mapping[str, Any] | None = None,
    **extra_fields: Any,
) -> dict[str, Any]:
    context = dict(base or {})
    merged_extras: dict[str, Any] = {}

    existing_extras = context.get("extras")
    if isinstance(existing_extras, Mapping):
        merged_extras.update(existing_extras)
    elif existing_extras is not None:
        merged_extras["context_extras"] = existing_extras

    if extras:
        merged_extras.update(dict(extras))

    for key, value in extra_fields.items():
        if value is not None:
            merged_extras[key] = value

    if merged_extras:
        context["extras"] = merged_extras
    else:
        context.pop("extras", None)

    return context
