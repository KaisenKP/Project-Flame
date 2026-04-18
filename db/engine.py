from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus, urlparse

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


def _clean(s: str | None) -> str:
    return (s or "").strip()


def _sanitize_host(host_raw: str) -> str:
    host_raw = host_raw.strip()

    # If they gave a URL like http://host/ or https://host:3306/path, parse it.
    if "://" in host_raw:
        u = urlparse(host_raw)
        host_raw = (u.netloc or u.path or "").strip()

    # Remove any trailing slash
    host_raw = host_raw.strip().rstrip("/")

    # If netloc accidentally includes credentials (rare), drop them
    if "@" in host_raw:
        host_raw = host_raw.split("@", 1)[1].strip()

    return host_raw


def _parse_host_and_port(host_raw: str, port_raw: str) -> tuple[str, int]:
    host_raw = _sanitize_host(host_raw)
    port_raw = port_raw.strip()

    # Support host:port inside DB_HOST
    if ":" in host_raw:
        left, right = host_raw.rsplit(":", 1)
        if right.isdigit():
            return left.strip(), int(right)
        if right == "":
            host_raw = left.strip()

    if not port_raw:
        return host_raw, 3306

    if port_raw.isdigit():
        return host_raw, int(port_raw)

    raise RuntimeError(
        f"Invalid DB_PORT value: {port_raw!r}. DB_PORT must be a valid integer."
    )


_DOTENV_LOADED = False


def _maybe_load_local_dotenv() -> None:
    """Load .env for local development only.

    Sparked Host production uses system environment variables directly, so this is
    intentionally a best-effort local fallback and never overrides existing values.
    """

    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return

    env_name = _clean(os.getenv("ENV") or os.getenv("APP_ENV") or os.getenv("PY_ENV")).lower()
    if env_name in {"prod", "production"}:
        _DOTENV_LOADED = True
        return

    if Path(".env").exists():
        load_dotenv(override=False)

    _DOTENV_LOADED = True


@dataclass(frozen=True)
class DbSettings:
    host: str
    port: int
    name: str
    user: str
    password: str
    pool_size: int
    max_overflow: int
    echo: bool
    source: str

    @staticmethod
    def from_env() -> "DbSettings":
        _maybe_load_local_dotenv()

        pool_size_raw = _clean(os.getenv("DB_POOL_SIZE"))
        max_overflow_raw = _clean(os.getenv("DB_MAX_OVERFLOW"))
        pool_size = int(pool_size_raw) if pool_size_raw.isdigit() else 5
        max_overflow = int(max_overflow_raw) if max_overflow_raw.isdigit() else 10

        echo = _clean(os.getenv("SQL_ECHO")).lower() in {"1", "true", "yes", "y", "on"}

        host_in = _clean(os.getenv("DB_HOST"))
        name_in = _clean(os.getenv("DB_NAME"))
        user_in = _clean(os.getenv("DB_USER"))
        password_in = _clean(os.getenv("DB_PASSWORD"))
        port_in = _clean(os.getenv("DB_PORT"))

        missing: list[str] = []
        if not host_in:
            missing.append("DB_HOST")
        if not name_in:
            missing.append("DB_NAME")
        if not user_in:
            missing.append("DB_USER")
        if not password_in:
            missing.append("DB_PASSWORD")

        if missing:
            raise RuntimeError(
                "Missing required database environment variable(s): "
                + ", ".join(missing)
                + ". Configure Sparked Host Apollo system variables for production or add them to a local .env for development."
            )

        host, port = _parse_host_and_port(host_in, port_in)

        return DbSettings(
            host=host,
            port=port,
            name=name_in,
            user=user_in,
            password=password_in,
            pool_size=pool_size,
            max_overflow=max_overflow,
            echo=echo,
            source="process_env",
        )

    def url(self) -> str:
        user = quote_plus(self.user)
        password = quote_plus(self.password)
        return f"mysql+aiomysql://{user}:{password}@{self.host}:{int(self.port)}/{self.name}"


_engine: AsyncEngine | None = None
_Session: async_sessionmaker[AsyncSession] | None = None


def init_engine() -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    global _engine, _Session
    if _engine is not None and _Session is not None:
        return _engine, _Session

    cfg = DbSettings.from_env()

    print(
        "[DB] startup config source=",
        cfg.source,
        f"host={cfg.host}",
        f"port={cfg.port}",
        f"name={cfg.name}",
        f"user={cfg.user}",
        f"pool={cfg.pool_size}",
        f"overflow={cfg.max_overflow}",
        f"echo={cfg.echo}",
        sep="",
    )

    _engine = create_async_engine(
        cfg.url(),
        echo=cfg.echo,
        pool_size=cfg.pool_size,
        max_overflow=cfg.max_overflow,
        pool_pre_ping=True,
        pool_recycle=1800,
    )
    _Session = async_sessionmaker(_engine, expire_on_commit=False, autoflush=False)
    return _engine, _Session


def get_engine() -> AsyncEngine:
    if _engine is None:
        init_engine()
    assert _engine is not None
    return _engine


def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    if _Session is None:
        init_engine()
    assert _Session is not None
    return _Session
