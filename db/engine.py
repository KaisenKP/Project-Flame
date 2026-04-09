from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import unquote, urlparse

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from config import db_defaults


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

    return host_raw, 3306


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
        pool_size_raw = _clean(os.getenv("DB_POOL_SIZE"))
        max_overflow_raw = _clean(os.getenv("DB_MAX_OVERFLOW"))
        pool_size = int(pool_size_raw) if pool_size_raw.isdigit() else 5
        max_overflow = int(max_overflow_raw) if max_overflow_raw.isdigit() else 10

        echo = _clean(os.getenv("SQL_ECHO")).lower() in {"1", "true", "yes", "y", "on"}

        db_url = _clean(os.getenv("DATABASE_URL") or os.getenv("DB_URL"))
        if db_url:
            parsed = urlparse(db_url)
            scheme = (parsed.scheme or "").lower()
            if scheme not in {"mysql", "mysql+aiomysql"}:
                raise RuntimeError(
                    "DATABASE_URL/DB_URL must use mysql:// or mysql+aiomysql://"
                )

            host = _clean(parsed.hostname)
            name = _clean((parsed.path or "").lstrip("/"))
            user = _clean(unquote(parsed.username or ""))
            password = _clean(unquote(parsed.password or ""))
            port = int(parsed.port) if parsed.port else 3306

            if not host or not name or not user or not password:
                missing = [
                    key
                    for key, value in {
                        "host": host,
                        "database": name,
                        "user": user,
                        "password": password,
                    }.items()
                    if not value
                ]
                raise RuntimeError(
                    "Incomplete DATABASE_URL/DB_URL. Missing: "
                    + ", ".join(missing)
                )

            return DbSettings(
                host=host,
                port=port,
                name=name,
                user=user,
                password=password,
                pool_size=pool_size,
                max_overflow=max_overflow,
                echo=echo,
                source="process_env",
            )

        host_in = _clean(os.getenv("DB_HOST"))
        name_in = _clean(os.getenv("DB_NAME"))
        user_in = _clean(os.getenv("DB_USER"))
        password_in = _clean(os.getenv("DB_PASS"))

        env_has_all = all((host_in, name_in, user_in, password_in))

        if env_has_all:
            port_in = _clean(os.getenv("DB_PORT"))
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

        fallback_host = _clean(getattr(db_defaults, "DB_HOST", ""))
        fallback_name = _clean(getattr(db_defaults, "DB_NAME", ""))
        fallback_user = _clean(getattr(db_defaults, "DB_USER", ""))
        fallback_pass = _clean(getattr(db_defaults, "DB_PASS", ""))
        fallback_port_raw = str(getattr(db_defaults, "DB_PORT", "")).strip()

        if not fallback_host or not fallback_name or not fallback_user or not fallback_pass:
            missing = [
                key
                for key, value in {
                    "DB_HOST": fallback_host,
                    "DB_NAME": fallback_name,
                    "DB_USER": fallback_user,
                    "DB_PASS": fallback_pass,
                }.items()
                if not value
            ]
            if host_in or name_in or user_in or password_in:
                raise RuntimeError(
                    "Incomplete process DB env vars and fallback config is incomplete. "
                    f"Missing in fallback: {', '.join(missing)}"
                )
            raise RuntimeError(
                "Missing DB env vars and fallback config is incomplete. "
                f"Missing in fallback: {', '.join(missing)}"
            )

        host, port = _parse_host_and_port(fallback_host, fallback_port_raw)

        return DbSettings(
            host=host,
            port=port,
            name=fallback_name,
            user=fallback_user,
            password=fallback_pass,
            pool_size=pool_size,
            max_overflow=max_overflow,
            echo=echo,
            source="repo_fallback",
        )

    def url(self) -> str:
        return (
            f"mysql+aiomysql://{self.user}:{self.password}"
            f"@{self.host}:{int(self.port)}/{self.name}"
            f"?charset=utf8mb4"
        )


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
