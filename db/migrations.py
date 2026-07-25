from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from db.models import Base

log = logging.getLogger("db.migrations")


MigrationUpgrade = Callable[[AsyncConnection], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    upgrade: MigrationUpgrade


async def _create_core_tables(connection: AsyncConnection) -> None:
    await connection.run_sync(lambda sync_connection: Base.metadata.create_all(sync_connection, checkfirst=True))


MIGRATIONS: tuple[Migration, ...] = (
    Migration(1, "core_sqlalchemy_models", _create_core_tables),
    Migration(2, "embed_asset_references", _create_core_tables),
    Migration(3, "sentinel_persistent_config", _create_core_tables),
)


async def run_migrations(engine: AsyncEngine) -> tuple[int, ...]:
    """Run ordered, idempotent migrations and return versions applied now."""

    async with engine.begin() as connection:
        await connection.execute(
            text(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version INT NOT NULL PRIMARY KEY, "
                "name VARCHAR(128) NOT NULL, "
                "applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
                ")"
            )
        )
        result = await connection.execute(text("SELECT version FROM schema_migrations"))
        applied = {int(version) for version in result.scalars().all()}
        applied_now: list[int] = []

        for migration in MIGRATIONS:
            if migration.version in applied:
                continue
            log.info("Applying database migration %s: %s", migration.version, migration.name)
            await migration.upgrade(connection)
            await connection.execute(
                text("INSERT INTO schema_migrations (version, name) VALUES (:version, :name)"),
                {"version": migration.version, "name": migration.name},
            )
            applied_now.append(migration.version)

    if applied_now:
        log.info("Database migrations applied: %s", ", ".join(str(version) for version in applied_now))
    else:
        log.info("Database schema is current")
    return tuple(applied_now)
