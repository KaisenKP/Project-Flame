from __future__ import annotations

import asyncio

from sqlalchemy import text

from services.db import sessions

WARN_TABLE = "mod_warnings"
_ready = False
_lock = asyncio.Lock()


async def ensure_warning_table() -> None:
    global _ready
    if _ready:
        return
    sql_warn = f"""
    CREATE TABLE IF NOT EXISTS {WARN_TABLE} (
        id BIGINT NOT NULL AUTO_INCREMENT,
        guild_id BIGINT NOT NULL,
        user_id BIGINT NOT NULL,
        moderator_id BIGINT NOT NULL,
        reason TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY ix_mod_warn_guild_user (guild_id, user_id)
    );
    """
    async with _lock:
        if _ready:
            return
        sessionmaker = sessions()
        async with sessionmaker() as session:
            async with session.begin():
                await session.execute(text(sql_warn))
        _ready = True


async def add_warning(*, guild_id: int, user_id: int, moderator_id: int, reason: str) -> None:
    await ensure_warning_table()
    sql = text(f"INSERT INTO {WARN_TABLE} (guild_id, user_id, moderator_id, reason) VALUES (:g, :u, :m, :r)")
    sessionmaker = sessions()
    async with sessionmaker() as session:
        async with session.begin():
            await session.execute(sql, {"g": int(guild_id), "u": int(user_id), "m": int(moderator_id), "r": reason[:1000]})
