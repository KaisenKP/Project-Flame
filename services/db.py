from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db.engine import get_sessionmaker


def sessions() -> async_sessionmaker[AsyncSession]:
    return get_sessionmaker()
