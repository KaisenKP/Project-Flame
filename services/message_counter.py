from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Tuple

from sqlalchemy import select

from db.models import ActivityDailyRow
from services.db import sessions
from services.users import ensure_user_rows


def _utc_day():
    return datetime.now(timezone.utc).date()


@dataclass
class _Key:
    guild_id: int
    user_id: int


class MessageCounterService:
    def __init__(self):
        self.sessionmaker = sessions()
        self._lock = asyncio.Lock()
        self._pending: Dict[Tuple[int, int], int] = {}
        self._flush_task: asyncio.Task | None = None
        self._running = False

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            self._flush_task = None
        await self.flush_now()

    async def track_message(self, *, guild_id: int, user_id: int) -> None:
        k = (int(guild_id), int(user_id))
        async with self._lock:
            self._pending[k] = int(self._pending.get(k, 0)) + 1

    async def flush_now(self) -> None:
        async with self._lock:
            if not self._pending:
                return
            snapshot = self._pending
            self._pending = {}

        day = _utc_day()

        async with self.sessionmaker() as session:
            async with session.begin():
                # Batch update per (guild, user)
                for (guild_id, user_id), inc in snapshot.items():
                    if inc <= 0:
                        continue

                    await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                    row = await session.scalar(
                        select(ActivityDailyRow).where(
                            ActivityDailyRow.guild_id == guild_id,
                            ActivityDailyRow.user_id == user_id,
                            ActivityDailyRow.day == day,
                        )
                    )
                    if row is None:
                        row = ActivityDailyRow(
                            guild_id=guild_id,
                            user_id=user_id,
                            day=day,
                            message_count=int(inc),
                            vc_seconds=0,
                            activity_score=0,
                        )
                        session.add(row)
                    else:
                        row.message_count = int(row.message_count) + int(inc)

    async def _flush_loop(self) -> None:
        # Flush often enough to be accurate, not often enough to be expensive
        while self._running:
            try:
                await asyncio.sleep(10.0)
                await self.flush_now()
            except asyncio.CancelledError:
                return
            except Exception:
                # If DB hiccups, we do not crash the bot.
                # Worst case: counts lag until DB is back.
                await asyncio.sleep(5.0)
