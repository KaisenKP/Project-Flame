from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any


TaskFailureHandler = Callable[[asyncio.Task[Any], BaseException], None]


class TaskSupervisor:
    """Own every background task so shutdown never leaves orphaned work behind."""

    def __init__(self, *, logger: logging.Logger, on_failure: TaskFailureHandler | None = None) -> None:
        self._logger = logger
        self._on_failure = on_failure
        self._tasks: set[asyncio.Task[Any]] = set()

    @property
    def tasks(self) -> tuple[asyncio.Task[Any], ...]:
        return tuple(self._tasks)

    def spawn(self, awaitable: Awaitable[Any], *, name: str) -> asyncio.Task[Any]:
        task = asyncio.create_task(awaitable, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._on_done)
        return task

    def _on_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            if self._on_failure is not None:
                self._on_failure(task, exc)
            else:
                self._logger.exception("Background task crashed: %s", task.get_name())

    async def cancel_all(self) -> None:
        tasks = tuple(self._tasks)
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
