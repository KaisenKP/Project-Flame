from __future__ import annotations

import asyncio

from flamebot.extensions import DEFAULT_MODULES, configured_extensions
from services.task_supervisor import TaskSupervisor


def test_extension_registry_is_explicit_and_filterable() -> None:
    extensions = configured_extensions()

    assert len(extensions) == len(DEFAULT_MODULES)
    assert "cogs.embed" in extensions
    assert "cogs.selfroles" in extensions
    assert "cogs.youtube_notifications" not in configured_extensions(deny_patterns=("cogs.youtube_*",))
    assert configured_extensions(allow_patterns=("cogs.embed",)) == ("cogs.embed",)


def test_task_supervisor_cancels_owned_tasks() -> None:
    async def scenario() -> None:
        supervisor = TaskSupervisor(logger=__import__("logging").getLogger("test"))
        started = asyncio.Event()

        async def worker() -> None:
            started.set()
            await asyncio.sleep(60)

        task = supervisor.spawn(worker(), name="test.worker")
        await started.wait()
        assert task in supervisor.tasks
        await supervisor.cancel_all()
        assert supervisor.tasks == ()
        assert task.cancelled()

    asyncio.run(scenario())
