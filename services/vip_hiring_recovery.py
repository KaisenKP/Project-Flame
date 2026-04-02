from __future__ import annotations

from services.vip_hiring_service import VipHiringService


async def reconcile_incomplete_jobs(*, service: VipHiringService) -> int:
    return await service.recover_stale_jobs()
