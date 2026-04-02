from __future__ import annotations

import os


def _clean(v: str | None) -> str:
    return (v or "").strip()


def _int(v: str | None, default: int) -> int:
    s = _clean(v)
    if not s:
        return default
    try:
        return int(s)
    except ValueError:
        return default


def _truthy(v: str | None, default: bool = False) -> bool:
    s = _clean(v).lower()
    if not s:
        return default
    return s in {"1", "true", "yes", "y", "on"}


VIP_ROLE_ID: int = _int(os.getenv("VIP_ROLE_ID"), 0)
GUILD_ID: int = _int(os.getenv("GUILD_ID"), 0)

STAMINA_DEFAULT_MAX: int = _int(os.getenv("STAMINA_DEFAULT_MAX"), 100)
STAMINA_DEFAULT_START: int = _int(os.getenv("STAMINA_DEFAULT_START"), 0)

STAMINA_REGEN_PER_HOUR_REGULAR: int = _int(os.getenv("STAMINA_REGEN_PER_HOUR_REGULAR"), 10)
STAMINA_REGEN_PER_HOUR_VIP: int = _int(os.getenv("STAMINA_REGEN_PER_HOUR_VIP"), 30)

WORK_STAMINA_COST: int = _int(os.getenv("WORK_STAMINA_COST"), 1)
