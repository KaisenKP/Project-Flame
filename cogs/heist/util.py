from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def fmt_int(v: int) -> str:
    return f"{int(v):,}"


def pct(v: int, total: int) -> str:
    if total <= 0:
        return "0%"
    return f"{max(0, min(100, int(v * 100 / total)))}%"
