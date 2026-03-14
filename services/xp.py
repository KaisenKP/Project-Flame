# services/xp.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class XpProgress:
    level: int
    xp_total: int
    xp_into_level: int
    xp_to_next: int
    pct: int


def xp_req_for_next(level: int) -> int:
    level = max(int(level), 1)

    if level <= 50:
        t = level - 1
        return int(round(30 + 6 * t + 0.5 * (t * t)))

    if level <= 80:
        r50 = xp_req_for_next(50)
        x = level - 50
        return int(round(r50 + 50 * x + 2 * (x * x)))

    if level <= 100:
        r80 = xp_req_for_next(80)
        x = level - 80
        return int(round(r80 + 120 * x + 5 * (x * x)))

    r100 = xp_req_for_next(100)
    x = level - 100
    return int(r100 * (2 ** x))


def level_from_xp(xp_total: int) -> Tuple[int, int, int]:
    xp_total = max(int(xp_total), 0)

    level = 1
    while True:
        need = xp_req_for_next(level)
        if xp_total < need:
            break
        xp_total -= need
        level += 1

        if level > 100000:
            break

    xp_into_level = xp_total
    xp_to_next = xp_req_for_next(level)
    return level, xp_into_level, xp_to_next


def get_xp_progress(xp_total: int) -> XpProgress:
    lvl, into, to_next = level_from_xp(xp_total)
    pct = 0
    if to_next > 0:
        pct = int((min(into / to_next, 1.0)) * 100)

    return XpProgress(
        level=lvl,
        xp_total=max(int(xp_total), 0),
        xp_into_level=into,
        xp_to_next=to_next,
        pct=pct,
    )


def render_xp_bar(xp_into_level: int, xp_to_next: int, width: int = 18) -> str:
    xp_to_next = max(int(xp_to_next), 1)
    xp_into_level = max(int(xp_into_level), 0)
    ratio = min(xp_into_level / xp_to_next, 1.0)

    filled = int(round(ratio * width))
    empty = max(width - filled, 0)

    return "▰" * filled + "▱" * empty
