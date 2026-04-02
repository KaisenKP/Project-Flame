from __future__ import annotations

from .catalog import HeistTarget


def cooldown_seconds(target: HeistTarget, *, outcome: str) -> int:
    base = int(target.duration_sec * 2.8)
    if outcome == "clean":
        return int(base * 1.15)
    if outcome == "busted":
        return int(base * 0.8)
    return base


def payout_from_progress(target: HeistTarget, *, progress: int, strikes: int, alarm: int) -> int:
    span = target.payout_max - target.payout_min
    base = target.payout_min + max(0, min(span, int(span * max(0, progress) / 100)))
    penalty = 1.0 - (strikes * 0.17) - (max(0, alarm - 65) / 260)
    return max(0, int(base * max(0.12, penalty)))


def split_even(total: int, user_ids: list[int]) -> dict[int, int]:
    if not user_ids:
        return {}
    each = total // len(user_ids)
    remainder = total % len(user_ids)
    out = {uid: each for uid in user_ids}
    for uid in user_ids[:remainder]:
        out[uid] += 1
    return out
