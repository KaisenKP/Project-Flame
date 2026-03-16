from __future__ import annotations

import random

FLAVOR_TEXTS = {
    "jackpot": [
        "The casino lights explode in celebration!",
        "Absolute insanity on the reels!",
    ],
    "strong_win": [
        "Huge hit! Keep the heat rolling.",
        "The reels are on your side right now.",
    ],
    "normal_win": [
        "Solid pull. Silver secured.",
        "A clean win. Nice spin.",
    ],
    "near_miss": [
        "So close… the reels almost lined up.",
        "The jackpot was one symbol away.",
    ],
    "cold_spin": [
        "Cold spin. The machine stays hungry.",
        "No bite this time. Reset and fire again.",
    ],
}


def flavor(category: str) -> str:
    return random.choice(FLAVOR_TEXTS.get(category, FLAVOR_TEXTS["cold_spin"]))


def lucky_spin_roll() -> tuple[bool, float]:
    if random.random() <= 0.03:
        return True, random.choice([2.0, 2.5, 3.0, 4.0, 5.0])
    return False, 1.0


def near_miss_bonus() -> tuple[str, float, float]:
    """returns type, refund_pct, next_mult"""
    r = random.random()
    if r < 0.4:
        return "refund", random.choice([0.1, 0.15, 0.2]), 1.0
    if r < 0.75:
        return "next_mult", 0.0, random.choice([1.15, 1.2, 1.25])
    return "luck", 0.0, 1.0


def chaos_modifier() -> tuple[bool, float, str]:
    if random.random() > 0.03:
        return False, 1.0, ""
    mult = round(random.uniform(0.5, 4.0), 2)
    return True, mult, f"⚡ CHAOS SPIN - Payout modifier {mult}x"


def jeff_event_roll() -> bool:
    return random.random() <= 0.0008
