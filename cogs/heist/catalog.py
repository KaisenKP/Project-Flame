from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HeistTarget:
    key: str
    name: str
    difficulty: str
    min_crew: int
    rec_crew: int
    duration_sec: int
    entry_cost: int
    payout_min: int
    payout_max: int
    rep_req: int
    heat_add: int
    risk: str
    identity: str


TARGETS: list[HeistTarget] = [
    HeistTarget("metro_credit_union", "Metro Credit Union", "Beginner", 2, 3, 210, 600_000, 4_000_000, 8_500_000, 0, 7, "Low", "Quick intro run with forgiving pressure."),
    HeistTarget("harbor_deposit", "Harbor Deposit Annex", "Early-Mid", 2, 3, 260, 1_800_000, 10_000_000, 19_000_000, 80, 12, "Medium", "More guards, tighter timing, louder exits."),
    HeistTarget("skyline_bullion", "Skyline Bullion Floor", "Mid-High", 3, 4, 320, 5_200_000, 24_000_000, 45_000_000, 250, 18, "High", "Surveillance-heavy money floor with cursed keypads."),
    HeistTarget("federal_reserve_annex", "Federal Reserve Annex", "High", 3, 4, 390, 12_000_000, 56_000_000, 95_000_000, 500, 26, "Severe", "Big money, louder response, messy escapes."),
    HeistTarget("aurora_mint", "Aurora Mint Blackout", "Endgame", 4, 4, 480, 30_000_000, 130_000_000, 220_000_000, 900, 38, "Nightmare", "Cinematic jackpot job with nasty lockdown risk."),
]

TARGET_BY_KEY = {t.key: t for t in TARGETS}

ROLES = ("hacker", "ghost", "enforcer", "driver")
