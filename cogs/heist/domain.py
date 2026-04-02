from __future__ import annotations

import random
from dataclasses import dataclass

from .catalog import HeistTarget, ROLES


@dataclass(frozen=True)
class Scenario:
    title: str
    body: str
    d_progress: int
    d_alarm: int
    d_strikes: int


SCENARIOS: dict[str, dict[str, list[Scenario]]] = {
    "entry": {
        "neutral": [
            Scenario("Reception Bought It", "The fake delivery act worked, then staff asked for help carrying boxes upstairs.", 12, 8, 0),
            Scenario("Keypad Villain Arc", "The keypad froze like it wanted character development.", 8, 10, 0),
            Scenario("Suspicious For Sport", "A guard picked someone to stare at for absolutely no reason.", 7, 11, 0),
        ],
        "good": [Scenario("Ghost Glide", "Ghost coverage threaded the hallway timing perfectly.", 16, 3, 0)],
        "bad": [Scenario("Badge Printer Betrayal", "Credentials went blurry at the worst possible second.", 4, 14, 1)],
    },
    "score": {
        "neutral": [
            Scenario("Vault Mood Swing", "The vault tools started acting cursed but still moved numbers.", 15, 9, 0),
            Scenario("Camera Loop Almost", "The loop held long enough to matter, barely.", 14, 8, 0),
            Scenario("Civilian Timing", "Someone chose this exact moment to need directions.", 10, 12, 0),
        ],
        "good": [Scenario("Hacker Speedrun", "Hacker coverage chewed through lock layers like snacks.", 20, 4, 0)],
        "bad": [Scenario("Alarm Echo", "A sensor ping echoed farther than anyone liked.", 8, 15, 1)],
    },
    "escape": {
        "neutral": [
            Scenario("Route Cursed Again", "The escape alley got blocked by peak nonsense.", 13, 10, 0),
            Scenario("Main Character Guard", "One guard started patrolling like a movie trailer.", 10, 12, 0),
        ],
        "good": [Scenario("Driver Miracle", "Driver coverage found a legal-ish route through chaos.", 18, 4, 0)],
        "bad": [Scenario("Tire Drama", "A tire disagreed with reality at speed.", 7, 13, 1)],
    },
}


def assign_roles(user_ids: list[int], *, seed: int) -> dict[int, str]:
    rng = random.Random(seed)
    shuffled = list(user_ids)
    rng.shuffle(shuffled)
    out: dict[int, str] = {}
    for idx, uid in enumerate(shuffled):
        out[uid] = ROLES[idx % len(ROLES)]
    return out


def pick_scenario(*, stage: str, rng: random.Random, coverage: set[str], alarm: int) -> Scenario:
    bucket = "neutral"
    if alarm >= 72:
        bucket = "bad"
    elif any(role in coverage for role in ("ghost", "hacker", "driver", "enforcer")) and rng.random() > 0.55:
        bucket = "good"
    pool = SCENARIOS[stage][bucket] + SCENARIOS[stage]["neutral"]
    return rng.choice(pool)


def stage_for_progress(progress: int) -> str:
    if progress < 33:
        return "entry"
    if progress < 72:
        return "score"
    return "escape"


def crew_quality(roles: list[str]) -> str:
    uniq = len(set(roles))
    if uniq >= 4:
        return "strong"
    if uniq == 3:
        return "decent"
    return "shaky"


def run_condition(*, alarm: int, strikes: int) -> str:
    if strikes >= 3:
        return "barely alive"
    if alarm >= 80:
        return "messy"
    if alarm >= 55:
        return "shaky"
    return "clean"


def outcome_label(*, progress: int, strikes: int, alarm: int) -> str:
    if strikes >= 3 or progress < 38:
        return "busted"
    if progress >= 92 and alarm < 45 and strikes == 0:
        return "clean"
    if progress >= 70:
        return "messy"
    return "partial"
