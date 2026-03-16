from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class SlotMachineConfig:
    key: str
    name: str
    min_bet: int
    max_bet: int
    unlock_level: int
    weights: Dict[str, int]
    triple_payouts: Dict[str, float]
    pair_multiplier: float
    jackpot_chance: float


MACHINES: dict[str, SlotMachineConfig] = {
    "classic": SlotMachineConfig(
        key="classic",
        name="Classic Slots",
        min_bet=25,
        max_bet=1000,
        unlock_level=1,
        weights={"cherry": 24, "lemon": 20, "grape": 18, "bell": 14, "clover": 8, "diamond": 6, "fire": 4, "crown": 3, "seven": 2, "skull": 1, "jeff": 0},
        triple_payouts={"cherry": 2.0, "lemon": 2.0, "grape": 2.25, "bell": 4.0, "clover": 5.0, "diamond": 8.0, "fire": 7.0, "crown": 12.0, "seven": 15.0},
        pair_multiplier=1.2,
        jackpot_chance=0.00035,
    ),
    "pirate": SlotMachineConfig(
        key="pirate",
        name="Pirate Slots",
        min_bet=50,
        max_bet=2000,
        unlock_level=5,
        weights={"cherry": 20, "lemon": 18, "grape": 16, "bell": 13, "clover": 10, "diamond": 7, "fire": 6, "crown": 5, "seven": 3, "skull": 2, "jeff": 0},
        triple_payouts={"cherry": 2.2, "lemon": 2.2, "grape": 2.5, "bell": 5.0, "clover": 6.0, "diamond": 10.0, "fire": 11.0, "crown": 17.0, "seven": 22.0},
        pair_multiplier=1.1,
        jackpot_chance=0.0005,
    ),
    "high_roller": SlotMachineConfig(
        key="high_roller",
        name="High Roller Slots",
        min_bet=500,
        max_bet=10000,
        unlock_level=15,
        weights={"cherry": 17, "lemon": 15, "grape": 14, "bell": 13, "clover": 11, "diamond": 10, "fire": 8, "crown": 6, "seven": 4, "skull": 2, "jeff": 0},
        triple_payouts={"cherry": 2.5, "lemon": 2.5, "grape": 3.0, "bell": 6.0, "clover": 8.0, "diamond": 14.0, "fire": 16.0, "crown": 22.0, "seven": 30.0},
        pair_multiplier=1.05,
        jackpot_chance=0.0008,
    ),
    "chaos": SlotMachineConfig(
        key="chaos",
        name="Chaos Slots",
        min_bet=100,
        max_bet=5000,
        unlock_level=25,
        weights={"cherry": 18, "lemon": 17, "grape": 16, "bell": 14, "clover": 11, "diamond": 8, "fire": 7, "crown": 5, "seven": 3, "skull": 1, "jeff": 1},
        triple_payouts={"cherry": 2.0, "lemon": 2.0, "grape": 2.5, "bell": 5.0, "clover": 7.0, "diamond": 12.0, "fire": 14.0, "crown": 20.0, "seven": 28.0, "jeff": 40.0},
        pair_multiplier=1.0,
        jackpot_chance=0.0007,
    ),
}


def machine_by_key(machine_key: str) -> SlotMachineConfig:
    return MACHINES.get(machine_key, MACHINES["classic"])
