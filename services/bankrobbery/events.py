from __future__ import annotations

import random
from collections.abc import Iterable

from .catalog import BankApproach, EventDefinition, FinalePhase


EVENTS: tuple[EventDefinition, ...] = (
    EventDefinition("security_loop", "Security Loop", "Camera feeds freeze for a precious window.", FinalePhase.LOOT, 14, {"alert_delta": -6, "loot_mult_bp": 11200}, positive=True),
    EventDefinition("bonus_cart", "Bonus Cart", "An overlooked cart adds pure upside.", FinalePhase.LOOT, 12, {"loot_flat": 4_000_000}, positive=True),
    EventDefinition("fast_crack", "Fast Crack", "The vault crack finishes early.", FinalePhase.VAULT, 10, {"vault_delta": 18}, positive=True),
    EventDefinition("inside_tip", "Inside Tip", "A quiet route opens for the escape.", FinalePhase.ESCAPE, 10, {"escape_delta": 14, "heat_delta": -3}, positive=True),
    EventDefinition("calm_window", "Calm Window", "The next loot round is safer than expected.", FinalePhase.LOOT, 10, {"alert_delta": -4, "next_round_safe": 1}, positive=True),
    EventDefinition("camera_sweep", "Camera Sweep", "A fresh sweep makes clean movement harder.", FinalePhase.ENTRY, 14, {"entry_delta": -12, "alert_delta": 8}, approaches=(BankApproach.SILENT, BankApproach.CON)),
    EventDefinition("guard_rotation", "Guard Rotation", "The guard pattern tightens.", FinalePhase.LOOT, 12, {"alert_delta": 10}),
    EventDefinition("jammed_door", "Jammed Door", "A jammed access point burns precious time.", FinalePhase.VAULT, 12, {"vault_delta": -14, "alert_delta": 4}),
    EventDefinition("dye_pack_panic", "Dye Pack Panic", "Bagged cash gets riskier to secure.", FinalePhase.LOOT, 11, {"loot_loss_bp": 1200, "alert_delta": 5}),
    EventDefinition("roadblock", "Roadblock", "A response unit clogs the main route.", FinalePhase.ESCAPE, 13, {"escape_delta": -16, "heat_delta": 5}),
    EventDefinition("backup_dispatch", "Backup Dispatch", "Dispatch spikes pressure across the district.", FinalePhase.ESCAPE, 10, {"escape_delta": -10, "alert_delta": 12}),
    EventDefinition("hidden_boxes", "Hidden Deposit Boxes", "A tucked-away cache pushes the greed meter higher.", FinalePhase.LOOT, 5, {"loot_flat": 9_000_000, "alert_delta": 4}, positive=True, rare=True),
    EventDefinition("manager_shortcut", "Dirty Manager Shortcut", "An internal shortcut trims the route dramatically.", FinalePhase.ENTRY, 4, {"entry_delta": 18, "vault_delta": 8}, positive=True, rare=True, approaches=(BankApproach.CON,)),
    EventDefinition("security_panic", "Security Panic Error", "Security overreacts and creates a hole in coverage.", FinalePhase.VAULT, 5, {"vault_delta": 15, "alert_delta": -3}, positive=True, rare=True),
    EventDefinition("elite_response", "Elite Response", "An elite response unit hits the route; the bag is still live if you punch through.", FinalePhase.ESCAPE, 4, {"escape_delta": -22, "loot_mult_bp": 10800, "rare_reward_chance_bp": 1500}, rare=True),
)


def events_for_phase(*, phase: FinalePhase, approach: BankApproach) -> list[EventDefinition]:
    results: list[EventDefinition] = []
    for event in EVENTS:
        if event.phase != phase:
            continue
        if event.approaches and approach not in event.approaches:
            continue
        results.append(event)
    return results


def choose_weighted_event(
    *,
    rng: random.Random,
    phase: FinalePhase,
    approach: BankApproach,
    positive_bias: int = 0,
    negative_bias: int = 0,
    excluded: Iterable[str] = (),
) -> EventDefinition | None:
    pool = [e for e in events_for_phase(phase=phase, approach=approach) if e.key not in set(excluded)]
    if not pool:
        return None
    weighted: list[tuple[EventDefinition, int]] = []
    for event in pool:
        weight = int(event.weight)
        if event.positive:
            weight += positive_bias
        else:
            weight += negative_bias
        weighted.append((event, max(weight, 1)))
    total = sum(weight for _, weight in weighted)
    roll = rng.randint(1, total)
    cursor = 0
    for event, weight in weighted:
        cursor += weight
        if roll <= cursor:
            return event
    return weighted[-1][0]
