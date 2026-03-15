import random

from adventure.models.adventure_state import AdventureMode, StageTemplate


def pick_stage(*, stages: list[StageTemplate], mode: AdventureMode, party_size: int, adventure_level: int) -> StageTemplate:
    pool = [
        s for s in stages
        if adventure_level >= int(s.min_adv_level)
        and (party_size > 1 or not s.party_only)
        and (mode == AdventureMode.PARTY or not s.party_only)
    ]
    if not pool:
        pool = [s for s in stages if not s.party_only]
    return random.choice(pool)
