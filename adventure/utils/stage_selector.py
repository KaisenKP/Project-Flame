import random

from adventure.models.adventure_state import AdventureMode, StageTag, StageTemplate


def pick_stage(
    *,
    stages: list[StageTemplate],
    mode: AdventureMode,
    party_size: int,
    adventure_level: int,
    excluded_keys: set[str] | None = None,
    recent_tags: list[StageTag] | None = None,
) -> StageTemplate:
    excluded = excluded_keys or set()
    history = recent_tags or []
    pool = [
        s for s in stages
        if adventure_level >= int(s.min_adv_level)
        and (party_size > 1 or not s.party_only)
        and (mode == AdventureMode.PARTY or not s.party_only)
        and s.key not in excluded
    ]
    if not pool:
        pool = [s for s in stages if not s.party_only and s.key not in excluded] or [s for s in stages if not s.party_only]
    weighted: list[tuple[StageTemplate, int]] = []
    for stage in pool:
        weight = 100
        if history and stage.tag != history[-1]:
            weight += 45
        if history and stage.tag not in history[-2:]:
            weight += 25
        if stage.tag in {StageTag.BOSS, StageTag.PUZZLE} and adventure_level >= max(10, int(stage.min_adv_level)):
            weight += 20
        weighted.append((stage, max(weight, 1)))
    return random.choices([stage for stage, _ in weighted], weights=[weight for _, weight in weighted], k=1)[0]
