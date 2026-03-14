# services/jobs_balance.py
from __future__ import annotations

from dataclasses import dataclass

from services.jobs_core import JobCategory, clamp_int


@dataclass(frozen=True)
class BalanceConfig:
    # -------------------------
    # USER XP (account leveling)
    # Exponential curve:
    # - level 1: ~base
    # - level 50: ~150 (with defaults below)
    # -------------------------
    user_xp_base: int = 8
    user_xp_growth: float = 1.0617  # tuned so lvl 50 ~= 150 when base=8
    user_xp_min: int = 6
    user_xp_max: int = 1500

    # Optional per-category multiplier for user xp
    user_xp_mult_easy: float = 1.00
    user_xp_mult_stable: float = 1.15
    user_xp_mult_hard: float = 1.30

    # -------------------------
    # JOB XP (job leveling)
    # Fast, not forever.
    # Base per tier + small scaling by job level + prestige.
    # -------------------------
    job_xp_easy_base: int = 22
    job_xp_stable_base: int = 30
    job_xp_hard_base: int = 40

    job_xp_per_job_level: int = 2
    job_xp_per_prestige: int = 6
    job_xp_min: int = 10
    job_xp_max: int = 300

    # -------------------------
    # STAMINA (cost per /work)
    # Your ask:
    # - max cost is 10 stamina
    # - prestige reduces cost by 1-2 flat
    # -------------------------
    stamina_cost_cap: int = 10
    stamina_cost_floor: int = 1

    stamina_easy_base: int = 8
    stamina_stable_base: int = 9
    stamina_hard_base: int = 10

    stamina_prestige_flat_reduction: int = 2  # change to 1 if you want slower discount
    stamina_per_job_level_reduction_every: int = 0  # set to 20 if you want: -1 per 20 job levels
    stamina_per_job_level_flat_reduction: int = 1   # only used if "every" > 0


CFG = BalanceConfig()


def user_xp_for_work(*, user_level: int, category: JobCategory) -> int:
    lvl = max(int(user_level), 1)

    raw = int(round(CFG.user_xp_base * (CFG.user_xp_growth ** (lvl - 1))))

    if category == JobCategory.EASY:
        raw = int(round(raw * CFG.user_xp_mult_easy))
    elif category == JobCategory.STABLE:
        raw = int(round(raw * CFG.user_xp_mult_stable))
    else:
        raw = int(round(raw * CFG.user_xp_mult_hard))

    return clamp_int(raw, CFG.user_xp_min, CFG.user_xp_max)


def job_xp_for_work(*, job_level: int, prestige: int, category: JobCategory) -> int:
    jl = max(int(job_level), 1)
    p = max(int(prestige), 0)

    if category == JobCategory.EASY:
        base = CFG.job_xp_easy_base
    elif category == JobCategory.STABLE:
        base = CFG.job_xp_stable_base
    else:
        base = CFG.job_xp_hard_base

    raw = base + (jl * CFG.job_xp_per_job_level) + (p * CFG.job_xp_per_prestige)
    return clamp_int(raw, CFG.job_xp_min, CFG.job_xp_max)


def stamina_cost_for_work(*, job_level: int, prestige: int, category: JobCategory) -> int:
    jl = max(int(job_level), 1)
    p = max(int(prestige), 0)

    if category == JobCategory.EASY:
        base = CFG.stamina_easy_base
    elif category == JobCategory.STABLE:
        base = CFG.stamina_stable_base
    else:
        base = CFG.stamina_hard_base

    cost = base

    # Prestige discount (your ask)
    cost -= p * CFG.stamina_prestige_flat_reduction

    # Optional job-level discount (off by default)
    if CFG.stamina_per_job_level_reduction_every > 0:
        cost -= (jl // CFG.stamina_per_job_level_reduction_every) * CFG.stamina_per_job_level_flat_reduction

    cost = clamp_int(cost, CFG.stamina_cost_floor, CFG.stamina_cost_cap)
    return cost
