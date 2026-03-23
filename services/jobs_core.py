# services/jobs_core.py
from __future__ import annotations

import random
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Sequence

from sqlalchemy import delete, select

from db.models import JobRow, UserJobSlotRow, XpRow

try:
    from services.job_progression import (
        JobTier,
        JobEffects,
        NoopInventoryAdapter,
        compute_effects_from_upgrades_and_items,
        award_job_xp,
        get_snapshot as get_job_snapshot,
    )
except Exception:
    from services.jobs_progression import (  # type: ignore[import-not-found]
        JobTier,
        JobEffects,
        NoopInventoryAdapter,
        compute_effects_from_upgrades_and_items,
        award_job_xp,
        get_snapshot as get_job_snapshot,
    )


# ============================================================
# BALANCE (edit here)
# ============================================================
# These are the BASE values per /work press before bonuses.
# Your cogs/work.py can still scale these (level, prestige, vip, upgrades, etc).
#
# Stamina rule: your request = "max 10 stamina each"
# So defaults here never exceed 10.
#
# If you want per-job overrides later, use JOB_XP_OVERRIDE etc below.
# ============================================================

class JobCategory(str, Enum):
    HARD = "hard"
    STABLE = "stable"
    EASY = "easy"


USER_XP_BY_CATEGORY: Dict[JobCategory, int] = {
    JobCategory.EASY: 35,
    JobCategory.STABLE: 45,
    JobCategory.HARD: 50,
}

JOB_XP_BY_CATEGORY: Dict[JobCategory, int] = {
    JobCategory.EASY: 16,
    JobCategory.STABLE: 20,
    JobCategory.HARD: 26,
}

STAMINA_BY_CATEGORY: Dict[JobCategory, int] = {
    JobCategory.EASY: 8,
    JobCategory.STABLE: 9,
    JobCategory.HARD: 10,
}

# Optional per-job overrides (leave empty unless you want specific tuning)
USER_XP_OVERRIDE: Dict[str, int] = {}
JOB_XP_OVERRIDE: Dict[str, int] = {}
STAMINA_OVERRIDE: Dict[str, int] = {}


JOB_UNLOCK_LEVEL: Dict[JobCategory, int] = {
    JobCategory.EASY: 1,
    JobCategory.STABLE: 40,
    JobCategory.HARD: 60,
}

JOB_UNLOCK_LEVEL_OVERRIDE: Dict[str, int] = {
    "artifact_hunter": 100,
    "drug_lord": 100,
    "dragon_slayer": 100,
    "business_ceo": 100,
    "space_miner": 100,
}

JOB_SWITCH_COST: Dict[JobCategory, int] = {
    JobCategory.EASY: 1000,
    JobCategory.STABLE: 5000,
    JobCategory.HARD: 10000,
}

MAX_EQUIPPED_JOB_SLOTS = 3


def _base_user_xp(job_key: str, category: JobCategory) -> int:
    key = (job_key or "").strip().lower()
    if key in USER_XP_OVERRIDE:
        return max(int(USER_XP_OVERRIDE[key]), 0)
    return max(int(USER_XP_BY_CATEGORY.get(category, 0)), 0)


def _base_job_xp(job_key: str, category: JobCategory) -> int:
    key = (job_key or "").strip().lower()
    if key in JOB_XP_OVERRIDE:
        return max(int(JOB_XP_OVERRIDE[key]), 0)
    return max(int(JOB_XP_BY_CATEGORY.get(category, 0)), 0)


def _base_stamina(job_key: str, category: JobCategory) -> int:
    key = (job_key or "").strip().lower()
    if key in STAMINA_OVERRIDE:
        return max(min(int(STAMINA_OVERRIDE[key]), 10), 1)
    v = int(STAMINA_BY_CATEGORY.get(category, 10))
    return max(min(v, 10), 1)


def unlock_level_for(job_key: str, category: JobCategory) -> int:
    key = (job_key or "").strip().lower()
    if key in JOB_UNLOCK_LEVEL_OVERRIDE:
        return max(int(JOB_UNLOCK_LEVEL_OVERRIDE[key]), 1)
    return max(int(JOB_UNLOCK_LEVEL.get(category, 1)), 1)


# -------------------------
# Helpers
# -------------------------
def fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def pct(n: int, d: int) -> int:
    d = max(int(d), 1)
    n = max(int(n), 0)
    return int(min(100, (n * 100) // d))


def bar(cur: int, need: int, width: int = 16) -> str:
    need = max(int(need), 1)
    cur = max(int(cur), 0)
    ratio = min(cur / need, 1.0)
    fill = int(round(ratio * width))
    fill = max(0, min(width, fill))
    return "▰" * fill + "▱" * (width - fill)


def roll_bp(chance_bp: int) -> bool:
    bp = max(int(chance_bp), 0)
    if bp <= 0:
        return False
    if bp >= 10000:
        return True
    return random.randint(1, 10000) <= bp


def apply_bp(value: int, bonus_bp: int) -> int:
    v = max(int(value), 0)
    bpv = int(bonus_bp)
    return max((v * (10_000 + bpv)) // 10_000, 0)


def sub_bp(value: int, discount_bp: int) -> int:
    v = max(int(value), 0)
    bpv = max(int(discount_bp), 0)
    return max((v * (10_000 - bpv)) // 10_000, 0)


def clamp_int(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(v)))


# -------------------------
# Tier helpers
# -------------------------
def tier_for_category(category: JobCategory) -> JobTier:
    if category == JobCategory.EASY:
        return JobTier.EASY
    if category == JobCategory.STABLE:
        return JobTier.STABLE
    return JobTier.HARD


def category_fail_bp(category: JobCategory, base_fail_bp: int) -> int:
    base = max(int(base_fail_bp), 0)
    if category == JobCategory.HARD:
        return base
    if category == JobCategory.STABLE:
        return int(base * 0.6)
    if category == JobCategory.EASY:
        return 0
    return base


# -------------------------
# Job Data
# -------------------------
@dataclass(frozen=True)
class JobAction:
    key: str
    weight: int
    min_silver: int
    max_silver: int
    text: str
    can_fail: bool = True


@dataclass(frozen=True)
class JobDef:
    key: str
    name: str
    category: JobCategory
    vip_only: bool = False
    cooldown_seconds: int = 60
    fail_chance_bp: int = 0
    bonus_chance_bp: int = 0
    bonus_multiplier: float = 1.0
    user_xp_gain: int = 0
    job_xp_gain: int = 0
    stamina_cost: int = 10
    actions: Sequence[JobAction] = ()


# -------------------------
# Job Definitions
# -------------------------
JOB_DEFS: Dict[str, JobDef] = {
    # EASY
    "fisherman": JobDef(
        key="fisherman",
        name="Fisherman",
        category=JobCategory.EASY,
        cooldown_seconds=60,
        fail_chance_bp=0,
        bonus_chance_bp=300,
        bonus_multiplier=2.5,
        user_xp_gain=_base_user_xp("fisherman", JobCategory.EASY),
        job_xp_gain=_base_job_xp("fisherman", JobCategory.EASY),
        stamina_cost=_base_stamina("fisherman", JobCategory.EASY),
        actions=(
            JobAction("small_catch", 30, 10, 22, "You reel in a small catch."),
            JobAction("old_boot", 18, 8, 18, "You catch an old boot. Still sellable."),
            JobAction("bucket_junk", 22, 12, 26, "A bucket of junk. Marketplace eats that up."),
            JobAction("fat_fish", 18, 18, 40, "A fat fish. Easy money."),
            JobAction("rare_find", 12, 22, 55, "Rare find in the net. Nice."),
        ),
    ),
    "miner": JobDef(
        key="miner",
        name="Miner",
        category=JobCategory.EASY,
        cooldown_seconds=60,
        fail_chance_bp=0,
        bonus_chance_bp=450,
        bonus_multiplier=2.0,
        user_xp_gain=_base_user_xp("miner", JobCategory.EASY),
        job_xp_gain=_base_job_xp("miner", JobCategory.EASY),
        stamina_cost=_base_stamina("miner", JobCategory.EASY),
        actions=(
            JobAction("ore_vein", 30, 14, 30, "You find a decent ore vein."),
            JobAction("gem_shard", 18, 18, 40, "You chip out a glittering gem shard."),
            JobAction("deep_tunnel", 18, 20, 44, "You go deeper and haul something heavy."),
            JobAction("ancient_cache", 14, 24, 55, "You uncover an old cache buried in stone."),
            JobAction("clean_strike", 20, 16, 36, "You land a clean strike and fill your pack."),
        ),
    ),
    "lumberjack": JobDef(
        key="lumberjack",
        name="Lumberjack",
        category=JobCategory.EASY,
        cooldown_seconds=60,
        fail_chance_bp=0,
        bonus_chance_bp=400,
        bonus_multiplier=2.1,
        user_xp_gain=_base_user_xp("lumberjack", JobCategory.EASY),
        job_xp_gain=_base_job_xp("lumberjack", JobCategory.EASY),
        stamina_cost=_base_stamina("lumberjack", JobCategory.EASY),
        actions=(
            JobAction("clean_chop", 28, 14, 30, "Clean chop. Good logs."),
            JobAction("knotty_tree", 22, 16, 34, "Knotty tree, but you manage."),
            JobAction("resin_haul", 18, 18, 40, "You gather resin and quality wood."),
            JobAction("perfect_timber", 16, 22, 55, "Perfect timber. Big buyer energy."),
            JobAction("trail_supply", 16, 12, 28, "You bundle smaller cuts for quick sales."),
        ),
    ),
    "messenger": JobDef(
        key="messenger",
        name="Messenger",
        category=JobCategory.EASY,
        cooldown_seconds=60,
        fail_chance_bp=0,
        bonus_chance_bp=350,
        bonus_multiplier=2.3,
        user_xp_gain=_base_user_xp("messenger", JobCategory.EASY),
        job_xp_gain=_base_job_xp("messenger", JobCategory.EASY),
        stamina_cost=_base_stamina("messenger", JobCategory.EASY),
        actions=(
            JobAction("quick_delivery", 30, 12, 24, "You run a quick delivery across town."),
            JobAction("express_route", 20, 16, 32, "You take an express route and get tipped."),
            JobAction("lost_letter", 18, 10, 22, "You find a lost letter and return it for pay."),
            JobAction("bulk_drop", 18, 18, 36, "You drop off a bundle of parcels."),
            JobAction("golden_tip", 14, 20, 50, "Someone hits you with a fat tip. W."),
        ),
    ),
    "cook": JobDef(
        key="cook",
        name="Cook",
        category=JobCategory.EASY,
        cooldown_seconds=60,
        fail_chance_bp=0,
        bonus_chance_bp=320,
        bonus_multiplier=2.4,
        user_xp_gain=_base_user_xp("cook", JobCategory.EASY),
        job_xp_gain=_base_job_xp("cook", JobCategory.EASY),
        stamina_cost=_base_stamina("cook", JobCategory.EASY),
        actions=(
            JobAction("line_work", 26, 12, 26, "You grind the line and rack up orders."),
            JobAction("special_of_day", 18, 16, 36, "Your special sells out. Nice."),
            JobAction("late_night_rush", 20, 14, 30, "Late night rush hits. You keep up."),
            JobAction("catering", 18, 18, 42, "You handle a small catering gig."),
            JobAction("five_star", 18, 20, 55, "A five-star review comes with a big tip."),
        ),
    ),
    # STABLE
    "farmer": JobDef(
        key="farmer",
        name="Farmer",
        category=JobCategory.STABLE,
        cooldown_seconds=75,
        fail_chance_bp=600,
        bonus_chance_bp=500,
        bonus_multiplier=2.0,
        user_xp_gain=_base_user_xp("farmer", JobCategory.STABLE),
        job_xp_gain=_base_job_xp("farmer", JobCategory.STABLE),
        stamina_cost=_base_stamina("farmer", JobCategory.STABLE),
        actions=(
            JobAction("morning_harvest", 28, 30, 70, "You pull a clean morning harvest."),
            JobAction("market_run", 22, 35, 80, "You sell at the market with steady demand."),
            JobAction("fertile_patch", 18, 40, 95, "You find a fertile patch and yield spikes."),
            JobAction("storm_damage", 14, 0, 0, "A storm trashes part of the crop.", can_fail=True),
            JobAction("bumper_crop", 18, 55, 120, "Bumper crop. Your pockets feel it."),
        ),
    ),
    "swordsman": JobDef(
        key="swordsman",
        name="Swordsman",
        category=JobCategory.STABLE,
        cooldown_seconds=75,
        fail_chance_bp=700,
        bonus_chance_bp=600,
        bonus_multiplier=2.2,
        user_xp_gain=_base_user_xp("swordsman", JobCategory.STABLE),
        job_xp_gain=_base_job_xp("swordsman", JobCategory.STABLE),
        stamina_cost=_base_stamina("swordsman", JobCategory.STABLE),
        actions=(
            JobAction("sparring", 28, 32, 75, "You spar and earn coin from spectators."),
            JobAction("escort", 20, 40, 92, "You escort a trader through risky roads."),
            JobAction("duel_win", 18, 50, 110, "You win a clean duel. Respect."),
            JobAction("injury", 14, 0, 0, "You take a nasty hit and have to back off.", can_fail=True),
            JobAction("arena_champion", 20, 65, 150, "Arena champ moment. Big payout."),
        ),
    ),
    # HARD
    "blacksmith": JobDef(
        key="blacksmith",
        name="Blacksmith",
        category=JobCategory.HARD,
        cooldown_seconds=90,
        fail_chance_bp=1800,
        bonus_chance_bp=800,
        bonus_multiplier=2.5,
        user_xp_gain=_base_user_xp("blacksmith", JobCategory.HARD),
        job_xp_gain=_base_job_xp("blacksmith", JobCategory.HARD),
        stamina_cost=_base_stamina("blacksmith", JobCategory.HARD),
        actions=(
            JobAction("basic_forge", 26, 55, 120, "You forge basic gear for steady coin."),
            JobAction("custom_order", 20, 80, 170, "Custom order completed. Pays well."),
            JobAction("rare_alloy", 18, 110, 240, "You work a rare alloy. Premium."),
            JobAction("ruined_piece", 16, 0, 0, "The piece warps and ruins in the quench.", can_fail=True),
            JobAction("masterwork", 20, 150, 320, "Masterwork comes out clean. Respect."),
        ),
    ),
    "bounty_hunter": JobDef(
        key="bounty_hunter",
        name="Bounty Hunter",
        category=JobCategory.HARD,
        cooldown_seconds=120,
        fail_chance_bp=2200,
        bonus_chance_bp=900,
        bonus_multiplier=2.8,
        user_xp_gain=_base_user_xp("bounty_hunter", JobCategory.HARD),
        job_xp_gain=_base_job_xp("bounty_hunter", JobCategory.HARD),
        stamina_cost=_base_stamina("bounty_hunter", JobCategory.HARD),
        actions=(
            JobAction("easy_target", 24, 80, 170, "Easy target. Quick payout."),
            JobAction("chase", 20, 120, 240, "Long chase. You still bag the reward."),
            JobAction("dirty_job", 18, 160, 320, "Dirty job. Big money, no questions."),
            JobAction("ambushed", 18, 0, 0, "You get ambushed. Contract fails.", can_fail=True),
            JobAction("wanted_elite", 20, 220, 480, "You catch an elite target. Huge."),
        ),
    ),
    "pirate": JobDef(
        key="pirate",
        name="Pirate",
        category=JobCategory.HARD,
        cooldown_seconds=120,
        fail_chance_bp=2500,
        bonus_chance_bp=1000,
        bonus_multiplier=3.0,
        user_xp_gain=_base_user_xp("pirate", JobCategory.HARD),
        job_xp_gain=_base_job_xp("pirate", JobCategory.HARD),
        stamina_cost=_base_stamina("pirate", JobCategory.HARD),
        actions=(
            JobAction("dock_pickpocket", 24, 70, 160, "You pick pockets at the docks."),
            JobAction("cargo_swipe", 20, 140, 280, "You swipe cargo. Easy flip."),
            JobAction("rum_runner", 18, 190, 360, "You run contraband. Pays fat."),
            JobAction("navy_spotted", 18, 0, 0, "Navy spots you. You flee empty-handed.", can_fail=True),
            JobAction("captains_chest", 20, 260, 520, "Captain’s chest acquired. Jackpot vibes."),
        ),
    ),
    "robber": JobDef(
        key="robber",
        name="Robber",
        category=JobCategory.HARD,
        cooldown_seconds=150,
        fail_chance_bp=3000,
        bonus_chance_bp=1100,
        bonus_multiplier=3.2,
        user_xp_gain=_base_user_xp("robber", JobCategory.HARD),
        job_xp_gain=_base_job_xp("robber", JobCategory.HARD),
        stamina_cost=_base_stamina("robber", JobCategory.HARD),
        actions=(
            JobAction("street_mug", 26, 60, 150, "Quick mug on a side street."),
            JobAction("shop_hold", 18, 180, 360, "You hit a shop. Risky but worth."),
            JobAction("safe_crack", 18, 250, 520, "You crack a safe like a pro."),
            JobAction("caught", 18, 0, 0, "You get caught. Job fails.", can_fail=True),
            JobAction("big_score", 20, 320, 680, "Big score. You vanish into the night."),
        ),
    ),
    "cheriff": JobDef(
        key="cheriff",
        name="Cheriff",
        category=JobCategory.HARD,
        cooldown_seconds=180,
        fail_chance_bp=2800,
        bonus_chance_bp=1500,
        bonus_multiplier=3.4,
        user_xp_gain=_base_user_xp("cheriff", JobCategory.HARD),
        job_xp_gain=_base_job_xp("cheriff", JobCategory.HARD),
        stamina_cost=_base_stamina("cheriff", JobCategory.HARD),
        actions=(
            JobAction("high_noon_duel", 18, 220, 460, "You win a high-noon duel and collect a heavy bounty."),
            JobAction("gang_crackdown", 20, 300, 620, "You shut down a violent gang operation across the county."),
            JobAction("hostage_rescue", 16, 380, 760, "You rescue hostages unharmed. The town pools a major reward."),
            JobAction("corrupt_deputy", 18, 0, 0, "A corrupt deputy leaks your route. The mission collapses.", can_fail=True),
            JobAction("legendary_manhunt", 28, 450, 980, "Legendary manhunt completed. Your badge becomes a myth."),
        ),
    ),
    "artifact_hunter": JobDef(
        key="artifact_hunter",
        name="Artifact Hunter",
        category=JobCategory.HARD,
        cooldown_seconds=180,
        fail_chance_bp=2300,
        bonus_chance_bp=1700,
        bonus_multiplier=3.5,
        user_xp_gain=_base_user_xp("artifact_hunter", JobCategory.HARD),
        job_xp_gain=_base_job_xp("artifact_hunter", JobCategory.HARD),
        stamina_cost=_base_stamina("artifact_hunter", JobCategory.HARD),
        actions=(
            JobAction("vault_scraps", 22, 380, 780, "You raid an ancient vault and cash out cracked relics."),
            JobAction("hidden_reliquary", 18, 520, 1100, "You uncover a hidden reliquary packed with black-market treasure."),
            JobAction("royal_collector", 16, 700, 1450, "A royal collector overpays for a forbidden piece."),
            JobAction("cursed_find", 14, 900, 1900, "A cursed artifact still sells for a fortune before the whispers start."),
            JobAction("counterfeit_relic", 15, 0, 0, "The relic is fake. The buyer walks and the run dies.", can_fail=True),
            JobAction("tomb_collapse", 15, 0, 0, "The tomb collapses before you can extract the haul.", can_fail=True),
        ),
    ),
    "drug_lord": JobDef(
        key="drug_lord",
        name="Drug Lord",
        category=JobCategory.HARD,
        cooldown_seconds=180,
        fail_chance_bp=3200,
        bonus_chance_bp=2200,
        bonus_multiplier=4.0,
        user_xp_gain=_base_user_xp("drug_lord", JobCategory.HARD),
        job_xp_gain=_base_job_xp("drug_lord", JobCategory.HARD),
        stamina_cost=_base_stamina("drug_lord", JobCategory.HARD),
        actions=(
            JobAction("territory_sweep", 22, 420, 900, "You lock down territory and collect a brutal cash sweep."),
            JobAction("cartel_shipment", 18, 700, 1500, "A massive shipment lands clean and the profit explodes."),
            JobAction("street_monopoly", 16, 980, 2100, "You crush the block and print monopoly money."),
            JobAction("cash_warehouse", 14, 1400, 3000, "You crack open a warehouse stuffed with dirty silver."),
            JobAction("rival_raid", 15, 0, 0, "A rival raid blows up the operation before payout lands.", can_fail=True),
            JobAction("crackdown", 15, 0, 0, "A crackdown seizes the whole move and leaves you empty.", can_fail=True),
        ),
    ),
    "dragon_slayer": JobDef(
        key="dragon_slayer",
        name="Dragon Slayer",
        category=JobCategory.HARD,
        cooldown_seconds=180,
        fail_chance_bp=2500,
        bonus_chance_bp=1800,
        bonus_multiplier=3.6,
        user_xp_gain=_base_user_xp("dragon_slayer", JobCategory.HARD),
        job_xp_gain=_base_job_xp("dragon_slayer", JobCategory.HARD),
        stamina_cost=_base_stamina("dragon_slayer", JobCategory.HARD),
        actions=(
            JobAction("nest_clear", 22, 450, 950, "You clear a dragon nest and haul scorched treasure."),
            JobAction("royal_bounty", 18, 700, 1350, "A royal bounty pays heavy for a confirmed dragon kill."),
            JobAction("heartscale_drop", 16, 900, 1750, "You secure pristine heartscales worth absurd silver."),
            JobAction("ancient_wyrm", 14, 1200, 2500, "An ancient wyrm falls and the kingdom showers you with silver."),
            JobAction("burned_loot", 15, 0, 0, "The dragon torches the loot before you can salvage it.", can_fail=True),
            JobAction("hunt_ruined", 15, 0, 0, "The hunt collapses when a stronger beast crashes the contract.", can_fail=True),
        ),
    ),
    "business_ceo": JobDef(
        key="business_ceo",
        name="Business CEO",
        category=JobCategory.STABLE,
        cooldown_seconds=165,
        fail_chance_bp=900,
        bonus_chance_bp=1300,
        bonus_multiplier=2.8,
        user_xp_gain=_base_user_xp("business_ceo", JobCategory.STABLE),
        job_xp_gain=_base_job_xp("business_ceo", JobCategory.STABLE),
        stamina_cost=_base_stamina("business_ceo", JobCategory.STABLE),
        actions=(
            JobAction("major_acquisition", 24, 620, 980, "A major acquisition closes and your quarter jumps."),
            JobAction("investor_surge", 22, 720, 1080, "Investors pile in and the company mints silver."),
            JobAction("dividend_explosion", 18, 820, 1250, "A dividend explosion turns the boardroom into a mint."),
            JobAction("global_expansion", 18, 980, 1450, "Global expansion opens a rich new market overnight."),
            JobAction("tax_audit", 10, 350, 700, "A tax audit clips the win, but you still cash out.", can_fail=False),
            JobAction("bad_quarter", 8, 0, 0, "A brutal quarter kills the deal flow and the run comes up empty.", can_fail=True),
        ),
    ),
    "space_miner": JobDef(
        key="space_miner",
        name="Space Miner",
        category=JobCategory.HARD,
        cooldown_seconds=180,
        fail_chance_bp=2700,
        bonus_chance_bp=1900,
        bonus_multiplier=3.8,
        user_xp_gain=_base_user_xp("space_miner", JobCategory.HARD),
        job_xp_gain=_base_job_xp("space_miner", JobCategory.HARD),
        stamina_cost=_base_stamina("space_miner", JobCategory.HARD),
        actions=(
            JobAction("void_crystal_vein", 22, 400, 860, "You tap a void crystal vein and the haul glows like silver."),
            JobAction("alien_core", 18, 650, 1380, "An alien core deposit pays out hard."),
            JobAction("starstorm_harvest", 16, 920, 1850, "You ride a starstorm harvest and strip insane ore."),
            JobAction("cosmic_motherlode", 14, 1250, 2700, "A cosmic motherlode changes the whole shift."),
            JobAction("hull_breach", 15, 0, 0, "A hull breach blows the cargo into deep space.", can_fail=True),
            JobAction("ore_detonation", 15, 0, 0, "Unstable ore detonates and wipes the payout.", can_fail=True),
        ),
    ),
    # VIP
    "influencer": JobDef(
        key="influencer",
        name="Influencer",
        category=JobCategory.STABLE,
        vip_only=True,
        cooldown_seconds=120,
        fail_chance_bp=200,
        bonus_chance_bp=1200,
        bonus_multiplier=2.2,
        user_xp_gain=_base_user_xp("influencer", JobCategory.STABLE),
        job_xp_gain=_base_job_xp("influencer", JobCategory.STABLE),
        stamina_cost=_base_stamina("influencer", JobCategory.STABLE),
        actions=(
            JobAction("brand_deal", 24, 140, 280, "Brand deal lands. Bag secured."),
            JobAction("viral_post", 20, 180, 360, "Viral post. Everyone’s watching."),
            JobAction("sponsored_stream", 18, 220, 440, "Sponsored stream goes hard."),
            JobAction("flop", 12, 90, 160, "It flops a bit, but you still earn.", can_fail=False),
            JobAction("superchat_rain", 26, 260, 520, "Superchats raining. Absolute cinema."),
        ),
    ),
    "onlychat_model": JobDef(
        key="onlychat_model",
        name="OnlyChat Model",
        category=JobCategory.STABLE,
        vip_only=True,
        cooldown_seconds=120,
        fail_chance_bp=0,
        bonus_chance_bp=900,
        bonus_multiplier=2.0,
        user_xp_gain=_base_user_xp("onlychat_model", JobCategory.STABLE),
        job_xp_gain=_base_job_xp("onlychat_model", JobCategory.STABLE),
        stamina_cost=_base_stamina("onlychat_model", JobCategory.STABLE),
        actions=(
            JobAction("tips", 28, 120, 240, "Tips roll in. Chat loves you."),
            JobAction("dm_wave", 18, 140, 280, "DM wave hits. Paid attention."),
            JobAction("fan_club", 20, 160, 320, "Fan club energy. Stable income."),
            JobAction("quiet_hour", 10, 90, 180, "Quiet hour, still some tips.", can_fail=False),
            JobAction("whale", 24, 260, 520, "A whale drops a fat tip. Respect."),
        ),
    ),
    "streamer": JobDef(
        key="streamer",
        name="Streamer",
        category=JobCategory.STABLE,
        vip_only=True,
        cooldown_seconds=120,
        fail_chance_bp=300,
        bonus_chance_bp=1000,
        bonus_multiplier=2.4,
        user_xp_gain=_base_user_xp("streamer", JobCategory.STABLE),
        job_xp_gain=_base_job_xp("streamer", JobCategory.STABLE),
        stamina_cost=_base_stamina("streamer", JobCategory.STABLE),
        actions=(
            JobAction("solid_viewers", 26, 130, 260, "Solid viewership. Good payouts."),
            JobAction("raid", 18, 220, 440, "Raid hits. Big spike."),
            JobAction("donos", 22, 170, 340, "Donos keep coming."),
            JobAction("scuffed_audio", 10, 90, 180, "Audio scuffed. Still paid.", can_fail=False),
            JobAction("frontpage", 24, 260, 520, "Frontpage moment. Huge."),
        ),
    ),
    "president": JobDef(
        key="president",
        name="President",
        category=JobCategory.HARD,
        vip_only=True,
        cooldown_seconds=180,
        fail_chance_bp=1200,
        bonus_chance_bp=1400,
        bonus_multiplier=3.0,
        user_xp_gain=_base_user_xp("president", JobCategory.HARD),
        job_xp_gain=_base_job_xp("president", JobCategory.HARD),
        stamina_cost=_base_stamina("president", JobCategory.HARD),
        actions=(
            JobAction("policy_push", 24, 220, 440, "You push a policy. Economy responds."),
            JobAction("press_event", 18, 260, 520, "Press event boosts your influence."),
            JobAction("trade_deal", 18, 320, 650, "Trade deal signed. Big gain."),
            JobAction("scandal", 16, 0, 0, "Scandal erupts. You lose momentum.", can_fail=True),
            JobAction("national_boost", 24, 360, 800, "Server-wide boost moment. Legendary."),
        ),
    ),
}


# -------------------------
# DB helpers (shared)
# -------------------------
async def ensure_job_row(session, *, key: str, name: str) -> JobRow:
    row = await session.scalar(select(JobRow).where(JobRow.key == key))
    if row is None:
        row = JobRow(key=key, name=name, enabled=True)
        session.add(row)
        await session.flush()
    return row


async def get_level(session, *, guild_id: int, user_id: int) -> int:
    xp = await session.scalar(
        select(XpRow).where(
            XpRow.guild_id == guild_id,
            XpRow.user_id == user_id,
        )
    )
    if xp is None:
        return 1
    return max(int(xp.level_cached), 1)


async def get_equipped_key(session, *, guild_id: int, user_id: int) -> Optional[str]:
    keys = await get_equipped_keys(session, guild_id=guild_id, user_id=user_id)
    return keys[0] if keys else None


async def get_equipped_keys(session, *, guild_id: int, user_id: int) -> list[str]:
    rows = await session.execute(
        select(UserJobSlotRow)
        .where(
            UserJobSlotRow.guild_id == guild_id,
            UserJobSlotRow.user_id == user_id,
            UserJobSlotRow.slot_index >= 0,
            UserJobSlotRow.slot_index < MAX_EQUIPPED_JOB_SLOTS,
        )
        .order_by(UserJobSlotRow.slot_index.asc())
    )

    out: list[str] = []
    for slot in rows.scalars():
        job = await session.get(JobRow, int(slot.job_id))
        if job is not None:
            out.append(str(job.key))
    return out


async def set_equipped_key(session, *, guild_id: int, user_id: int, job_key: str) -> None:
    await set_equipped_keys(session, guild_id=guild_id, user_id=user_id, job_keys=[job_key])


async def set_equipped_keys(session, *, guild_id: int, user_id: int, job_keys: Sequence[str]) -> list[str]:
    cleaned: list[str] = []
    for key in job_keys:
        k = (key or "").strip().lower()
        if not k or k in cleaned:
            continue
        if k not in JOB_DEFS:
            raise ValueError(f"Unknown job key: {k}")
        cleaned.append(k)
        if len(cleaned) >= MAX_EQUIPPED_JOB_SLOTS:
            break

    await session.execute(
        delete(UserJobSlotRow).where(
            UserJobSlotRow.guild_id == guild_id,
            UserJobSlotRow.user_id == user_id,
        )
    )

    for idx, key in enumerate(cleaned):
        d = JOB_DEFS[key]
        job_row = await session.scalar(select(JobRow).where(JobRow.key == key))
        if job_row is None:
            job_row = JobRow(key=key, name=d.name, enabled=True)
            session.add(job_row)
            await session.flush()

        slot = UserJobSlotRow(
            guild_id=guild_id,
            user_id=user_id,
            slot_index=idx,
            job_id=int(job_row.id),
        )
        session.add(slot)

    await session.flush()
    return cleaned


async def rotate_equipped_jobs(session, *, guild_id: int, user_id: int) -> list[str]:
    keys = await get_equipped_keys(session, guild_id=guild_id, user_id=user_id)
    if len(keys) <= 1:
        return keys
    rotated = [*keys[1:], keys[0]]
    await set_equipped_keys(session, guild_id=guild_id, user_id=user_id, job_keys=rotated)
    return rotated


async def get_job_row_by_key(session, *, job_key: str) -> Optional[JobRow]:
    key = (job_key or "").strip().lower()
    if not key:
        return None
    return await session.scalar(select(JobRow).where(JobRow.key == key))


async def get_or_create_job_row(session, *, job_key: str, name: Optional[str] = None) -> JobRow:
    d = JOB_DEFS.get(job_key)
    if d is None:
        raise ValueError("Unknown job key")

    row = await get_job_row_by_key(session, job_key=job_key)
    if row is None:
        row = JobRow(key=job_key, name=name or d.name, enabled=True)
        session.add(row)
        await session.flush()
    elif name:
        clean_name = str(name).strip()
        if clean_name and getattr(row, "name", None) != clean_name:
            row.name = clean_name
            await session.flush()
    return row


def _first_attr(obj_or_cls, names: list[str]) -> str | None:
    for n in names:
        if hasattr(obj_or_cls, n):
            return n
    return None


def _get_str(obj, names: list[str], default: Optional[str] = None) -> Optional[str]:
    name = _first_attr(obj, names)
    if not name:
        return default
    try:
        v = getattr(obj, name)
        if v is None:
            return default
        s = str(v).strip()
        return s if s else default
    except Exception:
        return default


def _set_attr(obj, names: list[str], value) -> bool:
    name = _first_attr(obj, names)
    if not name:
        return False
    try:
        setattr(obj, name, value)
        return True
    except Exception:
        return False


def job_row_image_get(row: JobRow) -> Optional[str]:
    return _get_str(row, ["work_image_url", "image_url", "work_image", "img_url", "banner_url", "picture_url"], None)


def job_row_image_set(row: JobRow, url: Optional[str]) -> bool:
    if url is None:
        return _set_attr(row, ["work_image_url", "image_url", "work_image", "img_url", "banner_url", "picture_url"], None)
    return _set_attr(row, ["work_image_url", "image_url", "work_image", "img_url", "banner_url", "picture_url"], str(url))


# -------------------------
# Progression wrappers
# -------------------------
async def _resolve_job_id(session, *, job_key: str) -> int:
    key = (job_key or "").strip().lower()
    if not key:
        raise ValueError("job_key required")
    row = await session.scalar(select(JobRow).where(JobRow.key == key))
    if row is None:
        raise ValueError(f"Unknown job key for progression: {key}")
    return int(row.id)


async def progression_snapshot(
    session,
    *,
    guild_id: int,
    user_id: int,
    job_key: str,
) -> object:
    d = JOB_DEFS.get((job_key or "").strip().lower())
    if d is None:
        raise ValueError(f"Unknown job key: {job_key}")

    job_id = await _resolve_job_id(session, job_key=d.key)
    tier = tier_for_category(d.category)

    return await get_job_snapshot(
        session,
        guild_id=int(guild_id),
        user_id=int(user_id),
        job_id=job_id,
        job_key=d.key,
        tier=tier,
    )


async def progression_award_job_xp(
    session,
    *,
    guild_id: int,
    user_id: int,
    job_key: str,
    base_xp: int,
) -> object:
    d = JOB_DEFS.get((job_key or "").strip().lower())
    if d is None:
        raise ValueError(f"Unknown job key: {job_key}")

    job_id = await _resolve_job_id(session, job_key=d.key)
    tier = tier_for_category(d.category)

    return await award_job_xp(
        session,
        guild_id=int(guild_id),
        user_id=int(user_id),
        job_id=job_id,
        job_key=d.key,
        tier=tier,
        base_xp=int(base_xp),
    )


async def progression_effects(
    session,
    *,
    guild_id: int,
    user_id: int,
    job_key: str,
    prestige: int,
    level: int,
    inv: NoopInventoryAdapter | None = None,
) -> JobEffects:
    d = JOB_DEFS.get((job_key or "").strip().lower())
    if d is None:
        raise ValueError(f"Unknown job key: {job_key}")

    job_id = await _resolve_job_id(session, job_key=d.key)
    if inv is None:
        inv = NoopInventoryAdapter()

    return await compute_effects_from_upgrades_and_items(
        session,
        guild_id=int(guild_id),
        user_id=int(user_id),
        job_id=job_id,
        prestige=int(prestige),
        level=int(level),
        inv=inv,
    )



# ============================================================
# NOTE: JOB SYSTEM OVERVIEW (how everything connects)
# ============================================================
# This project has TWO parallel progression tracks:
#   1) USER LEVEL (global)  -> stored in XpRow (services/xp_award.py updates this)
#   2) JOB PROGRESSION      -> stored in JobProgressRow (services/job_progression.py or jobs_progression.py)
#
# The "Jobs" feature is basically:
#   /job   -> picks what job is equipped
#   /work  -> consumes stamina, rolls outcome, pays silver, awards XP (user + job)
#
#
# ----------------------------
# FILES + RESPONSIBILITIES
# ----------------------------
# cogs/jobs.py
#   - UI entrypoint for job selection
#   - /job opens the panel (embed + dropdown + buttons)
#   - /job <key> asks to equip/switch to a specific job (with confirmation view)
#   - Enforces VIP locks + user-level unlocks (lvl 1/40/60) before equip
#   - Calls services.jobs_core for job definitions + equip persistence
#   - Uses services.jobs_views (Views) to handle button callbacks
#   - Uses services.jobs_embeds for consistent embeds
#
# cogs/work.py
#   - Gameplay entrypoint for running a job
#   - Reads equipped job from DB (UserJobSlotRow -> JobRow)
#   - Validates unlocks/VIP + cooldowns
#   - Spends stamina via StaminaService.try_spend()
#   - Picks an action from JOB_DEFS[job].actions (weighted)
#   - Calculates fail chance + payout + bonus rolls
#   - Adds Silver to WalletRow
#   - Calls award_xp(...) to award USER XP (XpRow)
#   - Calls award_job_xp(...) to award JOB XP (JobProgressRow)
#   - Builds the final "work result" embed
#
# services/jobs_core.py
#   - "Source of truth" for job definitions (JOB_DEFS)
#   - Contains category rules:
#       - unlock levels per category
#       - switch costs per category
#       - default base user/job XP + stamina per category (your tuning knobs)
#       - tier mapping (EASY/STABLE/HARD -> JobTier for job progression curve)
#   - Contains DB helper functions:
#       - get_equipped_key / set_equipped_key
#       - ensure_job_row / get_or_create_job_row
#       - optional job image field getter/setter (if JobRow supports it)
#   - Contains progression wrapper helpers:
#       - progression_snapshot / progression_award_job_xp / progression_effects
#       - These bridge the rest of the bot to job_progression API
#
# services/jobs_progression.py  (or services/job_progression.py)
#   - The JOB leveling engine
#   - Stores job_level + job_xp + job_title in JobProgressRow
#   - Defines:
#       - XP needed curve per job level (xp_needed_for_level)
#       - Level caps per prestige (level_cap_for)
#       - Prestige + title system (encode/decode_title, title_for)
#   - award_job_xp():
#       - takes base job XP from /work
#       - applies effects (optional)
#       - advances job level / prestige and updates DB row
#       - returns snapshot + delta (leveled_up / prestiged) for embeds
#
# services/jobs_embeds.py
#   - Pure embed factory functions
#   - Should NOT touch DB
#   - Makes the jobs panel embed + per-job info embed
#
# services/jobs_views.py
#   - Discord UI Views (dropdowns/buttons)
#   - Handles:
#       - selecting a job to preview
#       - switching pages (standard vs VIP)
#       - confirm equip button -> calls set_equipped_key + charges silver
#   - This is UI code and depends on discord.py by design
#
# services/stamina.py (StaminaService)
#   - Owns stamina rules + persistence
#   - try_spend() checks/updates stamina atomically inside your DB transaction
#
# services/xp_award.py
#   - Owns USER XP rules + persistence (XpRow)
#   - award_xp() increments XP, updates cached level, etc.
#
#
# ----------------------------
# DATA FLOW (high level)
# ----------------------------
# /job
#   cogs/jobs.py
#     -> checks VIP + user level unlocks
#     -> confirm view (services/jobs_views.py)
#         -> set_equipped_key() (services/jobs_core.py)
#         -> optionally charge switch cost (WalletRow)
#
# /work
#   cogs/work.py
#     -> read equipped job key (services/jobs_core.get_equipped_key)
#     -> get JobDef from JOB_DEFS
#     -> spend stamina (StaminaService.try_spend)
#     -> roll fail/bonus/payout using JobDef config
#     -> update WalletRow
#     -> award USER XP (award_xp)
#     -> award JOB XP (award_job_xp from job_progression)
#     -> show results embed
#
#
# ----------------------------
# TUNING (where to change things)
# ----------------------------
# - Job base rewards + base stamina: services/jobs_core.py (BALANCE section)
# - Job leveling speed (job XP curve): services/jobs_progression.py (xp_needed_for_level params)
# - User leveling speed (user XP curve): services/xp_award.py (whatever curve you use there)
# - Stamina regen/max: services/stamina.py
#
# Keep UI out of "core logic" modules when possible:
# - jobs_core / jobs_progression should remain discord-free
# - jobs_embeds / jobs_views are allowed to import discord (they are UI)
# ============================================================
