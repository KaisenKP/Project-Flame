from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Iterable, List, Optional, Protocol, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import JobProgressRow


# ============================================================
# Public Concepts
# ============================================================

class JobTier(str, Enum):
    EASY = "easy"
    STABLE = "stable"
    HARD = "hard"


@dataclass(frozen=True)
class JobProgressSnapshot:
    guild_id: int
    user_id: int
    job_id: int
    job_key: str

    prestige: int
    title: str

    level: int
    xp_into_level: int
    xp_needed: int

    total_job_xp_bank: int


@dataclass(frozen=True)
class JobProgressDelta:
    old_prestige: int
    new_prestige: int
    old_title: str
    new_title: str

    old_level: int
    new_level: int

    old_xp_into: int
    new_xp_into: int

    xp_gained: int

    leveled_up: bool
    prestiged: bool


@dataclass(frozen=True)
class JobAwardResult:
    snapshot: JobProgressSnapshot
    delta: JobProgressDelta
    effects: "JobEffects"


# ============================================================
# Title + Prestige persistence (no schema changes)
# ============================================================

_META_RE = re.compile(r"^\[P(?P<p>\d+)\]\s*(?P<title>.+?)\s*$")


def encode_title(prestige: int, title: str) -> str:
    p = max(int(prestige), 0)
    t = (title or "").strip() or "Recruit"
    return f"[P{p}] {t}"


def decode_title(stored: Optional[str]) -> Tuple[int, str]:
    s = (stored or "").strip()
    if not s:
        return 0, "Recruit"
    m = _META_RE.match(s)
    if not m:
        return 0, s
    p = int(m.group("p"))
    title = (m.group("title") or "").strip() or "Recruit"
    return max(p, 0), title


# ============================================================
# Level caps per prestige
#   P0 cap: 10
#   P1 cap: 30
#   P2 cap: 50
#   P3+: +50 per prestige (tunable)
# ============================================================

PRESTIGE_CAPS: Dict[int, int] = {
    0: 10,
    1: 30,
    2: 50,
}


def level_cap_for(prestige: int) -> int:
    p = max(int(prestige), 0)
    if p in PRESTIGE_CAPS:
        return PRESTIGE_CAPS[p]
    return 50 + (p - 2) * 50


# ============================================================
# XP curve per level
# ============================================================

@dataclass(frozen=True)
class XPCurveParams:
    base: int
    per_level_add: int
    prestige_mult_bp: int
    tier_mult_bp: int


DEFAULT_CURVE = XPCurveParams(
    base=120,
    per_level_add=18,
    prestige_mult_bp=1800,  # +18% per prestige
    tier_mult_bp=0,
)

TIER_CURVE_BP: Dict[JobTier, int] = {
    JobTier.EASY: 900,
    JobTier.STABLE: 1200,
    JobTier.HARD: 1600,
}


def xp_needed_for_level(
    *,
    tier: JobTier,
    prestige: int,
    level: int,
    params: XPCurveParams = DEFAULT_CURVE,
) -> int:
    lvl = max(int(level), 1)
    p = max(int(prestige), 0)

    base = params.base + params.per_level_add * (lvl - 1)

    tier_bp = TIER_CURVE_BP.get(tier, 1200)
    prestige_bp = 10_000 + params.prestige_mult_bp * p

    v = base
    v = (v * (10_000 + tier_bp)) // 10_000
    v = (v * prestige_bp) // 10_000

    return max(int(v), 25)


# ============================================================
# Global XP buff (x10)
# - Applied to every award_job_xp call
# - Basis points: (10k + bp) / 10k
#   x10 => bp = 90,000
# ============================================================

GLOBAL_JOB_XP_MULT_BP = 90_000


# ============================================================
# Titles per job (data-driven)
# Big list so you can ship without missing content
# - Index == prestige number
# - If prestige exceeds list length: keep last title
# ============================================================

JOB_TITLES: Dict[str, List[str]] = {
    "miner": [
        "Recruit Miner",
        "Apprentice Miner",
        "Tunnel Digger",
        "Ore Scout",
        "Cave Delver",
        "Ironhand Miner",
        "Deepcore Miner",
        "Gem Cutter",
        "Vein Whisperer",
        "Stonebound Veteran",
        "Master Miner",
        "Legendary Miner",
        "Mythic Miner",
        "Void Miner",
        "Abyssal Prospector",
        "Catacomb King",
        "Relic Breaker",
        "Titan Drillmaster",
        "Eclipse Excavator",
        "Worldbreaker Miner",
    ],
    "fisherman": [
        "Dockhand",
        "Net Toss Apprentice",
        "River Angler",
        "Lakeside Fisher",
        "Bait Master",
        "Harbor Harvester",
        "Reef Runner",
        "Stormline Fisher",
        "Deep-Sea Hunter",
        "Captain of the Nets",
        "Tidecaller Veteran",
        "Legend of the Tides",
        "Mythic Mariner",
        "Kraken Teaser",
        "Abyss Navigator",
        "Pearl King",
        "Ghost Ship Captain",
        "Siren’s Chosen",
        "Ocean Sovereign",
        "Worldsea Myth",
    ],
    "lumberjack": [
        "Woodcutter",
        "Sapling Splitter",
        "Log Hauler",
        "Forest Worker",
        "Axe Apprentice",
        "Timber Runner",
        "Knotbreaker",
        "Bark Veteran",
        "Grove Warden",
        "Old-Growth Keeper",
        "Master Lumberjack",
        "Warden of the Woods",
        "Mythic Timberlord",
        "Ironbark Reaper",
        "Emerald Sawmaster",
        "Ancient Rootcaller",
        "Hollowwood King",
        "Wildheart Titan",
        "Eclipse Forester",
        "Worldtree Legend",
    ],
    "swordsman": [
        "Novice Blade",
        "Rusty Duelist",
        "Steel Apprentice",
        "Court Fencer",
        "Arena Duelist",
        "Mercenary Blade",
        "Veteran Swordsman",
        "Knight-Errant",
        "Blade Master",
        "Warborn Champion",
        "Living Legend",
        "Mythic Sword Saint",
        "Dragonfang Duelist",
        "Voidsteel Executioner",
        "Moonlit Kensai",
        "Iron Tempest",
        "Warlord of Edges",
        "Eclipse Blademaster",
        "Fate-Cleaver",
        "Worldbreaker Saint",
    ],
    "farmer": [
        "Fieldhand",
        "Seed Runner",
        "Sprout Tender",
        "Soil Worker",
        "Grower",
        "Irrigation Keeper",
        "Harvest Veteran",
        "Crop Whisperer",
        "Barnstead Guardian",
        "Granary Master",
        "Master Farmer",
        "Agricultural Legend",
        "Mythic Earthshaper",
        "Stormplow Keeper",
        "Sunroot Cultivator",
        "Golden Yield King",
        "Moonfield Warden",
        "Eclipse Harvester",
        "Breadlord Eternal",
        "Worldsoil Myth",
    ],
    "blacksmith": [
        "Forge Recruit",
        "Hammer Apprentice",
        "Coal Tender",
        "Anvil Worker",
        "Steel Turner",
        "Chainmaker",
        "Blade Forger",
        "Armor Mender",
        "Master Smith",
        "Runeforge Artisan",
        "Mythic Blacksmith",
        "Legendary Forgemaster",
        "Dragonfire Smith",
        "Void Alloy Crafter",
        "Relic Temperer",
        "Titan Foundry Chief",
        "Eclipse Armorer",
        "Fateforged Artisan",
        "Worldsteel Architect",
        "Forge Sovereign",
    ],
    "bounty_hunter": [
        "Contract Rookie",
        "Trail Sniffer",
        "Wanted Runner",
        "Tracker",
        "Marksman",
        "Contract Veteran",
        "Shadow Pursuer",
        "Deadeye Hunter",
        "Elite Captor",
        "Warrant Master",
        "Legendary Hunter",
        "Mythic Bounty Lord",
        "Ghost Tracker",
        "Void Pursuer",
        "Relic Seeker",
        "Titan Binder",
        "Eclipse Enforcer",
        "Fate Collector",
        "World Warrant",
        "Law of Legends",
    ],
    "pirate": [
        "Deckhand",
        "Cabin Raider",
        "Dock Cutpurse",
        "Rum Runner",
        "Boarding Rogue",
        "Sea Reaver",
        "Privateer",
        "Corsair Veteran",
        "Captain of Coin",
        "Dread Captain",
        "Legendary Pirate",
        "Mythic Marauder",
        "Kraken Raider",
        "Abyss Corsair",
        "Ghost Flag Captain",
        "Titan Plunderlord",
        "Eclipse Buccaneer",
        "Fate’s Raider",
        "Worldsea Tyrant",
        "Ocean Warlord",
    ],
    "robber": [
        "Pickpocket",
        "Back-Alley Thief",
        "Lock Learner",
        "Street Mugger",
        "Safecracker",
        "Heist Runner",
        "Shadow Robber",
        "Vault Ghost",
        "Master Thief",
        "Crime Boss",
        "Legendary Robber",
        "Mythic Kingpin",
        "Void Burglar",
        "Relic Snatcher",
        "Titan Heister",
        "Eclipse Phantom",
        "Fate’s Bandit",
        "World Vaultbreaker",
        "Night Sovereign",
        "Urban Myth",
    ],
    "influencer": [
        "Trend Rookie",
        "Small Creator",
        "Collab Magnet",
        "Viral Spark",
        "Brand Friendly",
        "Content Machine",
        "Audience Favorite",
        "Sponsored Star",
        "Hype Captain",
        "Culture Maker",
        "Legendary Influencer",
        "Mythic Icon",
        "Eclipse Celebrity",
        "World Trendsetter",
    ],
    "onlychat_model": [
        "Chat Darling",
        "Attention Magnet",
        "DM Tease",
        "Top Chatter",
        "Fan Club Favorite",
        "VIP Whisper",
        "Hype Model",
        "Premium Icon",
        "Legendary OnlyChat",
        "Mythic Muse",
        "Eclipse Temptation",
        "World-Class Model",
    ],
    "streamer": [
        "Live Rookie",
        "Mic Check",
        "On-Cam Creator",
        "Chat Entertainer",
        "Raid Survivor",
        "Stream Grinder",
        "Partner Vibes",
        "Frontpage Moment",
        "Legendary Streamer",
        "Mythic Broadcaster",
        "Eclipse Headliner",
        "Worldstage Star",
    ],
    "president": [
        "Council Member",
        "Local Leader",
        "Policy Pusher",
        "Public Speaker",
        "Executive Force",
        "National Voice",
        "Commander",
        "High Office",
        "Legendary President",
        "Mythic Sovereign",
        "Eclipse Ruler",
        "World Leader",
    ],
}


def title_for(job_key: str, prestige: int) -> str:
    key = (job_key or "").strip().lower()
    p = max(int(prestige), 0)
    titles = JOB_TITLES.get(key)
    if not titles:
        return f"{key.title()} Rank {p + 1}"
    if p < len(titles):
        return titles[p]
    return titles[-1]


# ============================================================
# Upgrades + Items (ship-ready)
# ============================================================

@dataclass(frozen=True)
class JobEffects:
    payout_bonus_bp: int = 0
    fail_reduction_bp: int = 0
    stamina_discount_bp: int = 0
    job_xp_bonus_bp: int = 0
    user_xp_bonus_bp: int = 0
    extra_roll_bp: int = 0
    rare_find_bp: int = 0

    def clamp(self) -> "JobEffects":
        return JobEffects(
            payout_bonus_bp=max(min(int(self.payout_bonus_bp), 50_000), -50_000),
            fail_reduction_bp=max(min(int(self.fail_reduction_bp), 10_000), 0),
            stamina_discount_bp=max(min(int(self.stamina_discount_bp), 9_500), 0),
            job_xp_bonus_bp=max(min(int(self.job_xp_bonus_bp), 50_000), -50_000),
            user_xp_bonus_bp=max(min(int(self.user_xp_bonus_bp), 50_000), -50_000),
            extra_roll_bp=max(min(int(self.extra_roll_bp), 10_000), 0),
            rare_find_bp=max(min(int(self.rare_find_bp), 10_000), 0),
        )


class JobExtrasAdapter(Protocol):
    async def get_job_effects(
        self,
        session: AsyncSession,
        *,
        guild_id: int,
        user_id: int,
        job_id: int,
        job_key: str,
        prestige: int,
        level: int,
    ) -> JobEffects: ...


class NoopExtrasAdapter:
    async def get_job_effects(
        self,
        session: AsyncSession,
        *,
        guild_id: int,
        user_id: int,
        job_id: int,
        job_key: str,
        prestige: int,
        level: int,
    ) -> JobEffects:
        return JobEffects()


def combine_effects(effects: Iterable[JobEffects]) -> JobEffects:
    payout = 0
    fail_red = 0
    stam_disc = 0
    job_xp = 0
    user_xp = 0
    extra = 0
    rare = 0

    for e in effects:
        payout += int(e.payout_bonus_bp)
        fail_red += int(e.fail_reduction_bp)
        stam_disc += int(e.stamina_discount_bp)
        job_xp += int(e.job_xp_bonus_bp)
        user_xp += int(e.user_xp_bonus_bp)
        extra += int(e.extra_roll_bp)
        rare += int(e.rare_find_bp)

    return JobEffects(
        payout_bonus_bp=payout,
        fail_reduction_bp=fail_red,
        stamina_discount_bp=stam_disc,
        job_xp_bonus_bp=job_xp,
        user_xp_bonus_bp=user_xp,
        extra_roll_bp=extra,
        rare_find_bp=rare,
    ).clamp()


# ============================================================
# Core progression engine
# ============================================================

@dataclass(frozen=True)
class _ProgressState:
    prestige: int
    title: str
    level: int
    xp_into: int


def _apply_xp_bonus(base_xp: int, bonus_bp: int) -> int:
    x = max(int(base_xp), 0)
    bp = int(bonus_bp)
    return max((x * (10_000 + bp)) // 10_000, 0)


def _advance_levels(
    *,
    tier: JobTier,
    job_key: str,
    prestige: int,
    title: str,
    level: int,
    xp_into: int,
    add_xp: int,
) -> Tuple[_ProgressState, bool, bool]:
    p = max(int(prestige), 0)
    lvl = max(int(level), 1)
    into = max(int(xp_into), 0)
    gained = max(int(add_xp), 0)

    leveled = False
    prestiged = False

    into += gained

    while True:
        cap = level_cap_for(p)
        needed = xp_needed_for_level(tier=tier, prestige=p, level=lvl)

        if into < needed:
            break

        into -= needed
        lvl += 1
        leveled = True

        if lvl > cap:
            prestiged = True
            p += 1
            lvl = 1
            into = 0
            title = title_for(job_key, p)

    return _ProgressState(prestige=p, title=title, level=lvl, xp_into=into), leveled, prestiged


# ============================================================
# DB access
# ============================================================

async def get_job_progress_row(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    job_id: int,
) -> Optional[JobProgressRow]:
    return await session.scalar(
        select(JobProgressRow).where(
            JobProgressRow.guild_id == guild_id,
            JobProgressRow.user_id == user_id,
            JobProgressRow.job_id == job_id,
        )
    )


async def ensure_job_progress_row(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    job_id: int,
    job_key: str,
) -> JobProgressRow:
    row = await get_job_progress_row(session, guild_id=guild_id, user_id=user_id, job_id=job_id)
    if row is not None:
        return row

    title = encode_title(0, title_for(job_key, 0))

    row = JobProgressRow(
        guild_id=guild_id,
        user_id=user_id,
        job_id=job_id,
        job_xp=0,
        job_level=1,
        job_title=title,
    )
    session.add(row)
    await session.flush()
    return row


# ============================================================
# Public API
# ============================================================

async def get_snapshot(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    job_id: int,
    job_key: str,
    tier: JobTier,
    extras: JobExtrasAdapter | None = None,
) -> JobProgressSnapshot:
    row = await ensure_job_progress_row(
        session,
        guild_id=guild_id,
        user_id=user_id,
        job_id=job_id,
        job_key=job_key,
    )

    prestige, base_title = decode_title(row.job_title)
    title = base_title or title_for(job_key, prestige)

    lvl = max(int(row.job_level), 1)
    xp_bank = max(int(row.job_xp), 0)

    needed = xp_needed_for_level(tier=tier, prestige=prestige, level=lvl)
    xp_into = xp_bank

    _ = extras
    return JobProgressSnapshot(
        guild_id=guild_id,
        user_id=user_id,
        job_id=job_id,
        job_key=job_key,
        prestige=prestige,
        title=title,
        level=lvl,
        xp_into_level=xp_into,
        xp_needed=needed,
        total_job_xp_bank=xp_bank,
    )


async def award_job_xp(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    job_id: int,
    job_key: str,
    tier: JobTier,
    base_xp: int,
    extras: JobExtrasAdapter | None = None,
) -> JobAwardResult:
    if extras is None:
        extras = NoopExtrasAdapter()

    row = await ensure_job_progress_row(
        session,
        guild_id=guild_id,
        user_id=user_id,
        job_id=job_id,
        job_key=job_key,
    )

    old_prestige, old_title = decode_title(row.job_title)
    old_title_clean = old_title or title_for(job_key, old_prestige)
    old_level = max(int(row.job_level), 1)
    old_xp_into = max(int(row.job_xp), 0)

    effects = await extras.get_job_effects(
        session,
        guild_id=guild_id,
        user_id=user_id,
        job_id=job_id,
        job_key=job_key,
        prestige=old_prestige,
        level=old_level,
    )
    effects = effects.clamp()

    gained = _apply_xp_bonus(base_xp, effects.job_xp_bonus_bp + GLOBAL_JOB_XP_MULT_BP)

    state, leveled, prestiged = _advance_levels(
        tier=tier,
        job_key=job_key,
        prestige=old_prestige,
        title=old_title_clean,
        level=old_level,
        xp_into=old_xp_into,
        add_xp=gained,
    )

    row.job_level = int(state.level)
    row.job_xp = int(state.xp_into)
    row.job_title = encode_title(state.prestige, state.title)

    await session.flush()

    needed = xp_needed_for_level(tier=tier, prestige=state.prestige, level=state.level)

    snap = JobProgressSnapshot(
        guild_id=guild_id,
        user_id=user_id,
        job_id=job_id,
        job_key=job_key,
        prestige=state.prestige,
        title=state.title,
        level=state.level,
        xp_into_level=state.xp_into,
        xp_needed=needed,
        total_job_xp_bank=state.xp_into,
    )

    delta = JobProgressDelta(
        old_prestige=old_prestige,
        new_prestige=state.prestige,
        old_title=old_title_clean,
        new_title=state.title,
        old_level=old_level,
        new_level=state.level,
        old_xp_into=old_xp_into,
        new_xp_into=state.xp_into,
        xp_gained=gained,
        leveled_up=bool(leveled),
        prestiged=bool(prestiged),
    )

    return JobAwardResult(snapshot=snap, delta=delta, effects=effects)


# ============================================================
# Retroactive boost migration (x10)
# ============================================================

def _total_xp_required_to_finish_prestige(*, tier: JobTier, prestige: int) -> int:
    cap = level_cap_for(prestige)
    total = 0
    for lvl in range(1, cap + 1):
        total += xp_needed_for_level(tier=tier, prestige=prestige, level=lvl)
    return total


def total_xp_from_state(
    *,
    tier: JobTier,
    job_key: str,
    prestige: int,
    level: int,
    xp_into: int,
) -> int:
    _ = job_key

    p = max(int(prestige), 0)
    lvl = max(int(level), 1)
    into = max(int(xp_into), 0)

    total = 0

    for pp in range(0, p):
        total += _total_xp_required_to_finish_prestige(tier=tier, prestige=pp)

    for ll in range(1, lvl):
        total += xp_needed_for_level(tier=tier, prestige=p, level=ll)

    total += into
    return total


def state_from_total_xp(
    *,
    tier: JobTier,
    job_key: str,
    total_xp: int,
) -> _ProgressState:
    base_title = title_for(job_key, 0)
    state, _leveled, _prestiged = _advance_levels(
        tier=tier,
        job_key=job_key,
        prestige=0,
        title=base_title,
        level=1,
        xp_into=0,
        add_xp=max(int(total_xp), 0),
    )
    return state


class JobMetaLookup(Protocol):
    async def get_meta(self, session: AsyncSession, *, job_id: int) -> Tuple[str, JobTier]:
        """
        Return (job_key, tier) for a given job_id.
        """
        ...


async def migrate_job_xp_multiplier(
    session: AsyncSession,
    *,
    factor: float = 10.0,
    lookup: JobMetaLookup,
    guild_id: int | None = None,
    dry_run: bool = True,
) -> int:
    """
    Retroactively boost progress as if users had earned (old_total_xp * factor).

    dry_run=True: does not flush changes
    returns: rows touched
    """
    if factor <= 0:
        return 0

    q = select(JobProgressRow)
    if guild_id is not None:
        q = q.where(JobProgressRow.guild_id == int(guild_id))

    rows = (await session.scalars(q)).all()
    touched = 0

    for row in rows:
        old_prestige, old_title = decode_title(row.job_title)
        old_title_clean = old_title or title_for("", old_prestige)
        old_level = max(int(row.job_level), 1)
        old_xp_into = max(int(row.job_xp), 0)

        job_key, tier = await lookup.get_meta(session, job_id=int(row.job_id))
        old_title_clean = old_title or title_for(job_key, old_prestige)

        old_total = total_xp_from_state(
            tier=tier,
            job_key=job_key,
            prestige=old_prestige,
            level=old_level,
            xp_into=old_xp_into,
        )

        new_total = int(old_total * factor)

        new_state = state_from_total_xp(
            tier=tier,
            job_key=job_key,
            total_xp=new_total,
        )

        row.job_level = int(new_state.level)
        row.job_xp = int(new_state.xp_into)
        row.job_title = encode_title(new_state.prestige, new_state.title)

        touched += 1

    if not dry_run:
        await session.flush()

    return touched


# ============================================================
# Upgrade + Item scaffolding (ship-ready hooks)
# ============================================================

class RequirementType(str, Enum):
    PRESTIGE_AT_LEAST = "prestige_at_least"
    LEVEL_AT_LEAST = "level_at_least"


@dataclass(frozen=True)
class Requirement:
    type: RequirementType
    value: int


@dataclass(frozen=True)
class UpgradeDef:
    key: str
    name: str
    description: str
    max_rank: int
    requirements: Tuple[Requirement, ...]
    effects_per_rank: JobEffects


@dataclass(frozen=True)
class ItemDef:
    key: str
    name: str
    description: str
    stackable: bool = True
    passive_effects: JobEffects = JobEffects()


def meets_requirements(
    *,
    prestige: int,
    level: int,
    reqs: Tuple[Requirement, ...],
) -> bool:
    p = max(int(prestige), 0)
    lvl = max(int(level), 1)
    for r in reqs:
        v = int(r.value)
        if r.type == RequirementType.PRESTIGE_AT_LEAST:
            if p < v:
                return False
        elif r.type == RequirementType.LEVEL_AT_LEAST:
            if lvl < v:
                return False
    return True


UPGRADES: Dict[str, UpgradeDef] = {
    "payout_boost": UpgradeDef(
        key="payout_boost",
        name="Payout Boost",
        description="Increases silver earned from successful work.",
        max_rank=10,
        requirements=(Requirement(RequirementType.LEVEL_AT_LEAST, 3),),
        effects_per_rank=JobEffects(payout_bonus_bp=250),
    ),
    "stamina_saver": UpgradeDef(
        key="stamina_saver",
        name="Stamina Saver",
        description="Reduces stamina cost per work.",
        max_rank=8,
        requirements=(Requirement(RequirementType.LEVEL_AT_LEAST, 5),),
        effects_per_rank=JobEffects(stamina_discount_bp=150),
    ),
    "fail_guard": UpgradeDef(
        key="fail_guard",
        name="Fail Guard",
        description="Reduces failure chance on jobs that can fail.",
        max_rank=6,
        requirements=(Requirement(RequirementType.PRESTIGE_AT_LEAST, 1),),
        effects_per_rank=JobEffects(fail_reduction_bp=120),
    ),
}

ITEMS: Dict[str, ItemDef] = {
    "lucky_charm": ItemDef(
        key="lucky_charm",
        name="Lucky Charm",
        description="Slightly increases payouts and rare outcome chances.",
        stackable=False,
        passive_effects=JobEffects(payout_bonus_bp=500, rare_find_bp=200),
    ),
    "stamina_tonic": ItemDef(
        key="stamina_tonic",
        name="Stamina Tonic",
        description="Passive stamina discount while owned.",
        stackable=True,
        passive_effects=JobEffects(stamina_discount_bp=300),
    ),
}


class JobInventoryAdapter(Protocol):
    async def get_owned_items(
        self,
        session: AsyncSession,
        *,
        guild_id: int,
        user_id: int,
    ) -> Dict[str, int]: ...

    async def get_upgrade_ranks(
        self,
        session: AsyncSession,
        *,
        guild_id: int,
        user_id: int,
        job_id: int,
    ) -> Dict[str, int]: ...


class NoopInventoryAdapter:
    async def get_owned_items(self, session: AsyncSession, *, guild_id: int, user_id: int) -> Dict[str, int]:
        return {}

    async def get_upgrade_ranks(self, session: AsyncSession, *, guild_id: int, user_id: int, job_id: int) -> Dict[str, int]:
        return {}


async def compute_effects_from_upgrades_and_items(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    job_id: int,
    prestige: int,
    level: int,
    inv: JobInventoryAdapter | None = None,
) -> JobEffects:
    if inv is None:
        inv = NoopInventoryAdapter()

    owned_items = await inv.get_owned_items(session, guild_id=guild_id, user_id=user_id)
    ranks = await inv.get_upgrade_ranks(session, guild_id=guild_id, user_id=user_id, job_id=job_id)

    effects_list: List[JobEffects] = []

    for item_key, qty in owned_items.items():
        if qty <= 0:
            continue
        idef = ITEMS.get(item_key)
        if not idef:
            continue
        times = qty if idef.stackable else 1
        for _ in range(times):
            effects_list.append(idef.passive_effects)

    for up_key, r in ranks.items():
        if r <= 0:
            continue
        udef = UPGRADES.get(up_key)
        if not udef:
            continue
        rank = min(int(r), int(udef.max_rank))
        if not meets_requirements(prestige=prestige, level=level, reqs=udef.requirements):
            continue
        for _ in range(rank):
            effects_list.append(udef.effects_per_rank)

    return combine_effects(effects_list)