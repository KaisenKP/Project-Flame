from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class RobberyTier(str, Enum):
    BEGINNER = "beginner"
    MID = "mid"
    HIGH = "high"
    ENDGAME = "endgame"


class BankApproach(str, Enum):
    SILENT = "silent"
    AGGRESSIVE = "aggressive"
    CON = "con"


class CrewRole(str, Enum):
    LEADER = "leader"
    HACKER = "hacker"
    DRIVER = "driver"
    ENFORCER = "enforcer"
    FLEX = "flex"
    UNASSIGNED = "unassigned"


class FinaleOutcome(str, Enum):
    CLEAN_SUCCESS = "clean_success"
    MESSY_SUCCESS = "messy_success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED_ESCAPE = "failed_escape"
    FULL_FAILURE = "full_failure"


class FinalePhase(str, Enum):
    ENTRY = "entry"
    VAULT = "vault"
    LOOT = "loot"
    ESCAPE = "escape"
    RESULTS = "results"


@dataclass(frozen=True)
class PrepDefinition:
    key: str
    name: str
    description: str
    bonus_text: str
    effects: dict[str, int]
    mandatory: bool = True


@dataclass(frozen=True)
class EventDefinition:
    key: str
    name: str
    description: str
    phase: FinalePhase
    weight: int
    effects: dict[str, int]
    positive: bool = False
    rare: bool = False
    approaches: tuple[BankApproach, ...] = ()


@dataclass(frozen=True)
class ApproachProfile:
    entry_bp: int
    vault_bp: int
    loot_bp: int
    escape_bp: int
    payout_mult_bp: int
    alert_per_round: int
    failure_tolerance: int
    heat_mult_bp: int
    event_bias: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class RobberyTemplate:
    robbery_id: str
    display_name: str
    tier: RobberyTier
    crew_min: int
    crew_max: int
    payout_min: int
    payout_max: int
    entry_cost: int
    recommended_rep: int
    heat_gain: int
    cooldown_seconds: int
    prep_count: int
    prep_keys: tuple[str, ...]
    weekly_lockout: bool
    description: str
    available_approaches: tuple[BankApproach, ...]
    approach_modifiers: dict[BankApproach, ApproachProfile]
    solo_allowed: bool = False
    tags: tuple[str, ...] = ()


PREP_DEFS: dict[str, PrepDefinition] = {
    "getaway_vehicle": PrepDefinition("getaway_vehicle", "Getaway Vehicle", "Secure a tuned escape rig for the exfil.", "+12% escape success.", {"escape_bp": 1200, "loot_loss_reduction_bp": 1500}),
    "disguise_prep": PrepDefinition("disguise_prep", "Disguise Prep", "Line up clean uniforms and movement cover.", "+10% Silent entry success.", {"entry_bp": 1000, "alert_delay": 1}),
    "jammer_prep": PrepDefinition("jammer_prep", "Jammer Prep", "Deploy signal noise to slow security calls.", "Delay alert escalation by 1 round.", {"alert_delay": 1, "vault_bp": 500}),
    "vault_scan": PrepDefinition("vault_scan", "Vault Scan", "Map the vault hardware before the hit.", "Reduce vault difficulty.", {"vault_bp": 1300}),
    "route_scout": PrepDefinition("route_scout", "Route Scout", "Scout side streets and fallback exits.", "Unlock a safer escape route.", {"escape_bp": 1000, "heat_bp": -800}),
    "credentials_spoof": PrepDefinition("credentials_spoof", "Credentials Spoof", "Seed fake credentials into access control.", "+10% Con entry success.", {"entry_bp": 900, "vault_bp": 600}),
    "extra_bags": PrepDefinition("extra_bags", "Extra Bags", "Bring reinforced carry bags.", "+15% loot carry capacity.", {"loot_bp": 1500}),
    "blackout_support": PrepDefinition("blackout_support", "Blackout Support", "Time a local systems blackout.", "Reveal 1 hidden finale event.", {"reveal_events": 1, "entry_bp": 600, "vault_bp": 600}),
    "inside_contact": PrepDefinition("inside_contact", "Inside Contact", "Acquire a discreet schedule handoff.", "Lower alert growth for one round.", {"alert_per_round": -1, "entry_bp": 600}),
    "blueprint_access": PrepDefinition("blueprint_access", "Blueprint Access", "Study maintenance routes and vault layout.", "Reduce loot loss on failed escape checks.", {"vault_bp": 800, "escape_bp": 800, "loot_loss_reduction_bp": 1000}),
}


APPROACHES: dict[BankApproach, ApproachProfile] = {
    BankApproach.SILENT: ApproachProfile(entry_bp=8700, vault_bp=10300, loot_bp=10800, escape_bp=9400, payout_mult_bp=11800, alert_per_round=11, failure_tolerance=1, heat_mult_bp=8500, event_bias={"positive": 900, "negative": -400}),
    BankApproach.AGGRESSIVE: ApproachProfile(entry_bp=10800, vault_bp=9600, loot_bp=9800, escape_bp=10600, payout_mult_bp=9400, alert_per_round=16, failure_tolerance=3, heat_mult_bp=12600, event_bias={"positive": 0, "negative": 650}),
    BankApproach.CON: ApproachProfile(entry_bp=9800, vault_bp=10000, loot_bp=10000, escape_bp=9900, payout_mult_bp=10200, alert_per_round=13, failure_tolerance=2, heat_mult_bp=10000, event_bias={"positive": 300, "negative": 150}),
}


TEMPLATES: dict[str, RobberyTemplate] = {
    "corner_branch": RobberyTemplate(
        robbery_id="corner_branch",
        display_name="Corner Branch Job",
        tier=RobberyTier.BEGINNER,
        crew_min=1,
        crew_max=2,
        payout_min=10_000_000,
        payout_max=18_000_000,
        entry_cost=1_250_000,
        recommended_rep=0,
        heat_gain=8,
        cooldown_seconds=8 * 3600,
        prep_count=2,
        prep_keys=("getaway_vehicle", "disguise_prep", "vault_scan", "route_scout"),
        weekly_lockout=False,
        description="Tutorial-friendly branch score with a short loot window and forgiving escape.",
        available_approaches=(BankApproach.SILENT, BankApproach.AGGRESSIVE, BankApproach.CON),
        approach_modifiers=APPROACHES,
        solo_allowed=True,
        tags=("solo", "starter", "short"),
    ),
    "downtown_reserve": RobberyTemplate(
        robbery_id="downtown_reserve",
        display_name="Downtown Reserve Hit",
        tier=RobberyTier.MID,
        crew_min=2,
        crew_max=3,
        payout_min=22_000_000,
        payout_max=40_000_000,
        entry_cost=3_500_000,
        recommended_rep=120,
        heat_gain=16,
        cooldown_seconds=16 * 3600,
        prep_count=3,
        prep_keys=("jammer_prep", "vault_scan", "credentials_spoof", "route_scout", "blueprint_access"),
        weekly_lockout=False,
        description="Hack-heavy reserve where strong vault control expands the loot window.",
        available_approaches=(BankApproach.SILENT, BankApproach.AGGRESSIVE, BankApproach.CON),
        approach_modifiers=APPROACHES,
        tags=("hack-heavy",),
    ),
    "pacific_dominion": RobberyTemplate(
        robbery_id="pacific_dominion",
        display_name="Pacific Dominion Job",
        tier=RobberyTier.HIGH,
        crew_min=3,
        crew_max=4,
        payout_min=45_000_000,
        payout_max=80_000_000,
        entry_cost=9_500_000,
        recommended_rep=320,
        heat_gain=24,
        cooldown_seconds=30 * 3600,
        prep_count=4,
        prep_keys=("getaway_vehicle", "jammer_prep", "vault_scan", "extra_bags", "route_scout", "blueprint_access"),
        weekly_lockout=False,
        description="High-risk prestige job where bag weight and greed pressure punish slow crews.",
        available_approaches=(BankApproach.SILENT, BankApproach.AGGRESSIVE, BankApproach.CON),
        approach_modifiers=APPROACHES,
        tags=("weight", "greed"),
    ),
    "bullion_exchange": RobberyTemplate(
        robbery_id="bullion_exchange",
        display_name="Bullion Exchange Raid",
        tier=RobberyTier.HIGH,
        crew_min=3,
        crew_max=4,
        payout_min=90_000_000,
        payout_max=150_000_000,
        entry_cost=18_000_000,
        recommended_rep=520,
        heat_gain=32,
        cooldown_seconds=42 * 3600,
        prep_count=4,
        prep_keys=("getaway_vehicle", "blackout_support", "extra_bags", "blueprint_access", "inside_contact", "route_scout"),
        weekly_lockout=False,
        description="Dense bullion haul with slower carry speed, fewer rounds, and giant value spikes.",
        available_approaches=(BankApproach.SILENT, BankApproach.AGGRESSIVE, BankApproach.CON),
        approach_modifiers=APPROACHES,
        tags=("bullion", "heavy"),
    ),
    "national_mint": RobberyTemplate(
        robbery_id="national_mint",
        display_name="National Mint Blackout",
        tier=RobberyTier.ENDGAME,
        crew_min=4,
        crew_max=4,
        payout_min=180_000_000,
        payout_max=300_000_000,
        entry_cost=42_000_000,
        recommended_rep=900,
        heat_gain=50,
        cooldown_seconds=7 * 24 * 3600,
        prep_count=5,
        prep_keys=("getaway_vehicle", "blackout_support", "jammer_prep", "vault_scan", "extra_bags", "inside_contact", "blueprint_access", "credentials_spoof"),
        weekly_lockout=True,
        description="Weekly elite score with multi-checkpoint pressure and the biggest take in the mode.",
        available_approaches=(BankApproach.SILENT, BankApproach.AGGRESSIVE, BankApproach.CON),
        approach_modifiers=APPROACHES,
        tags=("elite", "weekly", "endgame"),
    ),
}


def get_template(robbery_id: str) -> RobberyTemplate:
    return TEMPLATES[str(robbery_id)]
