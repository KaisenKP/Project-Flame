from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Optional, Sequence

import discord

from services.jobs_core import JobCategory, JobDef, apply_bp, clamp_int, fmt_int, roll_bp

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobPresentation:
    fantasy: str
    payout_style: str
    risk_level: str
    perk_summary: str
    danger_summary: str
    can_trigger_danger: bool


@dataclass(frozen=True)
class EndgameRandomEventDefinition:
    key: str
    name: str
    description: str
    chance_bp: int
    payout_multiplier_bp: int = 0
    bonus_silver_flat: int = 0
    stamina_delta: int = 0
    fail_override: bool | None = None


@dataclass(frozen=True)
class DangerOutcome:
    key: str
    text: str
    weight: int
    payout_multiplier_bp: int = 0
    flat_silver: int = 0
    xp_multiplier_bp: int = 0
    flat_xp: int = 0
    fail_run: bool = False


@dataclass(frozen=True)
class DangerChoice:
    key: str
    label: str
    style: discord.ButtonStyle
    description: str
    outcomes: Sequence[DangerOutcome]


@dataclass(frozen=True)
class DangerEncounter:
    key: str
    title: str
    description: str
    stake_text: str
    choices: Sequence[DangerChoice]
    safe_choice_key: str


@dataclass(frozen=True)
class DangerResolution:
    encounter: DangerEncounter
    choice: DangerChoice
    outcome: DangerOutcome
    payout: int
    job_xp: int
    failed: bool
    choice_label: str
    summary_line: str
    detail_line: str
    timed_out: bool = False


@dataclass(frozen=True)
class NormalInteractionOutcome:
    key: str
    text: str
    weight: int
    payout_multiplier_bp: int = 0
    flat_silver: int = 0
    xp_multiplier_bp: int = 0
    flat_xp: int = 0


@dataclass(frozen=True)
class NormalInteraction:
    key: str
    title: str
    description: str
    stake_text: str
    outcomes: Sequence[NormalInteractionOutcome]


@dataclass(frozen=True)
class NormalResolution:
    interaction: NormalInteraction
    outcome: NormalInteractionOutcome
    payout: int
    job_xp: int
    summary_line: str


@dataclass(frozen=True)
class _DangerSeed:
    key: str
    title: str
    flavor: str
    stake: str
    safe_label: str
    medium_label: str
    greedy_label: str


@dataclass(frozen=True)
class _NormalSeed:
    key: str
    title: str
    flavor: str
    stake: str


JOB_PRESENTATIONS: dict[str, JobPresentation] = {
    "artifact_hunter": JobPresentation(
        fantasy="Legendary relic runner pulling wealth out of lost vaults.",
        payout_style="High variance with massive rare relic spikes.",
        risk_level="Medium risk",
        perk_summary="Massive jackpot potential from rare relic finds.",
        danger_summary="Danger Encounters can force greedy vault decisions.",
        can_trigger_danger=True,
    ),
    "drug_lord": JobPresentation(
        fantasy="Fictional underworld kingpin crushing the city for silver.",
        payout_style="Highest ceiling in the game with brutal swings.",
        risk_level="Extreme risk",
        perk_summary="Highest ceiling in the game, but crackdowns hit hard.",
        danger_summary="Danger Encounters force high-pressure deal and territory choices.",
        can_trigger_danger=True,
    ),
    "dragon_slayer": JobPresentation(
        fantasy="Mythic contract hunter cashing out impossible dragon kills.",
        payout_style="Elite burst income from bounty and rare part spikes.",
        risk_level="Medium-high risk",
        perk_summary="Dragon contracts and rare parts can explode your payout.",
        danger_summary="Danger Encounters force kill, loot, or survival decisions.",
        can_trigger_danger=True,
    ),
    "business_ceo": JobPresentation(
        fantasy="Corporate emperor printing silver through executive dominance.",
        payout_style="The most stable elite income path.",
        risk_level="Low risk",
        perk_summary="The most stable elite income path.",
        danger_summary="Occasional executive crisis choices with safer outcomes.",
        can_trigger_danger=True,
    ),
    "space_miner": JobPresentation(
        fantasy="Deep-space extractor chasing absurd cosmic deposits.",
        payout_style="Wild volatility with cracked jackpot ore pulls.",
        risk_level="High risk",
        perk_summary="Deep-space deposits can turn one run into a fortune.",
        danger_summary="Danger Encounters force drill, stabilize, or eject choices.",
        can_trigger_danger=True,
    ),
}


ENDGAME_EVENT_CATALOG: dict[str, tuple[EndgameRandomEventDefinition, ...]] = {
    "artifact_hunter": (
        EndgameRandomEventDefinition("ancient_vault", "Ancient Vault", "A sealed vault cracks open and boosts the haul.", 850, payout_multiplier_bp=2800),
        EndgameRandomEventDefinition("hidden_reliquary", "Hidden Reliquary", "A secret reliquary adds priceless relic stock.", 720, bonus_silver_flat=340),
        EndgameRandomEventDefinition("royal_collector", "Royal Collector", "A collector starts a bidding war for your find.", 520, payout_multiplier_bp=6500),
        EndgameRandomEventDefinition("cursed_find", "Cursed Find", "The relic is dangerous, but the payoff is huge.", 420, payout_multiplier_bp=9000),
        EndgameRandomEventDefinition("counterfeit_relic", "Counterfeit Relic", "One piece is fake and drags the sale down.", 500, payout_multiplier_bp=-3000),
        EndgameRandomEventDefinition("tomb_collapse", "Tomb Collapse", "You barely escape and only salvage part of the haul.", 380, payout_multiplier_bp=-5000),
    ),
    "drug_lord": (
        EndgameRandomEventDefinition("territory_sweep", "Territory Sweep", "A full sweep of your turf floods the stash.", 820, payout_multiplier_bp=2600),
        EndgameRandomEventDefinition("cartel_shipment", "Cartel Shipment", "A giant shipment lands clean and spikes the run.", 650, bonus_silver_flat=420),
        EndgameRandomEventDefinition("street_monopoly", "Street Monopoly", "You lock down the block and your take surges.", 480, payout_multiplier_bp=8000),
        EndgameRandomEventDefinition("corrupt_official", "Corrupt Official", "A dirty payoff keeps the money flowing.", 430, payout_multiplier_bp=4500),
        EndgameRandomEventDefinition("rival_raid", "Rival Raid", "A rival hit strips part of the take.", 520, payout_multiplier_bp=-4500),
        EndgameRandomEventDefinition("cash_warehouse_hit", "Cash Warehouse Hit", "A warehouse score detonates your payout.", 280, payout_multiplier_bp=12000),
    ),
    "dragon_slayer": (
        EndgameRandomEventDefinition("dragon_nest", "Dragon Nest", "A nest cache adds scorched treasure to the reward.", 760, payout_multiplier_bp=2400),
        EndgameRandomEventDefinition("royal_bounty", "Royal Bounty", "The crown posts a premium bounty on your target.", 650, bonus_silver_flat=360),
        EndgameRandomEventDefinition("heartscale_drop", "Heartscale Drop", "A pristine heartscale drop sells for a fortune.", 500, payout_multiplier_bp=6200),
        EndgameRandomEventDefinition("slayers_trophy", "Slayer's Trophy Bonus", "Your trophy draw turns the hunt legendary.", 360, payout_multiplier_bp=10000),
        EndgameRandomEventDefinition("burned_battlefield", "Burned Battlefield", "Fire ruins part of the contract payout.", 500, payout_multiplier_bp=-3500),
        EndgameRandomEventDefinition("ancient_wyrm", "Ancient Wyrm Hunt", "A mythic contract pays out at absurd rates.", 260, payout_multiplier_bp=11500),
    ),
    "business_ceo": (
        EndgameRandomEventDefinition("major_acquisition", "Major Acquisition", "The board closes a huge acquisition on your terms.", 780, payout_multiplier_bp=1800),
        EndgameRandomEventDefinition("executive_buyout", "Executive Buyout", "A buyout premium lands straight in your pocket.", 620, bonus_silver_flat=260),
        EndgameRandomEventDefinition("investor_surge", "Investor Surge", "Investors flood the round and smooth the quarter.", 560, payout_multiplier_bp=2200),
        EndgameRandomEventDefinition("dividend_explosion", "Dividend Explosion", "A dividend spike makes this shift elite.", 420, payout_multiplier_bp=4800),
        EndgameRandomEventDefinition("global_expansion", "Global Expansion", "A new market opens and lifts the run.", 380, payout_multiplier_bp=6000),
        EndgameRandomEventDefinition("tax_audit", "Tax Audit", "An audit clips some margin but not the whole quarter.", 420, payout_multiplier_bp=-2000),
    ),
    "space_miner": (
        EndgameRandomEventDefinition("void_crystal_vein", "Void Crystal Vein", "A radiant crystal seam blows up the pull.", 800, payout_multiplier_bp=2600),
        EndgameRandomEventDefinition("alien_core_deposit", "Alien Core Deposit", "An alien core deposit adds insane ore value.", 620, bonus_silver_flat=380),
        EndgameRandomEventDefinition("starstorm_harvest", "Starstorm Harvest", "You catch a harvest window inside a starstorm.", 460, payout_multiplier_bp=7000),
        EndgameRandomEventDefinition("reactor_surge", "Reactor Surge", "The reactor screams but output goes crazy.", 420, payout_multiplier_bp=9500),
        EndgameRandomEventDefinition("derelict_drill_site", "Derelict Drill Site", "An abandoned rig still has premium ore inside.", 500, payout_multiplier_bp=3600),
        EndgameRandomEventDefinition("hull_breach", "Hull Breach", "A breach forces you to dump part of the load.", 520, payout_multiplier_bp=-5000),
    ),
}


DANGER_TRIGGER_BP: dict[str, int] = {
    "artifact_hunter": 1450,
    "drug_lord": 1700,
    "dragon_slayer": 1400,
    "business_ceo": 750,
    "space_miner": 1550,
}

NORMAL_TRIGGER_BP: dict[str, int] = {
    "artifact_hunter": 3200,
    "drug_lord": 2800,
    "dragon_slayer": 3000,
    "business_ceo": 3400,
    "space_miner": 3000,
}


def _pick_weighted(items: Sequence, *, weight_attr: str = "weight"):
    total = sum(max(int(getattr(item, weight_attr, 0)), 0) for item in items)
    if total <= 0:
        return items[0]
    roll = random.randint(1, total)
    acc = 0
    for item in items:
        acc += max(int(getattr(item, weight_attr, 0)), 0)
        if roll <= acc:
            return item
    return items[-1]


def _danger_choices(*, safe_label: str, medium_label: str, greedy_label: str, safer_tone: str, medium_tone: str, greedy_tone: str) -> tuple[DangerChoice, ...]:
    return (
        DangerChoice(
            key="safe",
            label=safe_label,
            style=discord.ButtonStyle.secondary,
            description=safer_tone,
            outcomes=(
                DangerOutcome("safe_win", "You play it cool and keep the haul clean.", 68, payout_multiplier_bp=900, flat_silver=110),
                DangerOutcome("safe_flat", "No fireworks, no disaster. Still a pro call.", 24, payout_multiplier_bp=250),
                DangerOutcome("safe_scratch", "A few losses on exit, but most value survives.", 8, payout_multiplier_bp=-1600),
            ),
        ),
        DangerChoice(
            key="medium",
            label=medium_label,
            style=discord.ButtonStyle.primary,
            description=medium_tone,
            outcomes=(
                DangerOutcome("medium_win", "Solid execution. The run gets a real boost.", 55, payout_multiplier_bp=2900, flat_silver=180),
                DangerOutcome("medium_mixed", "It mostly works, but costs you a slice.", 30, payout_multiplier_bp=900),
                DangerOutcome("medium_fail", "Bad timing. Part of the value gets torched.", 15, payout_multiplier_bp=-4200, flat_xp=-2),
            ),
        ),
        DangerChoice(
            key="greedy",
            label=greedy_label,
            style=discord.ButtonStyle.danger,
            description=greedy_tone,
            outcomes=(
                DangerOutcome("greedy_jackpot", "The greedy line hits. Absolute silver chaos.", 32, payout_multiplier_bp=7600, flat_silver=450, flat_xp=3),
                DangerOutcome("greedy_ok", "You force it and still come out ahead.", 28, payout_multiplier_bp=2800),
                DangerOutcome("greedy_boom", "The gamble detonates. You limp out empty-handed.", 40, fail_run=True, flat_xp=-4),
            ),
        ),
    )


def _build_danger_from_seed(job_key: str, seed: _DangerSeed) -> DangerEncounter:
    return DangerEncounter(
        key=seed.key,
        title=f"Danger Encounter • {seed.title}",
        description=seed.flavor,
        stake_text=seed.stake,
        safe_choice_key="safe",
        choices=_danger_choices(
            safe_label=seed.safe_label,
            medium_label=seed.medium_label,
            greedy_label=seed.greedy_label,
            safer_tone="Low risk. Protect the run.",
            medium_tone="Balanced risk. Good upside.",
            greedy_tone="High risk. Big spike or big collapse.",
        ),
    )


def _build_normal_from_seed(seed: _NormalSeed) -> NormalInteraction:
    return NormalInteraction(
        key=seed.key,
        title=seed.title,
        description=seed.flavor,
        stake_text=seed.stake,
        outcomes=(
            NormalInteractionOutcome("win", "Clean little win. Your shift feels smooth.", 62, payout_multiplier_bp=1300, flat_silver=90),
            NormalInteractionOutcome("big_win", "Lucky break! This one paid better than expected.", 23, payout_multiplier_bp=2600, flat_silver=140, flat_xp=1),
            NormalInteractionOutcome("mixed", "Funny detour. Tiny setback, still net positive vibes.", 15, payout_multiplier_bp=-900),
        ),
    )


def _danger_seed_map() -> dict[str, tuple[_DangerSeed, ...]]:
    return {
        "artifact_hunter": (
            _DangerSeed("artifact_cursed_relic_pulse", "Cursed Relic Pulse", "A relic in your pack starts pulsing and heating up.", "The longer you hold it, the bigger the value and danger.", "Seal It in Lead", "Stabilize and Sprint", "Channel the Pulse"),
            _DangerSeed("artifact_false_floor_vault", "False Floor Vault", "The vault floor cracks beneath your boots.", "You can save yourself, save loot, or risk both.", "Grab Rope Anchor", "Leap with the Haul", "Dive for Deep Cache"),
            _DangerSeed("artifact_rival_crew_ambush", "Rival Crew Ambush", "Another relic crew jumps you before extraction.", "Fight, bluff, or overextend for their stash too.", "Smoke and Evade", "Trade Blows", "Counter-Raid Them"),
            _DangerSeed("artifact_blood_seal_door", "Blood Seal Door", "A sealed chamber demands a blood price to open safely.", "Skip, pay a little, or force all the way in.", "Walk Away", "Offer a Drop", "Break the Seal"),
            _DangerSeed("artifact_whispering_urn", "Whispering Urn", "An urn whispers coordinates and promises bigger treasure.", "Could be jackpot clues or a cursed wild-goose chase.", "Ignore the Voices", "Test One Coordinate", "Follow Every Whisper"),
            _DangerSeed("artifact_dart_wall_trigger", "Dart Wall Trigger", "A hidden trap clicks and poisoned darts prime.", "Your haul depends on how fast and greedy you react.", "Raise Shield", "Dash Zig-Zag", "Grab Idol Mid-Sprint"),
            _DangerSeed("artifact_tomb_floodgate", "Tomb Floodgate", "The room fills rapidly with black water.", "Save your bag, save your team, or loot one more shelf.", "Evacuate Clean", "Secure Main Satchel", "Loot While Swimming"),
            _DangerSeed("artifact_guardian_idol_wakeup", "Guardian Idol Wake-Up", "A stone guardian activates as you lift an artifact.", "Retreat smart, outplay it, or taunt it for hidden chambers.", "Drop and Back Off", "Distract and Exit", "Bait Secret Door"),
            _DangerSeed("artifact_mirror_catacomb", "Mirror Catacomb", "The path splits into mirrored halls; one is cursed.", "Pick safe route or risk the mirrored jackpot corridor.", "Mark and Return", "Probe with Tools", "Sprint the Bright Hall"),
            _DangerSeed("artifact_sun_disc_overload", "Sun Disc Overload", "A glowing sun disc starts charging toward overload.", "Bank what you have or squeeze power from the disc.", "Cut Power", "Bleed Off Charge", "Overclock the Disc"),
            _DangerSeed("artifact_black_market_doublecross", "Black Market Doublecross", "Your buyer arrives with extra muscle and fake payment.", "Take a safer cut or push an explosive counter-deal.", "Abort Deal", "Renegotiate Fast", "Reverse the Doublecross"),
            _DangerSeed("artifact_sand_pit_collapse", "Sand Pit Collapse", "The dig site caves in around a rare cache.", "Rescue path first, or mine deeper while it falls.", "Climb Out", "Pull One Crate", "Drill to Core Vault"),
            _DangerSeed("artifact_echo_hall_disturbance", "Echo Hall Disturbance", "Your footsteps wake something in the walls.", "Silence helps, speed helps, greed tempts.", "Freeze and Listen", "Quiet Sprint", "Crack Wall Reliquary"),
            _DangerSeed("artifact_venom_statue_bite", "Venom Statue Bite", "A serpent idol snaps and injects venom.", "Stabilize first or gamble on finishing the grab.", "Use Antidote", "Patch and Continue", "Ignore It, Push On"),
            _DangerSeed("artifact_grave_wind_lanterns", "Grave Wind Lanterns", "Guide lanterns blow out in a spirit-heavy chamber.", "Blind retreat or eerie shortcut through whispers.", "Relight and Exit", "Follow Compass", "Follow the Ghost Wind"),
            _DangerSeed("artifact_bone_choir", "Bone Choir Chamber", "A pile of bones rattles into a shrieking choir.", "The noise masks movement but may summon worse.", "Retreat Quietly", "Use Noise Cover", "Loot the Choir Altar"),
            _DangerSeed("artifact_timeworn_bridge", "Timeworn Rope Bridge", "A rope bridge over a fissure starts tearing.", "Cross light, cross loaded, or grab hanging relic crates.", "Cross Empty", "Cross with Pack", "Swing for Crates"),
            _DangerSeed("artifact_greed_coin_fever", "Greed Coin Fever", "A gold pile seems to mesmerize your crew.", "Break focus, skim profits, or embrace coin fever.", "Snap Crew Out", "Quick Scoop", "Take the Whole Pile"),
            _DangerSeed("artifact_shifting_compass", "Shifting Compass", "Your relic compass spins and points everywhere.", "Trust map, trust instinct, or chase the wild signal.", "Return to Map", "Triangulate", "Chase the Spin"),
            _DangerSeed("artifact_petrified_guide", "Petrified Guide", "Your hired guide freezes and refuses to move.", "Protect them, press alone, or drag them into danger.", "Escort Out", "Scout Solo", "Force the Route"),
            _DangerSeed("artifact_ancient_debt_marker", "Ancient Debt Marker", "A relic mark appears on your arm and hunters notice.", "Lay low, bargain, or flex for reputation and bounty.", "Hide the Mark", "Pay Off Watchers", "Flash the Mark"),
            _DangerSeed("artifact_idol_open_eyes", "Idol with Open Eyes", "A ceremonial idol opens its eyes after pickup.", "Return it, appease it, or demand more treasure.", "Set It Back", "Offer Trinket", "Command the Idol"),
            _DangerSeed("artifact_collapsing_archive", "Collapsing Archive Room", "Ancient shelves crash while you grab tablets.", "Prioritize exits, curated tablets, or full shelf haul.", "Save Notes", "Grab Key Tablets", "Take Entire Shelf"),
            _DangerSeed("artifact_smuggler_tunnel_cavein", "Smuggler Tunnel Cave-In", "Your escape tunnel collapses behind you.", "Keep moving smartly or detour to a rumored stash.", "Follow Airflow", "Blast Side Path", "Dig for Hidden Cache"),
            _DangerSeed("artifact_relic_hunger", "Relic Hunger", "A sentient artifact offers more value if fed something important.", "Feed scraps, feed profit, or feed something painful.", "Refuse the Deal", "Feed Spare Gear", "Feed Prime Relic"),
        ),
        "business_ceo": (
            _DangerSeed("ceo_hostile_buyout_leak", "Hostile Buyout Leak", "News breaks that a rival is trying to swallow your company.", "Control the narrative or swing for a brutal counterplay.", "Issue Calm Statement", "Buy Time with PR", "Launch Counter-Bid"),
            _DangerSeed("ceo_boardroom_coup", "Boardroom Coup", "Executives are quietly trying to replace you.", "Secure allies, negotiate, or force a scorched-earth vote.", "Lock Core Votes", "Offer Concessions", "Purge the Board"),
            _DangerSeed("ceo_data_breach_countdown", "Data Breach Countdown", "Sensitive customer data may be leaking now.", "Patch quickly or gamble on stealth containment.", "Full Shutdown", "Segment Systems", "Trace While Live"),
            _DangerSeed("ceo_product_recall_panic", "Product Recall Panic", "Your flagship product may need immediate recall.", "Protect trust or gamble on one more sales cycle.", "Recall Now", "Targeted Recall", "Delay and Sell"),
            _DangerSeed("ceo_union_walkout", "Union Walkout Threat", "Workers are ready to walk unless you act.", "Settle safely, compromise, or call their bluff.", "Accept Demands", "Offer New Terms", "Force Hardline"),
            _DangerSeed("ceo_pr_meltdown", "PR Meltdown Live", "A scandal is exploding online in real time.", "Own it, redirect it, or start a risky narrative war.", "Apologize Fast", "Drop Receipts", "Trendjack the Chaos"),
            _DangerSeed("ceo_investor_revolt", "Investor Revolt", "Major investors demand an emergency response.", "Stabilize trust or gamble on huge projections.", "Protect Guidance", "Promise Aggressive Plan", "All-In Forecast"),
            _DangerSeed("ceo_shipping_hijack", "Shipping Route Hijack", "A premium shipment disappears mid-delivery.", "Take insured route or attempt dramatic recovery.", "Claim Insurance", "Split Recovery Teams", "Intercept Live"),
            _DangerSeed("ceo_counterfeit_scandal", "Counterfeit Batch Scandal", "Fake products flood the market under your brand.", "Clamp down safely or overreach for a reputation spike.", "Pull and Verify", "Selective Sweep", "Public Bounty War"),
            _DangerSeed("ceo_tax_audit_trap", "Tax Audit Trap", "Authorities freeze part of your operations.", "Comply cleanly or attempt aggressive accounting maneuvers.", "Cooperate Fully", "Negotiate Scope", "Exploit Loophole"),
            _DangerSeed("ceo_payroll_vanish", "Payroll Vanish", "A payroll transfer disappears before payday.", "Keep morale safe or gamble on rapid forensic recovery.", "Emergency Reserve", "Partial Advance", "Recover in One Shot"),
            _DangerSeed("ceo_luxury_launch_disaster", "Luxury Launch Disaster", "A live product launch starts failing publicly.", "Cut losses or risk a live save for huge hype.", "Cancel Stream", "Patch on Stage", "Reboot Live Demo"),
            _DangerSeed("ceo_cyber_ransom", "Cyber Ransom Window", "Hackers lock critical systems and demand a response.", "Contain, negotiate, or trap them with risky counterstrike.", "Isolate Systems", "Delay and Bargain", "Counterhack Push"),
            _DangerSeed("ceo_factory_sabotage", "Sabotaged Factory Line", "A production line was intentionally sabotaged.", "Stabilize output or race for maximum recovery.", "Pause Production", "Reassign Lines", "Force Overtime Sprint"),
            _DangerSeed("ceo_client_extortion", "Client Extortion Push", "A giant client pressures you into a brutal deal.", "Protect margin, bend slightly, or bet it all on exclusivity.", "Decline Pressure", "Offer Limited Deal", "Sign Brutal Contract"),
            _DangerSeed("ceo_supplier_collapse", "Supplier Collapse", "A key supplier fails during a major push.", "Secure backups or gamble on a single replacement.", "Switch to Backup", "Hybrid Sourcing", "One Supplier Gamble"),
            _DangerSeed("ceo_blackmail_envelope", "Blackmail Envelope", "Someone sends proof of internal misconduct.", "Handle quietly or weaponize disclosure for gain.", "Internal Cleanup", "Controlled Disclosure", "Leak and Spin"),
            _DangerSeed("ceo_lawsuit_ambush", "Lawsuit Ambush", "A legal filing hits at the worst possible time.", "Settle, fight carefully, or launch full legal war.", "Settle Early", "File Counter", "Scorched Courtroom"),
            _DangerSeed("ceo_market_rumor", "Market Manipulation Rumor", "Rumors tank your stock and panic spreads.", "Stabilize guidance or attempt a high-risk rebound play.", "Calm Market", "Buyback Lite", "Massive Buyback"),
            _DangerSeed("ceo_corrupt_middleman", "Corrupt Middleman Bust", "A shady middleman is exposed and your name gets dragged.", "Distance now or gamble on controlling the story.", "Cut Ties", "Cooperate Publicly", "Own the Narrative"),
            _DangerSeed("ceo_jet_diversion", "Exec Jet Emergency Diversion", "A critical in-person deal is interrupted mid-flight.", "Delay safely or improvise a risky remote close.", "Reschedule", "Remote Negotiation", "Force Midnight Close"),
            _DangerSeed("ceo_celeb_implosion", "Celebrity Endorsement Implosion", "Your paid face of the brand becomes a live disaster.", "Cut them loose or ride scandal momentum.", "Terminate Deal", "Temporary Pause", "Exploit the Drama"),
            _DangerSeed("ceo_poison_pill", "Acquisition Poison Pill", "A merger opportunity turns out toxic.", "Back out or gamble on aggressive restructuring.", "Walk Away", "Renegotiate Terms", "Force the Merger"),
            _DangerSeed("ceo_fire_code_sweep", "Warehouse Fire Code Sweep", "A major facility is suddenly under compliance attack.", "Comply safely or gamble on an accelerated workaround.", "Pass Inspection", "Prioritize Zones", "Bypass Bottleneck"),
            _DangerSeed("ceo_media_sting", "Media Sting Interview", "A hostile interviewer corners you on breaking news.", "Stay careful or swing for viral dominance.", "Stick to Facts", "Controlled Clapback", "Go Off Script"),
        ),
        "dragon_slayer": (
            _DangerSeed("slayer_wyrmling_decoy", "Wyrmling Decoy Nest", "A weak nest is bait for a bigger predator.", "Back out, scout, or charge for two bounties.", "Retreat to Ridge", "Track the Real Nest", "Ambush the Predator"),
            _DangerSeed("slayer_molten_bridge", "Molten Bridge Rush", "A bridge cracks over lava during pursuit.", "Cross safely, cross loaded, or grab burning hoard crates.", "Drop Weight", "Timed Sprint", "Loot Mid-Cross"),
            _DangerSeed("slayer_ancient_roar", "Ancient Roar Paralysis", "A roar locks your body with fear.", "Ground yourself or gamble on aggressive advance.", "Brace and Breathe", "Push Through", "Roar Back"),
            _DangerSeed("slayer_tail_sweep", "Tail Sweep Canyon", "A tail strike threatens to launch you off a cliff.", "Defend position or trade everything for one hit.", "Shield Stance", "Hook and Recover", "Leap for Finisher"),
            _DangerSeed("slayer_hoard_mimic", "Hoard Mimic Chamber", "Not all treasure in this lair is treasure.", "Take verified loot or gamble on suspicious shine.", "Loot Only Marked", "Probe with Spear", "Grab the Crown Pile"),
            _DangerSeed("slayer_ashstorm", "Ashstorm Blindside", "Burning ash kills your sight mid-hunt.", "Play defensive or exploit chaos for fast trophies.", "Mask Up", "Sound-Track Target", "Charge Blind"),
            _DangerSeed("slayer_ballista_panic", "Berserk Ballista Crew", "Your support crew panics and misfires.", "Reform line or use chaos to force risky burst damage.", "Reset Formation", "Manual Aim Call", "Volley Everything"),
            _DangerSeed("slayer_cursed_trophy", "Cursed Trophy Pulse", "A dragon trophy starts spreading a curse.", "Seal it, study it, or consume its power.", "Seal and Bag", "Runesmith Check", "Channel Curse"),
            _DangerSeed("slayer_broodmother_feint", "Broodmother Feint", "Your target was bait for something worse.", "Withdraw, split hunt, or pursue both contracts.", "Fall Back", "Pick One Target", "Hunt Both"),
            _DangerSeed("slayer_heartfire_eggs", "Heartfire Egg Chamber", "An unstable egg chamber overheats around you.", "Extract safe or crack rare eggs for insane value.", "Evacuate", "Take One Egg", "Harvest the Chamber"),
            _DangerSeed("slayer_bone_necroflame", "Bone Pile Necroflame", "A graveyard of bones erupts in cursed fire.", "Survive first or harvest necroflame cores.", "Sanctify Ground", "Cut Through", "Mine the Flame"),
            _DangerSeed("slayer_wingbeat_edge", "Wingbeat Cliff Edge", "A wing gust threatens to throw you off balance.", "Anchor, reposition, or leap for an aerial strike.", "Drive Piton", "Hook Wing", "Skyborne Finisher"),
            _DangerSeed("slayer_guild_betrayal", "Hunter Guild Betrayal", "Another slayer sabotages your hunt for reward.", "Secure evidence or gamble on duel + kill.", "Report and Reset", "Challenge for Share", "Dual Hunt Showdown"),
            _DangerSeed("slayer_poison_drakes", "Poison Fang Drakes", "Small drakes swarm while the main beast circles.", "Thin swarm safely or gamble on alpha strike.", "Clear Swarm", "Focus Leader", "Ignore and Rush Boss"),
            _DangerSeed("slayer_harpoon_snag", "Chain Harpoon Snag", "Your anti-dragon gear locks you in a bad spot.", "Cut free, reroute, or hold for giant trophy chance.", "Cut the Chain", "Reel and Pivot", "Hold and Tank"),
            _DangerSeed("slayer_gold_madness", "Gold-Mad Frenzy", "The hoard itself starts affecting your judgment.", "Snap out, skim profit, or gorge on greed.", "Leave the Hoard", "Quick Weigh", "Claim It All"),
            _DangerSeed("slayer_frost_split", "Frost Dragon Breath Split", "Freezing breath cracks the battlefield apart.", "Stabilize position or jump shards for flank loot.", "Plant Banner", "Shard-Hop", "Charge Through Ice"),
            _DangerSeed("slayer_thunder_wyvern", "Thunder Wyvern Dive", "A storm wyvern crashes into the fight.", "Disengage, adapt, or force double bounty.", "Break Contact", "Split Priorities", "Take Double Contract"),
            _DangerSeed("slayer_sleeping_mountain", "Sleeping Mountain Warning", "Terrain shakes like the mountain is waking.", "Exit now or push for legendary mountain-core loot.", "Retreat to Camp", "Secure Mid-Route Loot", "Mine Mountain Core"),
            _DangerSeed("slayer_oath_circle", "Draconic Oath Circle", "A rune circle offers power at hidden cost.", "Ignore, sample, or swear the full oath.", "Refuse Oath", "Borrow Power", "Bind Oath"),
            _DangerSeed("slayer_molten_sink", "Molten Treasure Sink", "Treasure starts sinking into lava during extraction.", "Save what you can or dive for premium pieces.", "Take Surface Loot", "Hook Key Crate", "Dive for Crown Cache"),
            _DangerSeed("slayer_crown_bait", "Crown Bait Trap", "A glorious crown on a pedestal is obvious bait.", "Leave it, disarm trap, or snatch and pray.", "Ignore Crown", "Disarm Carefully", "Snatch the Crown"),
            _DangerSeed("slayer_scale_rot", "Scale Rot Spores", "Spores from infected scales weaken the arena.", "Cleanse zone or gamble on fast kill in contamination.", "Purge Spores", "Fight on Edge", "Full Assault"),
            _DangerSeed("slayer_sky_lair_collapse", "Sky Lair Collapse", "The dragon's aerial perch starts breaking apart.", "Stabilize rope lines or use collapse for a risky finisher.", "Secure Lines", "Controlled Drop", "Ride the Collapse"),
            _DangerSeed("slayer_last_breath", "Last Breath Detonation", "A downed dragon starts charging one final blast.", "Shield up, interrupt cleanly, or overcommit for trophy.", "Take Cover", "Precision Interrupt", "Point-Blank Finish"),
        ),
        "drug_lord": (
            _DangerSeed("underworld_burner_betrayal", "Burner Phone Betrayal", "A trusted burner line suddenly sends a setup signal.", "Stay ghost, test contact, or walk into the trap for bigger score.", "Burn the Line", "Ping New Channel", "Answer the Setup"),
            _DangerSeed("underworld_dockside_heat", "Dockside Heat", "Too many eyes are on tonight's dockside handoff.", "Abort quietly or force a flashy swap under pressure.", "Pull Out", "Use Decoy Van", "Run Hot Exchange"),
            _DangerSeed("underworld_marked_van", "Rival Crew Marked Van", "Your transport gets tagged by a rival operation.", "Shake tail safely or bait rivals into overextension.", "Swap Vehicles", "Fake Route", "Lead Them to Vault"),
            _DangerSeed("underworld_stolen_ledger", "Stolen Ledger", "A ledger with valuable names and numbers goes missing.", "Contain damage or chase an instant recovery.", "Lock Contacts", "Offer Finder Fee", "Raid Recovery Spot"),
            _DangerSeed("underworld_fake_enforcers", "Fake Enforcer Sweep", "People claiming to be your muscle show up acting wrong.", "Verify carefully or exploit confusion aggressively.", "Check Signals", "Split the Crew", "Use Them as Bait"),
            _DangerSeed("underworld_informant_whisper", "Informant Whisper", "Someone in the room is feeding info elsewhere.", "Quietly rotate operations or set a dramatic trap.", "Change Plans", "Seed False Intel", "Spring the Trap"),
            _DangerSeed("underworld_tracker_ping", "Van Tracker Ping", "A hidden tracker activates during a run.", "Dump cargo, reroute, or reverse-track for bonus take.", "Kill Route", "Jammer Loop", "Backtrace Rivals"),
            _DangerSeed("underworld_bad_batch", "Bad Batch Panic", "Shipment issues cause blowback across your network.", "Pause and contain or force a dangerous cleanup sale.", "Quarantine Stock", "Selective Pullback", "Flash Clearance Push"),
            _DangerSeed("underworld_tag_war", "Territory Tag War", "New markings show another crew testing your zone.", "Stabilize calmly or launch a risky rep play.", "Hold Position", "Parley at Neutral", "Paint the Whole Block"),
            _DangerSeed("underworld_dye_pack", "Dirty Money Dye Pack", "Cash from a drop turns out compromised.", "Isolate loss or gamble on cleaning all of it fast.", "Burn Tainted Stack", "Filter and Sort", "Wash Whole Drop"),
            _DangerSeed("underworld_safehouse_mole", "Mole in the Safehouse", "Your protected location suddenly feels very unprotected.", "Move quietly or stage a risky loyalty test.", "Relocate Team", "Layer Checkpoints", "Bait the Mole"),
            _DangerSeed("underworld_seized_crate", "Seized Crate Alert", "A critical crate goes dark and may be intercepted.", "Write it off or attempt a high-pressure reclaim.", "Cut Losses", "Shadow the Route", "Hit the Intercept"),
            _DangerSeed("underworld_snake_meet", "Snake at the Meet", "A face-to-face meetup feels wrong instantly.", "Exit clean or push meeting for extra leverage.", "Cancel Meeting", "Shorten Terms", "Press for Full Terms"),
            _DangerSeed("underworld_silent_grid", "Silent Phone Grid", "Every normal line of contact goes quiet.", "Play safe with backups or blast a risky all-call.", "Fallback Channel", "Runners Only", "Citywide Ping"),
            _DangerSeed("underworld_courier_flip", "Courier Flip", "A runner looks ready to switch sides mid-job.", "Contain quietly or make a dramatic public example.", "Reassign Run", "Shadow Courier", "Force Loyalty Trial"),
            _DangerSeed("underworld_camera_loop", "Warehouse Camera Loop Break", "Your camera cover fails and movement is exposed.", "Cut lights or force operations anyway.", "Darken the Site", "Patch the Loop", "Keep Loading Fast"),
            _DangerSeed("underworld_bribe_chain", "Bribe Chain Collapse", "Someone in your protection chain stops playing along.", "Pause lanes or gamble on an expensive power move.", "Freeze Activity", "Patch New Contact", "Buy the Whole Chain"),
            _DangerSeed("underworld_rooftop_escape", "Rooftop Escape", "A deal implodes and the only exit is vertical.", "Escape light or carry everything across rooftops.", "Drop Excess", "Leap with Pack", "Grab Rival Bag Too"),
            _DangerSeed("underworld_hidden_recorder", "Hidden Recorder", "A conversation may have been captured.", "Contain statement or exploit chaos with a fake trail.", "Lawyer Up", "Controlled Leak", "Feed False Story"),
            _DangerSeed("underworld_asset_freeze", "Asset Freeze Rumor", "Word spreads your cash flow may get locked.", "Move conservatively or gamble on one giant transfer.", "Secure Reserves", "Split Transfers", "One Mega Move"),
            _DangerSeed("underworld_ghost_raid", "Ghost Warehouse Raid", "A warehouse is hit by an unknown force.", "Protect survivors or chase ghost crew for payback.", "Stabilize Crew", "Track Footprints", "Immediate Counter-Raid"),
            _DangerSeed("underworld_counterfeit_ring", "Counterfeit Stamp Ring", "Fake branded product is damaging your rep.", "Clean market slowly or force a dramatic reputation war.", "Pull Fakes", "Reward Tips", "Flood with Real Product"),
            _DangerSeed("underworld_corner_riot", "Riot at the Corner", "Street panic threatens a profitable zone.", "Cool things down or exploit rush demand dangerously.", "Disperse Crowd", "Controlled Window", "Run Hot Sales"),
            _DangerSeed("underworld_boss_smells_blood", "Boss Above You Smells Blood", "Someone higher up thinks you look weak.", "Play loyal, negotiate status, or challenge loudly.", "Show Respect", "Offer Bigger Cut", "Public Flex"),
            _DangerSeed("underworld_double_agent_driver", "Double-Agent Driver", "Your transport driver may be playing both sides.", "Replace driver or stage a risky counter-run.", "Swap Driver", "Run with Escort", "Feed Dual Route"),
        ),
        "space_miner": (
            _DangerSeed("space_volatile_pocket", "Volatile Ore Pocket", "A glowing ore pocket starts building pressure in the rig.", "Stabilize or overclock for a cosmic payday.", "Vent Slowly", "Pulse Drill", "Overclock Core"),
            _DangerSeed("space_coolant_leak", "Reactor Coolant Leak", "Coolant drops fast while the drill runs hot.", "Preserve reactor or force one more deep pull.", "Emergency Cooldown", "Throttle Cycle", "Max Burn Window"),
            _DangerSeed("space_hull_fracture", "Hull Microfracture Spread", "A crack starts crawling across the platform hull.", "Patch now or mine while integrity falls.", "Seal Fracture", "Patch While Drilling", "Ignore and Harvest"),
            _DangerSeed("space_pirate_ping", "Pirate Ping on Scanner", "Unknown ships appear at sensor edge.", "Evade clean or bait pirates near rich debris.", "Silent Drift", "Call Escort Drone", "Ambush Their Approach"),
            _DangerSeed("space_graviton_surge", "Graviton Surge", "Local gravity warps your rig and cargo.", "Stabilize safely or ride the surge into deeper vein.", "Anchor Rig", "Short Surge Ride", "Full Gravity Slingshot"),
            _DangerSeed("space_crystal_collapse", "Crystal Nest Collapse", "A crystal cave folds inward around your haul.", "Extract now or grab inner nest gems.", "Extract Safe", "Recover Core Crate", "Dive Inner Chamber"),
            _DangerSeed("space_oxygen_failure", "Oxygen Ration Failure", "Reserve air systems start misreading.", "Protect crew air or gamble on fast finish.", "Cycle to Backup", "Triage and Drill", "Sprint the Shaft"),
            _DangerSeed("space_solar_flare", "Solar Flare Window", "Radiation spikes are about to slam the site.", "Shield up or chase the final flare-lit seam.", "Close Shutters", "Timed Exposure", "Flare-Harvest Run"),
            _DangerSeed("space_drill_jam", "Drill Arm Jam", "Primary drill jams while holding a rich vein.", "Reset safely or force the arm through.", "Power Down", "Manual Reset", "Force the Torque"),
            _DangerSeed("space_spore_cloud", "Alien Spore Cloud", "A drifting cloud starts eating equipment seals.", "Quarantine gear or mine through contamination.", "Seal Compartments", "Thread the Cloud", "Harvest in Spores"),
            _DangerSeed("space_cargo_magnet", "Cargo Magnet Failure", "Your haul drifts loose toward open space.", "Secure partial cargo or attempt heroic full catch.", "Lock Core Pallets", "Tractor Sweep", "Jetpack Full Catch"),
            _DangerSeed("space_moonquake", "Moonquake Burst", "The whole rock face starts shaking apart.", "Ride it out safely or crack open deeper pockets.", "Retreat to Shell", "Stabilize and Continue", "Drill Through Quake"),
            _DangerSeed("space_turret_wakeup", "Automated Turret Wake-Up", "Ancient defense systems come online near the vein.", "Disable carefully or rush the zone for rare salvage.", "Take Cover", "EMP Burst", "Rush the Turrets"),
            _DangerSeed("space_distress_lure", "Smuggler Distress Lure", "A distress signal near a rich lane may be bait.", "Avoid trap or gamble on huge salvage.", "Ignore Signal", "Scout with Drone", "Board the Source"),
            _DangerSeed("space_cutter_backfire", "Plasma Cutter Backfire", "Your cutter starts overcharging in your hands.", "Cool down tool or gamble on a single power cut.", "Vent Cutter", "Timed Slice", "Full-Power Carve"),
            _DangerSeed("space_void_ice_break", "Void Ice Sheet Break", "A frozen shelf cracks under your boots.", "Backtrack safely or leap for trapped ore crates.", "Backstep", "Hook Across", "Carry Full Load"),
            _DangerSeed("space_core_pressure", "Core Pressure Alarm", "Deep drill pressure climbs toward catastrophic.", "Bleed pressure or attempt one legendary pull.", "Release Pressure", "Controlled Pull", "Redline the Core"),
            _DangerSeed("space_rogue_bot", "Rogue Bot Revolt", "Mining drones stop obeying priorities.", "Reboot fleet or exploit chaos for extra haul.", "Hard Reboot", "Split Commands", "Let Bots Free-Mine"),
            _DangerSeed("space_dark_matter", "Dark Matter Vein Temptation", "A strange vein promises insane value and unknown consequences.", "Skip weird science or touch the impossible seam.", "Mark and Leave", "Sample Safely", "Full Dark Drill"),
            _DangerSeed("space_debris_ring", "Debris Ring Entry", "Fast debris starts shredding your route out.", "Take safe route or blast through for faster sell.", "Detour Out", "Timed Burn", "Punch the Ring"),
            _DangerSeed("space_salvage_dispute", "Salvage Claim Dispute", "Another team claims the field and closes in.", "Arbitrate quietly or contest aggressively.", "Log Formal Claim", "Split the Vein", "Contest the Whole Field"),
            _DangerSeed("space_dock_clamp", "Dock Clamp Failure", "Extraction transport cannot lock properly.", "Delay launch or jump with unstable lock.", "Hold Position", "Manual Clamp", "Launch Half-Locked"),
            _DangerSeed("space_meteor_needles", "Meteor Needle Storm", "Tiny high-speed shards start pelting the rig.", "Shield up or harvest exposed ore under fire.", "Deploy Shields", "Move Between Bursts", "Harvest in Storm"),
            _DangerSeed("space_echo_signal", "Echo Signal Below", "A signal from beneath the rock asks to be followed.", "Ignore mystery or descend for bizarre riches.", "Ignore Ping", "Probe Tunnel", "Follow the Voice"),
            _DangerSeed("space_blackout_shaft", "Blackout in the Shaft", "Lights, map feed, and comms die underground.", "Regroup in dark or push blind for jackpot vein.", "Climb to Beacon", "Use Glow Flares", "Mine in Darkness"),
        ),
    }


def _normal_seed_map() -> dict[str, tuple[_NormalSeed, ...]]:
    return {
        "artifact_hunter": (
            _NormalSeed("artifact_map_fragment", "Lucky Map Fragment", "A folded map scrap lines up perfectly with your route.", "A clean detour could add easy value."),
            _NormalSeed("artifact_fake_curse", "Harmless Fake Curse", "The scary curse tablet just says 'return by Tuesday.'", "Crew laughs, tension drops, pace improves."),
            _NormalSeed("artifact_collector_offer", "Eager Collector Offer", "A collector sends a polite overbid before you even leave.", "Quick sale opportunity."),
            _NormalSeed("artifact_journal_clue", "Old Journal Clue", "A dusty journal points to a side niche full of trinkets.", "Small but reliable upside."),
            _NormalSeed("artifact_coin_stash", "Dusty Coin Stash", "Behind a collapsed urn sits a neat coin pocket.", "Easy extra haul."),
            _NormalSeed("artifact_rival_banter", "Rival Banter", "A rival crew calls your loadout 'museum cosplay.'", "Winning the day feels extra profitable."),
            _NormalSeed("artifact_clean_extraction", "Clean Extraction Lane", "A forgotten smugglers' route cuts your exit time in half.", "Faster sell, better margin."),
            _NormalSeed("artifact_museum_tip", "Museum Tip-Off", "A curator quietly messages: 'that symbol pays premium tonight.'", "Targeted flip chance."),
            _NormalSeed("artifact_charm", "Lucky Pocket Charm", "Your guide hands you a lucky charm that somehow works.", "Tiny luck bump."),
            _NormalSeed("artifact_auction_hype", "Auction Hype", "Collectors start arguing in your inbox before listing goes live.", "Price tension means upside."),
        ),
        "business_ceo": (
            _NormalSeed("ceo_surprise_investor", "Surprise Investor Interest", "A new fund asks for a fast call and sounds excited.", "Healthy upside with little heat."),
            _NormalSeed("ceo_employee_idea", "Employee Idea Pays Off", "A staff suggestion quietly cuts operating waste.", "Margin boost unlocked."),
            _NormalSeed("ceo_viral_marketing", "Viral Marketing Win", "A tiny campaign post explodes for free.", "Cheaper acquisition, better quarter."),
            _NormalSeed("ceo_luxury_client_tip", "Luxury Client Tip", "A premium client recommends your brand to their circle.", "High-ticket lead bump."),
            _NormalSeed("ceo_competitor_slip", "Competitor Embarrassment", "A rival fumbles their announcement spectacularly.", "You get a clean opening."),
            _NormalSeed("ceo_smooth_earnings", "Smooth Earnings Call", "Analysts actually nod instead of attacking.", "Market confidence improves."),
            _NormalSeed("ceo_team_morale", "Team Morale Spike", "Your ops team crushes a sprint and memes about it.", "Productivity rises."),
            _NormalSeed("ceo_supply_discount", "Bulk Supply Discount", "A supplier offers surprise volume pricing.", "CFO smiles for once."),
            _NormalSeed("ceo_press_feature", "Friendly Press Feature", "A business outlet calls you 'annoyingly effective.'", "Brand aura boost."),
            _NormalSeed("ceo_micro_acquisition", "Micro-Acquisition", "You quietly absorb a tiny startup with useful tech.", "Steady synergy gain."),
        ),
        "dragon_slayer": (
            _NormalSeed("slayer_scale_fragment", "Rare Scale Fragment", "A clean scale fragment drops and appraises well.", "Free trophy money."),
            _NormalSeed("slayer_tavern_rumor", "Tavern Rumor Shortcut", "A bartender shares a reliable shortcut to the lair.", "Time and stamina saved."),
            _NormalSeed("slayer_blacksmith_tune", "Blacksmith Tune-Up", "A friendly smith tightens your gear on credit.", "Safer hits, better haul."),
            _NormalSeed("slayer_villager_tip", "Scared Villager Tip", "A villager points out a hidden dragon stash crack.", "Small side loot."),
            _NormalSeed("slayer_trophy_appraisal", "Bonus Trophy Appraisal", "A hunter appraiser overpays for clean workmanship.", "Unexpected premium."),
            _NormalSeed("slayer_banner_cheer", "Banner Cheer", "Town kids chant your title as you pass.", "Morale buff energy."),
            _NormalSeed("slayer_squire_help", "Squire Assist", "A guild squire sorted your bolts perfectly.", "Everything feels smoother."),
            _NormalSeed("slayer_stable_wind", "Favorable Wind", "Crosswinds line up for your approach.", "Positioning advantage."),
            _NormalSeed("slayer_contract_bonus", "Contract Bonus Clause", "A clerk forgot to remove a hazard stipend.", "Extra silver appears."),
            _NormalSeed("slayer_clean_hide", "Clean Hide Recovery", "A pristine hide section survives the battle.", "Crafting buyers pay up."),
        ),
        "drug_lord": (
            _NormalSeed("underworld_smooth_collection", "Smooth Collection", "Every handshake lands clean tonight.", "No drama, solid take."),
            _NormalSeed("underworld_rival_backs_off", "Rival Backs Off", "A rival crew clocks your numbers and declines conflict.", "Easy breathing room."),
            _NormalSeed("underworld_runner_overdeliver", "Loyal Runner Overdelivers", "A trusted runner brings extra product and exact books.", "Rare professionalism in chaos."),
            _NormalSeed("underworld_rep_flash", "Flashy Reputation Moment", "People whisper your name before you enter the block.", "Negotiations tilt your way."),
            _NormalSeed("underworld_hidden_stash", "Hidden Stash Recovery", "An old dead-drop still has untouched bundles.", "Free upside from old planning."),
            _NormalSeed("underworld_easy_handoff", "Easy Handoff", "A tense meet turns out weirdly polite.", "Time saved equals money."),
            _NormalSeed("underworld_quiet_street", "Quiet Street Window", "Patrol noise dips right when you need it.", "Smooth route bonus."),
            _NormalSeed("underworld_loyalty_toast", "Crew Loyalty Toast", "Your crew does a dramatic loyalty speech mid-shift.", "Morale pays dividends."),
            _NormalSeed("underworld_debt_paid", "Old Debt Paid", "A forgotten debtor shows up with full payment.", "Unexpected clean cash."),
            _NormalSeed("underworld_club_contact", "Club Contact", "A nightlife contact opens a high-margin lane.", "Stylish profit bump."),
        ),
        "space_miner": (
            _NormalSeed("space_rich_seam", "Rich Ore Seam", "Your scanner catches a clean high-density seam.", "Simple and profitable."),
            _NormalSeed("space_forgotten_cache", "Forgotten Cargo Cache", "A drifting pod still contains valuable spare ore.", "Easy scoop."),
            _NormalSeed("space_drone_side_crystal", "Drone Side Crystal", "A helper drone pings a side crystal nest.", "Minor detour, nice gain."),
            _NormalSeed("space_smooth_route", "Smooth Extraction Route", "Debris thins and your exit lane looks perfect.", "Faster haul-to-market."),
            _NormalSeed("space_clean_scan", "Clean Scan Bonus", "Sensor calibration is perfect for once.", "Yield estimates improve."),
            _NormalSeed("space_salvage_ping", "Friendly Salvage Ping", "Another miner shares a low-risk salvage coordinate.", "Mutual profit vibes."),
            _NormalSeed("space_reactor_stable", "Reactor Running Sweet", "Your reactor hums at ideal efficiency.", "Stable bonus output."),
            _NormalSeed("space_lucky_tether", "Lucky Tether Catch", "A loose crate drifts straight into your tether.", "Free haul added."),
            _NormalSeed("space_ai_optimization", "AI Optimization", "Rig AI suggests a tiny route change that works.", "Smarter extraction loop."),
            _NormalSeed("space_station_premium", "Station Premium Demand", "Dock market asks for your exact ore profile.", "Quick premium sale."),
        ),
    }


DANGER_CATALOG: dict[str, tuple[DangerEncounter, ...]] = {
    key: tuple(_build_danger_from_seed(key, seed) for seed in seeds)
    for key, seeds in _danger_seed_map().items()
}

NORMAL_CATALOG: dict[str, tuple[NormalInteraction, ...]] = {
    key: tuple(_build_normal_from_seed(seed) for seed in seeds)
    for key, seeds in _normal_seed_map().items()
}


def presentation_for(job_key: str) -> Optional[JobPresentation]:
    return JOB_PRESENTATIONS.get((job_key or "").strip().lower())


def event_defs_for_endgame(job_key: str) -> tuple[EndgameRandomEventDefinition, ...]:
    return ENDGAME_EVENT_CATALOG.get((job_key or "").strip().lower(), ())


def should_trigger_danger(job_key: str) -> bool:
    key = (job_key or "").strip().lower()
    return roll_bp(DANGER_TRIGGER_BP.get(key, 0))


def pick_danger_encounter(job_key: str) -> Optional[DangerEncounter]:
    pool = DANGER_CATALOG.get((job_key or "").strip().lower(), ())
    if not pool:
        return None
    return random.choice(pool)


def should_trigger_normal(job_key: str) -> bool:
    key = (job_key or "").strip().lower()
    return roll_bp(NORMAL_TRIGGER_BP.get(key, 0))


def pick_normal_interaction(job_key: str) -> Optional[NormalInteraction]:
    pool = NORMAL_CATALOG.get((job_key or "").strip().lower(), ())
    if not pool:
        return None
    return random.choice(pool)


def resolve_normal_interaction(*, interaction: NormalInteraction, payout: int, job_xp: int) -> NormalResolution:
    payout = max(int(payout), 0)
    job_xp = max(int(job_xp), 0)
    outcome = _pick_weighted(interaction.outcomes)
    final_payout = max(apply_bp(payout, int(outcome.payout_multiplier_bp)) + int(outcome.flat_silver), 0)
    final_xp = max(apply_bp(job_xp, int(outcome.xp_multiplier_bp)) + int(outcome.flat_xp), 0)
    return NormalResolution(
        interaction=interaction,
        outcome=outcome,
        payout=final_payout,
        job_xp=final_xp,
        summary_line=f"**{interaction.title}** — {outcome.text}",
    )


def resolve_danger_choice(*, encounter: DangerEncounter, choice_key: str, payout: int, job_xp: int) -> DangerResolution:
    payout = max(int(payout), 0)
    job_xp = max(int(job_xp), 0)
    choice = next((item for item in encounter.choices if item.key == choice_key), encounter.choices[0])
    outcome = _pick_weighted(choice.outcomes)

    if outcome.fail_run:
        updated_payout = 0
        failed = True
    else:
        updated_payout = max(apply_bp(payout, int(outcome.payout_multiplier_bp)) + int(outcome.flat_silver), 0)
        failed = False

    updated_xp = max(apply_bp(job_xp, int(outcome.xp_multiplier_bp)) + int(outcome.flat_xp), 0)

    if failed:
        summary = "Danger play failed."
    elif outcome.payout_multiplier_bp >= 6000:
        summary = "Massive danger spike."
    elif outcome.payout_multiplier_bp < 0:
        summary = "You stabilized the run."
    else:
        summary = "Danger play paid off."

    detail = outcome.text

    return DangerResolution(
        encounter=encounter,
        choice=choice,
        outcome=outcome,
        payout=max(int(updated_payout), 0),
        job_xp=max(int(updated_xp), 0),
        failed=failed,
        choice_label=choice.label,
        summary_line=summary,
        detail_line=detail,
    )


def build_danger_embed(*, user: discord.abc.User, d: JobDef, encounter: DangerEncounter, payout: int) -> discord.Embed:
    color = discord.Color.red() if d.category == JobCategory.HARD else discord.Color.orange()
    lines = [f"**{choice.label}** — {choice.description}" for choice in encounter.choices]
    embed = discord.Embed(
        title=encounter.title,
        description=(
            f"**{d.name}**\n"
            f"{encounter.description}\n\n"
            f"**Current haul:** **{fmt_int(payout)}** Silver\n"
            f"**At stake:** {encounter.stake_text}\n\n"
            f"**Choices**\n" + "\n".join(lines)
        ),
        color=color,
    )
    embed.set_author(name=str(user), icon_url=getattr(getattr(user, "display_avatar", None), "url", None))
    embed.set_footer(text="Choose once. Timeout auto-picks the safest option.")
    return embed


def build_danger_result_embed(*, user: discord.abc.User, d: JobDef, resolution: DangerResolution, stamina_cost: int, user_xp: int, job_xp: int, progress_after, next_job_name: str | None, xp_needed_value: int) -> discord.Embed:
    outcome = "❌ FAILED" if resolution.failed else "✅ DANGER RESOLVED"
    embed = discord.Embed(
        title=f"{d.name} Danger Result",
        description=(
            f"**{resolution.encounter.title.replace('Danger Encounter • ', '')}**\n"
            f"Choice: **{resolution.choice_label}**\n"
            f"{resolution.summary_line}\n"
            f"{resolution.detail_line}\n\n"
            f"{outcome} • 💰 **{fmt_int(resolution.payout)}** Silver • ⚡ **-{fmt_int(stamina_cost)}** Stamina"
        ),
        color=discord.Color.red() if resolution.failed else discord.Color.blurple(),
    )
    embed.set_author(name=str(user), icon_url=getattr(getattr(user, "display_avatar", None), "url", None))
    embed.add_field(
        name="Progress",
        value=(
            f"Prestige **{int(progress_after.prestige)}** • Level **{int(progress_after.level)}**\n"
            f"Job XP **{fmt_int(int(progress_after.xp))}/{fmt_int(max(int(xp_needed_value), 1))}**"
        ),
        inline=False,
    )
    embed.add_field(name="Gains", value=f"🧠 User XP: **+{fmt_int(user_xp)}**\n🧰 Job XP: **+{fmt_int(job_xp)}**", inline=False)
    if resolution.timed_out:
        embed.add_field(name="Timeout", value="You hesitated, so the safest option resolved automatically.", inline=False)
    if next_job_name:
        embed.set_footer(text=f"Next shift: {next_job_name} • Use /job to edit your 3 job slots")
    return embed


class DangerEncounterView(discord.ui.View):
    def __init__(
        self,
        *,
        owner_id: int,
        timeout_seconds: float,
        encounter: DangerEncounter,
        resolver,
    ):
        super().__init__(timeout=timeout_seconds)
        self.owner_id = int(owner_id)
        self.encounter = encounter
        self.resolver = resolver
        self._resolved = False
        self._lock = asyncio.Lock()
        self._message: Optional[discord.Message] = None
        for idx, choice in enumerate(encounter.choices):
            button = discord.ui.Button(label=choice.label, style=choice.style, row=idx // 3)
            button.callback = self._make_callback(choice.key)
            self.add_item(button)

    def bind_message(self, message: discord.Message) -> None:
        self._message = message

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Only the player who triggered this encounter can choose.", ephemeral=True)
            return False
        return True

    def _disable_all(self) -> None:
        for child in self.children:
            child.disabled = True

    def _make_callback(self, choice_key: str):
        async def callback(interaction: discord.Interaction) -> None:
            async with self._lock:
                if self._resolved:
                    await interaction.response.send_message("This encounter is already resolved.", ephemeral=True)
                    return
                self._resolved = True
                self._disable_all()
            await interaction.response.defer()
            await self.resolver(interaction=interaction, choice_key=choice_key, timed_out=False, view=self)

        return callback

    async def on_timeout(self) -> None:
        async with self._lock:
            if self._resolved:
                return
            self._resolved = True
            self._disable_all()
        if self._message is not None:
            try:
                await self._message.edit(view=self)
            except Exception:
                return
        await self.resolver(interaction=None, choice_key=self.encounter.safe_choice_key, timed_out=True, view=self)
