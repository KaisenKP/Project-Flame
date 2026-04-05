from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import random
from typing import Iterable, Optional, Sequence


RUN_MODE_SAFE = "safe"
RUN_MODE_STANDARD = "standard"
RUN_MODE_AGGRESSIVE = "aggressive"
RUN_MODES = (RUN_MODE_SAFE, RUN_MODE_STANDARD, RUN_MODE_AGGRESSIVE)
EVENT_TYPE_POSITIVE = "positive"
EVENT_TYPE_NEGATIVE = "negative"
EVENT_TYPE_NEUTRAL = "neutral"

EVENT_RARITY_COMMON = "common"
EVENT_RARITY_UNCOMMON = "uncommon"
EVENT_RARITY_RARE = "rare"
EVENT_RARITY_EPIC = "epic"
EVENT_RARITY_LEGENDARY = "legendary"
EVENT_RARITIES = (
    EVENT_RARITY_COMMON,
    EVENT_RARITY_UNCOMMON,
    EVENT_RARITY_RARE,
    EVENT_RARITY_EPIC,
    EVENT_RARITY_LEGENDARY,
)

MAX_EVENT_STACKS = 2
EVENT_CHECK_INTERVAL_MINUTES = 60
EVENT_COOLDOWN_MINUTES = 60
EVENT_DURATION_CAP_MINUTES = 4 * 60
WORKER_PERCENT_CAP_BP = 8500
MANAGER_POSITIVE_CAP_BP = 3000
MANAGER_NEGATIVE_CAP_BP = 3500
GLOBAL_PORTFOLIO_CAP_BP = 1800


@dataclass(frozen=True, slots=True)
class RunModeDef:
    key: str
    label: str
    profit_bp: int
    positive_event_bp: int
    negative_event_bp: int
    frequency_bp: int
    stability_bp: int
    description: str


@dataclass(frozen=True, slots=True)
class EventDef:
    key: str
    name: str
    event_type: str
    rarity: str
    weight: int
    duration_hours: int
    multiplier_bp: int = 0
    pause_minutes: int = 0
    description: str = ""
    level_multiplier_bp: int = 0
    duration_level_bonus_minutes: int = 0


@dataclass(frozen=True, slots=True)
class BusinessTraitDef:
    key: str
    category: str
    base_profit_multiplier_bp: int
    stability: int
    event_frequency_bp: int
    positive_event_weight_bp: int
    negative_event_weight_bp: int
    rare_event_weight_bp: int
    max_run_duration_modifier_bp: int
    positive_bias: str
    risk_label: str
    event_pool: tuple[EventDef, ...]


@dataclass(frozen=True, slots=True)
class SynergyDef:
    key: str
    business_keys: frozenset[str]
    bonus_bp: int
    description: str
    applies_to: str = "global"


RUN_MODE_DEFS = {
    RUN_MODE_SAFE: RunModeDef(RUN_MODE_SAFE, "Safe", -1200, -200, -1800, -2500, 2200, "Lower yield, calmer shifts, and softer setbacks."),
    RUN_MODE_STANDARD: RunModeDef(RUN_MODE_STANDARD, "Standard", 0, 0, 0, 0, 0, "Balanced profit, risk, and event flow."),
    RUN_MODE_AGGRESSIVE: RunModeDef(RUN_MODE_AGGRESSIVE, "Aggressive", 1800, 700, 1500, 2800, -1800, "Higher output ceiling with faster event churn and rougher hits."),
}


def _event(
    key: str,
    name: str,
    event_type: str,
    rarity: str,
    weight: int,
    duration_hours: int,
    *,
    multiplier_bp: int = 0,
    pause: int = 0,
    description: str = "",
    level_bp: int = 0,
    duration_level_bonus_minutes: int = 0,
) -> EventDef:
    return EventDef(
        key,
        name,
        event_type,
        rarity,
        weight,
        duration_hours,
        multiplier_bp,
        pause,
        description,
        level_bp,
        duration_level_bonus_minutes,
    )


BUSINESS_TRAITS: dict[str, BusinessTraitDef] = {
    "restaurant": BusinessTraitDef("restaurant", "hospitality", 10000, 60, 10600, 12000, 9000, 10800, 10000, "Popularity spikes", "Medium risk", (
        _event("lunch_wave", "Lunch Wave", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 18, 1, multiplier_bp=4500, description="Office crowds flood your tables."),
        _event("delivery_app_featured", "Delivery App Feature", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 14, 2, multiplier_bp=9000, description="Your menu is featured in-app for peak traffic."),
        _event("food_creator_spotlight", "Food Creator Spotlight", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 8, 2, multiplier_bp=18000, description="A creator post drives explosive paid orders."),
        _event("citywide_food_frenzy", "Citywide Food Frenzy", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 4, 1, multiplier_bp=32000, description="A city trend turns your kitchen into a money printer."),
        _event("celebrity_buyout", "Celebrity Buyout", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=80000, description="Private celebrity event buys out service windows."),
        _event("supply_chain_break", "Supply Chain Break", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 9, 1, multiplier_bp=-4000, description="Key ingredients arrive late and premium dishes pause."),
        _event("inspection_lockdown", "Inspection Lockdown", EVENT_TYPE_NEGATIVE, EVENT_RARITY_RARE, 5, 2, multiplier_bp=-7000, pause=15, description="Compliance checks slow prep and seating flow."),
    )),
    "farm": BusinessTraitDef("farm", "agriculture", 9400, 86, 9000, 11000, 7600, 9400, 11200, "Reliable harvests", "Low risk", (
        _event("ideal_weather", "Ideal Weather", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 17, 2, multiplier_bp=4000, description="Perfect rain and sunlight maximize crop output."),
        _event("premium_crop_contract", "Premium Crop Contract", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 12, 2, multiplier_bp=8000, description="A wholesaler pays premium rates for fresh yield."),
        _event("mechanized_harvest", "Mechanized Harvest", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 7, 1, multiplier_bp=17000, description="Harvest tooling runs at top speed this shift."),
        _event("record_harvest", "Record Harvest", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 3, 2, multiplier_bp=30000, description="Multiple fields hit peak production simultaneously."),
        _event("genetic_super_yield", "Genetic Super Yield", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=70000, description="A breakthrough strain delivers absurdly dense output."),
        _event("pest_swarm", "Pest Swarm", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 8, 1, multiplier_bp=-3500, description="Pests force emergency treatment and lower sellable yield."),
        _event("water_rights_restriction", "Water Restriction", EVENT_TYPE_NEGATIVE, EVENT_RARITY_RARE, 5, 2, multiplier_bp=-6500, description="Regional restrictions cut irrigation efficiency."),
    )),
    "nightclub": BusinessTraitDef("nightclub", "entertainment", 10800, 42, 12200, 12400, 12200, 12600, 10000, "Explosive nightlife", "High risk", (
        _event("prime_time_stampede", "Prime Time Stampede", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 15, 1, multiplier_bp=5000, description="Crowd turnout explodes during peak floor time."),
        _event("vip_bottle_service_rush", "VIP Bottle Rush", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 12, 2, multiplier_bp=12000, description="High-value tables over-index on bottle service."),
        _event("headline_dj", "Headline DJ", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 8, 2, multiplier_bp=21000, description="A headliner set spikes entries and premium spend."),
        _event("global_afterparty", "Global Afterparty", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 3, 1, multiplier_bp=38000, description="A tour afterparty converts your venue into a hotspot."),
        _event("ultra_vip_takeover", "Ultra VIP Takeover", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=90000, description="Whale guests flood high-margin experiences."),
        _event("noise_violation", "Noise Violation", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 9, 1, multiplier_bp=-4500, description="Restrictions force reduced capacity windows."),
        _event("security_lockdown", "Security Lockdown", EVENT_TYPE_NEGATIVE, EVENT_RARITY_RARE, 6, 2, multiplier_bp=-8000, pause=20, description="An incident causes prolonged floor disruption."),
    )),
    "factory": BusinessTraitDef("factory", "industrial", 11200, 58, 9800, 9800, 11200, 10200, 10800, "Heavy base output", "Downtime risk", (
        _event("line_efficiency_spike", "Line Efficiency Spike", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 15, 1, multiplier_bp=4500, description="Assembly timing optimization boosts throughput."),
        _event("bulk_export_order", "Bulk Export Order", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 11, 2, multiplier_bp=11000, description="An export customer fills your production queue."),
        _event("automation_tuning", "Automation Tuning", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 7, 2, multiplier_bp=22000, description="Control systems push precision and volume together."),
        _event("overnight_hypercycle", "Overnight Hypercycle", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 3, 1, multiplier_bp=40000, description="Robotic lines sustain peak output across the hour."),
        _event("defense_contract_surge", "Defense Contract Surge", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=85000, description="A classified contract drives extraordinary demand."),
        _event("machine_jam", "Machine Jam", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 9, 1, multiplier_bp=-4200, description="Critical machinery jams and reroutes capacity."),
        _event("safety_audit_hold", "Safety Audit Hold", EVENT_TYPE_NEGATIVE, EVENT_RARITY_RARE, 5, 2, multiplier_bp=-7600, pause=20, description="Audit actions force reduced line speed."),
    )),
    "casino": BusinessTraitDef("casino", "gaming", 11400, 34, 12200, 12600, 12400, 13200, 10000, "Whale variance", "Very high risk", (
        _event("weekend_footfall", "Weekend Footfall", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 14, 1, multiplier_bp=6000, description="Foot traffic surges across slots and tables."),
        _event("vip_table_rotation", "VIP Table Rotation", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 12, 2, multiplier_bp=14000, description="High-stakes tables stay occupied nonstop."),
        _event("jackpot_streak", "Jackpot Streak", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 7, 1, multiplier_bp=25000, description="Jackpot momentum drives relentless player spend."),
        _event("international_high_rollers", "International High Rollers", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 3, 2, multiplier_bp=50000, description="Ultra-wealth guests ignite premium gaming spend."),
        _event("whale_convention", "Whale Convention", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=100000, description="Legendary bettors saturate every top-end table."),
        _event("compliance_intervention", "Compliance Intervention", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 9, 1, multiplier_bp=-5000, description="Enhanced checks reduce active high-stakes seats."),
        _event("fraud_lockout", "Fraud Lockout", EVENT_TYPE_NEGATIVE, EVENT_RARITY_RARE, 5, 2, multiplier_bp=-9000, pause=20, description="Fraud alarms trigger temporary payout controls."),
    )),
    "tech_company": BusinessTraitDef("tech_company", "technology", 10400, 38, 12400, 12800, 11200, 13600, 9800, "Explosive growth", "Unstable", (
        _event("ad_campaign_breakthrough", "Ad Campaign Breakthrough", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 16, 1, multiplier_bp=5500, description="Acquisition costs drop while conversion spikes."),
        _event("enterprise_uplift", "Enterprise Uplift", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 12, 2, multiplier_bp=13000, description="Enterprise upgrades accelerate monetization."),
        _event("viral_launch", "Viral Launch", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 8, 1, multiplier_bp=26000, description="A launch wave compounds paid user growth."),
        _event("platform_blowup", "Platform Blowup", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 3, 2, multiplier_bp=52000, description="Your platform dominates feeds and app charts."),
        _event("unicorn_funding_spree", "Unicorn Funding Spree", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=95000, description="Huge capital inflow supercharges paid growth loops."),
        _event("incident_response", "Incident Response", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 9, 1, multiplier_bp=-4800, description="Outages and hotfixes stall revenue capture."),
        _event("regulatory_hold", "Regulatory Hold", EVENT_TYPE_NEGATIVE, EVENT_RARITY_RARE, 5, 2, multiplier_bp=-8500, pause=15, description="Regulatory review pauses key monetization channels."),
    )),
    "shipping_company": BusinessTraitDef("shipping_company", "logistics", 11000, 62, 9800, 10000, 9800, 10400, 12200, "Route optimization", "Delay risk", (
        _event("favorable_currents", "Favorable Currents", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 15, 1, multiplier_bp=4000, description="Currents and winds reduce transit waste."),
        _event("priority_manifest", "Priority Manifest", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 11, 2, multiplier_bp=10000, description="Premium cargo is routed through your fleet."),
        _event("fleet_overclock", "Fleet Overclock", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 7, 2, multiplier_bp=20000, description="Turnaround speed and load factor spike together."),
        _event("global_port_window", "Global Port Window", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 3, 1, multiplier_bp=36000, description="Multiple ports clear congestion simultaneously."),
        _event("sovereign_contract_corridor", "Sovereign Contract Corridor", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=82000, description="Government corridor access unlocks massive routes."),
        _event("port_backlog", "Port Backlog", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 9, 1, multiplier_bp=-3800, description="Container backlog delays your highest-margin lanes."),
        _event("fuel_shock", "Fuel Shock", EVENT_TYPE_NEGATIVE, EVENT_RARITY_RARE, 5, 2, multiplier_bp=-7200, description="Fuel volatility cuts effective route profitability."),
    )),
    "hotel": BusinessTraitDef("hotel", "lodging", 10600, 64, 10200, 10800, 9200, 11000, 10800, "Occupancy waves", "Operational risk", (
        _event("tourist_surge", "Tourist Surge", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 16, 1, multiplier_bp=4500, description="Walk-ins and bookings jump above forecast."),
        _event("conference_block_booking", "Conference Block Booking", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 12, 2, multiplier_bp=11000, description="Corporate blocks fill premium inventory."),
        _event("luxury_upsell_run", "Luxury Upsell Run", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 7, 2, multiplier_bp=21000, description="Suites and concierge packages sell out."),
        _event("global_summit_hosting", "Global Summit Hosting", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 3, 1, multiplier_bp=39000, description="High-value delegates saturate occupancy."),
        _event("royal_delegation_stay", "Royal Delegation Stay", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=86000, description="Ultra-premium bookings dominate every floor."),
        _event("staff_shortage", "Staff Shortage", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 9, 1, multiplier_bp=-4000, description="Front-desk and service constraints reduce occupancy."),
        _event("utility_failure", "Utility Failure", EVENT_TYPE_NEGATIVE, EVENT_RARITY_RARE, 5, 2, multiplier_bp=-7600, pause=15, description="Facility systems fail and premium rooms close."),
    )),
    "movie_studio": BusinessTraitDef("movie_studio", "media", 11600, 48, 11800, 12000, 11800, 12800, 10400, "Hype cycles", "Blockbuster variance", (
        _event("trailer_momentum", "Trailer Momentum", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 14, 1, multiplier_bp=6000, description="Trailer buzz increases licensing value."),
        _event("streaming_prebuy", "Streaming Pre-Buy", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 11, 2, multiplier_bp=14000, description="Platform pre-buys lock in strong margins."),
        _event("festival_breakout", "Festival Breakout", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 7, 1, multiplier_bp=26000, description="Critical acclaim spikes downstream sales."),
        _event("franchise_resurgence", "Franchise Resurgence", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 3, 2, multiplier_bp=52000, description="Back-catalog and merch demand surge together."),
        _event("global_box_office_shockwave", "Global Box Office Shockwave", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=98000, description="Massive audience response cascades into every channel."),
        _event("production_overrun", "Production Overrun", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 9, 1, multiplier_bp=-5000, description="Budget and schedule slippage hit margins."),
        _event("lead_actor_delay", "Lead Actor Delay", EVENT_TYPE_NEGATIVE, EVENT_RARITY_RARE, 5, 2, multiplier_bp=-9000, pause=15, description="A schedule conflict pauses premium scenes."),
    )),
    "space_mining": BusinessTraitDef("space_mining", "offworld", 12400, 30, 12600, 13000, 13000, 14000, 9800, "Extreme extraction spikes", "Critical failure risk", (
        _event("stable_orbit_window", "Stable Orbit Window", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 13, 1, multiplier_bp=7000, description="Orbital drift stabilizes extraction cadence."),
        _event("rare_ore_cluster", "Rare Ore Cluster", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 10, 2, multiplier_bp=16000, description="A profitable mineral band is locked and mined."),
        _event("deep_core_strike", "Deep Core Strike", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 7, 2, multiplier_bp=30000, description="Deep drilling accesses unusually rich deposits."),
        _event("prototype_drone_swarm", "Prototype Drone Swarm", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 3, 1, multiplier_bp=60000, description="Autonomous swarms multiply refined ore throughput."),
        _event("ancient_megadeposit", "Ancient Megadeposit", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=120000, description="A legendary deposit transforms the run economy."),
        _event("radiation_front", "Radiation Front", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 9, 1, multiplier_bp=-5500, description="Radiation shielding protocols reduce operational tempo."),
        _event("reactor_trip", "Reactor Trip", EVENT_TYPE_NEGATIVE, EVENT_RARITY_RARE, 5, 2, multiplier_bp=-10000, pause=20, description="Power instability forces emergency output limits."),
    )),
    "liquor_store": BusinessTraitDef("liquor_store", "nightlife_trade", 13200, 52, 12000, 12600, 9800, 14500, 10400, "Rare bottle rushes", "Stock risk", (
        _event("rush_night", "Rush Night", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 14, 1, multiplier_bp=7800, description="Nightlife traffic pours into your premium shelves."),
        _event("vip_case_drop", "VIP Case Drop", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 10, 2, multiplier_bp=18000, description="A premium merch run clears in minutes."),
        _event("collector_bidding_war", "Collector Bidding War", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 7, 1, multiplier_bp=32000, description="Rare bottles spark a bidding war."),
        _event("citywide_party_weekend", "Citywide Party Weekend", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 3, 2, multiplier_bp=60000, description="Every district is buying premium stock at once."),
        _event("legendary_cellar_unlock", "Legendary Cellar Unlock", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=125000, description="An ultra-rare cellar unlock prints money."),
        _event("dry_shelf_crunch", "Dry Shelf Crunch", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 8, 1, multiplier_bp=-5200, description="Stock runs low and premium sales cool off."),
    )),
    "underground_market": BusinessTraitDef("underground_market", "black_market", 13600, 36, 13200, 13400, 12600, 15200, 10000, "Hot item swings", "Very high risk", (
        _event("hot_item_ping", "Hot Item Ping", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 14, 1, multiplier_bp=8500, description="A hot item catches fire instantly."),
        _event("silent_auction_hit", "Silent Auction Hit", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 11, 2, multiplier_bp=20500, description="An under-the-table deal lands at peak margin."),
        _event("perfect_flip_chain", "Perfect Flip Chain", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 7, 1, multiplier_bp=35000, description="Back-to-back flips chain into huge gains."),
        _event("ghost_manifest", "Ghost Manifest", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 3, 2, multiplier_bp=64000, description="A rare manifest opens impossible inventory."),
        _event("midnight_gold_rush", "Midnight Gold Rush", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=130000, description="One hot run carries the whole cycle."),
        _event("deal_burn", "Deal Burn", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 9, 1, multiplier_bp=-6400, description="A weak flip burns your best window."),
    )),
    "cartel": BusinessTraitDef("cartel", "power_network", 14200, 44, 11400, 11800, 11800, 15000, 10600, "Control snowball", "Pressure risk", (
        _event("territory_lock", "Territory Lock", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 14, 1, multiplier_bp=9200, description="Control stays tight and cash flow rises."),
        _event("pressure_wave", "Pressure Wave", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 11, 2, multiplier_bp=22000, description="Pressure on the map boosts all active lanes."),
        _event("chain_takeover", "Chain Takeover", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 7, 2, multiplier_bp=39000, description="Multiple operations flip under your control."),
        _event("iron_night", "Iron Night", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 3, 1, multiplier_bp=70000, description="The whole machine runs at max pressure."),
        _event("total_lock", "Total Lock", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=138000, description="You held total control across every route."),
        _event("control_crack", "Control Crack", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 9, 1, multiplier_bp=-7000, description="A crack in control weakens your take."),
    )),
    "shadow_government": BusinessTraitDef("shadow_government", "meta_power", 14800, 58, 10800, 12000, 9000, 16000, 11200, "Economy control", "Exposure risk", (
        _event("favor_collection", "Favor Collection", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 13, 1, multiplier_bp=9800, description="A favor cashes in quietly."),
        _event("silent_directive", "Silent Directive", EVENT_TYPE_POSITIVE, EVENT_RARITY_UNCOMMON, 10, 2, multiplier_bp=23000, description="A quiet order tilts payouts your way."),
        _event("market_string_pull", "Market String Pull", EVENT_TYPE_POSITIVE, EVENT_RARITY_RARE, 7, 1, multiplier_bp=42000, description="You pull the right string at the right time."),
        _event("network_override", "Network Override", EVENT_TYPE_POSITIVE, EVENT_RARITY_EPIC, 3, 2, multiplier_bp=76000, description="Your network boosts everything at once."),
        _event("economic_blackout", "Economic Blackout", EVENT_TYPE_POSITIVE, EVENT_RARITY_LEGENDARY, 1, 2, multiplier_bp=145000, description="You bend the whole economy for one cycle."),
        _event("leak_scare", "Leak Scare", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 8, 1, multiplier_bp=-5600, description="Rumors force a short pullback."),
    )),
}

DEFAULT_TRAIT = BusinessTraitDef("default", "general", 10000, 60, 10000, 10000, 10000, 10000, 10000, "Balanced", "Medium", (
    _event("efficiency_wave", "Efficiency Wave", EVENT_TYPE_POSITIVE, EVENT_RARITY_COMMON, 10, 1, multiplier_bp=3000, description="Operations sync up and output rises."),
    _event("supplier_drag", "Supplier Drag", EVENT_TYPE_NEGATIVE, EVENT_RARITY_UNCOMMON, 8, 1, multiplier_bp=-3200, description="Suppliers miss SLAs and throughput slips."),
))

SYNERGIES = (
    SynergyDef("farm_supply", frozenset({"restaurant", "farm"}), 800, "Farm supplies restaurant. +8% restaurant flow while both run.", "restaurant"),
    SynergyDef("cross_promo", frozenset({"restaurant", "nightclub"}), 500, "Cross-promotion warms up nightlife and late food traffic.", "global"),
    SynergyDef("automation_pipeline", frozenset({"factory", "tech_company"}), 1000, "Automation pipeline sharpens industrial efficiency.", "global"),
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def get_run_mode(key: Optional[str]) -> RunModeDef:
    return RUN_MODE_DEFS.get(str(key or RUN_MODE_STANDARD).strip().lower(), RUN_MODE_DEFS[RUN_MODE_STANDARD])


def get_business_trait(key: str) -> BusinessTraitDef:
    return BUSINESS_TRAITS.get(str(key).strip().lower(), DEFAULT_TRAIT)


def get_run_mode_for_level(level: int, stored_key: Optional[str]) -> RunModeDef:
    chosen = get_run_mode(stored_key)
    if chosen.key == RUN_MODE_AGGRESSIVE and int(level) < 50:
        return RUN_MODE_DEFS[RUN_MODE_STANDARD]
    return chosen


def worker_role_label(worker_type: str, business_key: str) -> str:
    worker_type = str(worker_type or "efficient").lower()
    mapping = {
        "restaurant": {"fast": "Cashier", "efficient": "Cook", "kind": "Host"},
        "farm": {"fast": "Harvester", "efficient": "Farmhand", "kind": "Caretaker"},
        "nightclub": {"fast": "Runner", "efficient": "Promoter", "kind": "Bartender"},
        "factory": {"fast": "Loader", "efficient": "Technician", "kind": "Inspector"},
        "tech_company": {"fast": "Growth Rep", "efficient": "Engineer", "kind": "Support Lead"},
        "liquor_store": {"fast": "Runner", "efficient": "Stock Lead", "kind": "Floor Host"},
        "underground_market": {"fast": "Scout", "efficient": "Broker", "kind": "Closer"},
        "cartel": {"fast": "Enforcer", "efficient": "Operator", "kind": "Fixer"},
        "shadow_government": {"fast": "Courier", "efficient": "Analyst", "kind": "Liaison"},
    }
    return mapping.get(business_key, {}).get(worker_type, worker_type.title())


def manager_role_label(business_key: str, slot_index: int = 0) -> str:
    roles = {
        "restaurant": ("Operations Manager", "Marketing Manager"),
        "farm": ("Farm Supervisor", "Logistics Manager"),
        "nightclub": ("Security Manager", "Marketing Manager"),
        "factory": ("Technical Lead", "Operations Manager"),
        "tech_company": ("Technical Lead", "Finance Manager"),
        "liquor_store": ("Floor Boss", "Supply Boss"),
        "underground_market": ("Deal Boss", "Intel Boss"),
        "cartel": ("Control Boss", "Pressure Boss"),
        "shadow_government": ("Power Broker", "Favor Broker"),
    }
    pool = roles.get(business_key, ("Operations Manager", "Finance Manager"))
    return pool[min(max(int(slot_index), 0), len(pool) - 1)]


def diminishing_worker_bonus_bp(total_bp: int) -> int:
    total_bp = max(int(total_bp), 0)
    if total_bp <= 2500:
        return total_bp
    softened = 2500 + int((total_bp - 2500) * 0.65)
    return min(softened, WORKER_PERCENT_CAP_BP)


def manager_positive_bonus_bp(rows: Sequence) -> int:
    total = 0
    for row in rows:
        rarity = str(getattr(row, 'rarity', 'common')).lower()
        base = {"common": 300, "rare": 600, "epic": 900, "legendary": 1250, "mythical": 1650}.get(rarity, 300)
        total += base
    return min(total, MANAGER_POSITIVE_CAP_BP)


def manager_negative_reduction_bp(rows: Sequence) -> int:
    total = 0
    for row in rows:
        rarity = str(getattr(row, 'rarity', 'common')).lower()
        base = {"common": 350, "rare": 700, "epic": 1100, "legendary": 1500, "mythical": 1850}.get(rarity, 350)
        total += base
    return min(total, MANAGER_NEGATIVE_CAP_BP)


def manager_downtime_reduction_bp(rows: Sequence) -> int:
    total = 0
    for row in rows:
        runtime_bonus = int(getattr(row, 'runtime_bonus_hours', 0) or 0)
        total += 400 + min(runtime_bonus * 35, 800)
    return min(total, 3000)


def manager_instant_reward_bonus_bp(rows: Sequence) -> int:
    total = 0
    for row in rows:
        total += 250 + int(getattr(row, 'auto_restart_charges', 0) or 0) * 120
    return min(total, 2000)


def calc_synergy_bonus_bp(business_key: str, running_keys: Iterable[str], owned_keys: Iterable[str]) -> tuple[int, list[str]]:
    running_keys = set(running_keys)
    owned_keys = set(owned_keys)
    total = 0
    labels: list[str] = []
    for synergy in SYNERGIES:
        if not synergy.business_keys.issubset(owned_keys):
            continue
        if synergy.applies_to == "global":
            if len(running_keys.intersection(synergy.business_keys)) == len(synergy.business_keys):
                total += synergy.bonus_bp
                labels.append(synergy.description)
        elif synergy.applies_to == business_key and business_key in running_keys and len(running_keys.intersection(synergy.business_keys)) == len(synergy.business_keys):
            total += synergy.bonus_bp
            labels.append(synergy.description)
    if len(running_keys) >= 3 and business_key in running_keys:
        total += 600
        labels.append("Business Empire +6% for keeping 3+ businesses active.")
    unique_types = len(owned_keys)
    if unique_types >= 5:
        total += 400
        labels.append("Diversified portfolio +4% management bonus from 5 unique businesses.")
    return min(total, GLOBAL_PORTFOLIO_CAP_BP), labels


def format_duration_minutes(minutes: int) -> str:
    minutes = max(int(minutes), 0)
    hours, rem = divmod(minutes, 60)
    if hours and rem:
        return f"{hours}h {rem}m"
    if hours:
        return f"{hours}h"
    return f"{rem}m"


def resolve_event_checkpoints(*, started_at: datetime, ends_at: datetime) -> list[datetime]:
    started_at = as_utc(started_at)
    ends_at = as_utc(ends_at)
    cursor = started_at + timedelta(minutes=EVENT_CHECK_INTERVAL_MINUTES)
    checkpoints: list[datetime] = []
    while cursor < ends_at:
        checkpoints.append(cursor)
        cursor += timedelta(minutes=EVENT_CHECK_INTERVAL_MINUTES)
    return checkpoints


def _weighted_choice(entries: Sequence[tuple[EventDef, float]], *, rng: random.Random) -> Optional[EventDef]:
    total = sum(weight for _, weight in entries if weight > 0)
    if total <= 0:
        return None
    roll = rng.random() * total
    upto = 0.0
    for event, weight in entries:
        if weight <= 0:
            continue
        upto += weight
        if roll <= upto:
            return event
    return entries[-1][0] if entries else None


def _rarity_weight_modifier(rarity: str, luck_bp: int) -> float:
    rarity = str(rarity or EVENT_RARITY_COMMON).lower()
    luck_scale = 1 + (max(int(luck_bp), 0) / 10000)
    if rarity == EVENT_RARITY_COMMON:
        return max(0.45, 1 - min(luck_scale * 0.18, 0.55))
    if rarity == EVENT_RARITY_UNCOMMON:
        return max(0.55, 1 - min(luck_scale * 0.09, 0.35))
    if rarity == EVENT_RARITY_RARE:
        return 1 + min(luck_scale * 0.16, 0.50)
    if rarity == EVENT_RARITY_EPIC:
        return 1 + min(luck_scale * 0.30, 0.95)
    if rarity == EVENT_RARITY_LEGENDARY:
        return 1 + min(luck_scale * 0.45, 1.35)
    return 1.0


def worker_event_frequency_bonus_bp(rows: Sequence) -> int:
    total = 0
    for row in rows:
        worker_type = str(getattr(row, "worker_type", "efficient")).lower()
        worker_bp = int(getattr(row, "percent_profit_bonus_bp", 0) or 0)
        if worker_type == "fast":
            total += 250 + min(worker_bp // 8, 500)
        elif worker_type == "kind":
            total += 150 + min(worker_bp // 10, 400)
    return min(total, 2600)


def worker_event_duration_bonus_bp(rows: Sequence) -> int:
    total = 0
    for row in rows:
        worker_type = str(getattr(row, "worker_type", "efficient")).lower()
        worker_bp = int(getattr(row, "percent_profit_bonus_bp", 0) or 0)
        if worker_type == "kind":
            total += 220 + min(worker_bp // 7, 750)
    return min(total, 3200)


def worker_positive_event_power_bonus_bp(rows: Sequence) -> int:
    total = 0
    for row in rows:
        worker_type = str(getattr(row, "worker_type", "efficient")).lower()
        worker_bp = int(getattr(row, "percent_profit_bonus_bp", 0) or 0)
        if worker_type == "efficient":
            total += 260 + min(worker_bp // 7, 850)
        elif worker_type == "fast":
            total += 120 + min(worker_bp // 12, 300)
    return min(total, 3500)


def worker_negative_event_mitigation_bp(rows: Sequence) -> int:
    total = 0
    for row in rows:
        worker_type = str(getattr(row, "worker_type", "efficient")).lower()
        worker_bp = int(getattr(row, "percent_profit_bonus_bp", 0) or 0)
        if worker_type == "kind":
            total += 300 + min(worker_bp // 6, 900)
        elif worker_type == "efficient":
            total += 120 + min(worker_bp // 14, 250)
    return min(total, 3800)


def worker_rarity_luck_bp(rows: Sequence) -> int:
    total = 0
    rarity_points = {"common": 40, "uncommon": 70, "rare": 120, "epic": 190, "mythic": 260, "mythical": 260}
    for row in rows:
        rarity = str(getattr(row, "rarity", "common")).lower()
        total += rarity_points.get(rarity, 50)
    return min(total, 2200)


def build_run_event_plan(*, run_id: int, business_key: str, level: int, worker_count: int, worker_rows: Sequence | None, manager_rows: Sequence, started_at: datetime, ends_at: datetime, run_mode_key: str) -> list[dict]:
    trait = get_business_trait(business_key)
    run_mode = get_run_mode_for_level(level, run_mode_key)
    rng = random.Random(f"business-run:{run_id}:{business_key}:{int(as_utc(started_at).timestamp())}")
    worker_rows = list(worker_rows or [])
    checkpoints = resolve_event_checkpoints(started_at=started_at, ends_at=ends_at)
    cooldown_until: Optional[datetime] = None
    active_events = 0
    plan: list[dict] = []
    base_chance = 0.17 * (trait.event_frequency_bp / 10000) * (1 + run_mode.frequency_bp / 10000)
    base_chance *= 1 + min(worker_count * 0.015, 0.12)
    base_chance *= 1 + worker_event_frequency_bonus_bp(worker_rows) / 10000
    base_chance *= 1 + manager_positive_bonus_bp(manager_rows) / 40000
    base_chance = max(0.05, min(base_chance, 0.42))
    worker_positive_bp = worker_positive_event_power_bonus_bp(worker_rows)
    worker_negative_mitigation_bp = worker_negative_event_mitigation_bp(worker_rows)
    worker_duration_bp = worker_event_duration_bonus_bp(worker_rows)
    worker_luck_bp = worker_rarity_luck_bp(worker_rows)
    rarity_mix_bonus = max(level - 25, 0) * 12

    for checkpoint in checkpoints:
        if cooldown_until and checkpoint < cooldown_until:
            continue
        if active_events >= MAX_EVENT_STACKS:
            break
        if rng.random() > base_chance:
            continue
        weighted: list[tuple[EventDef, float]] = []
        for event in trait.event_pool:
            weight = float(event.weight)
            weight *= _rarity_weight_modifier(event.rarity, worker_luck_bp + rarity_mix_bonus)
            if event.event_type == EVENT_TYPE_POSITIVE:
                weight *= trait.positive_event_weight_bp / 10000
                weight *= 1 + run_mode.positive_event_bp / 10000
                weight *= 1 + manager_positive_bonus_bp(manager_rows) / 20000
            elif event.event_type == EVENT_TYPE_NEGATIVE:
                weight *= trait.negative_event_weight_bp / 10000
                weight *= 1 + run_mode.negative_event_bp / 10000
                weight *= 1 - min(
                    (manager_negative_reduction_bp(manager_rows) + worker_negative_mitigation_bp) / 11000,
                    0.75,
                )
            weighted.append((event, max(weight, 0.1)))
        picked = _weighted_choice(weighted, rng=rng)
        if picked is None:
            continue
        duration_minutes = int(picked.duration_hours) * 60
        duration_minutes += max(0, int(level - 1)) * int(picked.duration_level_bonus_minutes)
        duration_minutes = int(round(duration_minutes * (1 + worker_duration_bp / 10000)))
        duration = min(max(duration_minutes, 60), EVENT_DURATION_CAP_MINUTES)
        pause = max(int(picked.pause_minutes), 0)
        if pause > 0:
            pause = max(5, int(round(pause * (1 - manager_downtime_reduction_bp(manager_rows) / 10000))))
        multiplier_bp = int(round(picked.multiplier_bp + (max(level - 1, 0) * picked.level_multiplier_bp)))
        if multiplier_bp < 0:
            mitigation = manager_negative_reduction_bp(manager_rows) + worker_negative_mitigation_bp
            multiplier_bp = int(round(multiplier_bp * (1 - mitigation / 10000)))
        elif multiplier_bp > 0:
            multiplier_bp = int(round(multiplier_bp * (1 + min(worker_count * 0.012, 0.15))))
            multiplier_bp = int(round(multiplier_bp * (1 + worker_positive_bp / 10000)))
        if picked.event_type == EVENT_TYPE_POSITIVE:
            multiplier_bp = max(multiplier_bp, 5000)
        plan.append({
            "event_key": picked.key,
            "name": picked.name,
            "event_type": picked.event_type,
            "rarity": picked.rarity,
            "description": picked.description,
            "starts_at_iso": checkpoint.isoformat(),
            "ends_at_iso": (checkpoint + timedelta(minutes=duration)).isoformat() if duration > 0 else None,
            "duration_minutes": duration,
            "multiplier_bp": multiplier_bp,
            "instant_bonus_hours": 0.0,
            "pause_minutes": pause,
        })
        cooldown_until = checkpoint + timedelta(minutes=EVENT_COOLDOWN_MINUTES)
        active_events += 1
    return plan


def summarize_active_events(event_plan: Sequence[dict], *, now: datetime) -> tuple[int, list[str]]:
    now = as_utc(now)
    total_bp = 0
    lines: list[str] = []
    for evt in event_plan:
        starts = as_utc(datetime.fromisoformat(evt["starts_at_iso"]))
        ends_raw = evt.get("ends_at_iso")
        ends = as_utc(datetime.fromisoformat(ends_raw)) if ends_raw else None
        if starts > now:
            continue
        if ends is not None and now >= ends:
            continue
        total_bp += int(evt.get("multiplier_bp", 0) or 0)
        rem = format_duration_minutes(int((ends - now).total_seconds() // 60)) if ends is not None else "instant"
        rarity = str(evt.get("rarity", "")).strip().title()
        rarity_tag = f"{rarity} • " if rarity else ""
        lines.append(f"{rarity_tag}{evt.get('name', 'Event')} {('+' if int(evt.get('multiplier_bp',0)) >= 0 else '')}{int(evt.get('multiplier_bp',0))/100:.0f}% • {rem}")
    return total_bp, lines[:3]
