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
EVENT_TYPE_RARE = "rare"
EVENT_TYPE_NEUTRAL = "neutral"

MAX_EVENT_STACKS = 3
EVENT_CHECK_INTERVAL_MINUTES = 60
EVENT_COOLDOWN_MINUTES = 90
EVENT_DURATION_CAP_MINUTES = 12 * 60
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
    weight: int
    duration_minutes: int
    multiplier_bp: int = 0
    instant_hours_low: float = 0.0
    instant_hours_high: float = 0.0
    pause_minutes: int = 0
    description: str = ""
    level_multiplier_bp: int = 0


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


def _event(key: str, name: str, event_type: str, weight: int, duration: int, *, multiplier_bp: int = 0, instant_low: float = 0.0, instant_high: float = 0.0, pause: int = 0, description: str = "", level_bp: int = 0) -> EventDef:
    return EventDef(key, name, event_type, weight, duration, multiplier_bp, instant_low, instant_high, pause, description, level_bp)


BUSINESS_TRAITS: dict[str, BusinessTraitDef] = {
    "restaurant": BusinessTraitDef("restaurant", "hospitality", 10000, 60, 10500, 13000, 9800, 10600, 10000, "Popularity spikes", "Medium risk", (
        _event("rush_hour", "Rush Hour", EVENT_TYPE_POSITIVE, 18, 120, multiplier_bp=2200, description="Dinner demand surges and tables flip faster.", level_bp=5),
        _event("influencer_post", "Influencer Post", EVENT_TYPE_POSITIVE, 10, 180, multiplier_bp=3500, description="A local creator sends a flood of orders."),
        _event("food_critic", "Food Critic Visit", EVENT_TYPE_POSITIVE, 8, 120, multiplier_bp=2600, instant_low=0.5, instant_high=1.2, description="Great review momentum boosts the current service."),
        _event("catering_order", "Catering Contract", EVENT_TYPE_RARE, 4, 0, instant_low=2.0, instant_high=3.5, description="A high-margin catering order lands mid-run."),
        _event("ingredient_shortage", "Ingredient Shortage", EVENT_TYPE_NEGATIVE, 9, 90, multiplier_bp=-1500, description="Short supply forces a weaker menu."),
        _event("health_inspection", "Health Inspection", EVENT_TYPE_NEGATIVE, 6, 90, multiplier_bp=-1800, pause=20, description="The kitchen slows under inspection pressure."),
        _event("viral_menu_item", "Viral Menu Item", EVENT_TYPE_RARE, 3, 180, multiplier_bp=4800, description="One dish explodes online and carries the rest of the run."),
    )),
    "farm": BusinessTraitDef("farm", "agriculture", 9400, 86, 8200, 10600, 7600, 9300, 11200, "Reliable harvests", "Low risk", (
        _event("fertile_harvest", "Fertile Harvest", EVENT_TYPE_POSITIVE, 16, 180, multiplier_bp=2500, description="Exceptional soil conditions lift output."),
        _event("good_weather", "Good Weather", EVENT_TYPE_POSITIVE, 13, 120, multiplier_bp=1500, description="Clean weather keeps everything on schedule."),
        _event("livestock_boom", "Livestock Bonus", EVENT_TYPE_POSITIVE, 10, 120, multiplier_bp=1800, instant_low=0.4, instant_high=1.0, description="Animal output spikes unexpectedly."),
        _event("seasonal_demand", "Seasonal Demand Spike", EVENT_TYPE_RARE, 4, 180, multiplier_bp=3600, description="Regional buyers overpay for fresh stock."),
        _event("pest_issue", "Pest Infestation", EVENT_TYPE_NEGATIVE, 8, 120, multiplier_bp=-1300, description="Fields need treatment before full output resumes."),
        _event("drought", "Drought", EVENT_TYPE_NEGATIVE, 6, 150, multiplier_bp=-1800, description="Dry conditions drag production down."),
        _event("broken_equipment", "Broken Equipment", EVENT_TYPE_NEGATIVE, 5, 90, multiplier_bp=-1400, pause=15, description="One machine goes down for repairs."),
    )),
    "nightclub": BusinessTraitDef("nightclub", "entertainment", 10800, 42, 11800, 12400, 11800, 12200, 10000, "Explosive nightlife", "High risk", (
        _event("vip_party", "VIP Party", EVENT_TYPE_RARE, 5, 0, instant_low=2.0, instant_high=4.0, description="Bottle service prints silver for a few hours at once."),
        _event("celebrity_visit", "Celebrity Visit", EVENT_TYPE_POSITIVE, 8, 120, multiplier_bp=5000, description="A celebrity sighting sends the line around the block."),
        _event("viral_dj", "Viral DJ Set", EVENT_TYPE_POSITIVE, 10, 120, multiplier_bp=3200, description="The DJ goes viral and cover sales spike."),
        _event("premium_bottle_rush", "Premium Bottle Rush", EVENT_TYPE_POSITIVE, 12, 90, multiplier_bp=2400, description="A wealthy crowd leans hard into premium menus."),
        _event("noise_complaint", "Noise Complaint", EVENT_TYPE_NEGATIVE, 10, 90, multiplier_bp=-1800, description="Security must cool the room and lower the energy."),
        _event("security_incident", "Security Incident", EVENT_TYPE_NEGATIVE, 7, 60, multiplier_bp=-2200, pause=25, description="A fight disrupts normal service flow."),
        _event("licensing_inspection", "Police Check", EVENT_TYPE_NEGATIVE, 5, 60, multiplier_bp=-2000, pause=15, description="Compliance checks slow admissions."),
    )),
    "factory": BusinessTraitDef("factory", "industrial", 11200, 58, 9600, 9800, 11200, 9800, 10800, "Heavy base output", "Downtime risk", (
        _event("machinery_boost", "Machinery Boost", EVENT_TYPE_POSITIVE, 12, 120, multiplier_bp=2400, description="A production line runs above spec."),
        _event("bulk_contract", "Bulk Contract", EVENT_TYPE_POSITIVE, 9, 180, multiplier_bp=2600, instant_low=0.8, instant_high=1.8, description="A new buyer fills excess capacity immediately."),
        _event("automation_cycle", "Automation Cycle", EVENT_TYPE_RARE, 4, 180, multiplier_bp=4200, description="Automation tuning squeezes out a huge run."),
        _event("equipment_jam", "Equipment Jam", EVENT_TYPE_NEGATIVE, 10, 90, multiplier_bp=-1700, pause=25, description="A jam robs the line of key throughput."),
        _event("maintenance_overrun", "Maintenance Overrun", EVENT_TYPE_NEGATIVE, 8, 120, multiplier_bp=-1900, description="Unexpected servicing eats productive time."),
        _event("safety_hold", "Safety Hold", EVENT_TYPE_NEGATIVE, 5, 60, multiplier_bp=-1200, pause=20, description="Ops pauses briefly after a safety trigger."),
    )),
    "tech_company": BusinessTraitDef("tech_company", "technology", 10400, 38, 12000, 12800, 11000, 13500, 9800, "Explosive growth", "Unstable", (
        _event("investor_boost", "Investor Boost", EVENT_TYPE_RARE, 4, 180, multiplier_bp=5200, description="Investor hype lifts every revenue stream."),
        _event("app_launch", "App Launch Success", EVENT_TYPE_POSITIVE, 11, 180, multiplier_bp=3200, instant_low=0.8, instant_high=1.6, description="A product release lands cleanly and drives signups."),
        _event("viral_trend", "Viral Trend", EVENT_TYPE_POSITIVE, 10, 120, multiplier_bp=3600, description="Word-of-mouth growth bends the run upward."),
        _event("enterprise_trial", "Enterprise Trial", EVENT_TYPE_POSITIVE, 8, 120, multiplier_bp=2400, description="A B2B trial suddenly converts."),
        _event("server_outage", "Server Outage", EVENT_TYPE_NEGATIVE, 9, 90, multiplier_bp=-2100, pause=30, description="Downtime slams productivity until systems recover."),
        _event("feature_regression", "Feature Regression", EVENT_TYPE_NEGATIVE, 7, 90, multiplier_bp=-1600, description="Rollback work steals focus from growth."),
        _event("compliance_fire", "Compliance Fire Drill", EVENT_TYPE_NEGATIVE, 5, 60, multiplier_bp=-1300, description="Legal review slows rollouts."),
    )),
}

DEFAULT_TRAIT = BusinessTraitDef("default", "general", 10000, 60, 10000, 10000, 10000, 10000, 10000, "Balanced", "Medium", (
    _event("efficiency_boost", "Efficiency Boost", EVENT_TYPE_POSITIVE, 10, 120, multiplier_bp=1600, description="The team finds a cleaner operating rhythm."),
    _event("supply_delay", "Supply Delay", EVENT_TYPE_NEGATIVE, 10, 90, multiplier_bp=-1500, description="A small delay clips the run."),
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
    }
    return mapping.get(business_key, {}).get(worker_type, worker_type.title())


def manager_role_label(business_key: str, slot_index: int = 0) -> str:
    roles = {
        "restaurant": ("Operations Manager", "Marketing Manager"),
        "farm": ("Farm Supervisor", "Logistics Manager"),
        "nightclub": ("Security Manager", "Marketing Manager"),
        "factory": ("Technical Lead", "Operations Manager"),
        "tech_company": ("Technical Lead", "Finance Manager"),
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


def build_run_event_plan(*, run_id: int, business_key: str, level: int, worker_count: int, manager_rows: Sequence, started_at: datetime, ends_at: datetime, run_mode_key: str) -> list[dict]:
    trait = get_business_trait(business_key)
    run_mode = get_run_mode_for_level(level, run_mode_key)
    rng = random.Random(f"business-run:{run_id}:{business_key}:{int(as_utc(started_at).timestamp())}")
    checkpoints = resolve_event_checkpoints(started_at=started_at, ends_at=ends_at)
    cooldown_until: Optional[datetime] = None
    active_events = 0
    plan: list[dict] = []
    base_chance = 0.17 * (trait.event_frequency_bp / 10000) * (1 + run_mode.frequency_bp / 10000)
    base_chance *= 1 + min(worker_count * 0.015, 0.12)
    base_chance *= 1 + manager_positive_bonus_bp(manager_rows) / 40000
    base_chance = max(0.05, min(base_chance, 0.42))

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
            if event.event_type == EVENT_TYPE_POSITIVE:
                weight *= trait.positive_event_weight_bp / 10000
                weight *= 1 + run_mode.positive_event_bp / 10000
                weight *= 1 + manager_positive_bonus_bp(manager_rows) / 20000
            elif event.event_type == EVENT_TYPE_NEGATIVE:
                weight *= trait.negative_event_weight_bp / 10000
                weight *= 1 + run_mode.negative_event_bp / 10000
                weight *= 1 - manager_negative_reduction_bp(manager_rows) / 12000
            elif event.event_type == EVENT_TYPE_RARE:
                weight *= trait.rare_event_weight_bp / 10000
                weight *= 1 + max(level - 1, 0) * 0.002
            weighted.append((event, max(weight, 0.1)))
        picked = _weighted_choice(weighted, rng=rng)
        if picked is None:
            continue
        duration = min(max(int(picked.duration_minutes), 0), EVENT_DURATION_CAP_MINUTES)
        pause = max(int(picked.pause_minutes), 0)
        if pause > 0:
            pause = max(5, int(round(pause * (1 - manager_downtime_reduction_bp(manager_rows) / 10000))))
        multiplier_bp = int(round(picked.multiplier_bp + (max(level - 1, 0) * picked.level_multiplier_bp)))
        if multiplier_bp < 0:
            multiplier_bp = int(round(multiplier_bp * (1 - manager_negative_reduction_bp(manager_rows) / 10000)))
        elif multiplier_bp > 0:
            multiplier_bp = int(round(multiplier_bp * (1 + min(worker_count * 0.012, 0.15))))
        instant_low = picked.instant_hours_low
        instant_high = picked.instant_hours_high
        if instant_high > 0:
            instant_hours = rng.uniform(instant_low, instant_high)
            instant_hours *= 1 + manager_instant_reward_bonus_bp(manager_rows) / 10000
        else:
            instant_hours = 0.0
        plan.append({
            "event_key": picked.key,
            "name": picked.name,
            "event_type": picked.event_type,
            "description": picked.description,
            "starts_at_iso": checkpoint.isoformat(),
            "ends_at_iso": (checkpoint + timedelta(minutes=duration)).isoformat() if duration > 0 else None,
            "duration_minutes": duration,
            "multiplier_bp": multiplier_bp,
            "instant_bonus_hours": round(instant_hours, 3),
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
        lines.append(f"{evt.get('name', 'Event')} {('+' if int(evt.get('multiplier_bp',0)) >= 0 else '')}{int(evt.get('multiplier_bp',0))/100:.0f}% • {rem}")
    return total_bp, lines[:3]
