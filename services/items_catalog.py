# services/items_catalog.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional


class ItemRarity(str, Enum):
    COMMON = "common"
    UNCOMMON = "uncommon"
    RARE = "rare"
    MYTHICAL = "mythical"


class EffectStacking(str, Enum):
    ADD = "add"
    REPLACE = "replace"
    REFRESH = "refresh"
    EXTEND = "extend"
    DENY = "deny"


@dataclass(frozen=True)
class EffectDef:
    effect_key: str
    group_key: str
    payload: Dict[str, int]
    duration_seconds: Optional[int] = None
    charges: Optional[int] = None
    stacking: EffectStacking = EffectStacking.ADD


@dataclass(frozen=True)
class ItemDef:
    key: str
    name: str
    rarity: ItemRarity
    price: int
    daily_limit: int
    tradable: bool
    effect: EffectDef
    description: str = ""
    inventory_description: str = ""


ITEMS: dict[str, ItemDef] = {
    # -----------------
    # Existing items
    # -----------------
    "energy_drink": ItemDef(
        key="energy_drink",
        name="Energy Drink",
        rarity=ItemRarity.COMMON,
        price=250,
        daily_limit=5,
        tradable=True,
        effect=EffectDef(
            effect_key="energy_instant",
            group_key="energy",
            payload={"stamina_add": 25},
        ),
    ),
    "protein_bar": ItemDef(
        key="protein_bar",
        name="Protein Bar",
        rarity=ItemRarity.COMMON,
        price=300,
        daily_limit=5,
        tradable=True,
        effect=EffectDef(
            effect_key="payout_boost",
            group_key="payout",
            payload={"payout_bonus_bp": 1000},
            duration_seconds=30 * 60,
            stacking=EffectStacking.REFRESH,
        ),
    ),
    "caffeine_gum": ItemDef(
        key="caffeine_gum",
        name="Caffeine Gum",
        rarity=ItemRarity.UNCOMMON,
        price=650,
        daily_limit=3,
        tradable=True,
        effect=EffectDef(
            effect_key="fail_reduction",
            group_key="success",
            payload={"fail_reduction_bp": 800},
            duration_seconds=45 * 60,
            stacking=EffectStacking.REFRESH,
        ),
    ),
    "wrist_wraps": ItemDef(
        key="wrist_wraps",
        name="Wrist Wraps",
        rarity=ItemRarity.RARE,
        price=2000,
        daily_limit=2,
        tradable=True,
        effect=EffectDef(
            effect_key="job_xp_boost",
            group_key="job_xp",
            payload={"job_xp_bonus_bp": 2500},
            duration_seconds=30 * 60,
            stacking=EffectStacking.REFRESH,
        ),
    ),
    "adrenaline_patch": ItemDef(
        key="adrenaline_patch",
        name="Adrenaline Patch",
        rarity=ItemRarity.MYTHICAL,
        price=5000,
        daily_limit=1,
        tradable=True,
        effect=EffectDef(
            effect_key="energy_cap_boost",
            group_key="energy_cap",
            payload={"stamina_cap_add": 50},
            duration_seconds=60 * 60,
            stacking=EffectStacking.REPLACE,
        ),
    ),

    # -----------------
    # +15 new items (no redundancies)
    # Each item introduces a distinct modifier field or mechanic.
    # -----------------

    # 1) Instant silver gain (flat)
    "found_wallet": ItemDef(
        key="found_wallet",
        name="Found Wallet",
        rarity=ItemRarity.UNCOMMON,
        price=900,
        daily_limit=2,
        tradable=True,
        effect=EffectDef(
            effect_key="silver_instant",
            group_key="silver_instant",
            payload={"silver_add": 1500},
        ),
    ),

    # 2) Next-work silver bonus (one charge)
    "tip_jar": ItemDef(
        key="tip_jar",
        name="Tip Jar",
        rarity=ItemRarity.COMMON,
        price=400,
        daily_limit=4,
        tradable=True,
        effect=EffectDef(
            effect_key="next_work_silver_bonus",
            group_key="next_work_bonus",
            payload={"next_work_silver_bonus": 250},
            charges=1,
            stacking=EffectStacking.ADD,
        ),
    ),

    # 3) Stamina cost discount for N works (charges)
    "creatine": ItemDef(
        key="creatine",
        name="Creatine",
        rarity=ItemRarity.UNCOMMON,
        price=1200,
        daily_limit=2,
        tradable=True,
        effect=EffectDef(
            effect_key="stamina_cost_discount",
            group_key="stamina_cost",
            payload={"stamina_cost_flat_delta": -1},
            charges=20,
            stacking=EffectStacking.REPLACE,
        ),
    ),

    # 4) Job XP instant injection
    "training_manual": ItemDef(
        key="training_manual",
        name="Training Manual",
        rarity=ItemRarity.RARE,
        price=2600,
        daily_limit=2,
        tradable=True,
        effect=EffectDef(
            effect_key="job_xp_instant",
            group_key="job_xp_instant",
            payload={"job_xp_add": 250},
        ),
    ),

    # 5) User XP instant injection
    "study_notes": ItemDef(
        key="study_notes",
        name="Study Notes",
        rarity=ItemRarity.UNCOMMON,
        price=1100,
        daily_limit=3,
        tradable=True,
        effect=EffectDef(
            effect_key="user_xp_instant",
            group_key="user_xp_instant",
            payload={"user_xp_add": 200},
        ),
    ),

    # 6) User XP timed bonus (separate from job_xp/payout)
    "study_sprint_timer": ItemDef(
        key="study_sprint_timer",
        name="Study Sprint Timer",
        rarity=ItemRarity.MYTHICAL,
        price=6500,
        daily_limit=1,
        tradable=True,
        effect=EffectDef(
            effect_key="user_xp_boost",
            group_key="user_xp",
            payload={"user_xp_bonus_bp": 15000},  # +150%
            duration_seconds=30 * 60,
            stacking=EffectStacking.REPLACE,
        ),
    ),

    # 7) Double payout chance for N works (charges)
    "lucky_coin": ItemDef(
        key="lucky_coin",
        name="Lucky Coin",
        rarity=ItemRarity.MYTHICAL,
        price=7000,
        daily_limit=1,
        tradable=True,
        effect=EffectDef(
            effect_key="double_payout_chance",
            group_key="double_payout",
            payload={"double_payout_chance_bp": 2500},  # +25%
            charges=20,
            stacking=EffectStacking.REPLACE,
        ),
    ),

    # 8) Fail chance reduction for N works (charges, distinct from timed success)
    "safety_harness": ItemDef(
        key="safety_harness",
        name="Safety Harness",
        rarity=ItemRarity.MYTHICAL,
        price=6200,
        daily_limit=1,
        tradable=True,
        effect=EffectDef(
            effect_key="fail_reduction_charges",
            group_key="success",
            payload={"fail_reduction_bp": 2000},  # -20%
            charges=25,
            stacking=EffectStacking.REPLACE,
        ),
    ),

    # 9) Regen bonus (bp) but longer and stronger than phone_charger concept (distinct name/use)
    "electrolyte_packet": ItemDef(
        key="electrolyte_packet",
        name="Electrolyte Packet",
        rarity=ItemRarity.UNCOMMON,
        price=950,
        daily_limit=3,
        tradable=True,
        effect=EffectDef(
            effect_key="regen_boost",
            group_key="regen",
            payload={"regen_bonus_bp": 2500},  # +25%
            duration_seconds=2 * 60 * 60,
            stacking=EffectStacking.REPLACE,
        ),
    ),

    # 10) Mythical regen booster (stronger tier)
    "electrolyte_mega_pack": ItemDef(
        key="electrolyte_mega_pack",
        name="Electrolyte Mega Pack",
        rarity=ItemRarity.MYTHICAL,
        price=5600,
        daily_limit=1,
        tradable=True,
        effect=EffectDef(
            effect_key="regen_boost_myth",
            group_key="regen",
            payload={"regen_bonus_bp": 5000},  # +50%
            duration_seconds=3 * 60 * 60,
            stacking=EffectStacking.REPLACE,
        ),
    ),

    # 11) Instant stamina bigger hit (distinct from energy_drink)
    "energy_shot": ItemDef(
        key="energy_shot",
        name="Energy Shot",
        rarity=ItemRarity.RARE,
        price=1800,
        daily_limit=2,
        tradable=True,
        effect=EffectDef(
            effect_key="energy_instant_big",
            group_key="energy",
            payload={"stamina_add": 60},
        ),
    ),

    # 12) Payout bonus but charge-based (not timed) for N works
    "commission_card": ItemDef(
        key="commission_card",
        name="Commission Card",
        rarity=ItemRarity.RARE,
        price=2400,
        daily_limit=2,
        tradable=True,
        effect=EffectDef(
            effect_key="payout_bonus_charges",
            group_key="payout",
            payload={"payout_bonus_bp": 1500},  # +15%
            charges=15,
            stacking=EffectStacking.REPLACE,
        ),
    ),

    # 13) Job XP timed booster higher tier than wrist_wraps
    "tool_upgrade_kit": ItemDef(
        key="tool_upgrade_kit",
        name="Tool Upgrade Kit",
        rarity=ItemRarity.MYTHICAL,
        price=6800,
        daily_limit=1,
        tradable=True,
        effect=EffectDef(
            effect_key="job_xp_boost_myth",
            group_key="job_xp",
            payload={"job_xp_bonus_bp": 10000},  # +100%
            duration_seconds=30 * 60,
            stacking=EffectStacking.REPLACE,
        ),
    ),

    # 14) Energy cap booster weaker tier (exclusive group, non-myth)
    "compression_sleeve": ItemDef(
        key="compression_sleeve",
        name="Compression Sleeve",
        rarity=ItemRarity.RARE,
        price=3000,
        daily_limit=1,
        tradable=True,
        effect=EffectDef(
            effect_key="energy_cap_small",
            group_key="energy_cap",
            payload={"stamina_cap_add": 25},
            duration_seconds=45 * 60,
            stacking=EffectStacking.REPLACE,
        ),
    ),

    # 15) Silver bonus multiplier on next work only (distinct from flat tip_jar)
    "discount_coupon": ItemDef(
        key="discount_coupon",
        name="Discount Coupon",
        rarity=ItemRarity.UNCOMMON,
        price=800,
        daily_limit=3,
        tradable=True,
        effect=EffectDef(
            effect_key="next_work_multiplier",
            group_key="next_work_multiplier",
            payload={"next_work_silver_mult_bp": 2000},  # +20% to next work payout
            charges=1,
            stacking=EffectStacking.ADD,
        ),
    ),
    "uno_reverse_wallet": ItemDef(
        key="uno_reverse_wallet",
        name="Uno Reverse Wallet",
        rarity=ItemRarity.MYTHICAL,
        price=125000,
        daily_limit=1,
        tradable=False,
        effect=EffectDef(
            effect_key="uno_reverse_wallet",
            group_key="pickpocket_defense",
            payload={"pickpocket_reverse": 1},
            charges=1,
            stacking=EffectStacking.ADD,
        ),
        description="Get robbed? Cute. This wallet instantly robs them back.",
        inventory_description="One-use trap wallet. If someone successfully pickpockets you, it instantly steals back from them.",
    ),
}
