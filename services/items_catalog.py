# services/items_catalog.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional


class ItemRarity(str, Enum):
    COMMON = "common"
    UNCOMMON = "uncommon"
    RARE = "rare"
    EPIC = "epic"
    LEGENDARY = "legendary"
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
        description="A quick jolt that restores 25 stamina instantly.",
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
        description="Power snack: +10% work payout for 30 minutes.",
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
        description="Stay sharp and steady with 8% less fail chance for 45 minutes.",
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
        description="Tight form, fast gains: +25% Job XP for 30 minutes.",
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
        description="Push beyond your limit with +50 max stamina for 1 hour.",
    ),

    # -----------------
    # +15 new items (no redundancies)
    # Each item introduces a distinct modifier field or mechanic.
    # -----------------

    # 1) Burst payout window (scaled, non-flat)
    "found_wallet": ItemDef(
        key="found_wallet",
        name="Found Wallet",
        rarity=ItemRarity.UNCOMMON,
        price=900,
        daily_limit=2,
        tradable=True,
        effect=EffectDef(
            effect_key="burst_window",
            group_key="burst",
            payload={"burst_chance_bp": 3000, "burst_payout_bp": 2000},
            duration_seconds=20 * 60,
            stacking=EffectStacking.REFRESH,
        ),
        description="For 20 minutes, your work has a 30% chance to burst for +20% payout.",
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
            payload={"next_work_payout_bp": 1200},
            charges=1,
            stacking=EffectStacking.ADD,
        ),
        description="Pocket tips ready: your next /work pays 12% more.",
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
        description="Efficient fuel: your next 20 works cost 1 less stamina each.",
    ),

    # 4) Job level gain on next work
    "training_manual": ItemDef(
        key="training_manual",
        name="Training Manual",
        rarity=ItemRarity.RARE,
        price=2600,
        daily_limit=2,
        tradable=True,
        effect=EffectDef(
            effect_key="job_level_gain",
            group_key="job_level_gain",
            payload={"job_level_gain": 1},
            charges=1,
            stacking=EffectStacking.ADD,
        ),
        description="Cram once, level once: gain +1 job level on your next work result.",
    ),

    # 5) Converted from legacy user XP item -> payout utility
    "study_notes": ItemDef(
        key="study_notes",
        name="Study Notes",
        rarity=ItemRarity.UNCOMMON,
        price=1100,
        daily_limit=3,
        tradable=True,
        effect=EffectDef(
            effect_key="payout_training",
            group_key="payout",
            payload={"payout_bonus_bp": 900},
            duration_seconds=25 * 60,
            stacking=EffectStacking.REFRESH,
        ),
        description="Refresher notes that boost work payout by 9% for 25 minutes.",
    ),

    # 6) Converted from legacy user XP timed item -> OP mythical combo
    "study_sprint_timer": ItemDef(
        key="study_sprint_timer",
        name="Study Sprint Timer",
        rarity=ItemRarity.MYTHICAL,
        price=6500,
        daily_limit=1,
        tradable=True,
        effect=EffectDef(
            effect_key="mythic_sprint_mode",
            group_key="mythic_sprint",
            payload={
                "payout_bonus_bp": 8000,
                "job_xp_bonus_bp": 7000,
                "rare_find_bp": 2800,
                "extra_roll_bp": 2000,
                "protection_bp": 3500,
            },
            duration_seconds=30 * 60,
            stacking=EffectStacking.REPLACE,
        ),
        description="Mythic focus mode for 30 minutes: huge payout, XP, and rare-find boosts.",
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
        description="Fortune flips your way: +25% chance to double payout for 20 works.",
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
        description="Secure every move with 20% less fail chance for your next 25 works.",
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
        description="Hydrate up: +25% stamina regeneration for 2 hours.",
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
        description="Mythic recovery blend granting +50% stamina regeneration for 3 hours.",
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
        description="A concentrated kick that restores 60 stamina instantly.",
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
        description="Premium rates unlocked: +15% payout on your next 15 works.",
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
        description="Mastercraft upgrade that doubles Job XP gains for 30 minutes.",
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
        description="Support gear that raises your max stamina by 25 for 45 minutes.",
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
            payload={"next_work_payout_bp": 2000},
            charges=1,
            stacking=EffectStacking.ADD,
        ),
        description="Cashback special: your next /work payout is boosted by 20%.",
    ),
    "golden_contract": ItemDef(
        key="golden_contract",
        name="Golden Contract",
        rarity=ItemRarity.LEGENDARY,
        price=11500,
        daily_limit=1,
        tradable=True,
        effect=EffectDef(
            effect_key="legendary_combo_contract",
            group_key="combo",
            payload={"combo_payout_step_bp": 550, "combo_max_stacks": 6},
            duration_seconds=45 * 60,
            stacking=EffectStacking.REFRESH,
        ),
        description="Build momentum for 45 minutes with a stacking combo payout bonus.",
    ),
    "chaos_dice": ItemDef(
        key="chaos_dice",
        name="Chaos Dice",
        rarity=ItemRarity.EPIC,
        price=4200,
        daily_limit=2,
        tradable=True,
        effect=EffectDef(
            effect_key="greed_roll",
            group_key="greed",
            payload={"greed_payout_bp": 3500, "greed_fail_bp": 900},
            duration_seconds=30 * 60,
            stacking=EffectStacking.REFRESH,
        ),
        description="High-risk buff: +35% work payout, but +9% fail chance for 30 minutes.",
        inventory_description="Roll the dice for bigger checks. For 30 minutes, successful work pays 35% more, but your job fail chance rises by 9%.",
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
