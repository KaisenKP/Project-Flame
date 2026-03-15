from __future__ import annotations

import random

from adventure.models.adventure_state import (
    AdventureClass,
    AdventureMode,
    PARTY_REWARD_BONUS_BP,
    PARTY_XP_BONUS_BP,
    StageTag,
    StageTemplate,
)
from adventure.models.player_runtime import PlayerRuntime
from adventure.models.rewards import AdventureRewards
from adventure.reward_engine import RewardEngine
from adventure.utils.rng import bonus_bp, fmt_int, roll_bp
from services.items_catalog import ITEMS


class StageResolver:
    def __init__(self, reward_engine: RewardEngine):
        self.reward_engine = reward_engine

    def resolve_stage(self, *, stage: StageTemplate, chosen: str, players: list[PlayerRuntime], mode: AdventureMode) -> tuple[AdventureRewards, list[str]]:
        party_size = len(players)
        adv_level = max(p.adventure_level for p in players)
        class_keys = [p.class_key for p in players]
        base_silver = random.randint(120, 260) + adv_level * random.randint(25, 45)
        base_xp = random.randint(40, 90) + adv_level * random.randint(10, 16)
        base_adv_xp = random.randint(35, 70) + adv_level * random.randint(8, 14)

        risk_bp = 1500
        if stage.tag in {StageTag.COMBAT, StageTag.BOSS, StageTag.TRAP}:
            risk_bp += 900
        if mode == AdventureMode.SOLO:
            risk_bp -= 450
        class_reward_bonus_bp = 0
        class_fail_reduce_bp = 0
        if AdventureClass.TREASURE_HUNTER.value in class_keys and stage.tag in {StageTag.TREASURE, StageTag.SOCIAL}:
            class_reward_bonus_bp += 1800
        if AdventureClass.ARCHMAGE.value in class_keys and stage.tag in {StageTag.MYSTIC, StageTag.PUZZLE}:
            class_reward_bonus_bp += 1600
        if AdventureClass.DRAGON_SLAYER.value in class_keys and stage.tag in {StageTag.COMBAT, StageTag.BOSS}:
            class_reward_bonus_bp += 1400
        if AdventureClass.BERSERKER.value in class_keys and stage.tag in {StageTag.COMBAT, StageTag.BOSS}:
            class_reward_bonus_bp += 2100
            risk_bp += 600
        if AdventureClass.STORM_KNIGHT.value in class_keys:
            class_fail_reduce_bp += 1200
        if AdventureClass.SHADOW_ASSASSIN.value in class_keys and stage.tag in {StageTag.TRAP, StageTag.SOCIAL}:
            class_fail_reduce_bp += 1000

        fail_chance_bp = max(600, risk_bp - class_fail_reduce_bp)
        rewards = AdventureRewards()
        lines: list[str] = []
        fail = roll_bp(fail_chance_bp)
        partial = (not fail) and roll_bp(2500)
        if fail:
            silver_loss = random.randint(20, 80)
            stamina_loss = random.randint(1, 4)
            if AdventureClass.STORM_KNIGHT.value in class_keys:
                silver_loss = max(5, silver_loss // 2)
                stamina_loss = max(1, stamina_loss - 1)
            rewards.silver -= silver_loss
            rewards.stamina_penalty += stamina_loss
            rewards.adventure_xp += max(6, base_adv_xp // 6)
            return rewards, ["💥 The plan implodes in spectacular fashion.", f"You lose **{fmt_int(silver_loss)} silver** and burn **{stamina_loss} stamina** escaping."]

        silver_gain = bonus_bp(base_silver, PARTY_REWARD_BONUS_BP.get(party_size, 0) + class_reward_bonus_bp)
        xp_gain = bonus_bp(base_xp, PARTY_XP_BONUS_BP.get(party_size, 0))
        adv_xp_gain = base_adv_xp
        lines.append("⚠️ Chaotic success. You survive, but it gets messy." if partial else "✅ The party executes the plan with questionable heroism.")
        if partial:
            silver_gain = max(20, silver_gain // 2)
            xp_gain = max(10, xp_gain // 2)
            adv_xp_gain = max(8, adv_xp_gain // 2)

        lines.extend(self._branch_lines(stage=stage, chosen=chosen, class_keys=class_keys))
        rewards.silver += silver_gain
        rewards.xp += xp_gain
        rewards.adventure_xp += adv_xp_gain
        if stage.tag in {StageTag.TREASURE, StageTag.MYSTIC, StageTag.PUZZLE} and roll_bp(3200):
            item_key = random.choice(["energy_drink", "protein_bar", "study_notes", "found_wallet", "training_manual", "caffeine_gum"])
            rewards.items[item_key] = rewards.items.get(item_key, 0) + 1
            lines.append(f"📦 Found item: **{ITEMS[item_key].name}**.")
        if AdventureClass.BEAST_TAMER.value in class_keys and roll_bp(1400):
            rewards.items["caffeine_gum"] = rewards.items.get("caffeine_gum", 0) + 1
            lines.append("🐺 A temporary animal companion drags bonus supplies to camp.")
        self.reward_engine.roll_lootbox(rewards=rewards, party_size=party_size, stage_tag=stage.tag, class_keys=class_keys)
        lines.append(f"Rewards: **+{fmt_int(silver_gain)} silver**, **+{fmt_int(xp_gain)} XP**, **+{fmt_int(adv_xp_gain)} Adventure XP**.")
        return rewards, lines

    def _branch_lines(self, *, stage: StageTemplate, chosen: str, class_keys: list[str]) -> list[str]:
        chosen_l = chosen.lower()
        if stage.key == "offended_bear":
            if "fight" in chosen_l:
                return ["Steel clashes with claws. Someone yells 'hit it with the plan!'", "The bear reconsiders its life choices and retreats into the trees."]
            if "climb" in chosen_l:
                return ["Everyone scrambles up branches with zero dignity.", "The bear huffs below, then steals your rations and leaves."]
            return ["You sprint as fast as fear allows.", "The bear gives chase, then gets distracted by a beehive."]
        if stage.tag == StageTag.MYSTIC:
            if AdventureClass.ARCHMAGE.value in class_keys:
                return ["Arcane glyphs unravel in your mind like a solved riddle."]
            return ["The strange symbols make sense... eventually."]
        if stage.tag == StageTag.PUZZLE:
            return ["Gears rotate, locks click, and the vault groans open a finger-width at a time."]
        if stage.tag == StageTag.BOSS:
            return ["The battlefield becomes pure chaos, but the party somehow holds the line."]
        return ["The situation shifts quickly, but luck stays on your side."]
