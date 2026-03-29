from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from services.jobs_core import apply_bp, clamp_int, roll_bp
from services.work_drops import _add_item, _add_lootbox

TOOL_EFFECT_CONFIG = {
    "double_action_chance_bp": 300,
    "double_action_payout_share_bp": 5_000,
    "double_action_job_xp_share_bp": 5_000,
    "critical_find_chance_bp": 500,
    "critical_find_silver_bonus_bp": 2_500,
    "critical_find_job_xp_bonus_bp": 2_500,
    "critical_find_silver_weight": 70,
    "critical_find_job_xp_weight": 20,
    "critical_find_lootbox_item_weight": 8,
    "critical_find_rare_item_weight": 2,
    "xp_burst_chance_bp": 600,
    "xp_burst_bonus_bp": 2_500,
    "failure_negation_chance_bp": 300,
    "rare_event_trigger_chance_bp": 200,
    "third_tool_no_stamina_cap_bp": 9_500,
}

TOOL_PROC_MESSAGES = {
    "no_stamina": "🔥 Tool Proc! No stamina consumed.",
    "double_action": "⚡ Tool Proc! Extra work action triggered.",
    "critical_silver": "💰 Critical Find! Bonus Silver found.",
    "critical_job_xp": "📘 Critical Find! Bonus job XP found.",
    "critical_lootbox": "🎁 Critical Find! You found a lootbox.",
    "critical_rare_item": "💎 Critical Find! You found a rare item.",
    "failure_negation": "🛡️ Tool Proc! Failure negated.",
    "xp_burst": "✨ XP Burst! Bonus job XP granted.",
    "rare_trigger": "🌌 Rare Trigger! A special event stirs.",
}


@dataclass(frozen=True)
class ToolPathState:
    is_default: bool
    is_middle: bool
    is_third: bool


@dataclass
class ToolProcOutcome:
    skipped_stamina: bool = False
    failure_negated: bool = False
    xp_burst_bonus: int = 0
    critical_silver_bonus: int = 0
    critical_job_xp_bonus: int = 0
    critical_lootbox: bool = False
    critical_rare_item: bool = False
    rare_event_triggered: bool = False
    double_action_payout: int = 0
    double_action_job_xp: int = 0
    messages: list[str] = field(default_factory=list)

    def add_message(self, text: str) -> None:
        if text and text not in self.messages:
            self.messages.append(text)


def third_tool_no_stamina_chance_bp(level: int) -> int:
    lvl = max(int(level), 0)
    if lvl <= 100:
        chance_pct = 5.0 + (lvl * 0.55)
    elif lvl <= 200:
        chance_pct = 60.0 + ((lvl - 100) * 0.20)
    elif lvl <= 300:
        chance_pct = 80.0 + ((lvl - 200) * 0.10)
    else:
        chance_pct = 90.0 + ((lvl - 300) * 0.01)
    chance_bp = int(round(chance_pct * 100.0))
    return clamp_int(chance_bp, 0, int(TOOL_EFFECT_CONFIG["third_tool_no_stamina_cap_bp"]))


def resolve_tool_path(*, tool_index: Optional[int]) -> ToolPathState:
    idx = int(tool_index) if tool_index is not None else -1
    return ToolPathState(
        is_default=idx == 0,
        is_middle=idx == 1,
        is_third=idx == 2,
    )


class WorkToolProcResolver:
    @staticmethod
    def roll_no_stamina(*, tool_path: ToolPathState, no_stamina_chance_bp: int) -> ToolProcOutcome:
        out = ToolProcOutcome()
        if tool_path.is_third and int(no_stamina_chance_bp) > 0 and roll_bp(int(no_stamina_chance_bp)):
            out.skipped_stamina = True
            out.add_message(TOOL_PROC_MESSAGES["no_stamina"])
        return out

    @staticmethod
    def roll_failure_negation(*, tool_path: ToolPathState, failed: bool) -> ToolProcOutcome:
        out = ToolProcOutcome()
        if tool_path.is_third and failed and roll_bp(int(TOOL_EFFECT_CONFIG["failure_negation_chance_bp"])):
            out.failure_negated = True
            out.add_message(TOOL_PROC_MESSAGES["failure_negation"])
        return out

    @staticmethod
    async def apply_success_effects(
        session: AsyncSession,
        *,
        guild_id: int,
        user_id: int,
        tool_path: ToolPathState,
        payout: int,
        job_xp: int,
    ) -> ToolProcOutcome:
        out = ToolProcOutcome()
        if not (tool_path.is_middle or tool_path.is_third):
            return out

        if roll_bp(int(TOOL_EFFECT_CONFIG["xp_burst_chance_bp"])):
            out.xp_burst_bonus = max(apply_bp(int(job_xp), int(TOOL_EFFECT_CONFIG["xp_burst_bonus_bp"])), 0)
            out.add_message(TOOL_PROC_MESSAGES["xp_burst"])

        if roll_bp(int(TOOL_EFFECT_CONFIG["critical_find_chance_bp"])):
            total_weight = (
                int(TOOL_EFFECT_CONFIG["critical_find_silver_weight"])
                + int(TOOL_EFFECT_CONFIG["critical_find_job_xp_weight"])
                + int(TOOL_EFFECT_CONFIG["critical_find_lootbox_item_weight"])
                + int(TOOL_EFFECT_CONFIG["critical_find_rare_item_weight"])
            )
            pick = random.randint(1, total_weight)
            silver_cut = int(TOOL_EFFECT_CONFIG["critical_find_silver_weight"])
            xp_cut = silver_cut + int(TOOL_EFFECT_CONFIG["critical_find_job_xp_weight"])
            lootbox_cut = xp_cut + int(TOOL_EFFECT_CONFIG["critical_find_lootbox_item_weight"])
            if pick <= silver_cut:
                out.critical_silver_bonus = max(apply_bp(int(payout), int(TOOL_EFFECT_CONFIG["critical_find_silver_bonus_bp"])), 0)
                out.add_message(TOOL_PROC_MESSAGES["critical_silver"])
            elif pick <= xp_cut:
                out.critical_job_xp_bonus = max(apply_bp(int(job_xp), int(TOOL_EFFECT_CONFIG["critical_find_job_xp_bonus_bp"])), 0)
                out.add_message(TOOL_PROC_MESSAGES["critical_job_xp"])
            elif pick <= lootbox_cut:
                await _add_lootbox(session, guild_id=guild_id, user_id=user_id, rarity="common")
                out.critical_lootbox = True
                out.add_message(TOOL_PROC_MESSAGES["critical_lootbox"])
            else:
                await _add_item(session, guild_id=guild_id, user_id=user_id, item_key="tool_upgrade_kit")
                out.critical_rare_item = True
                out.add_message(TOOL_PROC_MESSAGES["critical_rare_item"])

        if tool_path.is_third and roll_bp(int(TOOL_EFFECT_CONFIG["rare_event_trigger_chance_bp"])):
            out.rare_event_triggered = True
            out.add_message(TOOL_PROC_MESSAGES["rare_trigger"])

        if roll_bp(int(TOOL_EFFECT_CONFIG["double_action_chance_bp"])):
            out.double_action_payout = max(apply_bp(int(payout), int(TOOL_EFFECT_CONFIG["double_action_payout_share_bp"])), 0)
            out.double_action_job_xp = max(apply_bp(int(job_xp), int(TOOL_EFFECT_CONFIG["double_action_job_xp_share_bp"])), 0)
            out.add_message(TOOL_PROC_MESSAGES["double_action"])

        return out
