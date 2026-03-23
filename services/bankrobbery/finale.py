from __future__ import annotations

import random
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from db.models import BankRobberyLobbyRow
from .catalog import BankApproach, CrewRole, FinaleOutcome, FinalePhase, get_template
from .events import choose_weighted_event
from .prep import prep_summary


@dataclass(frozen=True)
class PhaseResult:
    phase: FinalePhase
    title: str
    description: str
    event_name: str | None
    delta_alert: int
    secured_cash: int
    done: bool = False


@dataclass(frozen=True)
class OutcomePayload:
    outcome: FinaleOutcome
    gross_take: int
    secured_take: int
    final_take: int
    heat_gain: int
    rep_gain: int
    bonus_rewards: dict[str, int]
    splits: dict[int, int]
    role_xp: dict[int, dict[str, int]]


ROLE_BP = {
    CrewRole.LEADER.value: {"entry": 400, "vault": 500, "loot": 300, "escape": 300},
    CrewRole.HACKER.value: {"entry": 200, "vault": 1400, "loot": 250, "escape": 100},
    CrewRole.DRIVER.value: {"entry": 100, "vault": 0, "loot": 150, "escape": 1400},
    CrewRole.ENFORCER.value: {"entry": 150, "vault": 150, "loot": 400, "escape": 900},
    CrewRole.FLEX.value: {"entry": 200, "vault": 200, "loot": 200, "escape": 200},
    CrewRole.UNASSIGNED.value: {"entry": 0, "vault": 0, "loot": 0, "escape": 0},
}


def _clamp(value: int, lo: int, hi: int) -> int:
    return max(lo, min(int(value), hi))


def _crew_bp(members: list, phase: str, solo: bool) -> int:
    total = 0
    for member in members:
        total += int(ROLE_BP.get(member.role, ROLE_BP[CrewRole.UNASSIGNED.value]).get(phase, 0))
    if solo:
        total = int(total * 0.7)
    return total


async def _ctx(session: AsyncSession, lobby: BankRobberyLobbyRow):
    from .lobby import list_participants

    members = await list_participants(session, lobby_id=lobby.id)
    template = get_template(lobby.robbery_id)
    approach = BankApproach(lobby.approach)
    prep_rows, prep_effects = await prep_summary(session, lobby)
    rng = random.Random(int(lobby.rng_seed) + int((lobby.state_json or {}).get("loot_round", 0)) + len((lobby.state_json or {}).get("timeline", [])))
    return template, approach, members, prep_rows, prep_effects, rng


async def run_entry(session: AsyncSession, lobby: BankRobberyLobbyRow) -> PhaseResult:
    state = dict(lobby.state_json or {})
    template, approach, members, _, prep_effects, rng = await _ctx(session, lobby)
    profile = template.approach_modifiers[approach]
    base = profile.entry_bp + _crew_bp(members, "entry", len(members) == 1) + prep_effects.get("entry_bp", 0)
    event = choose_weighted_event(rng=rng, phase=FinalePhase.ENTRY, approach=approach, positive_bias=profile.event_bias.get("positive", 0), negative_bias=profile.event_bias.get("negative", 0), excluded=state.get("seen_events", []))
    if event:
        base += int(event.effects.get("entry_delta", 0) * 100)
    entry_roll = rng.randint(7200, 13200)
    alert = max(0, int(state.get("alert", 0)) + int(event.effects.get("alert_delta", 0) if event else 0))
    outcome = "clean" if base >= entry_roll + 600 else "rough" if base >= entry_roll - 500 else "compromised"
    if outcome == "clean":
        alert = max(0, alert - 4)
    elif outcome == "compromised":
        alert += 14
    state["entry_state"] = outcome
    state["alert"] = alert
    state.setdefault("seen_events", []).append(event.key if event else "none")
    state.setdefault("timeline", []).append({"phase": "entry", "text": f"Entry result: {outcome}."})
    lobby.current_phase = FinalePhase.VAULT.value
    lobby.state_json = state
    return PhaseResult(phase=FinalePhase.ENTRY, title="Entry", description=f"The crew enters {outcome}. Threshold {base:,} vs {entry_roll:,}.", event_name=event.name if event else None, delta_alert=alert - int((lobby.state_json or {}).get("alert", 0)), secured_cash=int(state.get("secured_cash", 0)))


async def run_vault(session: AsyncSession, lobby: BankRobberyLobbyRow) -> PhaseResult:
    state = dict(lobby.state_json or {})
    template, approach, members, _, prep_effects, rng = await _ctx(session, lobby)
    profile = template.approach_modifiers[approach]
    base = profile.vault_bp + _crew_bp(members, "vault", len(members) == 1) + prep_effects.get("vault_bp", 0)
    event = choose_weighted_event(rng=rng, phase=FinalePhase.VAULT, approach=approach, positive_bias=profile.event_bias.get("positive", 0), negative_bias=profile.event_bias.get("negative", 0), excluded=state.get("seen_events", []))
    vault_delta = 0
    alert_delta = 0
    if event:
        vault_delta += int(event.effects.get("vault_delta", 0))
        alert_delta += int(event.effects.get("alert_delta", 0))
    progress_gain = _clamp((base - rng.randint(7800, 12400)) // 120 + 22 + vault_delta, 8, 42)
    state["vault_progress"] = int(state.get("vault_progress", 0)) + progress_gain
    state["alert"] = max(0, int(state.get("alert", 0)) + alert_delta)
    rounds = 2
    if state["vault_progress"] >= 32:
        rounds = 3
    if state["vault_progress"] >= 48:
        rounds = 4
    if template.robbery_id in {"bullion_exchange", "national_mint"}:
        rounds = max(2, rounds - 1)
    state["loot_rounds_total"] = rounds
    state.setdefault("timeline", []).append({"phase": "vault", "text": f"Vault access secured {rounds} loot rounds."})
    lobby.current_phase = FinalePhase.LOOT.value
    lobby.state_json = state
    return PhaseResult(phase=FinalePhase.VAULT, title="Vault Access", description=f"Vault progress gained: {progress_gain}. Loot rounds opened: {rounds}.", event_name=event.name if event else None, delta_alert=alert_delta, secured_cash=int(state.get("secured_cash", 0)))


async def run_loot_round(session: AsyncSession, lobby: BankRobberyLobbyRow, *, push: bool) -> PhaseResult:
    state = dict(lobby.state_json or {})
    template, approach, members, _, prep_effects, rng = await _ctx(session, lobby)
    profile = template.approach_modifiers[approach]
    if not push:
        lobby.current_phase = FinalePhase.ESCAPE.value
        state.setdefault("timeline", []).append({"phase": "loot", "text": "Crew banks the current take and moves to escape."})
        lobby.state_json = state
        return PhaseResult(phase=FinalePhase.LOOT, title="Loot Window", description="The crew walks with the secured take and transitions to escape.", event_name=None, delta_alert=0, secured_cash=int(state.get("secured_cash", 0)))
    current_round = int(state.get("loot_round", 0)) + 1
    state["loot_round"] = current_round
    total_rounds = int(state.get("loot_rounds_total", 2))
    quality_span = template.payout_max - template.payout_min
    per_round = template.payout_min // max(total_rounds, 1)
    per_round += quality_span // max(total_rounds + 1, 1)
    base = per_round * profile.payout_mult_bp // 10000
    base = base * (10000 + _crew_bp(members, "loot", len(members) == 1) + prep_effects.get("loot_bp", 0)) // 10000
    event = choose_weighted_event(rng=rng, phase=FinalePhase.LOOT, approach=approach, positive_bias=profile.event_bias.get("positive", 0), negative_bias=profile.event_bias.get("negative", 0), excluded=state.get("seen_events", []))
    alert_delta = profile.alert_per_round + max(0, current_round - 1) * 3 - prep_effects.get("alert_per_round", 0)
    if event:
        alert_delta += int(event.effects.get("alert_delta", 0))
        base = base * int(event.effects.get("loot_mult_bp", 10000)) // 10000
        base += int(event.effects.get("loot_flat", 0))
    if current_round > total_rounds:
        base = base * 78 // 100
        alert_delta += 8 + (current_round - total_rounds) * 4
    state["secured_cash"] = int(state.get("secured_cash", 0)) + max(base, 0)
    state["gross_cash"] = max(int(state.get("gross_cash", 0)), int(state["secured_cash"]))
    state["alert"] = max(0, int(state.get("alert", 0)) + alert_delta)
    state.setdefault("seen_events", []).append(event.key if event else f"loot_{current_round}")
    state.setdefault("timeline", []).append({"phase": "loot", "text": f"Round {current_round}: secured {base:,}."})
    if state["alert"] >= 100:
        lobby.current_phase = FinalePhase.ESCAPE.value
        state["escape_forced"] = True
    lobby.state_json = state
    return PhaseResult(phase=FinalePhase.LOOT, title=f"Loot Round {current_round}", description=f"The crew stuffs another haul and secures {base:,} Silver gross. Greed pressure rises.", event_name=event.name if event else None, delta_alert=alert_delta, secured_cash=int(state.get("secured_cash", 0)), done=lobby.current_phase == FinalePhase.ESCAPE.value)


async def use_override(session: AsyncSession, lobby: BankRobberyLobbyRow) -> bool:
    state = dict(lobby.state_json or {})
    if state.get("override_used"):
        return False
    state["override_used"] = True
    state["alert"] = max(0, int(state.get("alert", 0)) - 10)
    state.setdefault("timeline", []).append({"phase": lobby.current_phase, "text": "Leader Override trims the pressure and stabilizes the run."})
    lobby.state_json = state
    return True


async def run_escape(session: AsyncSession, lobby: BankRobberyLobbyRow) -> PhaseResult:
    state = dict(lobby.state_json or {})
    template, approach, members, _, prep_effects, rng = await _ctx(session, lobby)
    profile = template.approach_modifiers[approach]
    base = profile.escape_bp + _crew_bp(members, "escape", len(members) == 1) + prep_effects.get("escape_bp", 0)
    event = choose_weighted_event(rng=rng, phase=FinalePhase.ESCAPE, approach=approach, positive_bias=profile.event_bias.get("positive", 0), negative_bias=profile.event_bias.get("negative", 0), excluded=state.get("seen_events", []))
    if event:
        base += int(event.effects.get("escape_delta", 0) * 100)
    pressure = int(state.get("alert", 0)) * 90
    if template.robbery_id in {"pacific_dominion", "bullion_exchange", "national_mint"}:
        pressure += int(state.get("loot_round", 0)) * 120
    score = base - pressure
    secured = int(state.get("secured_cash", 0))
    loss_bp = 0
    escape_state = "clean"
    if score < 1600:
        escape_state = "collapse"
        loss_bp = 7000
    elif score < 3200:
        escape_state = "shaky"
        loss_bp = 3400
    elif score < 5200:
        escape_state = "rough"
        loss_bp = 1500
    if event:
        loss_bp += int(event.effects.get("loot_loss_bp", 0))
    loss_bp = max(0, loss_bp - prep_effects.get("loot_loss_reduction_bp", 0))
    final_secured = max(0, secured * (10000 - loss_bp) // 10000)
    state["escape_state"] = escape_state
    state["secured_cash"] = final_secured
    state.setdefault("timeline", []).append({"phase": "escape", "text": f"Escape resolves {escape_state}; retained {final_secured:,}."})
    lobby.current_phase = FinalePhase.RESULTS.value
    lobby.state_json = state
    return PhaseResult(phase=FinalePhase.ESCAPE, title="Escape", description=f"The getaway runs {escape_state}. Retained {final_secured:,} after pressure and route losses.", event_name=event.name if event else None, delta_alert=int(event.effects.get("alert_delta", 0) if event else 0), secured_cash=final_secured, done=True)


async def calculate_outcome(session: AsyncSession, lobby: BankRobberyLobbyRow) -> OutcomePayload:
    from .lobby import list_participants
    from .heat import heat_penalty_multiplier

    members = await list_participants(session, lobby_id=lobby.id)
    template = get_template(lobby.robbery_id)
    state = dict(lobby.state_json or {})
    secured = int(state.get("secured_cash", 0))
    gross = max(secured, int(state.get("gross_cash", 0)))
    alert = int(state.get("alert", 0))
    entry_state = state.get("entry_state", "compromised")
    escape_state = state.get("escape_state", "collapse")
    if secured <= 0:
        outcome = FinaleOutcome.FULL_FAILURE
    elif escape_state == "clean" and entry_state == "clean" and alert < 35:
        outcome = FinaleOutcome.CLEAN_SUCCESS
    elif escape_state in {"clean", "rough"} and secured >= template.payout_min:
        outcome = FinaleOutcome.MESSY_SUCCESS
    elif escape_state == "collapse":
        outcome = FinaleOutcome.FAILED_ESCAPE if secured > 0 else FinaleOutcome.FULL_FAILURE
    else:
        outcome = FinaleOutcome.PARTIAL_SUCCESS
    final_take = secured
    if outcome == FinaleOutcome.CLEAN_SUCCESS:
        final_take = final_take * 108 // 100
    elif outcome == FinaleOutcome.FULL_FAILURE:
        final_take = final_take * 10 // 100
    elif outcome == FinaleOutcome.FAILED_ESCAPE:
        final_take = final_take * 55 // 100
    rep_gain = {
        FinaleOutcome.CLEAN_SUCCESS: 48,
        FinaleOutcome.MESSY_SUCCESS: 34,
        FinaleOutcome.PARTIAL_SUCCESS: 20,
        FinaleOutcome.FAILED_ESCAPE: 10,
        FinaleOutcome.FULL_FAILURE: 4,
    }[outcome] + len(members) * 2
    heat_gain = template.heat_gain + max(0, alert // 14)
    if outcome == FinaleOutcome.CLEAN_SUCCESS:
        heat_gain = max(4, heat_gain - 6)
    bonus_rewards: dict[str, int] = {}
    if template.tier.value in {"high", "endgame"} and outcome in {FinaleOutcome.CLEAN_SUCCESS, FinaleOutcome.MESSY_SUCCESS}:
        bonus_rewards["diamonds"] = 4 if template.tier.value == "high" else 12
    if template.robbery_id == "national_mint" and outcome == FinaleOutcome.CLEAN_SUCCESS:
        bonus_rewards["crowns"] = 2
    if outcome == FinaleOutcome.CLEAN_SUCCESS and alert == 0:
        bonus_rewards["lootboxes_epic"] = 1
    elif outcome in {FinaleOutcome.CLEAN_SUCCESS, FinaleOutcome.MESSY_SUCCESS} and random.Random(int(lobby.rng_seed) + secured).randint(1, 100) <= 18:
        bonus_rewards["lootboxes_rare"] = 1
    splits = {int(member.user_id): final_take * int(member.cut_percent) // 100 for member in members}
    role_xp: dict[int, dict[str, int]] = {}
    for member in members:
        xp = 16 + (8 if outcome in {FinaleOutcome.CLEAN_SUCCESS, FinaleOutcome.MESSY_SUCCESS} else 2)
        role_xp[int(member.user_id)] = {member.role: xp}
    return OutcomePayload(outcome=outcome, gross_take=gross, secured_take=secured, final_take=final_take, heat_gain=heat_gain, rep_gain=rep_gain, bonus_rewards=bonus_rewards, splits=splits, role_xp=role_xp)
