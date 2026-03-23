from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import BankRobberyCooldownRow, BankRobberyLobbyRow, BankRobberyParticipantRow
from services.users import ensure_user_rows
from .catalog import BankApproach, CrewRole, get_template
from .heat import can_enter
from .prep import ensure_prep_rows
from .progression import get_or_create_profile, utc_now
from .rewards import get_or_create_wallet


def _default_state(template_id: str, approach: BankApproach) -> dict:
    return {
        "robbery_id": template_id,
        "approach": approach.value,
        "alert": 0,
        "secured_cash": 0,
        "gross_cash": 0,
        "vault_progress": 0,
        "entry_state": "pending",
        "escape_state": "pending",
        "active_modifiers": [],
        "timeline": [],
        "hidden_events": [],
        "loot_round": 0,
        "override_used": False,
        "results_applied": False,
        "finalized": False,
    }


async def get_active_lobby_for_user(session: AsyncSession, *, guild_id: int, user_id: int) -> BankRobberyLobbyRow | None:
    participant = await session.scalar(select(BankRobberyParticipantRow).where(BankRobberyParticipantRow.guild_id == guild_id, BankRobberyParticipantRow.user_id == user_id))
    if participant is None:
        return None
    return await session.get(BankRobberyLobbyRow, participant.lobby_id)


async def create_lobby(session: AsyncSession, *, guild_id: int, leader_user_id: int, robbery_id: str, approach: BankApproach) -> BankRobberyLobbyRow:
    template = get_template(robbery_id)
    await ensure_user_rows(session, guild_id=guild_id, user_id=leader_user_id)
    profile = await get_or_create_profile(session, guild_id=guild_id, user_id=leader_user_id)
    ok, reason = can_enter(personal_heat=int(profile.personal_heat), robbery_heat=template.heat_gain)
    if not ok:
        raise ValueError(reason or "heat_locked")
    if int(profile.heist_rep) < int(template.recommended_rep):
        raise ValueError(f"Need {template.recommended_rep:,} Heist Rep for this target.")
    existing = await get_active_lobby_for_user(session, guild_id=guild_id, user_id=leader_user_id)
    if existing is not None:
        raise ValueError("You already have an active Bank Robbery lobby.")
    wallet = await get_or_create_wallet(session, guild_id=guild_id, user_id=leader_user_id)
    if int(wallet.silver) < int(template.entry_cost):
        raise ValueError(f"Need {template.entry_cost:,} Silver entry capital.")
    wallet.silver -= int(template.entry_cost)
    wallet.silver_spent += int(template.entry_cost)
    cooldown = await session.scalar(select(BankRobberyCooldownRow).where(BankRobberyCooldownRow.guild_id == guild_id, BankRobberyCooldownRow.user_id == leader_user_id, BankRobberyCooldownRow.robbery_id == robbery_id, BankRobberyCooldownRow.ends_at > utc_now()))
    if cooldown is not None:
        raise ValueError("This target is still on cooldown for you.")
    lobby = BankRobberyLobbyRow(
        guild_id=guild_id,
        leader_user_id=leader_user_id,
        robbery_id=robbery_id,
        approach=approach.value,
        stage="lobby",
        status="open",
        current_phase="entry",
        entry_cost_paid=template.entry_cost,
        rng_seed=random.randint(1, 2_000_000_000),
        state_json=_default_state(robbery_id, approach),
    )
    session.add(lobby)
    await session.flush()
    participant = BankRobberyParticipantRow(lobby_id=lobby.id, guild_id=guild_id, user_id=leader_user_id, role=CrewRole.LEADER.value, cut_percent=100 if template.crew_min == 1 else 40, ready=True, confirmed_cuts=template.crew_min == 1)
    session.add(participant)
    await session.flush()
    await ensure_prep_rows(session, lobby)
    return lobby


async def join_lobby(session: AsyncSession, *, guild_id: int, user_id: int, leader_user_id: int) -> BankRobberyLobbyRow:
    if await get_active_lobby_for_user(session, guild_id=guild_id, user_id=user_id):
        raise ValueError("You are already in a Bank Robbery lobby.")
    lobby = await session.scalar(select(BankRobberyLobbyRow).where(BankRobberyLobbyRow.guild_id == guild_id, BankRobberyLobbyRow.leader_user_id == leader_user_id, BankRobberyLobbyRow.status == "open"))
    if lobby is None:
        raise ValueError("No open lobby found for that leader.")
    template = get_template(lobby.robbery_id)
    count = await session.execute(select(BankRobberyParticipantRow).where(BankRobberyParticipantRow.lobby_id == lobby.id))
    members = list(count.scalars())
    if len(members) >= template.crew_max:
        raise ValueError("That crew is already full.")
    session.add(BankRobberyParticipantRow(lobby_id=lobby.id, guild_id=guild_id, user_id=user_id, role=CrewRole.UNASSIGNED.value, cut_percent=max(0, (100 - sum(int(m.cut_percent) for m in members)) // 2), ready=False, confirmed_cuts=False))
    return lobby


async def leave_lobby(session: AsyncSession, *, guild_id: int, user_id: int) -> tuple[BankRobberyLobbyRow | None, bool]:
    participant = await session.scalar(select(BankRobberyParticipantRow).where(BankRobberyParticipantRow.guild_id == guild_id, BankRobberyParticipantRow.user_id == user_id))
    if participant is None:
        return None, False
    lobby = await session.get(BankRobberyLobbyRow, participant.lobby_id)
    if lobby is None:
        return None, False
    is_leader = int(lobby.leader_user_id) == int(user_id)
    if is_leader:
        await session.execute(delete(BankRobberyParticipantRow).where(BankRobberyParticipantRow.lobby_id == lobby.id))
        await session.delete(lobby)
        return lobby, True
    await session.delete(participant)
    return lobby, False


async def assign_role(session: AsyncSession, *, lobby: BankRobberyLobbyRow, actor_user_id: int, target_user_id: int, role: CrewRole) -> None:
    if int(lobby.leader_user_id) != int(actor_user_id):
        raise ValueError("Only the leader can assign roles.")
    participant = await session.scalar(select(BankRobberyParticipantRow).where(BankRobberyParticipantRow.lobby_id == lobby.id, BankRobberyParticipantRow.user_id == target_user_id))
    if participant is None:
        raise ValueError("That player is not in the crew.")
    if role != CrewRole.UNASSIGNED:
        existing = await session.scalar(select(BankRobberyParticipantRow).where(BankRobberyParticipantRow.lobby_id == lobby.id, BankRobberyParticipantRow.role == role.value, BankRobberyParticipantRow.user_id != target_user_id))
        if existing is not None:
            raise ValueError(f"{role.value.title()} is already assigned.")
    participant.role = role.value


async def set_ready(session: AsyncSession, *, guild_id: int, user_id: int, ready: bool) -> BankRobberyLobbyRow:
    participant = await session.scalar(select(BankRobberyParticipantRow).where(BankRobberyParticipantRow.guild_id == guild_id, BankRobberyParticipantRow.user_id == user_id))
    if participant is None:
        raise ValueError("You are not in a robbery crew.")
    participant.ready = bool(ready)
    return await session.get(BankRobberyLobbyRow, participant.lobby_id)


async def set_cuts(session: AsyncSession, *, lobby: BankRobberyLobbyRow, actor_user_id: int, cuts: dict[int, int]) -> None:
    if int(lobby.leader_user_id) != int(actor_user_id):
        raise ValueError("Only the leader can set cuts.")
    if lobby.locked_cuts:
        raise ValueError("Cuts are already locked for this finale.")
    members = list((await session.execute(select(BankRobberyParticipantRow).where(BankRobberyParticipantRow.lobby_id == lobby.id))).scalars())
    total = sum(int(v) for v in cuts.values())
    if total != 100:
        raise ValueError("Cuts must total exactly 100.")
    member_ids = {int(m.user_id) for m in members}
    if set(cuts) != member_ids:
        raise ValueError("Cuts must include every active crew member exactly once.")
    for member in members:
        member.cut_percent = int(cuts[int(member.user_id)])
        member.confirmed_cuts = int(member.user_id) == int(actor_user_id)


async def confirm_cuts(session: AsyncSession, *, guild_id: int, user_id: int) -> BankRobberyLobbyRow:
    participant = await session.scalar(select(BankRobberyParticipantRow).where(BankRobberyParticipantRow.guild_id == guild_id, BankRobberyParticipantRow.user_id == user_id))
    if participant is None:
        raise ValueError("You are not in a robbery crew.")
    participant.confirmed_cuts = True
    return await session.get(BankRobberyLobbyRow, participant.lobby_id)


async def list_participants(session: AsyncSession, *, lobby_id: int) -> list[BankRobberyParticipantRow]:
    result = await session.execute(select(BankRobberyParticipantRow).where(BankRobberyParticipantRow.lobby_id == lobby_id).order_by(BankRobberyParticipantRow.joined_at.asc()))
    return list(result.scalars())


async def validate_launch(session: AsyncSession, *, lobby: BankRobberyLobbyRow) -> list[str]:
    template = get_template(lobby.robbery_id)
    members = await list_participants(session, lobby_id=lobby.id)
    problems: list[str] = []
    if len(members) < template.crew_min:
        problems.append(f"Need at least {template.crew_min} crew members.")
    if len(members) > template.crew_max:
        problems.append(f"Crew exceeds max size of {template.crew_max}.")
    if len(members) == 1 and not template.solo_allowed:
        problems.append("Solo is only allowed for Corner Branch Job.")
    if any(not m.ready for m in members):
        problems.append("Every crew member must be marked ready.")
    if any(not m.confirmed_cuts for m in members):
        problems.append("Every crew member must confirm the cut split.")
    rows = await ensure_prep_rows(session, lobby)
    completed = sum(1 for row in rows if row.completed)
    if completed < template.prep_count:
        problems.append(f"Need at least {template.prep_count} completed prep jobs.")
    roles = {m.role for m in members}
    if CrewRole.LEADER.value not in roles:
        problems.append("Leader role is required.")
    return problems


async def start_finale(session: AsyncSession, *, lobby: BankRobberyLobbyRow, actor_user_id: int) -> None:
    if int(lobby.leader_user_id) != int(actor_user_id):
        raise ValueError("Only the leader can launch the finale.")
    problems = await validate_launch(session, lobby=lobby)
    if problems:
        raise ValueError(" ".join(problems))
    lobby.stage = "finale"
    lobby.status = "active"
    lobby.locked_cuts = True
    lobby.finale_started_at = utc_now()
    state = dict(lobby.state_json or {})
    state["timeline"] = list(state.get("timeline", []))
    state["timeline"].append({"at": utc_now().isoformat(), "text": "Finale launched."})
    lobby.state_json = state


async def finalize_lobby(session: AsyncSession, *, lobby: BankRobberyLobbyRow) -> None:
    members = await list_participants(session, lobby_id=lobby.id)
    template = get_template(lobby.robbery_id)
    for member in members:
        ends_at = utc_now() + timedelta(seconds=template.cooldown_seconds)
        row = await session.scalar(select(BankRobberyCooldownRow).where(BankRobberyCooldownRow.guild_id == lobby.guild_id, BankRobberyCooldownRow.user_id == member.user_id, BankRobberyCooldownRow.robbery_id == lobby.robbery_id))
        if row is None:
            row = BankRobberyCooldownRow(guild_id=lobby.guild_id, user_id=member.user_id, robbery_id=lobby.robbery_id, ends_at=ends_at, weekly_lockout_key="weekly" if template.weekly_lockout else None)
            session.add(row)
        else:
            row.ends_at = ends_at
            row.weekly_lockout_key = "weekly" if template.weekly_lockout else None
    lobby.status = "completed"
    lobby.stage = "results"
    lobby.completed_at = utc_now()
    await session.flush()
    await session.execute(delete(BankRobberyParticipantRow).where(BankRobberyParticipantRow.lobby_id == lobby.id))
