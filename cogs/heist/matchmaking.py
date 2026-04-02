from __future__ import annotations

import random

from db.models import BankRobberyLobbyRow, BankRobberyParticipantRow
from services.bankrobbery.rewards import get_or_create_wallet
from services.users import ensure_user_rows

from .catalog import HeistTarget
from .domain import assign_roles
from .repo import get_lobby_members, get_user_lobby


def _base_state(target_key: str) -> dict:
    return {"target_key": target_key, "results": None}


async def create_crew(session, *, guild_id: int, leader_id: int, target: HeistTarget) -> BankRobberyLobbyRow:
    existing = await get_user_lobby(session, guild_id=guild_id, user_id=leader_id)
    if existing is not None:
        raise ValueError("You are already in a crew.")
    await ensure_user_rows(session, guild_id=guild_id, user_id=leader_id)
    wallet = await get_or_create_wallet(session, guild_id=guild_id, user_id=leader_id)
    if int(wallet.silver) < target.entry_cost:
        raise ValueError(f"Need {target.entry_cost:,} Silver entry cost.")
    wallet.silver -= target.entry_cost
    wallet.silver_spent += target.entry_cost
    lobby = BankRobberyLobbyRow(
        guild_id=guild_id,
        leader_user_id=leader_id,
        robbery_id=target.key,
        approach="auto",
        stage="lobby",
        status="open",
        current_phase="entry",
        entry_cost_paid=target.entry_cost,
        rng_seed=random.randint(1, 2_000_000_000),
        state_json=_base_state(target.key),
    )
    session.add(lobby)
    await session.flush()
    session.add(BankRobberyParticipantRow(lobby_id=lobby.id, guild_id=guild_id, user_id=leader_id, role="ghost", ready=False, cut_percent=0, confirmed_cuts=True))
    await session.flush()
    return lobby


async def join_crew(session, *, guild_id: int, user_id: int, lobby: BankRobberyLobbyRow, target: HeistTarget) -> None:
    if await get_user_lobby(session, guild_id=guild_id, user_id=user_id):
        raise ValueError("You are already in a crew.")
    members = await get_lobby_members(session, lobby_id=lobby.id)
    if len(members) >= 4:
        raise ValueError("Crew is full.")
    session.add(BankRobberyParticipantRow(lobby_id=lobby.id, guild_id=guild_id, user_id=user_id, role="ghost", ready=False, cut_percent=0, confirmed_cuts=True))
    await session.flush()
    await auto_assign_roles(session, lobby=lobby)


async def auto_assign_roles(session, *, lobby: BankRobberyLobbyRow) -> None:
    members = await get_lobby_members(session, lobby_id=lobby.id)
    mapping = assign_roles([int(m.user_id) for m in members], seed=int(lobby.rng_seed))
    for m in members:
        m.role = mapping[int(m.user_id)]


async def toggle_ready(session, *, guild_id: int, user_id: int) -> BankRobberyLobbyRow:
    lobby = await get_user_lobby(session, guild_id=guild_id, user_id=user_id)
    if lobby is None:
        raise ValueError("You are not in a crew.")
    members = await get_lobby_members(session, lobby_id=lobby.id)
    member = next((m for m in members if int(m.user_id) == int(user_id)), None)
    if member is None:
        raise ValueError("Crew membership missing.")
    member.ready = not bool(member.ready)
    return lobby


async def leave_crew(session, *, guild_id: int, user_id: int) -> tuple[bool, BankRobberyLobbyRow | None]:
    lobby = await get_user_lobby(session, guild_id=guild_id, user_id=user_id)
    if lobby is None:
        return False, None
    members = await get_lobby_members(session, lobby_id=lobby.id)
    member = next((m for m in members if int(m.user_id) == int(user_id)), None)
    if member is not None:
        await session.delete(member)
    await session.flush()
    remaining = await get_lobby_members(session, lobby_id=lobby.id)
    if not remaining:
        await session.delete(lobby)
        return True, None
    if int(lobby.leader_user_id) == int(user_id):
        lobby.leader_user_id = int(remaining[0].user_id)
    return False, lobby
