from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import BankRobberyLobbyRow, BankRobberyPrepProgressRow
from .catalog import PREP_DEFS, get_template


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def ensure_prep_rows(session: AsyncSession, lobby: BankRobberyLobbyRow) -> list[BankRobberyPrepProgressRow]:
    template = get_template(lobby.robbery_id)
    result = await session.execute(
        select(BankRobberyPrepProgressRow)
        .where(BankRobberyPrepProgressRow.lobby_id == lobby.id)
        .order_by(BankRobberyPrepProgressRow.prep_key.asc())
    )
    rows = list(result.scalars())
    existing = {row.prep_key for row in rows}
    for key in template.prep_keys:
        if key in existing:
            continue
        row = BankRobberyPrepProgressRow(lobby_id=lobby.id, guild_id=lobby.guild_id, prep_key=key, completed=False)
        session.add(row)
        rows.append(row)
    await session.flush()
    return rows


async def complete_prep(session: AsyncSession, *, lobby: BankRobberyLobbyRow, prep_key: str, user_id: int) -> BankRobberyPrepProgressRow:
    await ensure_prep_rows(session, lobby)
    row = await session.scalar(select(BankRobberyPrepProgressRow).where(BankRobberyPrepProgressRow.lobby_id == lobby.id, BankRobberyPrepProgressRow.prep_key == prep_key))
    if row is None:
        raise ValueError("unknown_prep")
    row.completed = True
    row.completed_by_user_id = user_id
    row.completed_at = utc_now()
    row.effectiveness_bp = 10000
    row.metadata_json = {"bonus_text": PREP_DEFS[prep_key].bonus_text}
    return row


async def prep_summary(session: AsyncSession, lobby: BankRobberyLobbyRow) -> tuple[list[BankRobberyPrepProgressRow], dict[str, int]]:
    rows = await ensure_prep_rows(session, lobby)
    effects: dict[str, int] = {}
    for row in rows:
        if not row.completed:
            continue
        for key, value in PREP_DEFS[row.prep_key].effects.items():
            effects[key] = int(effects.get(key, 0)) + int(value)
    return rows, effects
