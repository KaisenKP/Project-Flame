# services/items_inventory.py
from __future__ import annotations

from typing import Dict

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from services.items_models import ItemInventoryRow
from services.items_catalog import ITEMS


# -----------------------
# Core helpers
# -----------------------

async def get_inventory(
    session: AsyncSession,
    guild_id: int,
    user_id: int,
) -> Dict[str, int]:
    """
    Returns {item_key: qty} for the user.
    """
    rows = await session.execute(
        select(ItemInventoryRow)
        .where(
            ItemInventoryRow.guild_id == guild_id,
            ItemInventoryRow.user_id == user_id,
            ItemInventoryRow.qty > 0,
        )
    )

    inventory: Dict[str, int] = {}
    for row in rows.scalars():
        inventory[row.item_key] = row.qty

    return inventory


async def get_item_qty(
    session: AsyncSession,
    guild_id: int,
    user_id: int,
    item_key: str,
) -> int:
    row = await session.scalar(
        select(ItemInventoryRow)
        .where(
            ItemInventoryRow.guild_id == guild_id,
            ItemInventoryRow.user_id == user_id,
            ItemInventoryRow.item_key == item_key,
        )
    )
    return row.qty if row else 0


# -----------------------
# Mutations
# -----------------------

async def add_item(
    session: AsyncSession,
    guild_id: int,
    user_id: int,
    item_key: str,
    qty: int = 1,
) -> None:
    if qty <= 0:
        return

    if item_key not in ITEMS:
        raise ValueError(f"Unknown item_key: {item_key}")

    row = await session.scalar(
        select(ItemInventoryRow)
        .where(
            ItemInventoryRow.guild_id == guild_id,
            ItemInventoryRow.user_id == user_id,
            ItemInventoryRow.item_key == item_key,
        )
        .with_for_update()
    )

    if row:
        row.qty += qty
    else:
        session.add(
            ItemInventoryRow(
                guild_id=guild_id,
                user_id=user_id,
                item_key=item_key,
                qty=qty,
            )
        )


async def remove_item(
    session: AsyncSession,
    guild_id: int,
    user_id: int,
    item_key: str,
    qty: int = 1,
) -> None:
    if qty <= 0:
        return

    row = await session.scalar(
        select(ItemInventoryRow)
        .where(
            ItemInventoryRow.guild_id == guild_id,
            ItemInventoryRow.user_id == user_id,
            ItemInventoryRow.item_key == item_key,
        )
        .with_for_update()
    )

    if not row or row.qty < qty:
        raise ValueError("Not enough items to remove")

    row.qty -= qty

    if row.qty <= 0:
        await session.execute(
            delete(ItemInventoryRow).where(
                ItemInventoryRow.guild_id == guild_id,
                ItemInventoryRow.user_id == user_id,
                ItemInventoryRow.item_key == item_key,
            )
        )


# -----------------------
# Transfers (trade-ready)
# -----------------------

async def transfer_item(
    session: AsyncSession,
    guild_id: int,
    from_user_id: int,
    to_user_id: int,
    item_key: str,
    qty: int = 1,
) -> None:
    if qty <= 0:
        return

    item_def = ITEMS.get(item_key)
    if not item_def:
        raise ValueError(f"Unknown item_key: {item_key}")

    if not item_def.tradable:
        raise ValueError("Item is not tradable")

    # Remove from sender first (will fail if insufficient)
    await remove_item(
        session=session,
        guild_id=guild_id,
        user_id=from_user_id,
        item_key=item_key,
        qty=qty,
    )

    # Add to receiver
    await add_item(
        session=session,
        guild_id=guild_id,
        user_id=to_user_id,
        item_key=item_key,
        qty=qty,
    )
