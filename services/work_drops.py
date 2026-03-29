from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ItemInventoryRow, LootboxInventoryRow
from services.items_catalog import ITEMS, ItemDef, ItemRarity


@dataclass(frozen=True)
class WorkDropResult:
    lootbox_rarity: Optional[str] = None
    item_key: Optional[str] = None


# Anti-spam guards.
_USER_DROP_WINDOW_SECONDS = 18.0
_USER_DROP_MAX_IN_WINDOW = 3
_USER_DROP_TIMES: dict[tuple[int, int], list[float]] = {}


def _roll_bp(chance_bp: int) -> bool:
    bp = max(int(chance_bp), 0)
    if bp <= 0:
        return False
    if bp >= 10_000:
        return True
    return random.randint(1, 10_000) <= bp


def _tier_mult_bp(job_tier: str) -> int:
    key = (job_tier or "").lower()
    if key == "hard":
        return 2_000
    if key == "stable":
        return 1_100
    return 0


def _progress_mult_bp(user_level: int, prestige: int) -> int:
    lvl = max(int(user_level), 1)
    pre = max(int(prestige), 0)
    return min((lvl // 4) * 90 + pre * 300, 4_200)


def _user_drop_allowed(guild_id: int, user_id: int, *, now: float) -> bool:
    key = (int(guild_id), int(user_id))
    arr = _USER_DROP_TIMES.get(key, [])
    arr = [t for t in arr if now - t <= _USER_DROP_WINDOW_SECONDS]
    if len(arr) >= _USER_DROP_MAX_IN_WINDOW:
        _USER_DROP_TIMES[key] = arr
        return False
    arr.append(now)
    _USER_DROP_TIMES[key] = arr
    return True


def _pick_lootbox_rarity(*, rare_find_bp: int) -> str:
    # Slightly scale upper tiers with rare-find.
    bonus = max(int(rare_find_bp), 0)
    table = [
        ("common", 7300 - min(bonus // 20, 500)),
        ("rare", 2000 + min(bonus // 25, 250)),
        ("epic", 550 + min(bonus // 35, 180)),
        ("legendary", 130 + min(bonus // 90, 50)),
        ("mythical", 20 + min(bonus // 120, 20)),
    ]
    total = sum(max(w, 1) for _, w in table)
    r = random.randint(1, total)
    acc = 0
    for rarity, weight in table:
        acc += max(weight, 1)
        if r <= acc:
            return rarity
    return "common"


def _pick_item_drop(*, rare_find_bp: int) -> Optional[ItemDef]:
    weighted: list[tuple[ItemDef, int]] = []
    bonus = max(int(rare_find_bp), 0)
    for it in ITEMS.values():
        if it.rarity == ItemRarity.COMMON:
            w = 180
        elif it.rarity == ItemRarity.UNCOMMON:
            w = 110
        elif it.rarity == ItemRarity.RARE:
            w = 45 + (bonus // 300)
        elif it.rarity == ItemRarity.EPIC:
            w = 14 + (bonus // 220)
        elif it.rarity == ItemRarity.LEGENDARY:
            w = 5 + (bonus // 180)
        else:
            w = 1 + (bonus // 120)
        weighted.append((it, max(int(w), 1)))

    total = sum(w for _, w in weighted)
    if total <= 0:
        return None
    r = random.randint(1, total)
    acc = 0
    for it, w in weighted:
        acc += w
        if r <= acc:
            return it
    return None


async def _add_lootbox(session: AsyncSession, *, guild_id: int, user_id: int, rarity: str) -> None:
    row = await session.scalar(
        select(LootboxInventoryRow).where(
            LootboxInventoryRow.guild_id == guild_id,
            LootboxInventoryRow.user_id == user_id,
            LootboxInventoryRow.rarity == rarity,
        )
    )
    if row is None:
        session.add(LootboxInventoryRow(guild_id=guild_id, user_id=user_id, rarity=rarity, amount=1))
    else:
        row.amount = max(int(row.amount), 0) + 1


async def _add_item(session: AsyncSession, *, guild_id: int, user_id: int, item_key: str) -> None:
    row = await session.scalar(
        select(ItemInventoryRow).where(
            ItemInventoryRow.guild_id == guild_id,
            ItemInventoryRow.user_id == user_id,
            ItemInventoryRow.item_key == item_key,
        )
    )
    if row is None:
        session.add(ItemInventoryRow(guild_id=guild_id, user_id=user_id, item_key=item_key, qty=1))
    else:
        row.qty = max(int(row.qty), 0) + 1


async def roll_and_grant_work_drops(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    job_tier: str,
    user_level: int,
    prestige: int,
    rare_find_bp: int,
    extra_roll_bp: int,
    lootbox_drop_bp: int = 0,
    item_drop_bp: int = 0,
) -> WorkDropResult:
    now = time.time()
    if not _user_drop_allowed(guild_id, user_id, now=now):
        return WorkDropResult()

    tier_bonus = _tier_mult_bp(job_tier)
    progress_bonus = _progress_mult_bp(user_level, prestige)
    proc_bonus = max(int(rare_find_bp), 0) // 2 + max(int(extra_roll_bp), 0) // 4

    lootbox_chance = 450 + tier_bonus + progress_bonus + proc_bonus + int(lootbox_drop_bp)
    item_chance = 240 + (tier_bonus // 2) + (progress_bonus // 2) + proc_bonus + int(item_drop_bp)

    lootbox_rarity: Optional[str] = None
    item_key: Optional[str] = None

    if _roll_bp(min(lootbox_chance, 9_500)):
        lootbox_rarity = _pick_lootbox_rarity(rare_find_bp=rare_find_bp)
        await _add_lootbox(session, guild_id=guild_id, user_id=user_id, rarity=lootbox_rarity)

    if _roll_bp(min(item_chance, 9_500)):
        item = _pick_item_drop(rare_find_bp=rare_find_bp)
        if item is not None:
            item_key = item.key
            await _add_item(session, guild_id=guild_id, user_id=user_id, item_key=item_key)

    return WorkDropResult(lootbox_rarity=lootbox_rarity, item_key=item_key)
