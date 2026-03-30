# cogs/shop.py
from __future__ import annotations

import hashlib
import logging
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands, tasks
from sqlalchemy import func, select

from db.models import ItemInventoryRow, ShopPurchaseRow, SundayAnnouncementStateRow, WalletRow
from services.db import sessions
from services.users import ensure_user_rows
from services.vip import is_vip_member

from services.items_catalog import ITEMS, ItemDef, ItemRarity
from services.jobs_core import clamp_int, fmt_int

# ============================================================
# Shop config
# ============================================================

SHOP_SLOTS = 5
SHOP_PRIMARY_SLOTS = 4
SHOP_ANCHOR_SLOT = 5

SHOP_RARITY_TABLES: Dict[str, Dict[str, int]] = {
    "normal": {
        ItemRarity.COMMON.value: 52,
        ItemRarity.UNCOMMON.value: 30,
        ItemRarity.RARE.value: 14,
        ItemRarity.EPIC.value: 3,
        ItemRarity.LEGENDARY.value: 1,
        ItemRarity.MYTHICAL.value: 0,
    },
    "sunday": {
        ItemRarity.RARE.value: 52,
        ItemRarity.EPIC.value: 30,
        ItemRarity.LEGENDARY.value: 14,
        ItemRarity.MYTHICAL.value: 4,
    },
}

ANCHOR_RARITY_TABLES: Dict[str, Dict[str, int]] = {
    "normal": {
        ItemRarity.EPIC.value: 65,
        ItemRarity.LEGENDARY.value: 27,
        ItemRarity.MYTHICAL.value: 8,
    },
    "sunday": {
        ItemRarity.EPIC.value: 42,
        ItemRarity.LEGENDARY.value: 40,
        ItemRarity.MYTHICAL.value: 18,
    },
}

SHOP_SUNDAY_ANNOUNCE_CHANNEL_ID = int((os.getenv("SHOP_SUNDAY_ANNOUNCE_CHANNEL_ID") or "0").strip() or "0")
SUNDAY_ANNOUNCE_INTERVAL_SECONDS = 60
SUNDAY_MIDDAY_OFFSET_HOURS = 12
SUNDAY_FINAL_WINDOW_MINUTES_BEFORE_RESET = 120
VIP_MYTHICAL_WEIGHT_BONUS = {
    "normal": 1,
    "sunday": 1,
}

# VIP reroll bookkeeping uses ShopPurchaseRow with a marker key
_REROLL_MARKER_ITEM_KEY = "__vip_shop_reroll__"

# UI
VIEW_TIMEOUT_SECONDS = 180
BTN_ROW_BUY = 0
BTN_ROW_REROLL = 1
BTN_ROW_REFRESH = 1
LIST_ITEMS_PER_PAGE = 5

# Branding
SHOP_TITLE = "Moist Mart"
SHOP_TAGLINE = "Temporary boosts. Permanent flex."
SHOP_ICON_URL = None  # set later if you want
LOG = logging.getLogger(__name__)


# ============================================================
# Time helpers (10 PM America/New_York boundary)
# NOTE: If you want perfect DST, swap to zoneinfo later.
# ============================================================

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _shop_day_anchor_utc(now: Optional[datetime] = None) -> datetime:
    now = now or _utc_now()

    # DST heuristic: March..October
    m = now.month
    likely_dst = 3 <= m <= 10
    reset_utc_hour = 2 if likely_dst else 3  # 10pm local

    anchor = now.replace(hour=reset_utc_hour, minute=0, second=0, microsecond=0)
    if now < anchor:
        anchor -= timedelta(days=1)
    return anchor


def _shop_day_id(now: Optional[datetime] = None) -> int:
    anchor = _shop_day_anchor_utc(now)
    return int(anchor.timestamp() // 86400)


def _shop_expires_at(now: Optional[datetime] = None) -> datetime:
    return _shop_day_anchor_utc(now) + timedelta(days=1)


def _anchor_local_weekday(anchor_utc: datetime) -> int:
    local_offset_hours = 4 if anchor_utc.hour == 2 else 5
    return (anchor_utc - timedelta(hours=local_offset_hours)).weekday()


def is_sunday_shop(now: Optional[datetime] = None) -> bool:
    return _anchor_local_weekday(_shop_day_anchor_utc(now)) == 6


def _pretty_reset_countdown(now: Optional[datetime] = None) -> str:
    now = now or _utc_now()
    exp = _shop_expires_at(now)
    delta = exp - now
    s = max(int(delta.total_seconds()), 0)
    h = s // 3600
    m = (s % 3600) // 60
    return f"{h}h {m:02d}m"


# ============================================================
# Deterministic RNG helpers
# ============================================================

def _hash_seed(*parts: object) -> int:
    s = "|".join(str(p) for p in parts)
    h = hashlib.sha256(s.encode("utf-8")).hexdigest()
    return int(h[:16], 16)


def _rng_for(guild_id: int, user_id: int, day_id: int, reroll_index: int, *, salt: str) -> random.Random:
    seed = _hash_seed(guild_id, user_id, day_id, reroll_index, salt)
    return random.Random(seed)


def _rarity_pools() -> Dict[str, List[ItemDef]]:
    pools: Dict[str, List[ItemDef]] = {
        ItemRarity.COMMON.value: [],
        ItemRarity.UNCOMMON.value: [],
        ItemRarity.RARE.value: [],
        ItemRarity.EPIC.value: [],
        ItemRarity.LEGENDARY.value: [],
        ItemRarity.MYTHICAL.value: [],
    }
    for it in ITEMS.values():
        pools[it.rarity.value].append(it)
    for k in list(pools.keys()):
        pools[k].sort(key=lambda x: x.key)
    return pools


def _weighted_roll(*, rng: random.Random, rarity_table: Dict[str, int]) -> str:
    valid: list[tuple[str, int]] = [(rarity, int(weight)) for rarity, weight in rarity_table.items() if int(weight) > 0]
    if not valid:
        raise RuntimeError("Rarity table must include at least one positive weight")
    total = sum(weight for _, weight in valid)
    roll = rng.randint(1, total)
    cur = 0
    for rarity, weight in valid:
        cur += weight
        if roll <= cur:
            return rarity
    return valid[-1][0]


def roll_shop_slot(*, rng: random.Random, rarity_table: Dict[str, int]) -> str:
    return _weighted_roll(rng=rng, rarity_table=rarity_table)


def roll_anchor_slot(*, rng: random.Random, anchor_table: Dict[str, int]) -> str:
    rarity = _weighted_roll(rng=rng, rarity_table=anchor_table)
    if rarity not in {ItemRarity.EPIC.value, ItemRarity.LEGENDARY.value, ItemRarity.MYTHICAL.value}:
        raise RuntimeError(f"Anchor rarity table generated invalid rarity: {rarity}")
    return rarity


def get_current_rarity_tables(now: Optional[datetime] = None) -> Tuple[Dict[str, int], Dict[str, int], bool]:
    sunday = is_sunday_shop(now)
    key = "sunday" if sunday else "normal"
    return SHOP_RARITY_TABLES[key], ANCHOR_RARITY_TABLES[key], sunday


def _is_sunday_day_id(day_id: int) -> bool:
    anchor_utc = datetime.fromtimestamp(day_id * 86400, tz=timezone.utc)
    return _anchor_local_weekday(anchor_utc) == 6


# ============================================================
# Shop roll (3C / 1U / 1R, Rare can upgrade to Mythical)
# ============================================================

@dataclass(frozen=True)
class ShopOffer:
    item_key: str
    name: str
    rarity: str
    price: int
    daily_limit: int
    description: str


def _offer_from_item(it: ItemDef) -> ShopOffer:
    desc = (getattr(it, "description", "") or "") if hasattr(it, "description") else ""
    if not desc:
        payload = it.effect.payload
        if "stamina_add" in payload:
            desc = f"Gain **+{int(payload['stamina_add'])}** stamina instantly."
        elif "payout_bonus_bp" in payload:
            desc = f"Boost payouts by **+{payload['payout_bonus_bp'] / 100:.2f}%**."
        elif "fail_reduction_bp" in payload:
            desc = f"Reduce fails by **{payload['fail_reduction_bp'] / 100:.2f}%**."
        elif "stamina_discount_bp" in payload:
            desc = f"Spend **{payload['stamina_discount_bp'] / 100:.2f}%** less stamina."
        elif "stamina_cost_flat_delta" in payload:
            desc = f"Work costs **{int(payload['stamina_cost_flat_delta']):+d}** stamina (flat)."
        elif "job_xp_bonus_bp" in payload:
            desc = f"Boost Job XP by **+{payload['job_xp_bonus_bp'] / 100:.2f}%**."
        elif "job_level_gain" in payload:
            desc = f"Gain **+{int(payload['job_level_gain'])}** job level(s) on your next work result."
        elif "job_xp_progress_bp" in payload:
            desc = f"Gain **+{payload['job_xp_progress_bp'] / 100:.2f}%** progress toward your next job level."
        elif "double_payout_chance_bp" in payload:
            desc = f"Add **+{payload['double_payout_chance_bp'] / 100:.2f}%** chance to 2x payout."
        elif "next_work_payout_bp" in payload:
            desc = f"Boost your next /work payout by **+{payload['next_work_payout_bp'] / 100:.2f}%**."
        elif "stamina_cap_add" in payload:
            desc = f"Increase max stamina by **+{int(payload['stamina_cap_add'])}** (temporary)."
        elif "rare_find_bp" in payload:
            desc = f"Increase rare drop findings by **+{payload['rare_find_bp'] / 100:.2f}%**."
        else:
            desc = "A temporary boost. Use it, get paid, repeat."
    return ShopOffer(
        item_key=it.key,
        name=it.name,
        rarity=it.rarity.value,
        price=int(it.price),
        daily_limit=int(it.daily_limit),
        description=desc,
    )


def compute_daily_shop_offers(
    *,
    guild_id: int,
    user_id: int,
    day_id: int,
    reroll_index: int,
    vip: bool,
    salt: str,
) -> List[ShopOffer]:
    pools = _rarity_pools()
    rng = _rng_for(guild_id, user_id, day_id, reroll_index, salt=salt)
    table_key = "sunday" if _is_sunday_day_id(day_id) else "normal"
    shop_rarity_table = dict(SHOP_RARITY_TABLES[table_key])
    anchor_rarity_table = dict(ANCHOR_RARITY_TABLES[table_key])
    if vip:
        mythic_bonus = int(VIP_MYTHICAL_WEIGHT_BONUS[table_key])
        shop_rarity_table[ItemRarity.MYTHICAL.value] = int(shop_rarity_table.get(ItemRarity.MYTHICAL.value, 0)) + mythic_bonus
        anchor_rarity_table[ItemRarity.MYTHICAL.value] = int(anchor_rarity_table.get(ItemRarity.MYTHICAL.value, 0)) + mythic_bonus

    for rarity in set(shop_rarity_table) | set(anchor_rarity_table):
        if rarity not in pools:
            raise RuntimeError(f"Unknown rarity in table: {rarity}")
        if int(shop_rarity_table.get(rarity, 0)) > 0 and not pools[rarity]:
            raise RuntimeError(f"No {rarity} items in catalog for shop")
        if int(anchor_rarity_table.get(rarity, 0)) > 0 and not pools[rarity]:
            raise RuntimeError(f"No {rarity} items in catalog for anchor")

    chosen: set[str] = set()

    def _pick_unique(pool: List[ItemDef]) -> ItemDef:
        for _ in range(64):
            it = rng.choice(pool)
            if it.key not in chosen:
                chosen.add(it.key)
                return it
        for it in pool:
            if it.key not in chosen:
                chosen.add(it.key)
                return it
        return pool[0]

    offers: List[ShopOffer] = []
    for _ in range(SHOP_PRIMARY_SLOTS):
        rarity = roll_shop_slot(rng=rng, rarity_table=shop_rarity_table)
        offers.append(_offer_from_item(_pick_unique(pools[rarity])))

    anchor_rarity = roll_anchor_slot(rng=rng, anchor_table=anchor_rarity_table)
    offers.append(_offer_from_item(_pick_unique(pools[anchor_rarity])))
    return offers


# ============================================================
# DB helpers
# ============================================================

async def _get_wallet(session, *, guild_id: int, user_id: int) -> WalletRow:
    w = await session.scalar(select(WalletRow).where(WalletRow.guild_id == guild_id, WalletRow.user_id == user_id))
    if w is None:
        w = WalletRow(guild_id=guild_id, user_id=user_id, silver=0, diamonds=0)
        session.add(w)
        await session.flush()
    return w


async def _purchased_qty_today(session, *, guild_id: int, user_id: int, day_id: int, item_key: str) -> int:
    q = await session.scalar(
        select(func.coalesce(func.sum(ShopPurchaseRow.qty), 0)).where(
            ShopPurchaseRow.guild_id == guild_id,
            ShopPurchaseRow.user_id == user_id,
            ShopPurchaseRow.shop_day_id == day_id,
            ShopPurchaseRow.item_key == item_key,
        )
    )
    return int(q or 0)


async def _vip_rerolls_used_today(session, *, guild_id: int, user_id: int, day_id: int) -> int:
    q = await session.scalar(
        select(func.coalesce(func.sum(ShopPurchaseRow.qty), 0)).where(
            ShopPurchaseRow.guild_id == guild_id,
            ShopPurchaseRow.user_id == user_id,
            ShopPurchaseRow.shop_day_id == day_id,
            ShopPurchaseRow.item_key == _REROLL_MARKER_ITEM_KEY,
        )
    )
    return int(q or 0)


async def _record_vip_reroll(session, *, guild_id: int, user_id: int, day_id: int) -> None:
    row = await session.scalar(
        select(ShopPurchaseRow).where(
            ShopPurchaseRow.guild_id == guild_id,
            ShopPurchaseRow.user_id == user_id,
            ShopPurchaseRow.shop_day_id == day_id,
            ShopPurchaseRow.item_key == _REROLL_MARKER_ITEM_KEY,
        )
    )
    if row is None:
        row = ShopPurchaseRow(
            guild_id=guild_id,
            user_id=user_id,
            shop_day_id=day_id,
            item_key=_REROLL_MARKER_ITEM_KEY,
            qty=0,
        )
        session.add(row)
        await session.flush()
    row.qty = int(row.qty) + 1


async def _add_item_to_inventory(
    session,
    *,
    guild_id: int,
    user_id: int,
    item_key: str,
    qty: int = 1,
) -> None:
    if qty <= 0:
        return

    row = await session.scalar(
        select(ItemInventoryRow).where(
            ItemInventoryRow.guild_id == guild_id,
            ItemInventoryRow.user_id == user_id,
            ItemInventoryRow.item_key == item_key,
        )
    )
    if row is None:
        row = ItemInventoryRow(
            guild_id=guild_id,
            user_id=user_id,
            item_key=item_key,
            qty=0,
        )
        session.add(row)
        await session.flush()

    row.qty = int(row.qty) + int(qty)


# ============================================================
# Embed building (real shop vibes)
# ============================================================

def _rarity_emoji(r: str) -> str:
    if r == ItemRarity.COMMON.value:
        return "⚪"
    if r == ItemRarity.UNCOMMON.value:
        return "🟢"
    if r == ItemRarity.RARE.value:
        return "🔵"
    if r == ItemRarity.EPIC.value:
        return "🟣"
    if r == ItemRarity.LEGENDARY.value:
        return "🟠"
    if r == ItemRarity.MYTHICAL.value:
        return "🌌"
    return "⚪"


def _rarity_label(r: str) -> str:
    if r == ItemRarity.COMMON.value:
        return "Common"
    if r == ItemRarity.UNCOMMON.value:
        return "Uncommon"
    if r == ItemRarity.RARE.value:
        return "Rare"
    if r == ItemRarity.EPIC.value:
        return "Epic"
    if r == ItemRarity.LEGENDARY.value:
        return "Legendary"
    if r == ItemRarity.MYTHICAL.value:
        return "Mythical"
    return r


RARITY_ORDER: list[ItemRarity] = [
    ItemRarity.COMMON,
    ItemRarity.UNCOMMON,
    ItemRarity.RARE,
    ItemRarity.EPIC,
    ItemRarity.LEGENDARY,
    ItemRarity.MYTHICAL,
]


def _catalog_by_rarity() -> Dict[str, List[ItemDef]]:
    grouped: Dict[str, List[ItemDef]] = {}
    for item in ITEMS.values():
        grouped.setdefault(item.rarity.value, []).append(item)
    for rarity_items in grouped.values():
        rarity_items.sort(key=lambda x: (x.price, x.name.lower(), x.key))
    return grouped


def _effect_summary(item: ItemDef) -> str:
    payload = item.effect.payload
    if "payout_bonus_bp" in payload:
        return f"+{payload['payout_bonus_bp'] / 100:.0f}% payout"
    if "stamina_add" in payload:
        return f"+{int(payload['stamina_add'])} stamina"
    if "fail_reduction_bp" in payload:
        return f"-{payload['fail_reduction_bp'] / 100:.0f}% fail chance"
    if "job_xp_bonus_bp" in payload:
        return f"+{payload['job_xp_bonus_bp'] / 100:.0f}% Job XP"
    if "stamina_cost_flat_delta" in payload:
        return f"{int(payload['stamina_cost_flat_delta']):+d} stamina cost"
    if "next_work_payout_bp" in payload:
        return f"+{payload['next_work_payout_bp'] / 100:.0f}% next /work payout"
    if "double_payout_chance_bp" in payload:
        return f"+{payload['double_payout_chance_bp'] / 100:.0f}% double payout chance"
    if "stamina_cap_add" in payload:
        return f"+{int(payload['stamina_cap_add'])} max stamina"
    if "regen_bonus_bp" in payload:
        return f"+{payload['regen_bonus_bp'] / 100:.0f}% stamina regen"
    if "job_level_gain" in payload:
        return f"+{int(payload['job_level_gain'])} next job level gain"
    if "rare_find_bp" in payload:
        return f"+{payload['rare_find_bp'] / 100:.0f}% rare-find chance"
    if "combo_payout_step_bp" in payload:
        step = payload["combo_payout_step_bp"] / 100
        stacks = int(payload.get("combo_max_stacks", 0))
        return f"+{step:.2f}% per combo stack ({stacks} max)"
    return "Special effect"


def _list_embed(*, rarity: str, items: List[ItemDef], page: int, total_pages: int) -> discord.Embed:
    color = _rarity_color([_offer_from_item(item) for item in items]) if items else discord.Color.blurple()
    emoji = _rarity_emoji(rarity)
    label = _rarity_label(rarity)
    embed = discord.Embed(
        title=f"🛍️ {SHOP_TITLE} • Item Browser",
        description=f"{emoji} **{label}** items",
        color=color,
    )

    if not items:
        embed.add_field(name="Nothing here yet", value="No items in this rarity yet.", inline=False)
    else:
        for item in items:
            duration = f" • {int(item.effect.duration_seconds // 60)}m" if item.effect.duration_seconds else ""
            charges = f" • {int(item.effect.charges)} charges" if item.effect.charges else ""
            rule = f"Limit: {int(item.daily_limit)}/day"
            summary = _effect_summary(item)
            embed.add_field(
                name=f"{item.name} • {fmt_int(int(item.price))} Silver",
                value=(
                    f"{item.description}\n"
                    f"✨ {summary}\n"
                    f"📦 {rule}{duration}{charges}"
                ),
                inline=False,
            )

    embed.set_footer(text=f"Page {page + 1}/{max(total_pages, 1)} • Use rarity buttons to browse")
    return embed


def _rarity_color(offers: List[ShopOffer]) -> discord.Color:
    for o in offers:
        if o.rarity == ItemRarity.MYTHICAL.value:
            return discord.Color.fuchsia()
    for o in offers:
        if o.rarity == ItemRarity.LEGENDARY.value:
            return discord.Color.gold()
    for o in offers:
        if o.rarity == ItemRarity.EPIC.value:
            return discord.Color.purple()
    for o in offers:
        if o.rarity == ItemRarity.RARE.value:
            return discord.Color.blue()
    for o in offers:
        if o.rarity == ItemRarity.UNCOMMON.value:
            return discord.Color.green()
    return discord.Color.gold()


def build_shop_embed(
    *,
    user: discord.abc.User,
    offers: List[ShopOffer],
    day_id: int,
    vip: bool,
    rerolls_used: int,
    wallet_silver: int,
    purchased_map: Dict[str, int],
) -> discord.Embed:
    reset_in = _pretty_reset_countdown(_utc_now())
    sunday = is_sunday_shop(_utc_now())
    color = _rarity_color(offers)

    event_line = "🔥 **Epic Store Sunday Active**\n" if sunday else ""
    embed = discord.Embed(
        title=f"🛍️ {SHOP_TITLE}",
        description=f"_{SHOP_TAGLINE}_\n\n{event_line}💰 **{fmt_int(wallet_silver)}** Silver • ⏳ Resets in **{reset_in}**",
        color=color,
    )

    for i, off in enumerate(offers, start=1):
        emoji = _rarity_emoji(off.rarity)
        rarity = _rarity_label(off.rarity)

        bought = int(purchased_map.get(off.item_key, 0))
        limit = max(int(off.daily_limit), 1)
        left = max(limit - bought, 0)

        if left <= 0:
            price_line = "✅ **Sold out for you today**"
        else:
            price_line = f"💸 Price: **{fmt_int(off.price)}** • Stock: **{fmt_int(left)}**"

        slot_prefix = "⭐ Anchor Slot · " if i == SHOP_ANCHOR_SLOT else ""
        embed.add_field(
            name=f"{i}. {slot_prefix}{emoji} {off.name}  ·  {rarity}",
            value=f"{off.description}\n{price_line}",
            inline=False,
        )

    vip_line = "🔒 VIP Reroll: **Locked**"
    if vip:
        vip_line = f"👑 VIP Reroll: **{rerolls_used}/1 used** • VIP mythical weight boost active"
    else:
        vip_line += " • Anchor guarantee always active"

    embed.add_field(
        name="Perks",
        value=f"{vip_line}\n🧾 Limits are per-item per shop day.",
        inline=False,
    )

    embed.set_footer(text=f"Shop Day: {day_id} • Use the buttons below. No chat spam.")
    avatar_url = getattr(getattr(user, "display_avatar", None), "url", None)
    if avatar_url:
        embed.set_author(name=str(user), icon_url=avatar_url)
    if SHOP_ICON_URL:
        embed.set_thumbnail(url=SHOP_ICON_URL)

    return embed


# ============================================================
# UI View
# ============================================================

class ShopView(discord.ui.View):
    def __init__(self, *, cog: "ShopCog", guild_id: int, user_id: int, day_id: int):
        super().__init__(timeout=VIEW_TIMEOUT_SECONDS)
        self.cog = cog
        self.guild_id = guild_id
        self.user_id = user_id
        self.day_id = day_id

        for idx in range(1, SHOP_SLOTS + 1):
            self.add_item(ShopBuyButton(slot=idx))

        self.add_item(ShopRerollButton())
        self.add_item(ShopRefreshButton())

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user is None:
            return False
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This shop panel isn’t for you.", ephemeral=True)
            return False
        return True


class ShopListRarityButton(discord.ui.Button):
    def __init__(self, *, rarity: str, row: int):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label=_rarity_label(rarity),
            row=row,
        )
        self.rarity = rarity

    async def callback(self, interaction: discord.Interaction):
        view: ShopListView = self.view  # type: ignore[assignment]
        await view.change_rarity(interaction, self.rarity)


class ShopListPageButton(discord.ui.Button):
    def __init__(self, *, direction: int):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label="Prev" if direction < 0 else "Next",
            row=2,
        )
        self.direction = direction

    async def callback(self, interaction: discord.Interaction):
        view: ShopListView = self.view  # type: ignore[assignment]
        await view.change_page(interaction, self.direction)


class ShopListView(discord.ui.View):
    def __init__(self, *, user_id: int, grouped_items: Dict[str, List[ItemDef]]):
        super().__init__(timeout=VIEW_TIMEOUT_SECONDS)
        self.user_id = user_id
        self.grouped_items = grouped_items
        self.message: discord.Message | None = None

        self.rarities = [r.value for r in RARITY_ORDER if grouped_items.get(r.value)]
        if not self.rarities:
            self.rarities = [ItemRarity.COMMON.value]

        self.selected_rarity = ItemRarity.COMMON.value if ItemRarity.COMMON.value in self.rarities else self.rarities[0]
        self.page = 0

        for idx, rarity in enumerate(self.rarities):
            self.add_item(ShopListRarityButton(rarity=rarity, row=0 if idx < 5 else 1))

        self.prev_button = ShopListPageButton(direction=-1)
        self.next_button = ShopListPageButton(direction=1)
        self.add_item(self.prev_button)
        self.add_item(self.next_button)
        self._sync_button_state()

    def _page_count(self) -> int:
        current = self.grouped_items.get(self.selected_rarity, [])
        return max((len(current) + LIST_ITEMS_PER_PAGE - 1) // LIST_ITEMS_PER_PAGE, 1)

    def _sync_button_state(self) -> None:
        page_count = self._page_count()
        self.page = clamp_int(self.page, 0, page_count - 1)
        self.prev_button.disabled = self.page <= 0
        self.next_button.disabled = self.page >= (page_count - 1)

        for child in self.children:
            if isinstance(child, ShopListRarityButton):
                child.style = discord.ButtonStyle.success if child.rarity == self.selected_rarity else discord.ButtonStyle.secondary

    def build_embed(self) -> discord.Embed:
        items = self.grouped_items.get(self.selected_rarity, [])
        page_count = self._page_count()
        start = self.page * LIST_ITEMS_PER_PAGE
        end = start + LIST_ITEMS_PER_PAGE
        return _list_embed(
            rarity=self.selected_rarity,
            items=items[start:end],
            page=self.page,
            total_pages=page_count,
        )

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user is None or interaction.user.id != self.user_id:
            await interaction.response.send_message("This shop list panel isn’t for you.", ephemeral=True)
            return False
        return True

    async def change_rarity(self, interaction: discord.Interaction, rarity: str) -> None:
        self.selected_rarity = rarity
        self.page = 0
        self._sync_button_state()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def change_page(self, interaction: discord.Interaction, direction: int) -> None:
        self.page += int(direction)
        self._sync_button_state()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def on_timeout(self) -> None:
        for child in self.children:
            child.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


class ShopBuyButton(discord.ui.Button):
    def __init__(self, slot: int):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label=f"Buy {slot}",
            row=BTN_ROW_BUY,
            custom_id=f"shop_buy_{slot}",
        )
        self.slot = slot

    async def callback(self, interaction: discord.Interaction):
        view: ShopView = self.view  # type: ignore[assignment]
        await view.cog._handle_buy(interaction, slot=self.slot)


class ShopRerollButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="VIP Reroll",
            row=BTN_ROW_REROLL,
            custom_id="shop_reroll",
        )

    async def callback(self, interaction: discord.Interaction):
        view: ShopView = self.view  # type: ignore[assignment]
        await view.cog._handle_reroll(interaction)


class ShopRefreshButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            style=discord.ButtonStyle.success,
            label="Refresh",
            row=BTN_ROW_REFRESH,
            custom_id="shop_refresh",
        )

    async def callback(self, interaction: discord.Interaction):
        view: ShopView = self.view  # type: ignore[assignment]
        await view.cog._handle_refresh(interaction)


# ============================================================
# Cog
# ============================================================

class ShopCog(commands.Cog):
    shop = app_commands.Group(name="shop", description="Open and browse the shop.")

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

        self._salt = "moist_arcade_shop_salt_v1"
        self._announcement_channel_id = SHOP_SUNDAY_ANNOUNCE_CHANNEL_ID
        self.sunday_announcement_loop.start()

    def cog_unload(self) -> None:
        self.sunday_announcement_loop.cancel()

    async def _get_or_create_sunday_state(self, session, *, guild_id: int) -> SundayAnnouncementStateRow:
        row = await session.scalar(select(SundayAnnouncementStateRow).where(SundayAnnouncementStateRow.guild_id == guild_id))
        if row is None:
            row = SundayAnnouncementStateRow(guild_id=guild_id, launch_sent=False, midday_sent=False, final_sent=False)
            session.add(row)
            await session.flush()
        return row

    async def _resolve_announcement_channel(self) -> discord.abc.Messageable | None:
        channel_id = int(self._announcement_channel_id)
        if channel_id <= 0:
            return None
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except Exception:
                LOG.exception("Failed to fetch Sunday shop announcement channel", extra={"channel_id": channel_id})
                return None
        if not isinstance(channel, discord.abc.Messageable):
            return None
        return channel

    async def _send_sunday_announcement(self, *, phase: str) -> bool:
        channel = await self._resolve_announcement_channel()
        if channel is None:
            return False

        if phase == "launch":
            embed = discord.Embed(
                title="🛍️ Epic Store Sunday is LIVE",
                description="The daily shop just refreshed with elevated rarity odds and a premium Anchor Slot.",
                color=discord.Color.purple(),
            )
        elif phase == "midday":
            embed = discord.Embed(
                title="⏰ Epic Store Sunday Midday Check-In",
                description="Midday reminder: today’s shop is still boosted. Grab your picks before the next reset.",
                color=discord.Color.blurple(),
            )
        elif phase == "final":
            embed = discord.Embed(
                title="⚠️ Epic Store Sunday Final Warning",
                description="Final window before reset. Sunday boosted odds end at the next daily shop refresh.",
                color=discord.Color.orange(),
            )
        else:
            return False

        await channel.send(embed=embed)
        return True

    async def handle_sunday_announcements(self) -> None:
        if self._announcement_channel_id <= 0:
            return
        now = _utc_now()
        if not is_sunday_shop(now):
            return

        reset_time = _shop_day_anchor_utc(now)
        expires_at = _shop_expires_at(now)
        midday_time = reset_time + timedelta(hours=SUNDAY_MIDDAY_OFFSET_HOURS)
        final_time = expires_at - timedelta(minutes=SUNDAY_FINAL_WINDOW_MINUTES_BEFORE_RESET)
        event_date = reset_time.date()

        phase: str | None = None
        if reset_time <= now < midday_time:
            phase = "launch"
        elif midday_time <= now < final_time:
            phase = "midday"
        elif final_time <= now < expires_at:
            phase = "final"

        if phase is None:
            return

        channel = await self._resolve_announcement_channel()
        guild = getattr(channel, "guild", None)
        if guild is None:
            return

        async with self.sessionmaker() as session:
            async with session.begin():
                state = await self._get_or_create_sunday_state(session, guild_id=guild.id)
                if state.last_event_date != event_date:
                    state.last_event_date = event_date
                    state.launch_sent = False
                    state.midday_sent = False
                    state.final_sent = False

                already_sent = {
                    "launch": bool(state.launch_sent),
                    "midday": bool(state.midday_sent),
                    "final": bool(state.final_sent),
                }[phase]
                if already_sent:
                    return

                sent = await self._send_sunday_announcement(phase=phase)
                if not sent:
                    return

                if phase == "launch":
                    state.launch_sent = True
                elif phase == "midday":
                    state.midday_sent = True
                elif phase == "final":
                    state.final_sent = True

    @tasks.loop(seconds=SUNDAY_ANNOUNCE_INTERVAL_SECONDS)
    async def sunday_announcement_loop(self) -> None:
        try:
            await self.handle_sunday_announcements()
        except Exception:
            LOG.exception("Sunday announcement loop tick failed")

    @sunday_announcement_loop.before_loop
    async def before_sunday_announcement_loop(self) -> None:
        await self.bot.wait_until_ready()

    async def _build_state(
        self,
        session,
        *,
        guild_id: int,
        user_id: int,
        vip: bool,
    ) -> Tuple[List[ShopOffer], WalletRow, Dict[str, int], int, int]:
        day_id = _shop_day_id(_utc_now())

        rerolls_used = await _vip_rerolls_used_today(session, guild_id=guild_id, user_id=user_id, day_id=day_id)
        reroll_index = clamp_int(rerolls_used, 0, 99)

        offers = compute_daily_shop_offers(
            guild_id=guild_id,
            user_id=user_id,
            day_id=day_id,
            reroll_index=reroll_index,
            vip=vip,
            salt=self._salt,
        )

        wallet = await _get_wallet(session, guild_id=guild_id, user_id=user_id)

        purchased_map: Dict[str, int] = {}
        for off in offers:
            purchased_map[off.item_key] = await _purchased_qty_today(
                session,
                guild_id=guild_id,
                user_id=user_id,
                day_id=day_id,
                item_key=off.item_key,
            )

        return offers, wallet, purchased_map, rerolls_used, day_id

    async def _send_shop_panel(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            return

        guild_id = interaction.guild.id
        user_id = interaction.user.id
        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)
                offers, wallet, purchased_map, rerolls_used, day_id = await self._build_state(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    vip=vip,
                )

                embed = build_shop_embed(
                    user=interaction.user,
                    offers=offers,
                    day_id=day_id,
                    vip=vip,
                    rerolls_used=rerolls_used,
                    wallet_silver=int(wallet.silver),
                    purchased_map=purchased_map,
                )

        view = ShopView(cog=self, guild_id=guild_id, user_id=user_id, day_id=day_id)

        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def _handle_refresh(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(thinking=False, ephemeral=True)
        await self._send_shop_panel(interaction)

    async def _handle_reroll(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            return

        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]
        if not vip:
            await interaction.response.send_message("VIP only.", ephemeral=True)
            return

        await interaction.response.defer(thinking=True, ephemeral=True)

        guild_id = interaction.guild.id
        user_id = interaction.user.id
        day_id = _shop_day_id(_utc_now())

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                used = await _vip_rerolls_used_today(session, guild_id=guild_id, user_id=user_id, day_id=day_id)
                if used >= 1:
                    await interaction.followup.send("You already used your VIP reroll today.", ephemeral=True)
                    return

                await _record_vip_reroll(session, guild_id=guild_id, user_id=user_id, day_id=day_id)

        await self._send_shop_panel(interaction)

    async def _handle_buy(self, interaction: discord.Interaction, *, slot: int) -> None:
        if interaction.guild is None:
            return

        if slot < 1 or slot > SHOP_SLOTS:
            await interaction.response.send_message("Invalid slot.", ephemeral=True)
            return

        await interaction.response.defer(thinking=True, ephemeral=True)

        guild_id = interaction.guild.id
        user_id = interaction.user.id
        vip = is_vip_member(interaction.user)  # type: ignore[arg-type]

        bought_name = None

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                offers, wallet, purchased_map, _, day_id = await self._build_state(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    vip=vip,
                )

                off = offers[slot - 1]
                item = ITEMS.get(off.item_key)
                if item is None:
                    await interaction.followup.send("That item no longer exists.", ephemeral=True)
                    return

                bought = int(purchased_map.get(off.item_key, 0))
                limit = max(int(off.daily_limit), 1)
                if bought >= limit:
                    await interaction.followup.send("Daily limit reached for that item.", ephemeral=True)
                    return

                price = int(off.price)
                if int(wallet.silver) < price:
                    await interaction.followup.send("Not enough silver.", ephemeral=True)
                    return

                wallet.silver = int(wallet.silver) - price
                if hasattr(wallet, "silver_spent"):
                    wallet.silver_spent = int(getattr(wallet, "silver_spent", 0)) + price

                row = await session.scalar(
                    select(ShopPurchaseRow).where(
                        ShopPurchaseRow.guild_id == guild_id,
                        ShopPurchaseRow.user_id == user_id,
                        ShopPurchaseRow.shop_day_id == day_id,
                        ShopPurchaseRow.item_key == off.item_key,
                    )
                )
                if row is None:
                    row = ShopPurchaseRow(
                        guild_id=guild_id,
                        user_id=user_id,
                        shop_day_id=day_id,
                        item_key=off.item_key,
                        qty=0,
                    )
                    session.add(row)
                    await session.flush()

                row.qty = int(row.qty) + 1

                await _add_item_to_inventory(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    item_key=off.item_key,
                    qty=1,
                )

                bought_name = off.name

        await interaction.followup.send(f"✅ Added **{bought_name or 'item'}** to your inventory.", ephemeral=True)
        await self._send_shop_panel(interaction)

    @shop.command(name="open", description="Open your daily shop offers (buttons).")
    async def shop_open_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return

        await interaction.response.defer(thinking=True, ephemeral=True)
        await self._send_shop_panel(interaction)

    @shop.command(name="list", description="Browse every shop item by rarity.")
    async def shop_list_cmd(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return

        grouped_items = _catalog_by_rarity()
        view = ShopListView(user_id=interaction.user.id, grouped_items=grouped_items)
        embed = view.build_embed()

        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()


async def setup(bot: commands.Bot):
    await bot.add_cog(ShopCog(bot))
