# cogs/shop.py
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import func, select

from db.models import ItemInventoryRow, ShopPurchaseRow, WalletRow
from services.db import sessions
from services.users import ensure_user_rows
from services.vip import is_vip_member

from services.items_catalog import ITEMS, ItemDef, ItemRarity
from services.jobs_core import clamp_int, fmt_int

# ============================================================
# Shop config
# ============================================================

SHOP_SLOTS = 5
SHOP_COMMON_SLOTS = 3
SHOP_UNCOMMON_SLOTS = 1
SHOP_RARE_SLOT = 1

MYTHICAL_UPGRADE_BASE = 0.02
MYTHICAL_UPGRADE_VIP = 0.03

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

    commons = pools[ItemRarity.COMMON.value]
    uncommons = pools[ItemRarity.UNCOMMON.value]
    rares = pools[ItemRarity.RARE.value]
    epics = pools[ItemRarity.EPIC.value]
    legendaries = pools[ItemRarity.LEGENDARY.value]
    mythicals = pools[ItemRarity.MYTHICAL.value]

    if len(commons) < SHOP_COMMON_SLOTS:
        raise RuntimeError("Not enough common items in catalog for shop")
    if len(uncommons) < SHOP_UNCOMMON_SLOTS:
        raise RuntimeError("Not enough uncommon items in catalog for shop")
    if len(rares) < SHOP_RARE_SLOT:
        raise RuntimeError("Not enough rare items in catalog for shop")
    if not mythicals:
        raise RuntimeError("No mythical items in catalog for shop")
    if not epics:
        epics = rares
    if not legendaries:
        legendaries = epics

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
    for _ in range(SHOP_COMMON_SLOTS):
        offers.append(_offer_from_item(_pick_unique(commons)))

    for _ in range(SHOP_UNCOMMON_SLOTS):
        offers.append(_offer_from_item(_pick_unique(uncommons)))

    rare_it = _pick_unique(rares)
    upgrade = MYTHICAL_UPGRADE_VIP if vip else MYTHICAL_UPGRADE_BASE
    roll = rng.random()
    if roll < float(upgrade):
        rare_it = _pick_unique(mythicals)
    elif roll < 0.08:
        rare_it = _pick_unique(legendaries)
    elif roll < 0.22:
        rare_it = _pick_unique(epics)

    offers.append(_offer_from_item(rare_it))
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


def _shop_embed(
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
    color = _rarity_color(offers)

    embed = discord.Embed(
        title=f"🛍️ {SHOP_TITLE}",
        description=f"_{SHOP_TAGLINE}_\n\n💰 **{fmt_int(wallet_silver)}** Silver • ⏳ Resets in **{reset_in}**",
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

        embed.add_field(
            name=f"{i}. {emoji} {off.name}  ·  {rarity}",
            value=f"{off.description}\n{price_line}",
            inline=False,
        )

    vip_line = "🔒 VIP Reroll: **Locked**"
    if vip:
        vip_line = f"👑 VIP Reroll: **{rerolls_used}/1 used**"
        vip_line += f" • Mythical upgrade: **{int(MYTHICAL_UPGRADE_VIP * 100)}%**"
    else:
        vip_line += f" • Mythical upgrade: **{int(MYTHICAL_UPGRADE_BASE * 100)}%**"

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

                embed = _shop_embed(
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
