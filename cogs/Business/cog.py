# cogs/Business/cog.py
from __future__ import annotations

"""
Business Cog

What this file does:
- Registers the /business slash command
- Builds and renders business-related embeds
- Handles Discord buttons and select menus
- Locks interactions so only the command user can use the panel
- Calls into .core for all business data and actions
- Refreshes views after buy / run / inspect actions

What this file does NOT do:
- It does not calculate business formulas
- It does not define the real business economy
- It does not handle hourly ticking / auto-payout logic
- It does not manage database row design directly
- It does not own worker or manager logic

What this file requires from other files in this package:

1) cogs/Business/core.py
This file must expose these names:

    BusinessActionResult
    BusinessCard
    BusinessDef
    BusinessHubSnapshot
    BusinessManageSnapshot
    buy_business
    fetch_business_defs
    get_business_hub_snapshot
    get_business_manage_snapshot
    start_business_run

2) cogs/Business/runtime.py
This file is not imported directly by this cog right now.
The expectation is that core.py may call runtime.py internally when needed.

3) services.db
Must provide:
- sessions()

4) services.users
Must provide:
- ensure_user_rows(...)
"""

from dataclasses import dataclass
from typing import List, Optional, Sequence

import discord
from discord import app_commands
from discord.ext import commands

from services.db import sessions
from services.users import ensure_user_rows

# =========================================================
# CORE CONTRACT IMPORTS
# =========================================================

try:
    from .core import (
        BusinessActionResult,
        BusinessCard,
        BusinessDef,
        BusinessHubSnapshot,
        BusinessManageSnapshot,
        ManagerAssignmentSlotSnapshot,
        WorkerAssignmentSlotSnapshot,
        buy_business,
        fetch_business_defs,
        get_business_hub_snapshot,
        get_business_manage_snapshot,
        get_manager_assignment_slots,
        get_worker_assignment_slots,
        hire_manager,
        hire_worker,
        remove_manager,
        remove_worker,
        start_business_run,
    )
except Exception:
    @dataclass(slots=True)
    class BusinessDef:
        key: str
        name: str
        emoji: str
        description: str
        cost_silver: int
        base_hourly_income: int
        base_upgrade_cost: int
        flavor: str = ""
        image_url: Optional[str] = None
        banner_url: Optional[str] = None

    @dataclass(slots=True)
    class BusinessCard:
        key: str
        name: str
        emoji: str
        owned: bool
        running: bool
        level: int
        prestige: int
        hourly_profit: int
        runtime_remaining_hours: int
        worker_slots_used: int
        worker_slots_total: int
        manager_slots_used: int
        manager_slots_total: int
        purchase_cost: int = 0
        image_url: Optional[str] = None

    @dataclass(slots=True)
    class BusinessHubSnapshot:
        silver_balance: int
        owned_count: int
        total_count: int
        total_hourly_income_active: int
        active_count: int
        cards: List[BusinessCard]

    @dataclass(slots=True)
    class BusinessManageSnapshot:
        key: str
        name: str
        emoji: str
        description: str
        flavor: str
        owned: bool
        running: bool
        level: int
        prestige: int
        hourly_profit: int
        base_hourly_income: int
        upgrade_cost: Optional[int]
        runtime_remaining_hours: int
        total_runtime_hours: int
        worker_slots_used: int
        worker_slots_total: int
        manager_slots_used: int
        manager_slots_total: int
        image_url: Optional[str] = None
        banner_url: Optional[str] = None
        notes: Optional[List[str]] = None

    @dataclass(slots=True)
    class BusinessActionResult:
        ok: bool
        message: str
        snapshot: Optional[BusinessHubSnapshot] = None
        manage_snapshot: Optional[BusinessManageSnapshot] = None
    @dataclass(slots=True)
    class WorkerAssignmentSlotSnapshot:
        slot_index: int
        assignment_id: Optional[int]
        worker_name: Optional[str]
        worker_type: Optional[str]
        rarity: Optional[str]
        flat_profit_bonus: int
        percent_profit_bonus_bp: int
        is_active: bool

    @dataclass(slots=True)
    class ManagerAssignmentSlotSnapshot:
        slot_index: int
        assignment_id: Optional[int]
        manager_name: Optional[str]
        rarity: Optional[str]
        runtime_bonus_hours: int
        profit_bonus_bp: int
        auto_restart_charges: int
        is_active: bool


    async def fetch_business_defs(session) -> Sequence[BusinessDef]:
        _ = session
        return [
            BusinessDef(
                key="restaurant",
                name="Restaurant",
                emoji="🍽️",
                description="A balanced starter business with clean, dependable income.",
                cost_silver=100_000,
                base_hourly_income=1_000,
                base_upgrade_cost=25_000,
                flavor="Your first proper business. Classy, simple, and steady.",
            ),
            BusinessDef(
                key="farm",
                name="Farm",
                emoji="🌾",
                description="Stable income with a calm, low-drama vibe.",
                cost_silver=250_000,
                base_hourly_income=1_800,
                base_upgrade_cost=60_000,
                flavor="Quiet money. Dirt, sweat, and a suspicious number of chickens.",
            ),
            BusinessDef(
                key="nightclub",
                name="Nightclub",
                emoji="🪩",
                description="Swingy profits and louder nights.",
                cost_silver=600_000,
                base_hourly_income=3_500,
                base_upgrade_cost=150_000,
                flavor="Half the money comes from vibes. The other half from poor decisions.",
            ),
            BusinessDef(
                key="factory",
                name="Factory",
                emoji="🏭",
                description="A heavy-output business built around production.",
                cost_silver=1_200_000,
                base_hourly_income=6_500,
                base_upgrade_cost=300_000,
                flavor="Loud, efficient, and probably violating something somewhere.",
            ),
            BusinessDef(
                key="casino",
                name="Casino",
                emoji="🎰",
                description="Big money, big risk, big gremlin energy.",
                cost_silver=2_500_000,
                base_hourly_income=10_000,
                base_upgrade_cost=625_000,
                flavor="A machine that legally weaponizes temptation.",
            ),
            BusinessDef(
                key="tech_company",
                name="Tech Company",
                emoji="💻",
                description="An upgrade-focused scaling business.",
                cost_silver=5_000_000,
                base_hourly_income=18_000,
                base_upgrade_cost=1_250_000,
                flavor="Buzzwords, dashboards, and someone definitely saying synergy unironically.",
            ),
            BusinessDef(
                key="shipping_company",
                name="Shipping Company",
                emoji="🚢",
                description="Long-haul profits built for extended operation.",
                cost_silver=9_000_000,
                base_hourly_income=30_000,
                base_upgrade_cost=2_250_000,
                flavor="Massive cargo, massive delays, massive invoices.",
            ),
            BusinessDef(
                key="hotel",
                name="Hotel",
                emoji="🏨",
                description="A staff-heavy business with lots of moving parts.",
                cost_silver=15_000_000,
                base_hourly_income=45_000,
                base_upgrade_cost=3_750_000,
                flavor="Customer service with a smile and a silent internal scream.",
            ),
            BusinessDef(
                key="movie_studio",
                name="Movie Studio",
                emoji="🎬",
                description="A flashy business driven by hype and momentum.",
                cost_silver=25_000_000,
                base_hourly_income=70_000,
                base_upgrade_cost=6_250_000,
                flavor="Drama in front of the camera and ten times more behind it.",
            ),
            BusinessDef(
                key="space_mining",
                name="Space Mining Company",
                emoji="🛰️",
                description="An absurd late-game income machine.",
                cost_silver=50_000_000,
                base_hourly_income=120_000,
                base_upgrade_cost=12_500_000,
                flavor="Mining rocks in space because Earth was apparently too easy.",
            ),
        ]

    async def get_business_hub_snapshot(session, *, guild_id: int, user_id: int) -> BusinessHubSnapshot:
        _ = session, guild_id, user_id
        defs = await fetch_business_defs(session)
        cards = [
            BusinessCard(
                key=d.key,
                name=d.name,
                emoji=d.emoji,
                owned=False,
                running=False,
                level=0,
                prestige=0,
                hourly_profit=d.base_hourly_income,
                runtime_remaining_hours=0,
                worker_slots_used=0,
                worker_slots_total=0,
                manager_slots_used=0,
                manager_slots_total=0,
                purchase_cost=d.cost_silver,
                image_url=d.image_url,
            )
            for d in defs
        ]
        return BusinessHubSnapshot(
            silver_balance=0,
            owned_count=0,
            total_count=len(cards),
            total_hourly_income_active=0,
            active_count=0,
            cards=cards,
        )

    async def get_business_manage_snapshot(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
    ) -> Optional[BusinessManageSnapshot]:
        _ = guild_id, user_id
        defs = await fetch_business_defs(session)
        d = next((x for x in defs if x.key == business_key), None)
        if d is None:
            return None
        return BusinessManageSnapshot(
            key=d.key,
            name=d.name,
            emoji=d.emoji,
            description=d.description,
            flavor=d.flavor,
            owned=False,
            running=False,
            level=0,
            prestige=0,
            hourly_profit=d.base_hourly_income,
            base_hourly_income=d.base_hourly_income,
            upgrade_cost=d.base_upgrade_cost,
            runtime_remaining_hours=0,
            total_runtime_hours=4,
            worker_slots_used=0,
            worker_slots_total=0,
            manager_slots_used=0,
            manager_slots_total=0,
            image_url=d.image_url,
            banner_url=d.banner_url,
            notes=[
                "core.py is not wired yet.",
                "This is fallback preview data from cog.py.",
            ],
        )

    async def buy_business(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
    ) -> BusinessActionResult:
        _ = session, guild_id, user_id, business_key
        return BusinessActionResult(
            ok=False,
            message="Business services are not wired yet. Build buy_business(...) in cogs/Business/core.py.",
        )

    async def start_business_run(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
    ) -> BusinessActionResult:
        _ = session, guild_id, user_id, business_key
        return BusinessActionResult(
            ok=False,
            message="Business services are not wired yet. Build start_business_run(...) in cogs/Business/core.py.",
        )


# =========================================================
# CONSTANTS
# =========================================================

VIEW_TIMEOUT = 180
EMBED_COLOR = discord.Color.from_rgb(88, 101, 242)
SUCCESS_COLOR = discord.Color.green()
ERROR_COLOR = discord.Color.red()


# =========================================================
# FORMATTERS
# =========================================================

def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _status_badge(running: bool, owned: bool) -> str:
    if not owned:
        return "🔒 Locked"
    if running:
        return "🟢 Running"
    return "⚪ Idle"


def _slot_text(used: int, total: int) -> str:
    return f"{_fmt_int(used)}/{_fmt_int(total)}"


def _safe_str(v: object, fallback: str = "Unknown") -> str:
    try:
        s = str(v).strip()
        return s or fallback
    except Exception:
        return fallback


def _parse_int(value: str, default: int = 0) -> int:
    text = str(value).strip()
    if not text:
        return int(default)
    sign = -1 if text.startswith("-") else 1
    digits = text[1:] if text.startswith("-") else text
    if not digits.isdigit():
        return int(default)
    return sign * int(digits)


def _trim(s: str, limit: int) -> str:
    if len(s) <= limit:
        return s
    if limit <= 3:
        return s[:limit]
    return s[: limit - 3] + "..."


def _showcase_image_from_cards(cards: Sequence[BusinessCard]) -> Optional[str]:
    running_owned = [c for c in cards if c.owned and c.running and c.image_url]
    if running_owned:
        return running_owned[0].image_url

    owned = [c for c in cards if c.owned and c.image_url]
    if owned:
        return owned[0].image_url

    locked = [c for c in cards if (not c.owned) and c.image_url]
    if locked:
        return locked[0].image_url

    return None


def _showcase_image_from_defs(defs: Sequence[BusinessDef]) -> Optional[str]:
    for d in defs:
        if d.image_url:
            return d.image_url
    return None


# =========================================================
# EMBED BUILDERS
# =========================================================

def _base_embed(*, title: str, description: str, color: discord.Color = EMBED_COLOR) -> discord.Embed:
    e = discord.Embed(title=title, description=description, color=color)
    e.set_footer(text="Business System • Chatbox Economy")
    return e


def _build_hub_embed(
    *,
    user: discord.abc.User,
    snap: BusinessHubSnapshot,
) -> discord.Embed:
    owned_cards = [c for c in snap.cards if c.owned]

    desc = (
        f"💰 **Silver:** `{_fmt_int(snap.silver_balance)}`\n"
        f"📈 **Active Income:** `{_fmt_int(snap.total_hourly_income_active)}/hr`\n"
        f"🏢 **Businesses Owned:** `{_fmt_int(snap.owned_count)}`"
    )

    e = _base_embed(title="🏢 Your Businesses", description=desc)
    e.set_author(
        name=_safe_str(user),
        icon_url=getattr(getattr(user, "display_avatar", None), "url", None),
    )

    showcase = _showcase_image_from_cards(owned_cards)
    if showcase:
        e.set_thumbnail(url=showcase)

    if not owned_cards:
        e.add_field(
            name="No Businesses Yet",
            value=(
                "You don’t own any businesses yet.\n"
                "Use **🛒 Buy** to get started."
            ),
            inline=False,
        )
        return e

    lines: list[str] = []
    for c in owned_cards[:10]:
        runtime_txt = f"{_fmt_int(c.runtime_remaining_hours)}h left" if c.running else "Not running"
        lines.append(
            f"{c.emoji} **{c.name}**\n"
            f"└ {_status_badge(c.running, c.owned)} • `{_fmt_int(c.hourly_profit)}/hr`\n"
            f"└ ⏱️ {runtime_txt}"
        )

    e.add_field(
        name="Your Business List",
        value="\n\n".join(lines),
        inline=False,
    )

    return e


def _build_buy_menu_embed(
    *,
    user: discord.abc.User,
    defs: Sequence[BusinessDef],
    snap: BusinessHubSnapshot,
) -> discord.Embed:
    available = [d for d in defs if not any(c.key == d.key and c.owned for c in snap.cards)]

    desc = (
        f"💰 **Your Silver:** `{_fmt_int(snap.silver_balance)}`\n"
        "Choose a business to buy."
    )
    e = _base_embed(title="🛒 Buy Business", description=desc)
    e.set_author(
        name=_safe_str(user),
        icon_url=getattr(getattr(user, "display_avatar", None), "url", None),
    )

    showcase = _showcase_image_from_defs(available)
    if showcase:
        e.set_thumbnail(url=showcase)

    if not available:
        e.add_field(
            name="All Businesses Owned",
            value="You already own every available business.",
            inline=False,
        )
        return e

    lines: list[str] = []
    for d in available[:25]:
        lines.append(
            f"{d.emoji} **{d.name}**\n"
            f"└ Cost: `{_fmt_int(d.cost_silver)}` Silver\n"
            f"└ Income: `{_fmt_int(d.base_hourly_income)}/hr`"
        )

    e.add_field(name="Available Businesses", value="\n\n".join(lines), inline=False)
    return e


def _build_run_menu_embed(
    *,
    user: discord.abc.User,
    snap: BusinessHubSnapshot,
) -> discord.Embed:
    owned = [c for c in snap.cards if c.owned]

    desc = "Choose one of your businesses to start running."
    e = _base_embed(title="▶️ Run Business", description=desc)
    e.set_author(
        name=_safe_str(user),
        icon_url=getattr(getattr(user, "display_avatar", None), "url", None),
    )

    showcase = _showcase_image_from_cards(owned)
    if showcase:
        e.set_thumbnail(url=showcase)

    if not owned:
        e.add_field(
            name="Nothing To Run",
            value="You do not own any businesses yet.",
            inline=False,
        )
        return e

    lines: list[str] = []
    for c in owned[:25]:
        runtime_txt = f"{_fmt_int(c.runtime_remaining_hours)}h left" if c.running else "Ready to start"
        lines.append(
            f"{c.emoji} **{c.name}**\n"
            f"└ {_status_badge(c.running, c.owned)}\n"
            f"└ Income: `{_fmt_int(c.hourly_profit)}/hr`\n"
            f"└ ⏱️ {runtime_txt}"
        )

    e.add_field(name="Owned Businesses", value="\n\n".join(lines), inline=False)
    return e


def _build_manage_menu_embed(
    *,
    user: discord.abc.User,
    snap: BusinessHubSnapshot,
) -> discord.Embed:
    owned = [c for c in snap.cards if c.owned]

    desc = "Select a business to view upgrades, staffing, and details."
    e = _base_embed(title="🛠️ Manage Businesses", description=desc)
    e.set_author(
        name=_safe_str(user),
        icon_url=getattr(getattr(user, "display_avatar", None), "url", None),
    )

    showcase = _showcase_image_from_cards(owned)
    if showcase:
        e.set_thumbnail(url=showcase)

    if not owned:
        e.add_field(
            name="Nothing To Manage",
            value="You do not own any businesses yet.",
            inline=False,
        )
        return e

    lines: list[str] = []
    for c in owned[:25]:
        lines.append(
            f"{c.emoji} **{c.name}**\n"
            f"└ Level `{_fmt_int(c.level)}` • Prestige `{_fmt_int(c.prestige)}`\n"
            f"└ Workers `{_slot_text(c.worker_slots_used, c.worker_slots_total)}` • Managers `{_slot_text(c.manager_slots_used, c.manager_slots_total)}`"
        )

    e.add_field(name="Your Businesses", value="\n\n".join(lines), inline=False)
    return e


def _build_business_detail_embed(
    *,
    user: discord.abc.User,
    snap: BusinessManageSnapshot,
) -> discord.Embed:
    owned_badge = "✅ Owned" if snap.owned else "🔒 Locked"
    running_badge = "🟢 Running" if snap.running else "⚪ Idle"

    desc = (
        f"{snap.emoji} **{snap.name}**\n"
        f"{_safe_str(snap.description)}\n\n"
        f"🏷️ **Status:** {owned_badge} • {running_badge}\n"
        f"📈 **Current Hourly Profit:** `{_fmt_int(snap.hourly_profit)}/hr`\n"
        f"💼 **Base Hourly Profit:** `{_fmt_int(snap.base_hourly_income)}/hr`\n"
        f"⏱️ **Runtime:** `{_fmt_int(snap.runtime_remaining_hours)}h` remaining / `{_fmt_int(snap.total_runtime_hours)}h` total"
    )

    e = _base_embed(title=f"{snap.emoji} Business Details", description=desc)
    e.set_author(name=_safe_str(user), icon_url=getattr(getattr(user, "display_avatar", None), "url", None))

    e.add_field(
        name="Progress",
        value=(
            f"🏗️ **Level:** `{_fmt_int(snap.level)}`\n"
            f"✨ **Prestige:** `{_fmt_int(snap.prestige)}`\n"
            f"⬆️ **Upgrade Cost:** `{_fmt_int(snap.upgrade_cost or 0)}` Silver"
        ),
        inline=True,
    )
    e.add_field(
        name="Staffing",
        value=(
            f"👷 **Workers:** `{_slot_text(snap.worker_slots_used, snap.worker_slots_total)}`\n"
            f"🧑‍💼 **Managers:** `{_slot_text(snap.manager_slots_used, snap.manager_slots_total)}`"
        ),
        inline=True,
    )
    e.add_field(
        name="Business Flavor",
        value=_safe_str(snap.flavor, "No flavor text configured yet."),
        inline=False,
    )

    if snap.notes:
        e.add_field(
            name="Notes",
            value="\n".join(f"• {x}" for x in snap.notes[:10]),
            inline=False,
        )

    if snap.banner_url:
        e.set_image(url=snap.banner_url)
    elif snap.image_url:
        e.set_thumbnail(url=snap.image_url)

    return e



def _build_worker_assignments_embed(
    *,
    user: discord.abc.User,
    detail: BusinessManageSnapshot,
    slots: Sequence[WorkerAssignmentSlotSnapshot],
) -> discord.Embed:
    e = _base_embed(title=f"👷 Worker Assignments • {detail.emoji} {detail.name}")
    e.set_author(name=_safe_str(user), icon_url=getattr(getattr(user, "display_avatar", None), "url", None))
    lines: list[str] = []
    for slot in slots:
        if slot.is_active:
            lines.append(
                f"`#{slot.slot_index}` **{_safe_str(slot.worker_name, 'Worker')}** ({_safe_str(slot.rarity, 'common')})\n"
                f"└ type `{_safe_str(slot.worker_type, 'efficient')}` • +{_fmt_int(slot.flat_profit_bonus)} flat • +{_fmt_int(slot.percent_profit_bonus_bp)} bp"
            )
        else:
            lines.append(f"`#{slot.slot_index}` *(empty)*")
    e.description = (
        f"Slots in use: `{_slot_text(detail.worker_slots_used, detail.worker_slots_total)}`\n"
        "Use **Hire Worker** to fill a free slot or **Remove Worker** to deactivate an assigned worker."
    )
    e.add_field(name="Slots", value="\n\n".join(lines) if lines else "No worker slots unlocked.", inline=False)
    return e


def _build_manager_assignments_embed(
    *,
    user: discord.abc.User,
    detail: BusinessManageSnapshot,
    slots: Sequence[ManagerAssignmentSlotSnapshot],
) -> discord.Embed:
    e = _base_embed(title=f"🧑‍💼 Manager Assignments • {detail.emoji} {detail.name}")
    e.set_author(name=_safe_str(user), icon_url=getattr(getattr(user, "display_avatar", None), "url", None))
    lines: list[str] = []
    for slot in slots:
        if slot.is_active:
            lines.append(
                f"`#{slot.slot_index}` **{_safe_str(slot.manager_name, 'Manager')}** ({_safe_str(slot.rarity, 'common')})\n"
                f"└ +{_fmt_int(slot.runtime_bonus_hours)}h runtime • +{_fmt_int(slot.profit_bonus_bp)} bp • auto `{_fmt_int(slot.auto_restart_charges)}`"
            )
        else:
            lines.append(f"`#{slot.slot_index}` *(empty)*")
    e.description = (
        f"Slots in use: `{_slot_text(detail.manager_slots_used, detail.manager_slots_total)}`\n"
        "Use **Hire Manager** to fill a free slot or **Remove Manager** to deactivate an assigned manager."
    )
    e.add_field(name="Slots", value="\n\n".join(lines) if lines else "No manager slots unlocked.", inline=False)
    return e


def _build_result_embed(*, title: str, message: str, ok: bool) -> discord.Embed:
    return _base_embed(
        title=("✅ " if ok else "❌ ") + title,
        description=message,
        color=SUCCESS_COLOR if ok else ERROR_COLOR,
    )


# =========================================================
# VIEW BASE
# =========================================================

class BusinessBaseView(discord.ui.View):
    def __init__(self, *, cog: "BusinessCog", owner_id: int, guild_id: int):
        super().__init__(timeout=VIEW_TIMEOUT)
        self.cog = cog
        self.owner_id = int(owner_id)
        self.guild_id = int(guild_id)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.owner_id:
            await interaction.response.send_message(
                "This business panel belongs to someone else.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self) -> None:
        for item in self.children:
            try:
                item.disabled = True
            except Exception:
                pass


# =========================================================
# SELECT MENUS
# =========================================================

class BuyBusinessSelect(discord.ui.Select):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        defs: Sequence[BusinessDef],
        snap: BusinessHubSnapshot,
    ):
        self.cog = cog
        self.owner_id = owner_id
        self.guild_id = guild_id

        owned_keys = {c.key for c in snap.cards if c.owned}
        available = [d for d in defs if d.key not in owned_keys]

        options: list[discord.SelectOption] = []
        for d in available[:25]:
            options.append(
                discord.SelectOption(
                    label=_trim(d.name, 100),
                    value=d.key,
                    description=_trim(f"Cost {_fmt_int(d.cost_silver)} • {_fmt_int(d.base_hourly_income)}/hr", 100),
                    emoji=d.emoji,
                )
            )

        if not options:
            options.append(
                discord.SelectOption(
                    label="No businesses available",
                    value="__none__",
                    description="You already own all businesses.",
                    emoji="✅",
                )
            )

        super().__init__(
            placeholder="Choose a business to buy...",
            min_values=1,
            max_values=1,
            options=options,
            disabled=(len(available) == 0),
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not self.values:
            await interaction.response.send_message("No business selected.", ephemeral=True)
            return

        picked = self.values[0]
        if picked == "__none__":
            await interaction.response.send_message("There is nothing left to buy.", ephemeral=True)
            return

        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)

                result = await buy_business(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=picked,
                )

                defs = await fetch_business_defs(session)
                hub = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_result_embed(
            title="Business Purchase",
            message=result.message,
            ok=result.ok,
        )
        view = BuyBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            defs=defs,
            hub_snapshot=hub,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )


class RunBusinessSelect(discord.ui.Select):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        snap: BusinessHubSnapshot,
    ):
        self.cog = cog
        self.owner_id = owner_id
        self.guild_id = guild_id

        owned = [c for c in snap.cards if c.owned]

        options: list[discord.SelectOption] = []
        for c in owned[:25]:
            desc = f"{_status_badge(c.running, c.owned)} • Profit {_fmt_int(c.hourly_profit)}/hr"
            options.append(
                discord.SelectOption(
                    label=_trim(c.name, 100),
                    value=c.key,
                    description=_trim(desc, 100),
                    emoji=c.emoji,
                )
            )

        if not options:
            options.append(
                discord.SelectOption(
                    label="No owned businesses",
                    value="__none__",
                    description="Buy a business first.",
                    emoji="🔒",
                )
            )

        super().__init__(
            placeholder="Choose a business to run...",
            min_values=1,
            max_values=1,
            options=options,
            disabled=(len(owned) == 0),
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not self.values:
            await interaction.response.send_message("No business selected.", ephemeral=True)
            return

        picked = self.values[0]
        if picked == "__none__":
            await interaction.response.send_message("You do not own a business yet.", ephemeral=True)
            return

        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)

                result = await start_business_run(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=picked,
                )
                hub = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_result_embed(
            title="Run Business",
            message=result.message,
            ok=result.ok,
        )
        view = RunBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=hub,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )


class ManageBusinessSelect(discord.ui.Select):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        snap: BusinessHubSnapshot,
    ):
        self.cog = cog
        self.owner_id = owner_id
        self.guild_id = guild_id

        owned = [c for c in snap.cards if c.owned]

        options: list[discord.SelectOption] = []
        for c in owned[:25]:
            options.append(
                discord.SelectOption(
                    label=_trim(c.name, 100),
                    value=c.key,
                    description=_trim(f"Lvl {_fmt_int(c.level)} • {_fmt_int(c.hourly_profit)}/hr • {_status_badge(c.running, c.owned)}", 100),
                    emoji=c.emoji,
                )
            )

        if not options:
            options.append(
                discord.SelectOption(
                    label="No owned businesses",
                    value="__none__",
                    description="Buy a business first.",
                    emoji="🔒",
                )
            )

        super().__init__(
            placeholder="Choose a business to manage...",
            min_values=1,
            max_values=1,
            options=options,
            disabled=(len(owned) == 0),
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not self.values:
            await interaction.response.send_message("No business selected.", ephemeral=True)
            return

        picked = self.values[0]
        if picked == "__none__":
            await interaction.response.send_message("You do not own a business yet.", ephemeral=True)
            return

        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)

                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=picked,
                )

        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return

        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        view = BusinessDetailView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            business_key=picked,
            owned=detail.owned,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )


# =========================================================
# VIEWS
# =========================================================

class BusinessHubView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        hub_snapshot: BusinessHubSnapshot,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.hub_snapshot = hub_snapshot
        self._configure_buttons()

    def _configure_buttons(self) -> None:
        owns_all = self.hub_snapshot.total_count > 0 and self.hub_snapshot.owned_count >= self.hub_snapshot.total_count
        self.buy_button.disabled = owns_all
        self.run_button.disabled = self.hub_snapshot.owned_count <= 0
        self.manage_button.disabled = self.hub_snapshot.owned_count <= 0

    @discord.ui.button(label="Buy", style=discord.ButtonStyle.success, emoji="🛒", row=0)
    async def buy_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                defs = await fetch_business_defs(session)
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_buy_menu_embed(user=interaction.user, defs=defs, snap=snap)
        view = BuyBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            defs=defs,
            hub_snapshot=snap,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )

    @discord.ui.button(label="Run", style=discord.ButtonStyle.primary, emoji="▶️", row=0)
    async def run_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_run_menu_embed(user=interaction.user, snap=snap)
        view = RunBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )

    @discord.ui.button(label="Manage", style=discord.ButtonStyle.secondary, emoji="🛠️", row=0)
    async def manage_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_manage_menu_embed(user=interaction.user, snap=snap)
        view = ManageBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=1)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_hub_embed(user=interaction.user, snap=snap)
        view = BusinessHubView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )


class BuyBusinessView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        defs: Sequence[BusinessDef],
        hub_snapshot: BusinessHubSnapshot,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.defs = list(defs)
        self.hub_snapshot = hub_snapshot
        self.add_item(
            BuyBusinessSelect(
                cog=cog,
                owner_id=owner_id,
                guild_id=guild_id,
                defs=self.defs,
                snap=hub_snapshot,
            )
        )

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji="⬅️", row=2)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_hub_embed(user=interaction.user, snap=snap)
        view = BusinessHubView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=2)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                defs = await fetch_business_defs(session)
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_buy_menu_embed(user=interaction.user, defs=defs, snap=snap)
        view = BuyBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            defs=defs,
            hub_snapshot=snap,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )


class RunBusinessView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        hub_snapshot: BusinessHubSnapshot,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.hub_snapshot = hub_snapshot
        self.add_item(
            RunBusinessSelect(
                cog=cog,
                owner_id=owner_id,
                guild_id=guild_id,
                snap=hub_snapshot,
            )
        )

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji="⬅️", row=2)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_hub_embed(user=interaction.user, snap=snap)
        view = BusinessHubView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=2)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_run_menu_embed(user=interaction.user, snap=snap)
        view = RunBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )


class ManageBusinessView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        hub_snapshot: BusinessHubSnapshot,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.hub_snapshot = hub_snapshot
        self.add_item(
            ManageBusinessSelect(
                cog=cog,
                owner_id=owner_id,
                guild_id=guild_id,
                snap=hub_snapshot,
            )
        )

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji="⬅️", row=2)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_hub_embed(user=interaction.user, snap=snap)
        view = BusinessHubView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=2)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_manage_menu_embed(user=interaction.user, snap=snap)
        view = ManageBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=snap,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )


class BusinessDetailView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        business_key: str,
        owned: bool = False,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.business_key = business_key
        self.upgrade_button.disabled = not owned
        self.workers_button.disabled = not owned
        self.managers_button.disabled = not owned

    @discord.ui.button(label="Run", style=discord.ButtonStyle.success, emoji="▶️", row=0)
    async def run_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await start_business_run(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                )
                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                )

        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return

        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        if result.message:
            embed.add_field(
                name="Action Result",
                value=("✅ " if result.ok else "❌ ") + result.message,
                inline=False,
            )

        view = BusinessDetailView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            business_key=self.business_key,
            owned=detail.owned,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=0)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                )

        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return

        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        view = BusinessDetailView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            business_key=self.business_key,
            owned=detail.owned,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji="⬅️", row=0)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                hub = await get_business_hub_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                )

        embed = _build_manage_menu_embed(user=interaction.user, snap=hub)
        view = ManageBusinessView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            hub_snapshot=hub,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=view,
        )

    @discord.ui.button(label="Upgrade", style=discord.ButtonStyle.primary, emoji="⬆️", row=1, disabled=True)
    async def upgrade_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.send_message(
            "Upgrade flow is not wired yet. That belongs in core.py next.",
            ephemeral=True,
        )

    @discord.ui.button(label="Employees", style=discord.ButtonStyle.secondary, emoji="👷", row=1, disabled=True)
    async def workers_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                )
                slots = await get_worker_assignment_slots(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                )
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        view = WorkerAssignmentsView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            business_key=self.business_key,
        )
        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=view)

    @discord.ui.button(label="Managers", style=discord.ButtonStyle.secondary, emoji="🧑‍💼", row=1, disabled=True)
    async def managers_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()

        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                )
                slots = await get_manager_assignment_slots(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                )
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        view = ManagerAssignmentsView(
            cog=self.cog,
            owner_id=self.owner_id,
            guild_id=self.guild_id,
            business_key=self.business_key,
        )
        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=view)


class HireWorkerModal(discord.ui.Modal, title="Hire Worker"):
    def __init__(self, view: "WorkerAssignmentsView"):
        super().__init__()
        self.view = view
        self.worker_name = discord.ui.TextInput(label="Worker Name", max_length=64)
        self.worker_type = discord.ui.TextInput(label="Worker Type (fast/efficient/kind)", default="efficient", max_length=16)
        self.rarity = discord.ui.TextInput(label="Rarity", default="common", max_length=16)
        self.flat_bonus = discord.ui.TextInput(label="Flat Profit Bonus", default="0", max_length=10)
        self.bp_bonus = discord.ui.TextInput(label="Percent Bonus (bp)", default="0", max_length=10)
        for item in (self.worker_name, self.worker_type, self.rarity, self.flat_bonus, self.bp_bonus):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        async with self.view.cog.sessionmaker() as session:
            async with session.begin():
                result = await hire_worker(
                    session,
                    guild_id=self.view.guild_id,
                    user_id=self.view.owner_id,
                    business_key=self.view.business_key,
                    worker_name=str(self.worker_name.value),
                    worker_type=str(self.worker_type.value),
                    rarity=str(self.rarity.value),
                    flat_profit_bonus=_parse_int(str(self.flat_bonus.value), 0),
                    percent_profit_bonus_bp=_parse_int(str(self.bp_bonus.value), 0),
                )
                detail = await get_business_manage_snapshot(session, guild_id=self.view.guild_id, user_id=self.view.owner_id, business_key=self.view.business_key)
                slots = await get_worker_assignment_slots(session, guild_id=self.view.guild_id, user_id=self.view.owner_id, business_key=self.view.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        embed.add_field(name="Action", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self.view)


class RemoveWorkerModal(discord.ui.Modal, title="Remove Worker"):
    def __init__(self, view: "WorkerAssignmentsView"):
        super().__init__()
        self.view = view
        self.slot_index = discord.ui.TextInput(label="Slot Index", placeholder="1", max_length=4)
        self.add_item(self.slot_index)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        async with self.view.cog.sessionmaker() as session:
            async with session.begin():
                result = await remove_worker(
                    session,
                    guild_id=self.view.guild_id,
                    user_id=self.view.owner_id,
                    business_key=self.view.business_key,
                    slot_index=_parse_int(str(self.slot_index.value), 0),
                )
                detail = await get_business_manage_snapshot(session, guild_id=self.view.guild_id, user_id=self.view.owner_id, business_key=self.view.business_key)
                slots = await get_worker_assignment_slots(session, guild_id=self.view.guild_id, user_id=self.view.owner_id, business_key=self.view.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        embed.add_field(name="Action", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self.view)


class HireManagerModal(discord.ui.Modal, title="Hire Manager"):
    def __init__(self, view: "ManagerAssignmentsView"):
        super().__init__()
        self.view = view
        self.manager_name = discord.ui.TextInput(label="Manager Name", max_length=64)
        self.rarity = discord.ui.TextInput(label="Rarity", default="common", max_length=16)
        self.runtime_bonus = discord.ui.TextInput(label="Runtime Bonus Hours", default="0", max_length=8)
        self.bp_bonus = discord.ui.TextInput(label="Profit Bonus (bp)", default="0", max_length=10)
        self.auto_restart = discord.ui.TextInput(label="Auto Restart Charges", default="0", max_length=8)
        for item in (self.manager_name, self.rarity, self.runtime_bonus, self.bp_bonus, self.auto_restart):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        async with self.view.cog.sessionmaker() as session:
            async with session.begin():
                result = await hire_manager(
                    session,
                    guild_id=self.view.guild_id,
                    user_id=self.view.owner_id,
                    business_key=self.view.business_key,
                    manager_name=str(self.manager_name.value),
                    rarity=str(self.rarity.value),
                    runtime_bonus_hours=_parse_int(str(self.runtime_bonus.value), 0),
                    profit_bonus_bp=_parse_int(str(self.bp_bonus.value), 0),
                    auto_restart_charges=_parse_int(str(self.auto_restart.value), 0),
                )
                detail = await get_business_manage_snapshot(session, guild_id=self.view.guild_id, user_id=self.view.owner_id, business_key=self.view.business_key)
                slots = await get_manager_assignment_slots(session, guild_id=self.view.guild_id, user_id=self.view.owner_id, business_key=self.view.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        embed.add_field(name="Action", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self.view)


class RemoveManagerModal(discord.ui.Modal, title="Remove Manager"):
    def __init__(self, view: "ManagerAssignmentsView"):
        super().__init__()
        self.view = view
        self.slot_index = discord.ui.TextInput(label="Slot Index", placeholder="1", max_length=4)
        self.add_item(self.slot_index)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        async with self.view.cog.sessionmaker() as session:
            async with session.begin():
                result = await remove_manager(
                    session,
                    guild_id=self.view.guild_id,
                    user_id=self.view.owner_id,
                    business_key=self.view.business_key,
                    slot_index=_parse_int(str(self.slot_index.value), 0),
                )
                detail = await get_business_manage_snapshot(session, guild_id=self.view.guild_id, user_id=self.view.owner_id, business_key=self.view.business_key)
                slots = await get_manager_assignment_slots(session, guild_id=self.view.guild_id, user_id=self.view.owner_id, business_key=self.view.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        embed.add_field(name="Action", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self.view)


class WorkerAssignmentsView(BusinessBaseView):
    def __init__(self, *, cog: "BusinessCog", owner_id: int, guild_id: int, business_key: str):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.business_key = business_key

    @discord.ui.button(label="Hire Worker", style=discord.ButtonStyle.success, emoji="➕", row=0)
    async def hire_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.send_modal(HireWorkerModal(self))

    @discord.ui.button(label="Remove Worker", style=discord.ButtonStyle.danger, emoji="➖", row=0)
    async def remove_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.send_modal(RemoveWorkerModal(self))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji="⬅️", row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned)
        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=view)


class ManagerAssignmentsView(BusinessBaseView):
    def __init__(self, *, cog: "BusinessCog", owner_id: int, guild_id: int, business_key: str):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.business_key = business_key

    @discord.ui.button(label="Hire Manager", style=discord.ButtonStyle.success, emoji="➕", row=0)
    async def hire_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.send_modal(HireManagerModal(self))

    @discord.ui.button(label="Remove Manager", style=discord.ButtonStyle.danger, emoji="➖", row=0)
    async def remove_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.send_modal(RemoveManagerModal(self))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji="⬅️", row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer()
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned)
        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=view)


# =========================================================
# COG
# =========================================================

class BusinessCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    async def _build_hub_for_user(
        self,
        *,
        guild_id: int,
        user_id: int,
    ) -> BusinessHubSnapshot:
        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)
                snap = await get_business_hub_snapshot(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                )
        return snap

    @app_commands.command(name="business", description="Open your business management hub.")
    async def business_cmd(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return

        guild_id = int(interaction.guild.id)
        user_id = int(interaction.user.id)

        await interaction.response.defer(thinking=True)

        try:
            snap = await self._build_hub_for_user(
                guild_id=guild_id,
                user_id=user_id,
            )
        except Exception as e:
            embed = _build_result_embed(
                title="Business Hub",
                message=f"Failed to load the business hub.\n```py\n{type(e).__name__}: {e}\n```",
                ok=False,
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        embed = _build_hub_embed(user=interaction.user, snap=snap)
        view = BusinessHubView(
            cog=self,
            owner_id=user_id,
            guild_id=guild_id,
            hub_snapshot=snap,
        )
        await interaction.followup.send(embed=embed, view=view)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(BusinessCog(bot))