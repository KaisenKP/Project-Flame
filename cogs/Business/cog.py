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
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import List, Optional, Sequence

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select

from db.models import BusinessOwnershipRow, WalletRow
from services.db import sessions
from services.achievements import check_and_grant_achievements, queue_achievement_announcements
from services.users import ensure_user_rows
from services.vip import is_vip_member
from .runtime import BusinessRuntimeEngine, CompletedRunNotice

log = logging.getLogger(__name__)

AUTO_HIRE_MAX_REROLLS = 250
AUTO_HIRE_ALLOWED_RARITIES = {"common", "uncommon", "rare", "epic", "mythic"}

_BUSINESS_RUNTIME_STATE_PATH = Path("data/business_runtime_state.json")

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
        HiredManagerSnapshot,
        HiredWorkerSnapshot,
        WorkerCandidateSnapshot,
        ManagerCandidateSnapshot,
        ManagerAssignmentSlotSnapshot,
        WorkerAssignmentSlotSnapshot,
        buy_business,
        fetch_business_defs,
        get_business_hub_snapshot,
        get_business_manage_snapshot,
        get_manager_assignment_slots,
        get_worker_assignment_slots,
        hire_manager,
        hire_manager_manual,
        hire_worker,
        hire_worker_manual,
        roll_worker_candidate,
        roll_manager_candidate,
        WORKER_CANDIDATE_REROLL_COST,
        MANAGER_CANDIDATE_REROLL_COST,
        remove_manager,
        remove_worker,
        start_business_run,
        stop_business_run,
        upgrade_business,
        prestige_business,
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
        visible_level: int
        max_level: int
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
        visible_level: int
        max_level: int
        prestige: int
        hourly_profit: int
        base_hourly_income: int
        upgrade_cost: Optional[int]
        prestige_cost: Optional[int]
        can_prestige: bool
        prestige_multiplier: str
        bulk_upgrade_1_unlocked: bool
        bulk_upgrade_5_unlocked: bool
        bulk_upgrade_10_unlocked: bool
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

    @dataclass(slots=True)
    class HiredWorkerSnapshot:
        slot_index: int
        worker_name: str
        worker_type: str
        rarity: str
        flat_profit_bonus: int
        percent_profit_bonus_bp: int
        hire_cost: int


    @dataclass(slots=True)
    class WorkerCandidateSnapshot:
        worker_name: str
        worker_type: str
        rarity: str
        flat_profit_bonus: int
        percent_profit_bonus_bp: int
        reroll_cost: int

    @dataclass(slots=True)
    class HiredManagerSnapshot:
        slot_index: int
        manager_name: str
        rarity: str
        runtime_bonus_hours: int
        profit_bonus_bp: int
        auto_restart_charges: int
        hire_cost: int

    @dataclass(slots=True)
    class ManagerCandidateSnapshot:
        manager_name: str
        rarity: str
        runtime_bonus_hours: int
        profit_bonus_bp: int
        auto_restart_charges: int
        reroll_cost: int


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
                prestige_base_cost=100_000,
                prestige_growth_rate="2.5",
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

    async def stop_business_run(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
    ) -> BusinessActionResult:
        _ = session, guild_id, user_id, business_key
        return BusinessActionResult(
            ok=False,
            message="Business services are not wired yet. Build stop_business_run(...) in cogs/Business/core.py.",
        )

    async def upgrade_business(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
        quantity: int = 1,
    ) -> BusinessActionResult:
        _ = session, guild_id, user_id, business_key
        return BusinessActionResult(
            ok=False,
            message="Business services are not wired yet. Build upgrade_business(...) in cogs/Business/core.py.",
        )


    async def prestige_business(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
    ) -> BusinessActionResult:
        return BusinessActionResult(
            ok=False,
            message="Business services are not wired yet. Build prestige_business(...) in cogs/Business/core.py.",
        )


try:
    WORKER_CANDIDATE_REROLL_COST
except NameError:
    WORKER_CANDIDATE_REROLL_COST = 500

try:
    MANAGER_CANDIDATE_REROLL_COST
except NameError:
    MANAGER_CANDIDATE_REROLL_COST = 1_000

try:
    roll_worker_candidate
except NameError:
    async def roll_worker_candidate(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
        reroll_cost: int = WORKER_CANDIDATE_REROLL_COST,
    ) -> BusinessActionResult:
        _ = session, guild_id, user_id, business_key, reroll_cost
        return BusinessActionResult(ok=False, message="Worker candidate services are not wired yet.")

try:
    roll_manager_candidate
except NameError:
    async def roll_manager_candidate(
        session,
        *,
        guild_id: int,
        user_id: int,
        business_key: str,
        reroll_cost: int = MANAGER_CANDIDATE_REROLL_COST,
    ) -> BusinessActionResult:
        _ = session, guild_id, user_id, business_key, reroll_cost
        return BusinessActionResult(ok=False, message="Manager candidate services are not wired yet.")

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


def _chunk_field_lines(lines: Sequence[str], *, max_chars: int = 1024) -> list[str]:
    """Split line items into embed-safe field chunks."""
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for line in lines:
        text = str(line)
        if len(text) > max_chars:
            text = _trim(text, max_chars)

        sep_len = 2 if current else 0  # separator for "\n\n"
        if current and (current_len + sep_len + len(text)) > max_chars:
            chunks.append("\n\n".join(current))
            current = [text]
            current_len = len(text)
            continue

        if current:
            current_len += sep_len
        current.append(text)
        current_len += len(text)

    if current:
        chunks.append("\n\n".join(current))
    return chunks


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

def _base_embed(*, title: str, description: str = "", color: discord.Color = EMBED_COLOR) -> discord.Embed:
    e = discord.Embed(title=title, description=description or "", color=color)
    e.set_footer(text="Business System • Chatbox Economy")
    return e


def _author_icon_url(user: Optional[discord.abc.User]) -> Optional[str]:
    return getattr(getattr(user, "display_avatar", None), "url", None)


def _interaction_message_id(interaction: discord.Interaction) -> Optional[int]:
    msg = getattr(interaction, "message", None)
    mid = getattr(msg, "id", None)
    return int(mid) if mid is not None else None


async def _resolve_panel_message_id(interaction: discord.Interaction) -> Optional[int]:
    """Best-effort resolver for the panel message ID across interaction types."""
    panel_message_id = _interaction_message_id(interaction)
    if panel_message_id is not None:
        return panel_message_id

    try:
        original = await interaction.original_response()
    except (discord.NotFound, discord.HTTPException, AttributeError):
        return None

    oid = getattr(original, "id", None)
    return int(oid) if oid is not None else None


async def _safe_defer(interaction: discord.Interaction, *, thinking: bool = False) -> None:
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(thinking=thinking)
    except discord.HTTPException:
        log.debug("Business interaction defer failed", exc_info=True)


async def _safe_edit_panel(
    interaction: discord.Interaction,
    *,
    embed: Optional[discord.Embed] = None,
    embeds: Optional[Sequence[discord.Embed]] = None,
    view: Optional[discord.ui.View] = None,
    message_id: Optional[int] = None,
) -> bool:
    panel_message_id = message_id
    if panel_message_id is None:
        msg = getattr(interaction, "message", None)
        panel_message_id = getattr(msg, "id", None)

    if panel_message_id is None:
        await interaction.followup.send(
            "This business panel expired. Please run `/business` again.",
            ephemeral=True,
        )
        return False

    try:
        kwargs = {"message_id": int(panel_message_id), "view": view}
        if embeds:
            kwargs["embeds"] = list(embeds)
        else:
            kwargs["embed"] = embed
        await interaction.followup.edit_message(**kwargs)
        return True
    except (discord.NotFound, discord.HTTPException, AttributeError):
        log.exception(
            "Failed to edit business panel message | guild_id=%s user_id=%s message_id=%s",
            getattr(getattr(interaction, "guild", None), "id", None),
            getattr(getattr(interaction, "user", None), "id", None),
            panel_message_id,
        )
        await interaction.followup.send(
            "I couldn't update that panel. Please run `/business` again.",
            ephemeral=True,
        )
        return False


def _build_hub_embed(
    *,
    user: discord.abc.User,
    snap: BusinessHubSnapshot,
) -> discord.Embed:
    owned_cards = [c for c in snap.cards if c.owned]

    desc = (
        f"`Silver` `{_fmt_int(snap.silver_balance)}` • "
        f"`Active` `{_fmt_int(snap.total_hourly_income_active)}/hr` • "
        f"`Owned` `{_fmt_int(snap.owned_count)}`"
    )

    e = _base_embed(title="🏢 Business Hub", description=desc)
    e.set_author(
        name=_safe_str(user),
        icon_url=_author_icon_url(user),
    )

    showcase = _showcase_image_from_cards(owned_cards)
    if showcase:
        e.set_thumbnail(url=showcase)

    if not owned_cards:
        e.add_field(
            name="No Businesses",
            value="Use **Buy** to purchase your first business.",
            inline=False,
        )
        return e

    rows: list[str] = []
    for c in owned_cards[:10]:
        state = "Running" if c.running else "Stopped"
        remaining = f"{_fmt_int(c.runtime_remaining_hours)}h" if c.running else "—"
        rows.append(
            f"{c.emoji} **{c.name}** • Lvl `{_fmt_int(c.level)}`\n"
            f"`Status` **{state}** • `Time Left` `{remaining}` • `Profit` `{_fmt_int(c.hourly_profit)}/hr`"
        )

    e.add_field(name="Overview", value="\n\n".join(rows), inline=False)
    e.add_field(
        name="Quick Actions",
        value="Pick a business below, then choose **Run**, **Stop**, **Manage**, **Workers**, **Managers**, or **Upgrade**.",
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
        "Pick a business to buy and start earning silver."
    )
    e = _base_embed(title="🛒 Buy Business", description=desc)
    e.set_author(
        name=_safe_str(user),
        icon_url=_author_icon_url(user),
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

    desc = "Pick one business to start now and collect hourly profit."
    e = _base_embed(title="▶️ Run Business", description=desc)
    e.set_author(
        name=_safe_str(user),
        icon_url=_author_icon_url(user),
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
        icon_url=_author_icon_url(user),
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
            f"└ Level `{_fmt_int(c.visible_level)}/{_fmt_int(c.max_level)}` • Prestige `{_fmt_int(c.prestige)}`\n"
            f"└ Workers `{_slot_text(c.worker_slots_used, c.worker_slots_total)}` • Managers `{_slot_text(c.manager_slots_used, c.manager_slots_total)}`"
        )

    e.add_field(name="Your Businesses", value="\n\n".join(lines), inline=False)
    return e


def _build_business_detail_embed(
    *,
    user: discord.abc.User,
    snap: BusinessManageSnapshot,
) -> discord.Embed:
    status = "🟢 Running" if snap.running else "🔴 Stopped"
    remaining = f"{_fmt_int(snap.runtime_remaining_hours)}h" if snap.running else "—"
    projected_total = int(snap.hourly_profit) * int(snap.total_runtime_hours)

    e = _base_embed(
        title=f"📊 {snap.emoji} {snap.name}",
        description=f"`Status` {status} • `Level` `{_fmt_int(snap.visible_level)}/{_fmt_int(snap.max_level)}` • `Prestige` `{_fmt_int(snap.prestige)}`",
    )
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))

    e.add_field(
        name="Run Status",
        value=(
            f"Runtime Remaining: `{remaining}`\n"
            f"Cycle Runtime: `{_fmt_int(snap.total_runtime_hours)}h`\n"
            f"Projected Cycle Profit: `{_fmt_int(projected_total)} Silver`"
        ),
        inline=True,
    )
    e.add_field(
        name="Income",
        value=(
            f"Current Profit: `{_fmt_int(snap.hourly_profit)}/hr`\n"
            f"Base Profit: `{_fmt_int(snap.base_hourly_income)}/hr`\n"
            f"Upgrade Cost: `{_fmt_int(snap.upgrade_cost or 0)} Silver`\n"
            f"Prestige Cost: `{_fmt_int(snap.prestige_cost or 0)} Silver`\n"
            f"Output Multiplier: `x{snap.prestige_multiplier}`"
        ),
        inline=True,
    )
    e.add_field(
        name="Staff",
        value=(
            f"Workers: `{_slot_text(snap.worker_slots_used, snap.worker_slots_total)}`\n"
            f"Managers: `{_slot_text(snap.manager_slots_used, snap.manager_slots_total)}`"
        ),
        inline=False,
    )

    progression_lines = [
        f"Bulk x1: {'Unlocked' if snap.bulk_upgrade_1_unlocked else 'Locked'}",
        f"Bulk x5: {'Unlocked' if snap.bulk_upgrade_5_unlocked else 'Locked'}",
        f"Bulk x10: {'Unlocked' if snap.bulk_upgrade_10_unlocked else 'Locked'}",
    ]
    if snap.can_prestige:
        progression_lines.append("Prestige available now.")
    else:
        progression_lines.append(f"Prestige unlocks at Level {snap.max_level}.")
    e.add_field(name="Progression", value="\n".join(f"• {x}" for x in progression_lines), inline=False)

    if snap.notes:
        e.add_field(name="Active Bonuses", value="\n".join(f"• {x}" for x in snap.notes[:6]), inline=False)

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
    title = f"👷 Worker Panel • {_safe_str(getattr(detail, 'emoji', None), '🏢')} {_safe_str(getattr(detail, 'name', None), 'Business')}"
    description = (
        f"Slots in use: `{_slot_text(getattr(detail, 'worker_slots_used', 0), getattr(detail, 'worker_slots_total', 0))}`\n"
        "Use **Hire Worker** to open recruitment, then **Reroll Worker** for a new candidate or **Cancel** to return."
    )
    e = _base_embed(title=title, description=description)
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))

    lines: list[str] = []
    for slot in slots or ():
        slot_index = _fmt_int(getattr(slot, 'slot_index', 0))
        is_active = bool(getattr(slot, 'is_active', False))
        if is_active:
            lines.append(
                f"`#{slot_index}` **{_safe_str(getattr(slot, 'worker_name', None), 'Worker')}** ({_safe_str(getattr(slot, 'rarity', None), 'common')})\n"
                f"└ Type `{_safe_str(getattr(slot, 'worker_type', None), 'efficient')}` • Rarity `{_safe_str(getattr(slot, 'rarity', None), 'common')}` • +{_fmt_int(getattr(slot, 'flat_profit_bonus', 0))} flat • +{_fmt_int(getattr(slot, 'percent_profit_bonus_bp', 0))} bp • Status `Active`"
            )
        else:
            lines.append(f"`#{slot_index}` *(empty)*")

    empty_text = "No workers assigned yet." if getattr(detail, 'worker_slots_total', 0) else "No worker slots unlocked."
    if not lines:
        e.add_field(name="Slots", value=empty_text, inline=False)
        return e

    chunks = _chunk_field_lines(lines)
    for idx, chunk in enumerate(chunks, start=1):
        field_name = "Slots" if len(chunks) == 1 else f"Slots ({idx}/{len(chunks)})"
        e.add_field(name=field_name, value=chunk, inline=False)
    return e


def _build_manager_assignments_embed(
    *,
    user: discord.abc.User,
    detail: BusinessManageSnapshot,
    slots: Sequence[ManagerAssignmentSlotSnapshot],
) -> discord.Embed:
    title = f"🧑‍💼 Manager Panel • {_safe_str(getattr(detail, 'emoji', None), '🏢')} {_safe_str(getattr(detail, 'name', None), 'Business')}"
    description = (
        f"Slots in use: `{_slot_text(getattr(detail, 'manager_slots_used', 0), getattr(detail, 'manager_slots_total', 0))}`\n"
        "Use **Hire Manager** to generate a candidate, **Reroll Manager** to refresh for 1,000 Silver, or **Remove Manager** to deactivate an assigned manager."
    )
    e = _base_embed(title=title, description=description)
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))

    lines: list[str] = []
    for slot in slots or ():
        slot_index = _fmt_int(getattr(slot, 'slot_index', 0))
        is_active = bool(getattr(slot, 'is_active', False))
        if is_active:
            lines.append(
                f"`#{slot_index}` **{_safe_str(getattr(slot, 'manager_name', None), 'Manager')}** ({_safe_str(getattr(slot, 'rarity', None), 'common')})\n"
                f"└ Rarity `{_safe_str(getattr(slot, 'rarity', None), 'common')}` • +{_fmt_int(getattr(slot, 'runtime_bonus_hours', 0))}h runtime • +{_fmt_int(getattr(slot, 'profit_bonus_bp', 0))} bp • auto `{_fmt_int(getattr(slot, 'auto_restart_charges', 0))}` • Status `Active`"
            )
        else:
            lines.append(f"`#{slot_index}` *(empty)*")

    empty_text = "No managers assigned yet." if getattr(detail, 'manager_slots_total', 0) else "No manager slots unlocked."
    if not lines:
        e.add_field(name="Slots", value=empty_text, inline=False)
        return e

    chunks = _chunk_field_lines(lines)
    for idx, chunk in enumerate(chunks, start=1):
        field_name = "Slots" if len(chunks) == 1 else f"Slots ({idx}/{len(chunks)})"
        e.add_field(name=field_name, value=chunk, inline=False)
    return e


def _build_result_embed(*, title: str, message: str, ok: bool) -> discord.Embed:
    return _base_embed(
        title=("✅ " if ok else "❌ ") + title,
        description=message,
        color=SUCCESS_COLOR if ok else ERROR_COLOR,
    )


def _build_worker_hire_result_embed(
    *,
    user: discord.abc.User,
    detail: BusinessManageSnapshot,
    hired: HiredWorkerSnapshot,
) -> discord.Embed:
    e = _base_embed(
        title=f"✅ Worker Hired • {_safe_str(getattr(detail, 'emoji', None), '🏢')} {_safe_str(getattr(detail, 'name', None), 'Business')}",
        description="Your new worker has been assigned automatically.",
        color=SUCCESS_COLOR,
    )
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))
    e.add_field(
        name="Worker",
        value=(
            f"**{_safe_str(hired.worker_name, 'Worker')}**\n"
            f"Type `{_safe_str(hired.worker_type, 'efficient')}` • Rarity `{_safe_str(hired.rarity, 'common')}`"
        ),
        inline=False,
    )
    e.add_field(
        name="Bonuses",
        value=f"+{_fmt_int(hired.flat_profit_bonus)} flat • +{_fmt_int(hired.percent_profit_bonus_bp)} bp",
        inline=False,
    )
    e.add_field(
        name="Assignment",
        value=f"Slot `#{_fmt_int(hired.slot_index)}` • Cost `Free`",
        inline=False,
    )
    e.set_footer(text="Worker successfully assigned.")
    return e


def _build_worker_candidate_embed(
    *,
    user: discord.abc.User,
    detail: BusinessManageSnapshot,
    candidate: WorkerCandidateSnapshot,
) -> discord.Embed:
    e = _base_embed(
        title="👷 Worker Candidate",
        description="Review this worker before hiring.",
    )
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))
    e.add_field(name="Name", value=f"**{_safe_str(candidate.worker_name, 'Worker')}**", inline=False)
    e.add_field(name="Worker Type", value=f"`{_safe_str(candidate.worker_type, 'efficient')}`", inline=True)
    e.add_field(name="Rarity", value=f"`{_safe_str(candidate.rarity, 'common')}`", inline=True)
    e.add_field(name="Flat Profit Bonus", value=f"+{_fmt_int(getattr(candidate, 'flat_profit_bonus', 0))}", inline=True)
    e.add_field(name="Percent Profit Bonus", value=f"+{_fmt_int(getattr(candidate, 'percent_profit_bonus_bp', 0))} bp", inline=True)
    e.set_footer(text=f"Reroll Cost: {_fmt_int(getattr(candidate, 'reroll_cost', WORKER_CANDIDATE_REROLL_COST))} Silver")
    return e




def _build_manager_candidate_embed(
    *,
    user: discord.abc.User,
    detail: BusinessManageSnapshot,
    candidate: ManagerCandidateSnapshot,
) -> discord.Embed:
    e = _base_embed(
        title="🧑‍💼 Manager Candidate",
        description="Review this manager before hiring.",
    )
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))
    e.add_field(name="Name", value=f"**{_safe_str(candidate.manager_name, 'Manager')}**", inline=False)
    e.add_field(name="Rarity", value=f"`{_safe_str(candidate.rarity, 'common')}`", inline=True)
    e.add_field(name="Runtime Bonus", value=f"+{_fmt_int(getattr(candidate, 'runtime_bonus_hours', 0))}h", inline=True)
    e.add_field(name="Profit Bonus", value=f"+{_fmt_int(getattr(candidate, 'profit_bonus_bp', 0))} bp", inline=True)
    e.add_field(name="Auto Restarts", value=f"{_fmt_int(getattr(candidate, 'auto_restart_charges', 0))}", inline=True)
    e.set_footer(text=f"Reroll Cost: {_fmt_int(getattr(candidate, 'reroll_cost', MANAGER_CANDIDATE_REROLL_COST))} Silver")
    return e

def _build_manager_hire_result_embed(
    *,
    user: discord.abc.User,
    detail: BusinessManageSnapshot,
    hired: HiredManagerSnapshot,
) -> discord.Embed:
    e = _base_embed(
        title=f"✅ Manager Hired • {_safe_str(getattr(detail, 'emoji', None), '🏢')} {_safe_str(getattr(detail, 'name', None), 'Business')}",
        description="Your new manager has been assigned automatically.",
        color=SUCCESS_COLOR,
    )
    e.set_author(name=_safe_str(user), icon_url=_author_icon_url(user))
    e.add_field(
        name="Manager",
        value=f"**{_safe_str(hired.manager_name, 'Manager')}**\nRarity `{_safe_str(hired.rarity, 'common')}`",
        inline=False,
    )
    e.add_field(
        name="Bonuses",
        value=(
            f"+{_fmt_int(hired.runtime_bonus_hours)}h runtime • +{_fmt_int(hired.profit_bonus_bp)} bp • "
            f"auto `{_fmt_int(hired.auto_restart_charges)}`"
        ),
        inline=False,
    )
    e.add_field(
        name="Assignment",
        value=f"Slot `#{_fmt_int(hired.slot_index)}` • Cost `{_fmt_int(hired.hire_cost)} Silver`",
        inline=False,
    )
    return e


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

        await _safe_defer(interaction)
        unlocked_achievements = []

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
                if result.ok:
                    unlocked_achievements = await check_and_grant_achievements(
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
        await _safe_edit_panel(interaction, embed=embed, view=view)
        if unlocked_achievements:
            queue_achievement_announcements(
                bot=self.cog.bot,
                guild_id=self.guild_id,
                user_id=self.owner_id,
                unlocks=unlocked_achievements,
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

        await _safe_defer(interaction)

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
        await _safe_edit_panel(interaction, embed=embed, view=view)


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

        await _safe_defer(interaction)

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
            upgrade_enabled=detail.owned,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)


# =========================================================
# VIEWS
# =========================================================

class HubBusinessSelect(discord.ui.Select):
    def __init__(self, *, view: "BusinessHubView"):
        self.hub_view = view
        owned = [c for c in view.hub_snapshot.cards if c.owned]
        options: list[discord.SelectOption] = []
        for c in owned[:25]:
            options.append(
                discord.SelectOption(
                    label=_trim(c.name, 100),
                    value=c.key,
                    description=_trim(f"Lvl {_fmt_int(c.level)} • {_status_badge(c.running, c.owned)} • {_fmt_int(c.hourly_profit)}/hr", 100),
                    emoji=c.emoji,
                    default=(c.key == view.selected_business_key),
                )
            )
        super().__init__(
            placeholder="Select business for controls...",
            min_values=1,
            max_values=1,
            options=options or [discord.SelectOption(label="No owned businesses", value="__none__")],
            disabled=(len(owned) == 0),
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        picked = self.values[0]
        if picked == "__none__":
            await interaction.response.send_message("You do not own a business yet.", ephemeral=True)
            return
        await _safe_defer(interaction)
        self.hub_view.selected_business_key = picked
        async with self.hub_view.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(session, guild_id=self.hub_view.guild_id, user_id=self.hub_view.owner_id)
        embed = _build_hub_embed(user=interaction.user, snap=snap)
        view = BusinessHubView(
            cog=self.hub_view.cog,
            owner_id=self.hub_view.owner_id,
            guild_id=self.hub_view.guild_id,
            hub_snapshot=snap,
            selected_business_key=picked,
        )
        await _safe_edit_panel(interaction, embed=embed, view=view)


class BusinessHubView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        hub_snapshot: BusinessHubSnapshot,
        selected_business_key: Optional[str] = None,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.hub_snapshot = hub_snapshot
        owned_cards = [c for c in hub_snapshot.cards if c.owned]
        self.selected_business_key = selected_business_key or (owned_cards[0].key if owned_cards else None)
        self.add_item(HubBusinessSelect(view=self))
        self._configure_buttons()

    def _configure_buttons(self) -> None:
        owns_all = self.hub_snapshot.total_count > 0 and self.hub_snapshot.owned_count >= self.hub_snapshot.total_count
        has_selected = bool(self.selected_business_key)
        selected_card = next((c for c in self.hub_snapshot.cards if c.key == self.selected_business_key), None)
        at_cap = bool(selected_card is not None and selected_card.visible_level >= selected_card.max_level)
        self.buy_button.disabled = owns_all
        self.manage_button.disabled = not has_selected
        self.run_button.disabled = not has_selected
        self.stop_button.disabled = not has_selected
        self.workers_button.disabled = not has_selected
        self.managers_button.disabled = not has_selected
        self.upgrade_button.disabled = (not has_selected) or at_cap

    async def _load_selected_detail(self):
        if not self.selected_business_key:
            return None
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                return await get_business_manage_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.selected_business_key,
                )

    @discord.ui.button(label="Manage Business", style=discord.ButtonStyle.secondary, emoji="🛠️", row=1)
    async def manage_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        detail = await self._load_selected_detail()
        if detail is None:
            await interaction.followup.send("Select an owned business first.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=detail.key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Run Business", style=discord.ButtonStyle.success, emoji="▶️", row=1)
    async def run_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        if not self.selected_business_key:
            await interaction.followup.send("Select an owned business first.", ephemeral=True)
            return
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await start_business_run(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.selected_business_key)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.selected_business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Run Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.selected_business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Stop Business", style=discord.ButtonStyle.danger, emoji="⏹️", row=1)
    async def stop_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        if not self.selected_business_key:
            await interaction.followup.send("Select an owned business first.", ephemeral=True)
            return
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await stop_business_run(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.selected_business_key)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.selected_business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Stop Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.selected_business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="View Workers", style=discord.ButtonStyle.secondary, emoji="👷", row=2)
    async def workers_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        detail = await self._load_selected_detail()
        if detail is None:
            await interaction.followup.send("Select an owned business first.", ephemeral=True)
            return
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                slots = await get_worker_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=detail.key)
        embed = _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        panel_message_id = await _resolve_panel_message_id(interaction)
        if panel_message_id is None:
            await interaction.followup.send("This business panel expired. Please run `/business` again.", ephemeral=True)
            return
        view = WorkerAssignmentsView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=detail.key, panel_message_id=panel_message_id, requester=interaction.user)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="View Managers", style=discord.ButtonStyle.secondary, emoji="🧑‍💼", row=2)
    async def managers_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        detail = await self._load_selected_detail()
        if detail is None:
            await interaction.followup.send("Select an owned business first.", ephemeral=True)
            return
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                slots = await get_manager_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=detail.key)
        embed = _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        panel_message_id = await _resolve_panel_message_id(interaction)
        if panel_message_id is None:
            await interaction.followup.send("This business panel expired. Please run `/business` again.", ephemeral=True)
            return
        view = ManagerAssignmentsView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=detail.key, panel_message_id=panel_message_id, requester=interaction.user)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Upgrade Business", style=discord.ButtonStyle.primary, emoji="⬆️", row=2)
    async def upgrade_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        if not self.selected_business_key:
            await interaction.followup.send("Select an owned business first.", ephemeral=True)
            return
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)
                result = await upgrade_business(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.selected_business_key)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.selected_business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Upgrade Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.selected_business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Buy", style=discord.ButtonStyle.success, emoji="🛒", row=3)
    async def buy_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                defs = await fetch_business_defs(session)
                snap = await get_business_hub_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id)
        embed = _build_buy_menu_embed(user=interaction.user, defs=defs, snap=snap)
        view = BuyBusinessView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, defs=defs, hub_snapshot=snap)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=3)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                snap = await get_business_hub_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id)
        embed = _build_hub_embed(user=interaction.user, snap=snap)
        view = BusinessHubView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, hub_snapshot=snap, selected_business_key=self.selected_business_key)
        await _safe_edit_panel(interaction, embed=embed, view=view)


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
        await _safe_defer(interaction)

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
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=2)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)

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
        await _safe_edit_panel(interaction, embed=embed, view=view)


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
        await _safe_defer(interaction)

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
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=2)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)

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
        await _safe_edit_panel(interaction, embed=embed, view=view)


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
        await _safe_defer(interaction)

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
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔄", row=2)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)

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
        await _safe_edit_panel(interaction, embed=embed, view=view)


class BusinessDetailView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        business_key: str,
        owned: Optional[bool] = None,
        upgrade_enabled: Optional[bool] = None,
        detail: Optional[BusinessManageSnapshot] = None,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.business_key = business_key
        is_enabled = bool(upgrade_enabled) if upgrade_enabled is not None else bool(owned)
        self.upgrade_button.disabled = (not is_enabled) or bool(getattr(detail, "upgrade_cost", None) is None)
        self.upgrade_5_button.disabled = (not is_enabled) or (not bool(getattr(detail, "bulk_upgrade_5_unlocked", False))) or bool(getattr(detail, "upgrade_cost", None) is None)
        self.upgrade_10_button.disabled = (not is_enabled) or (not bool(getattr(detail, "bulk_upgrade_10_unlocked", False))) or bool(getattr(detail, "upgrade_cost", None) is None)
        self.prestige_button.disabled = (not is_enabled) or (not bool(getattr(detail, "can_prestige", False)))
        if detail is not None and not bool(getattr(detail, "can_prestige", False)):
            self.remove_item(self.prestige_button)
        self.workers_button.disabled = not is_enabled
        self.managers_button.disabled = not is_enabled
        self.run_button.disabled = not is_enabled
        self.stop_button.disabled = not is_enabled

    async def _reload_detail(self):
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                return await get_business_manage_snapshot(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                )

    @discord.ui.button(label="Run Business", style=discord.ButtonStyle.success, emoji="▶️", row=0)
    async def run_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await start_business_run(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Run Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Stop Business", style=discord.ButtonStyle.danger, emoji="⏹️", row=0)
    async def stop_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await stop_business_run(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Stop Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Status", style=discord.ButtonStyle.secondary, emoji="📡", row=0)
    async def status_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        detail = await self._reload_detail()
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Status", value="Current runtime, bonuses, and staffing shown above.", inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Upgrade Business", style=discord.ButtonStyle.primary, emoji="⬆️", row=1)
    async def upgrade_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)
                result = await upgrade_business(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Upgrade Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Upgrade x5", style=discord.ButtonStyle.primary, emoji="5️⃣", row=1)
    async def upgrade_5_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)
                result = await upgrade_business(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key, quantity=5)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Upgrade x5", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Upgrade x10", style=discord.ButtonStyle.primary, emoji="🔟", row=1)
    async def upgrade_10_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)
                result = await upgrade_business(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key, quantity=10)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Upgrade x10", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Prestige Business", style=discord.ButtonStyle.success, emoji="🌟", row=2)
    async def prestige_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=self.guild_id, user_id=self.owner_id)
                result = await prestige_business(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        embed.add_field(name="Prestige Business", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="View Workers", style=discord.ButtonStyle.secondary, emoji="👷", row=3, disabled=True)
    async def workers_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                slots = await get_worker_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        panel_message_id = await _resolve_panel_message_id(interaction)
        if panel_message_id is None:
            await interaction.followup.send("This business panel expired. Please run `/business` again.", ephemeral=True)
            return
        view = WorkerAssignmentsView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, panel_message_id=panel_message_id, requester=interaction.user)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="View Managers", style=discord.ButtonStyle.secondary, emoji="🧑‍💼", row=3, disabled=True)
    async def managers_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                slots = await get_manager_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        panel_message_id = await _resolve_panel_message_id(interaction)
        if panel_message_id is None:
            await interaction.followup.send("This business panel expired. Please run `/business` again.", ephemeral=True)
            return
        view = ManagerAssignmentsView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, panel_message_id=panel_message_id, requester=interaction.user)
        await _safe_edit_panel(interaction, embed=embed, view=view)

    @discord.ui.button(label="Back to Business Hub", style=discord.ButtonStyle.secondary, emoji="⬅️", row=4)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                hub = await get_business_hub_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id)
        embed = _build_hub_embed(user=interaction.user, snap=hub)
        view = BusinessHubView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, hub_snapshot=hub, selected_business_key=self.business_key)
        await _safe_edit_panel(interaction, embed=embed, view=view)



class RemoveWorkerModal(discord.ui.Modal, title="Remove Worker"):
    def __init__(self, view: "WorkerAssignmentsView"):
        super().__init__()
        self.parent_view = view
        self.slot_index = discord.ui.TextInput(label="Reply with worker slot #", placeholder="1", max_length=4)
        self.add_item(self.slot_index)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        requested_slot = _parse_int(str(self.slot_index.value), 0)
        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
                slots = await get_worker_assignment_slots(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
        if detail is None:
            await interaction.response.send_message("That business could not be found.", ephemeral=True)
            return
        selected_slot = next((slot for slot in slots if int(getattr(slot, "slot_index", 0)) == int(requested_slot)), None)
        if not selected_slot or not bool(getattr(selected_slot, "is_active", False)):
            await _safe_defer(interaction)
            embed = _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots)
            embed.add_field(name="Action", value=f"❌ No active worker found in slot **#{_fmt_int(requested_slot)}**.", inline=False)
            await _safe_edit_panel(interaction, embed=embed, view=self.parent_view, message_id=self.parent_view.panel_message_id)
            return

        worker_name = _safe_str(getattr(selected_slot, "worker_name", None), "Worker")
        worker_rarity = _safe_str(getattr(selected_slot, "rarity", None), "common")
        worker_type = _safe_str(getattr(selected_slot, "worker_type", None), "efficient")

        embed = discord.Embed(
            title="Confirm Worker Removal",
            description=(
                f"You are removing worker **{worker_name}** from slot **#{_fmt_int(requested_slot)}**.\n"
                "Are you sure you want to continue?"
            ),
            color=discord.Color.orange(),
        )
        embed.add_field(name="Worker", value=f"{worker_name} ({worker_rarity})", inline=True)
        embed.add_field(name="Type", value=worker_type, inline=True)
        await interaction.response.send_message(
            embed=embed,
            ephemeral=True,
            view=ConfirmWorkerRemovalView(parent_view=self.parent_view, slot_index=int(requested_slot), worker_name=worker_name),
        )


class ConfirmWorkerRemovalView(discord.ui.View):
    def __init__(self, *, parent_view: "WorkerAssignmentsView", slot_index: int, worker_name: str):
        super().__init__(timeout=60)
        self.parent_view = parent_view
        self.slot_index = int(slot_index)
        self.worker_name = str(worker_name)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.parent_view.owner_id:
            await interaction.response.send_message("This confirmation belongs to someone else.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm Remove", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                result = await remove_worker(
                    session,
                    guild_id=self.parent_view.guild_id,
                    user_id=self.parent_view.owner_id,
                    business_key=self.parent_view.business_key,
                    slot_index=self.slot_index,
                )
                detail = await get_business_manage_snapshot(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
                slots = await get_worker_assignment_slots(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        embed.add_field(name="Action", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        await _safe_edit_panel(interaction, embed=embed, view=self.parent_view, message_id=self.parent_view.panel_message_id)
        await interaction.edit_original_response(content="Removal confirmed.", embed=None, view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.edit_message(content="Worker removal cancelled.", embed=None, view=None)


class RemoveManagerModal(discord.ui.Modal, title="Remove Manager"):
    def __init__(self, view: "ManagerAssignmentsView"):
        super().__init__()
        self.parent_view = view
        self.slot_index = discord.ui.TextInput(label="Reply with manager slot #", placeholder="1", max_length=4)
        self.add_item(self.slot_index)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        requested_slot = _parse_int(str(self.slot_index.value), 0)
        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
                slots = await get_manager_assignment_slots(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
        if detail is None:
            await interaction.response.send_message("That business could not be found.", ephemeral=True)
            return
        selected_slot = next((slot for slot in slots if int(getattr(slot, "slot_index", 0)) == int(requested_slot)), None)
        if not selected_slot or not bool(getattr(selected_slot, "is_active", False)):
            await _safe_defer(interaction)
            embed = _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots)
            embed.add_field(name="Action", value=f"❌ No active manager found in slot **#{_fmt_int(requested_slot)}**.", inline=False)
            await _safe_edit_panel(interaction, embed=embed, view=self.parent_view, message_id=self.parent_view.panel_message_id)
            return

        manager_name = _safe_str(getattr(selected_slot, "manager_name", None), "Manager")
        manager_rarity = _safe_str(getattr(selected_slot, "rarity", None), "common")
        runtime_bonus = _fmt_int(getattr(selected_slot, "runtime_bonus_hours", 0))
        profit_bonus = _fmt_int(getattr(selected_slot, "profit_bonus_bp", 0))

        embed = discord.Embed(
            title="Confirm Manager Removal",
            description=(
                f"You are removing manager **{manager_name}** from slot **#{_fmt_int(requested_slot)}**.\n"
                "Are you sure you want to continue?"
            ),
            color=discord.Color.orange(),
        )
        embed.add_field(name="Manager", value=f"{manager_name} ({manager_rarity})", inline=True)
        embed.add_field(name="Bonuses", value=f"+{runtime_bonus}h runtime • +{profit_bonus} bp", inline=True)
        await interaction.response.send_message(
            embed=embed,
            ephemeral=True,
            view=ConfirmManagerRemovalView(parent_view=self.parent_view, slot_index=int(requested_slot), manager_name=manager_name),
        )


class ConfirmManagerRemovalView(discord.ui.View):
    def __init__(self, *, parent_view: "ManagerAssignmentsView", slot_index: int, manager_name: str):
        super().__init__(timeout=60)
        self.parent_view = parent_view
        self.slot_index = int(slot_index)
        self.manager_name = str(manager_name)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.parent_view.owner_id:
            await interaction.response.send_message("This confirmation belongs to someone else.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm Remove", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                result = await remove_manager(
                    session,
                    guild_id=self.parent_view.guild_id,
                    user_id=self.parent_view.owner_id,
                    business_key=self.parent_view.business_key,
                    slot_index=self.slot_index,
                )
                detail = await get_business_manage_snapshot(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
                slots = await get_manager_assignment_slots(session, guild_id=self.parent_view.guild_id, user_id=self.parent_view.owner_id, business_key=self.parent_view.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        embed.add_field(name="Action", value=("✅ " if result.ok else "❌ ") + result.message, inline=False)
        await _safe_edit_panel(interaction, embed=embed, view=self.parent_view, message_id=self.parent_view.panel_message_id)
        await interaction.edit_original_response(content="Removal confirmed.", embed=None, view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.edit_message(content="Manager removal cancelled.", embed=None, view=None)


class AutoHireWorkersModal(discord.ui.Modal, title="Auto-Hire Workers"):
    def __init__(self, view: "WorkerAssignmentsView"):
        super().__init__()
        self.parent_view = view
        self.rarity_filter = discord.ui.TextInput(label="Allowed rarities", placeholder="rare, epic, mythic (or all)", default="all", max_length=64)
        self.reroll_count = discord.ui.TextInput(label="Max rerolls budget", placeholder="15", default="15", max_length=4)
        self.add_item(self.rarity_filter)
        self.add_item(self.reroll_count)

    def _parse_allowed_rarities(self) -> set[str]:
        raw = str(self.rarity_filter.value or "all").strip().lower()
        if raw in {"", "all", "any", "*"}:
            return set(AUTO_HIRE_ALLOWED_RARITIES)
        allowed = {part.strip() for part in raw.replace("|", ",").split(",") if part.strip()}
        return {r for r in allowed if r in AUTO_HIRE_ALLOWED_RARITIES}

    async def on_submit(self, interaction: discord.Interaction) -> None:
        rerolls = _clamp_int(_parse_int(str(self.reroll_count.value), 0), 1, AUTO_HIRE_MAX_REROLLS)
        allowed_rarities = self._parse_allowed_rarities()
        if not allowed_rarities:
            await interaction.response.send_message("Please enter valid rarity filters: common, uncommon, rare, epic, mythic.", ephemeral=True)
            return

        total_cost = rerolls * WORKER_CANDIDATE_REROLL_COST
        embed = discord.Embed(
            title="Confirm Worker Auto-Hire",
            description=f"Auto-Hire can spend up to **{_fmt_int(total_cost)} Silver** for **{_fmt_int(rerolls)} rerolls**.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Allowed rarities", value=f"`{', '.join(sorted(allowed_rarities))}`", inline=False)
        embed.add_field(name="Flow", value="Auto-Hire keeps rolling and hires matches until slots are full or budget is spent.", inline=False)
        await interaction.response.send_message(
            embed=embed,
            view=ConfirmWorkerAutoHireView(parent_view=self.parent_view, rerolls=rerolls, allowed_rarities=allowed_rarities),
            ephemeral=True,
        )


class AutoHireManagersModal(discord.ui.Modal, title="Auto-Hire Managers"):
    def __init__(self, view: "ManagerAssignmentsView"):
        super().__init__()
        self.parent_view = view
        self.rarity_filter = discord.ui.TextInput(label="Allowed rarities", placeholder="rare, epic, mythic (or all)", default="all", max_length=64)
        self.reroll_count = discord.ui.TextInput(label="Max rerolls budget", placeholder="15", default="15", max_length=4)
        self.add_item(self.rarity_filter)
        self.add_item(self.reroll_count)

    def _parse_allowed_rarities(self) -> set[str]:
        raw = str(self.rarity_filter.value or "all").strip().lower()
        if raw in {"", "all", "any", "*"}:
            return set(AUTO_HIRE_ALLOWED_RARITIES)
        allowed = {part.strip() for part in raw.replace("|", ",").split(",") if part.strip()}
        return {r for r in allowed if r in AUTO_HIRE_ALLOWED_RARITIES}

    async def on_submit(self, interaction: discord.Interaction) -> None:
        rerolls = _clamp_int(_parse_int(str(self.reroll_count.value), 0), 1, AUTO_HIRE_MAX_REROLLS)
        allowed_rarities = self._parse_allowed_rarities()
        if not allowed_rarities:
            await interaction.response.send_message("Please enter valid rarity filters: common, uncommon, rare, epic, mythic.", ephemeral=True)
            return

        total_cost = rerolls * MANAGER_CANDIDATE_REROLL_COST
        embed = discord.Embed(
            title="Confirm Manager Auto-Hire",
            description=f"Auto-Hire can spend up to **{_fmt_int(total_cost)} Silver** for **{_fmt_int(rerolls)} rerolls**.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Allowed rarities", value=f"`{', '.join(sorted(allowed_rarities))}`", inline=False)
        embed.add_field(name="Flow", value="Auto-Hire keeps rolling and hires matches until slots are full or budget is spent.", inline=False)
        await interaction.response.send_message(
            embed=embed,
            view=ConfirmManagerAutoHireView(parent_view=self.parent_view, rerolls=rerolls, allowed_rarities=allowed_rarities),
            ephemeral=True,
        )


class ConfirmWorkerAutoHireView(discord.ui.View):
    def __init__(self, *, parent_view: "WorkerAssignmentsView", rerolls: int, allowed_rarities: set[str]):
        super().__init__(timeout=120)
        self.parent_view = parent_view
        self.rerolls = int(rerolls)
        self.allowed_rarities = set(allowed_rarities)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.parent_view.owner_id:
            await interaction.response.send_message("This confirmation belongs to someone else.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm Auto-Hire", style=discord.ButtonStyle.success, emoji="✅")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer(ephemeral=True)
        hires, rerolls_used, slots_full = 0, 0, False
        last_error = ""

        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                for _ in range(self.rerolls):
                    roll_result = await roll_worker_candidate(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                        reroll_cost=WORKER_CANDIDATE_REROLL_COST,
                    )
                    if not roll_result.ok or roll_result.worker_candidate is None:
                        last_error = roll_result.message
                        break
                    rerolls_used += 1
                    c = roll_result.worker_candidate
                    if str(getattr(c, "rarity", "common")).strip().lower() not in self.allowed_rarities:
                        continue
                    hire_result = await hire_worker_manual(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                        worker_name=str(getattr(c, "worker_name", "Worker")),
                        worker_type=str(getattr(c, "worker_type", "efficient")),
                        rarity=str(getattr(c, "rarity", "common")),
                        flat_profit_bonus=int(getattr(c, "flat_profit_bonus", 0) or 0),
                        percent_profit_bonus_bp=int(getattr(c, "percent_profit_bonus_bp", 0) or 0),
                        charge_silver=False,
                    )
                    if not hire_result.ok:
                        last_error = hire_result.message
                        slots_full = "slots are full" in str(hire_result.message).lower()
                        break
                    hires += 1

        self.parent_view.current_candidate = None
        suffix = " Worker slots are full." if slots_full else (f" {last_error}" if last_error else "")
        await self.parent_view._show_recruitment_board(interaction, action_message=f"✅ Auto-Hire complete: hired **{_fmt_int(hires)}** workers in **{_fmt_int(rerolls_used)}** rerolls.{suffix}")
        await interaction.followup.send(f"Auto-Hire finished. Spent **{_fmt_int(rerolls_used * WORKER_CANDIDATE_REROLL_COST)} Silver**.", ephemeral=True)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.send_message("Auto-Hire cancelled.", ephemeral=True)


class ConfirmManagerAutoHireView(discord.ui.View):
    def __init__(self, *, parent_view: "ManagerAssignmentsView", rerolls: int, allowed_rarities: set[str]):
        super().__init__(timeout=120)
        self.parent_view = parent_view
        self.rerolls = int(rerolls)
        self.allowed_rarities = set(allowed_rarities)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.parent_view.owner_id:
            await interaction.response.send_message("This confirmation belongs to someone else.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm Auto-Hire", style=discord.ButtonStyle.success, emoji="✅")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.defer(ephemeral=True)
        hires, rerolls_used, slots_full = 0, 0, False
        last_error = ""

        async with self.parent_view.cog.sessionmaker() as session:
            async with session.begin():
                for _ in range(self.rerolls):
                    roll_result = await roll_manager_candidate(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                        reroll_cost=MANAGER_CANDIDATE_REROLL_COST,
                    )
                    if not roll_result.ok or roll_result.manager_candidate is None:
                        last_error = roll_result.message
                        break
                    rerolls_used += 1
                    c = roll_result.manager_candidate
                    if str(getattr(c, "rarity", "common")).strip().lower() not in self.allowed_rarities:
                        continue
                    hire_result = await hire_manager_manual(
                        session,
                        guild_id=self.parent_view.guild_id,
                        user_id=self.parent_view.owner_id,
                        business_key=self.parent_view.business_key,
                        manager_name=str(getattr(c, "manager_name", "Manager")),
                        rarity=str(getattr(c, "rarity", "common")),
                        runtime_bonus_hours=int(getattr(c, "runtime_bonus_hours", 0) or 0),
                        profit_bonus_bp=int(getattr(c, "profit_bonus_bp", 0) or 0),
                        auto_restart_charges=int(getattr(c, "auto_restart_charges", 0) or 0),
                        charge_silver=False,
                    )
                    if not hire_result.ok:
                        last_error = hire_result.message
                        slots_full = "slots are full" in str(hire_result.message).lower()
                        break
                    hires += 1

        self.parent_view.current_candidate = None
        suffix = " Manager slots are full." if slots_full else (f" {last_error}" if last_error else "")
        await self.parent_view._show_recruitment_board(interaction, action_message=f"✅ Auto-Hire complete: hired **{_fmt_int(hires)}** managers in **{_fmt_int(rerolls_used)}** rerolls.{suffix}")
        await interaction.followup.send(f"Auto-Hire finished. Spent **{_fmt_int(rerolls_used * MANAGER_CANDIDATE_REROLL_COST)} Silver**.", ephemeral=True)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.send_message("Auto-Hire cancelled.", ephemeral=True)


class WorkerAssignmentsView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        business_key: str,
        panel_message_id: int,
        requester: Optional[discord.abc.User] = None,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.business_key = business_key
        self.panel_message_id = int(panel_message_id)
        self.current_candidate: Optional[WorkerCandidateSnapshot] = None
        member: Optional[discord.Member] = requester if isinstance(requester, discord.Member) else None
        if member is None:
            guild = self.cog.bot.get_guild(self.guild_id)
            if guild is not None:
                member = guild.get_member(self.owner_id)
        self.is_vip = is_vip_member(member)
        self.auto_hire_button.disabled = not self.is_vip

    async def _send_auto_hire_reply(self, interaction: discord.Interaction) -> None:
        rerolls = 15
        allowed_rarities = set(AUTO_HIRE_ALLOWED_RARITIES)
        total_cost = rerolls * WORKER_CANDIDATE_REROLL_COST
        embed = discord.Embed(
            title="Confirm Worker Auto-Hire",
            description=f"Auto-Hire can spend up to **{_fmt_int(total_cost)} Silver** for **{_fmt_int(rerolls)} rerolls**.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Allowed rarities", value=f"`{', '.join(sorted(allowed_rarities))}`", inline=False)
        embed.add_field(name="Flow", value="Auto-Hire keeps rolling and hires matches until slots are full or budget is spent.", inline=False)
        await interaction.response.send_message(
            "VIP Auto-Reroll started with default settings (all rarities, 15 rerolls).",
            embed=embed,
            view=ConfirmWorkerAutoHireView(parent_view=self, rerolls=rerolls, allowed_rarities=allowed_rarities),
            ephemeral=True,
        )

    async def _refresh_assignments_embed(self, interaction: discord.Interaction) -> Optional[tuple[BusinessManageSnapshot, Sequence[WorkerAssignmentSlotSnapshot], discord.Embed]]:
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                slots = await get_worker_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return None
        return detail, slots, _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots)

    async def _show_recruitment_board(self, interaction: discord.Interaction, action_message: Optional[str] = None) -> None:
        payload = await self._refresh_assignments_embed(interaction)
        if payload is None:
            return
        detail, _slots, assignments_embed = payload
        if self.current_candidate is None:
            assignments_embed.add_field(
                name="Recruitment Board",
                value=f"Press **Hire Worker** to generate a candidate for **{_fmt_int(WORKER_CANDIDATE_REROLL_COST)} Silver**.",
                inline=False,
            )
            if action_message:
                assignments_embed.add_field(name="Action", value=action_message, inline=False)
            await _safe_edit_panel(interaction, embed=assignments_embed, view=self, message_id=self.panel_message_id)
            return

        candidate_embed = _build_worker_candidate_embed(user=interaction.user, detail=detail, candidate=self.current_candidate)
        if action_message:
            candidate_embed.add_field(name="Action", value=action_message, inline=False)
        await _safe_edit_panel(interaction, embeds=[candidate_embed, assignments_embed], view=self, message_id=self.panel_message_id)

    @discord.ui.button(label="Hire Worker", style=discord.ButtonStyle.success, emoji="➕", row=0)
    async def hire_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)

        if self.current_candidate is None:
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    result = await roll_worker_candidate(
                        session,
                        guild_id=self.guild_id,
                        user_id=self.owner_id,
                        business_key=self.business_key,
                        reroll_cost=WORKER_CANDIDATE_REROLL_COST,
                    )
            if not result.ok or result.worker_candidate is None:
                await self._show_recruitment_board(interaction, action_message="❌ " + result.message)
                return
            self.current_candidate = result.worker_candidate
            await self._show_recruitment_board(interaction, action_message="✅ Candidate generated. Hiring is free.")
            return

        candidate = self.current_candidate
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await hire_worker_manual(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                    worker_name=str(getattr(candidate, "worker_name", "Worker")),
                    worker_type=str(getattr(candidate, "worker_type", "efficient")),
                    rarity=str(getattr(candidate, "rarity", "common")),
                    flat_profit_bonus=int(getattr(candidate, "flat_profit_bonus", 0) or 0),
                    percent_profit_bonus_bp=int(getattr(candidate, "percent_profit_bonus_bp", 0) or 0),
                    charge_silver=False,
                )
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                slots = await get_worker_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return

        assignments_embed = _build_worker_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        if result.ok and result.hired_worker is not None:
            self.current_candidate = None
            result_embed = _build_worker_hire_result_embed(user=interaction.user, detail=detail, hired=result.hired_worker)
            await _safe_edit_panel(interaction, embeds=[result_embed, assignments_embed], view=self, message_id=self.panel_message_id)
            return
        await self._show_recruitment_board(interaction, action_message="❌ " + result.message)

    @discord.ui.button(label="Reroll Worker", style=discord.ButtonStyle.primary, emoji="🎲", row=0)
    async def reroll_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await roll_worker_candidate(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                    reroll_cost=WORKER_CANDIDATE_REROLL_COST,
                )
        if not result.ok or result.worker_candidate is None:
            await self._show_recruitment_board(interaction, action_message="❌ " + result.message)
            return
        self.current_candidate = result.worker_candidate
        await self._show_recruitment_board(interaction, action_message="✅ Candidate rerolled.")

    @discord.ui.button(label="Auto-Hire (VIP)", style=discord.ButtonStyle.secondary, emoji="⭐", row=1)
    async def auto_hire_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        if not self.is_vip:
            await interaction.response.send_message("Auto-Hire is a VIP feature.", ephemeral=True)
            return
        await self._send_auto_hire_reply(interaction)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="⬅️", row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        self.current_candidate = None
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view, message_id=self.panel_message_id)


class ManagerAssignmentsView(BusinessBaseView):
    def __init__(
        self,
        *,
        cog: "BusinessCog",
        owner_id: int,
        guild_id: int,
        business_key: str,
        panel_message_id: int,
        requester: Optional[discord.abc.User] = None,
    ):
        super().__init__(cog=cog, owner_id=owner_id, guild_id=guild_id)
        self.business_key = business_key
        self.panel_message_id = int(panel_message_id)
        self.current_candidate: Optional[ManagerCandidateSnapshot] = None
        member: Optional[discord.Member] = requester if isinstance(requester, discord.Member) else None
        if member is None:
            guild = self.cog.bot.get_guild(self.guild_id)
            if guild is not None:
                member = guild.get_member(self.owner_id)
        self.is_vip = is_vip_member(member)
        self.auto_hire_button.disabled = not self.is_vip

    async def _send_auto_hire_reply(self, interaction: discord.Interaction) -> None:
        rerolls = 15
        allowed_rarities = set(AUTO_HIRE_ALLOWED_RARITIES)
        total_cost = rerolls * MANAGER_CANDIDATE_REROLL_COST
        embed = discord.Embed(
            title="Confirm Manager Auto-Hire",
            description=f"Auto-Hire can spend up to **{_fmt_int(total_cost)} Silver** for **{_fmt_int(rerolls)} rerolls**.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Allowed rarities", value=f"`{', '.join(sorted(allowed_rarities))}`", inline=False)
        embed.add_field(name="Flow", value="Auto-Hire keeps rolling and hires matches until slots are full or budget is spent.", inline=False)
        await interaction.response.send_message(
            "VIP Auto-Reroll started with default settings (all rarities, 15 rerolls).",
            embed=embed,
            view=ConfirmManagerAutoHireView(parent_view=self, rerolls=rerolls, allowed_rarities=allowed_rarities),
            ephemeral=True,
        )

    async def _refresh_assignments_embed(self, interaction: discord.Interaction) -> Optional[tuple[BusinessManageSnapshot, Sequence[ManagerAssignmentSlotSnapshot], discord.Embed]]:
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                slots = await get_manager_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return None
        return detail, slots, _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots)

    async def _show_recruitment_board(self, interaction: discord.Interaction, action_message: Optional[str] = None) -> None:
        payload = await self._refresh_assignments_embed(interaction)
        if payload is None:
            return
        detail, _slots, assignments_embed = payload
        if self.current_candidate is None:
            assignments_embed.add_field(
                name="Recruitment Board",
                value=f"Press **Hire Manager** to generate a candidate for **{_fmt_int(MANAGER_CANDIDATE_REROLL_COST)} Silver**.",
                inline=False,
            )
            if action_message:
                assignments_embed.add_field(name="Action", value=action_message, inline=False)
            await _safe_edit_panel(interaction, embed=assignments_embed, view=self, message_id=self.panel_message_id)
            return

        candidate_embed = _build_manager_candidate_embed(user=interaction.user, detail=detail, candidate=self.current_candidate)
        if action_message:
            candidate_embed.add_field(name="Action", value=action_message, inline=False)
        await _safe_edit_panel(interaction, embeds=[candidate_embed, assignments_embed], view=self, message_id=self.panel_message_id)

    @discord.ui.button(label="Hire Manager", style=discord.ButtonStyle.success, emoji="➕", row=0)
    async def hire_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)

        if self.current_candidate is None:
            async with self.cog.sessionmaker() as session:
                async with session.begin():
                    result = await roll_manager_candidate(
                        session,
                        guild_id=self.guild_id,
                        user_id=self.owner_id,
                        business_key=self.business_key,
                        reroll_cost=MANAGER_CANDIDATE_REROLL_COST,
                    )
            if not result.ok or result.manager_candidate is None:
                await self._show_recruitment_board(interaction, action_message="❌ " + result.message)
                return
            self.current_candidate = result.manager_candidate
            await self._show_recruitment_board(interaction, action_message="✅ Candidate generated. Hiring is free.")
            return

        candidate = self.current_candidate
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await hire_manager_manual(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                    manager_name=str(getattr(candidate, "manager_name", "Manager")),
                    rarity=str(getattr(candidate, "rarity", "common")),
                    runtime_bonus_hours=int(getattr(candidate, "runtime_bonus_hours", 0) or 0),
                    profit_bonus_bp=int(getattr(candidate, "profit_bonus_bp", 0) or 0),
                    auto_restart_charges=int(getattr(candidate, "auto_restart_charges", 0) or 0),
                    charge_silver=False,
                )
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
                slots = await get_manager_assignment_slots(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return

        assignments_embed = _build_manager_assignments_embed(user=interaction.user, detail=detail, slots=slots)
        if result.ok and result.hired_manager is not None:
            self.current_candidate = None
            result_embed = _build_manager_hire_result_embed(user=interaction.user, detail=detail, hired=result.hired_manager)
            await _safe_edit_panel(interaction, embeds=[result_embed, assignments_embed], view=self, message_id=self.panel_message_id)
            return
        await self._show_recruitment_board(interaction, action_message="❌ " + result.message)

    @discord.ui.button(label="Reroll Manager", style=discord.ButtonStyle.primary, emoji="🎲", row=0)
    async def reroll_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                result = await roll_manager_candidate(
                    session,
                    guild_id=self.guild_id,
                    user_id=self.owner_id,
                    business_key=self.business_key,
                    reroll_cost=MANAGER_CANDIDATE_REROLL_COST,
                )
        if not result.ok or result.manager_candidate is None:
            await self._show_recruitment_board(interaction, action_message="❌ " + result.message)
            return
        self.current_candidate = result.manager_candidate
        await self._show_recruitment_board(interaction, action_message="✅ Candidate rerolled.")

    @discord.ui.button(label="Auto-Hire (VIP)", style=discord.ButtonStyle.secondary, emoji="⭐", row=1)
    async def auto_hire_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        if not self.is_vip:
            await interaction.response.send_message("Auto-Hire is a VIP feature.", ephemeral=True)
            return
        await self._send_auto_hire_reply(interaction)

    @discord.ui.button(label="Remove Manager", style=discord.ButtonStyle.danger, emoji="➖", row=1)
    async def remove_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        await interaction.response.send_modal(RemoveManagerModal(self))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji="⬅️", row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        _ = button
        self.current_candidate = None
        await _safe_defer(interaction)
        async with self.cog.sessionmaker() as session:
            async with session.begin():
                detail = await get_business_manage_snapshot(session, guild_id=self.guild_id, user_id=self.owner_id, business_key=self.business_key)
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        embed = _build_business_detail_embed(user=interaction.user, snap=detail)
        view = BusinessDetailView(cog=self.cog, owner_id=self.owner_id, guild_id=self.guild_id, business_key=self.business_key, owned=detail.owned, detail=detail)
        await _safe_edit_panel(interaction, embed=embed, view=view, message_id=self.panel_message_id)


# =========================================================
# COG
# =========================================================

class BusinessCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self.runtime_engine = BusinessRuntimeEngine(on_run_completed=self._notify_business_run_completed)

    def _load_runtime_state(self) -> dict:
        default = {
            "refund_migration_ran": False,
            "notification_prefs": {},
            "pending_summaries": {},
        }
        try:
            if not _BUSINESS_RUNTIME_STATE_PATH.exists():
                return default
            raw = json.loads(_BUSINESS_RUNTIME_STATE_PATH.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return default
            default.update(raw)
            return default
        except Exception:
            return default

    def _save_runtime_state(self, state: dict) -> None:
        _BUSINESS_RUNTIME_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _BUSINESS_RUNTIME_STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")

    def _state_key(self, *, guild_id: int, user_id: int) -> str:
        return f"{int(guild_id)}:{int(user_id)}"

    def _notifications_enabled_for(self, *, guild_id: int, user_id: int) -> bool:
        state = self._load_runtime_state()
        prefs = dict(state.get("notification_prefs", {}))
        return bool(prefs.get(self._state_key(guild_id=guild_id, user_id=user_id), True))

    def _set_notifications_enabled_for(self, *, guild_id: int, user_id: int, enabled: bool) -> None:
        state = self._load_runtime_state()
        prefs = dict(state.get("notification_prefs", {}))
        prefs[self._state_key(guild_id=guild_id, user_id=user_id)] = bool(enabled)
        state["notification_prefs"] = prefs
        self._save_runtime_state(state)

    def _push_pending_summary(self, *, guild_id: int, user_id: int, summary: dict) -> None:
        state = self._load_runtime_state()
        pending = dict(state.get("pending_summaries", {}))
        key = self._state_key(guild_id=guild_id, user_id=user_id)
        items = list(pending.get(key, []))
        items.append(summary)
        pending[key] = items[-20:]
        state["pending_summaries"] = pending
        self._save_runtime_state(state)

    def _pop_pending_summaries(self, *, guild_id: int, user_id: int) -> list[dict]:
        state = self._load_runtime_state()
        pending = dict(state.get("pending_summaries", {}))
        key = self._state_key(guild_id=guild_id, user_id=user_id)
        items = list(pending.pop(key, []))
        state["pending_summaries"] = pending
        self._save_runtime_state(state)
        return items

    def _new_upgrade_cost(self, *, base_hourly_income: int, level: int) -> int:
        lvl = max(int(level), 0)
        first = min(lvl, 10)
        bp = first * 3500
        if lvl > 10:
            bp += min(lvl - 10, 10) * 1500
        if lvl > 20:
            bp += (lvl - 20) * 800
        cur = int(round(int(base_hourly_income) * (10_000 + bp) / 10_000))

        nxt_lvl = lvl + 1
        first_n = min(nxt_lvl, 10)
        bp_n = first_n * 3500
        if nxt_lvl > 10:
            bp_n += min(nxt_lvl - 10, 10) * 1500
        if nxt_lvl > 20:
            bp_n += (nxt_lvl - 20) * 800
        nxt = int(round(int(base_hourly_income) * (10_000 + bp_n) / 10_000))
        delta = max(nxt - cur, 1)
        return max(int(round(delta * 12)), 1)

    async def _run_one_time_upgrade_refund(self) -> None:
        state = self._load_runtime_state()
        if bool(state.get("refund_migration_ran", False)):
            return

        refunded_total = 0
        refunded_rows = 0

        async with self.sessionmaker() as session:
            async with session.begin():
                defs = await fetch_business_defs(session)
                def_map = {str(d.key): d for d in defs}
                ownership_rows = list((await session.scalars(select(BusinessOwnershipRow))).all())

                for row in ownership_rows:
                    defn = def_map.get(str(row.business_key))
                    if defn is None:
                        continue
                    level = max(int(row.level or 0), 0)
                    if level <= 0:
                        continue
                    legacy_spent = 0
                    new_spent = 0
                    for i in range(level):
                        legacy_spent += int(defn.base_upgrade_cost) * (2 ** i)
                        new_spent += self._new_upgrade_cost(base_hourly_income=int(defn.base_hourly_income), level=i)
                    refund = max(int(legacy_spent) - int(new_spent), 0)
                    if refund <= 0:
                        continue

                    wallet = await session.scalar(
                        select(WalletRow).where(
                            WalletRow.guild_id == int(row.guild_id),
                            WalletRow.user_id == int(row.user_id),
                        )
                    )
                    if wallet is None:
                        wallet = WalletRow(
                            guild_id=int(row.guild_id),
                            user_id=int(row.user_id),
                            silver=0,
                            diamonds=0,
                        )
                        session.add(wallet)
                        await session.flush()

                    wallet.silver = int(wallet.silver or 0) + refund
                    wallet.silver_earned = int(wallet.silver_earned or 0) + refund
                    row.total_spent = max(int(row.total_spent or 0) - refund, 0)
                    refunded_total += refund
                    refunded_rows += 1

        state["refund_migration_ran"] = True
        self._save_runtime_state(state)
        log.info("Business refund migration done | rows=%s refunded_total=%s", refunded_rows, refunded_total)

    async def _notify_business_run_completed(self, notice: CompletedRunNotice) -> None:
        business_name = notice.business_key.replace("_", " ").title()
        async with self.sessionmaker() as session:
            async with session.begin():
                defs = await fetch_business_defs(session)
        def_map = {str(d.key): d for d in defs}
        matched = def_map.get(str(notice.business_key))
        if matched is not None:
            business_name = f"{matched.emoji} {matched.name}"

        event_summary = "No special events this run."
        if notice.event_outcomes:
            net_delta = sum(int(evt.silver_delta) for evt in notice.event_outcomes)
            sign = "+" if net_delta >= 0 else ""
            event_summary = (
                f"Events: **{len(notice.event_outcomes)}** triggered "
                f"(net {sign}{net_delta:,} Silver vs baseline)."
            )

        summary = {
            "business_name": business_name,
            "hours_paid_total": int(notice.hours_paid_total),
            "silver_paid_total": int(notice.silver_paid_total),
            "event_count": len(notice.event_outcomes),
            "net_event_delta": int(sum(int(evt.silver_delta) for evt in notice.event_outcomes)),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

        message = (
            f"<@{notice.user_id}> your business run finished: **{business_name}**\n"
            f"• Runtime paid: **{notice.hours_paid_total}h**\n"
            f"• Total earned: **{notice.silver_paid_total:,} Silver**\n"
            f"• {event_summary}"
        )

        notify_enabled = self._notifications_enabled_for(guild_id=int(notice.guild_id), user_id=int(notice.user_id))
        if not notify_enabled:
            self._push_pending_summary(guild_id=int(notice.guild_id), user_id=int(notice.user_id), summary=summary)
            return

        guild = self.bot.get_guild(int(notice.guild_id))
        if guild is None:
            return

        channels: list[discord.abc.Messageable] = []
        if guild.system_channel is not None:
            channels.append(guild.system_channel)
        for channel in guild.text_channels:
            if channel in channels:
                continue
            me = guild.me
            if me is None:
                continue
            perms = channel.permissions_for(me)
            if perms.send_messages:
                channels.append(channel)
            if len(channels) >= 3:
                break

        for channel in channels:
            try:
                await channel.send(message)
                return
            except Exception:
                continue

        user = self.bot.get_user(int(notice.user_id))
        if user is None:
            try:
                user = await self.bot.fetch_user(int(notice.user_id))
            except Exception:
                user = None
        if user is not None:
            try:
                await user.send(message)
                return
            except Exception:
                self._push_pending_summary(guild_id=int(notice.guild_id), user_id=int(notice.user_id), summary=summary)
                return
        self._push_pending_summary(guild_id=int(notice.guild_id), user_id=int(notice.user_id), summary=summary)

    async def _ensure_business_prestige_merge(self) -> None:
        state = self._load_runtime_state()
        merge_state = dict(state.get("business_prestige_system", {}))
        if bool(merge_state.get("merged", False)):
            return
        merge_state["merged"] = True
        state["business_prestige_system"] = merge_state
        self._save_runtime_state(state)
        log.info("Business prestige system merge flag written.")

    async def cog_load(self) -> None:
        await self._ensure_business_prestige_merge()
        await self._run_one_time_upgrade_refund()
        log.info(
            "Business runtime start requested | cog=%s running=%s",
            self.__class__.__name__,
            self.runtime_engine.running,
        )
        await self.runtime_engine.start_loop()
        log.info(
            "Business runtime started | cog=%s running=%s",
            self.__class__.__name__,
            self.runtime_engine.running,
        )

    async def cog_unload(self) -> None:
        log.info(
            "Business runtime stop requested | cog=%s running=%s",
            self.__class__.__name__,
            self.runtime_engine.running,
        )
        await self.runtime_engine.stop_loop()
        log.info(
            "Business runtime stopped | cog=%s running=%s",
            self.__class__.__name__,
            self.runtime_engine.running,
        )

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

        await _safe_defer(interaction, thinking=True)

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
        pending = self._pop_pending_summaries(guild_id=guild_id, user_id=user_id)
        if pending:
            lines = []
            for item in pending[-5:]:
                business_name = str(item.get("business_name", "Business"))
                silver = int(item.get("silver_paid_total", 0))
                hours = int(item.get("hours_paid_total", 0))
                net = int(item.get("net_event_delta", 0))
                sign = "+" if net >= 0 else ""
                lines.append(f"• **{business_name}** — {silver:,} Silver over {hours}h (events {sign}{net:,})")
            embed.add_field(
                name="📋 Offline Business Summary",
                value="\n".join(lines),
                inline=False,
            )
        view = BusinessHubView(
            cog=self,
            owner_id=user_id,
            guild_id=guild_id,
            hub_snapshot=snap,
        )
        await interaction.followup.send(embed=embed, view=view)

    @app_commands.command(name="business_notifications", description="Toggle business completion pings for your account.")
    async def business_notifications_cmd(self, interaction: discord.Interaction, enabled: bool) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        guild_id = int(interaction.guild.id)
        user_id = int(interaction.user.id)
        self._set_notifications_enabled_for(guild_id=guild_id, user_id=user_id, enabled=bool(enabled))
        msg = "Business completion notifications are now **ON**." if enabled else "Business completion notifications are now **OFF**. You'll see summaries next time you run `/business`."
        await interaction.response.send_message(msg, ephemeral=True)



    @app_commands.command(name="business_admin_hire_worker", description="[Admin/Debug] Manually hire a worker with explicit stats.")
    @app_commands.checks.has_permissions(administrator=True)
    async def business_admin_hire_worker_cmd(
        self,
        interaction: discord.Interaction,
        business_key: str,
        worker_name: str,
        worker_type: str,
        rarity: str,
        flat_profit_bonus: int,
        percent_profit_bonus_bp: int,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True, thinking=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                result = await hire_worker_manual(
                    session,
                    guild_id=int(interaction.guild.id),
                    user_id=int(interaction.user.id),
                    business_key=business_key,
                    worker_name=worker_name,
                    worker_type=worker_type,
                    rarity=rarity,
                    flat_profit_bonus=flat_profit_bonus,
                    percent_profit_bonus_bp=percent_profit_bonus_bp,
                )
                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=int(interaction.guild.id),
                    user_id=int(interaction.user.id),
                    business_key=business_key,
                )
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        if result.ok and result.hired_worker is not None:
            embed = _build_worker_hire_result_embed(user=interaction.user, detail=detail, hired=result.hired_worker)
            embed.add_field(name="Mode", value="Admin/Debug Manual Hire", inline=False)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        await interaction.followup.send(
            embed=_build_result_embed(title="Admin Worker Hire", message=result.message, ok=False),
            ephemeral=True,
        )

    @app_commands.command(name="business_admin_hire_manager", description="[Admin/Debug] Manually hire a manager with explicit stats.")
    @app_commands.checks.has_permissions(administrator=True)
    async def business_admin_hire_manager_cmd(
        self,
        interaction: discord.Interaction,
        business_key: str,
        manager_name: str,
        rarity: str,
        runtime_bonus_hours: int,
        profit_bonus_bp: int,
        auto_restart_charges: int,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True, thinking=True)
        async with self.sessionmaker() as session:
            async with session.begin():
                result = await hire_manager_manual(
                    session,
                    guild_id=int(interaction.guild.id),
                    user_id=int(interaction.user.id),
                    business_key=business_key,
                    manager_name=manager_name,
                    rarity=rarity,
                    runtime_bonus_hours=runtime_bonus_hours,
                    profit_bonus_bp=profit_bonus_bp,
                    auto_restart_charges=auto_restart_charges,
                )
                detail = await get_business_manage_snapshot(
                    session,
                    guild_id=int(interaction.guild.id),
                    user_id=int(interaction.user.id),
                    business_key=business_key,
                )
        if detail is None:
            await interaction.followup.send("That business could not be found.", ephemeral=True)
            return
        if result.ok and result.hired_manager is not None:
            embed = _build_manager_hire_result_embed(user=interaction.user, detail=detail, hired=result.hired_manager)
            embed.add_field(name="Mode", value="Admin/Debug Manual Hire", inline=False)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        await interaction.followup.send(
            embed=_build_result_embed(title="Admin Manager Hire", message=result.message, ok=False),
            ephemeral=True,
        )

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(BusinessCog(bot))
