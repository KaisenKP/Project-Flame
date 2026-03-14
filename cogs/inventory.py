# cogs/inventory.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ActiveEffectRow, ItemInventoryRow, StaminaRow
from services.db import sessions
from services.stamina import StaminaService
from services.users import ensure_user_rows

from services.items_catalog import EffectStacking, ITEMS, ItemDef
from services.jobs_core import clamp_int, fmt_int


UTC = timezone.utc

VIEW_TIMEOUT_SECONDS = 180


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _safe_int(v: object, default: int = 0) -> int:
    try:
        return int(v)  # type: ignore[arg-type]
    except Exception:
        return default


def _payload_int(payload: object, key: str, default: int = 0) -> int:
    if not isinstance(payload, dict):
        return default
    return _safe_int(payload.get(key, default), default)


def _is_effect_active(row: ActiveEffectRow, now: datetime) -> bool:
    exp = getattr(row, "expires_at", None)
    if exp is not None:
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=UTC)
        if exp <= now:
            return False

    ch = getattr(row, "charges_remaining", None)
    if ch is not None and int(ch) <= 0:
        return False

    return True


def _rarity_emoji(rarity_value: str) -> str:
    if rarity_value == "common":
        return "⚪"
    if rarity_value == "uncommon":
        return "🟢"
    if rarity_value == "rare":
        return "🔵"
    if rarity_value == "mythical":
        return "🟣"
    return "📦"


def _effect_summary(it: ItemDef) -> str:
    p = it.effect.payload
    dur = it.effect.duration_seconds
    ch = it.effect.charges

    def _dur_str(seconds: int) -> str:
        s = max(int(seconds), 0)
        if s < 60:
            return f"{s}s"
        m = s // 60
        if m < 60:
            return f"{m}m"
        h = m // 60
        m2 = m % 60
        if m2 == 0:
            return f"{h}h"
        return f"{h}h {m2:02d}m"

    lines: List[str] = []

    if "stamina_add" in p:
        lines.append(f"⚡ +{fmt_int(_payload_int(p, 'stamina_add'))} stamina (instant)")
    if "silver_add" in p:
        lines.append(f"💰 +{fmt_int(_payload_int(p, 'silver_add'))} silver (instant)")

    if "payout_bonus_bp" in p:
        lines.append(f"💸 +{_payload_int(p, 'payout_bonus_bp')/100:.2f}% payout")
    if "fail_reduction_bp" in p:
        lines.append(f"🛡️ -{_payload_int(p, 'fail_reduction_bp')/100:.2f}% fail chance")
    if "stamina_discount_bp" in p:
        lines.append(f"⚡ -{_payload_int(p, 'stamina_discount_bp')/100:.2f}% stamina cost")
    if "stamina_cost_flat_delta" in p:
        lines.append(f"⚡ stamina cost {_payload_int(p, 'stamina_cost_flat_delta'):+d} (flat)")
    if "job_xp_bonus_bp" in p:
        lines.append(f"🧰 +{_payload_int(p, 'job_xp_bonus_bp')/100:.2f}% job XP")
    if "user_xp_bonus_bp" in p:
        lines.append(f"🧠 +{_payload_int(p, 'user_xp_bonus_bp')/100:.2f}% user XP")
    if "double_payout_chance_bp" in p:
        lines.append(f"🪙 +{_payload_int(p, 'double_payout_chance_bp')/100:.2f}% 2x payout chance")
    if "stamina_cap_add" in p:
        lines.append(f"🔋 +{fmt_int(_payload_int(p, 'stamina_cap_add'))} max stamina (temp)")

    meta: List[str] = []
    if dur is not None:
        meta.append(f"⏳ {_dur_str(int(dur))}")
    if ch is not None:
        meta.append(f"🎯 {fmt_int(int(ch))} charges")
    if meta:
        lines.append(" • ".join(meta))

    if not lines:
        return "Temporary boost (catalog payload missing a summary key)."
    return "\n".join(lines)


async def _get_or_create_inv_row(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    item_key: str,
) -> ItemInventoryRow:
    row = await session.scalar(
        select(ItemInventoryRow).where(
            ItemInventoryRow.guild_id == guild_id,
            ItemInventoryRow.user_id == user_id,
            ItemInventoryRow.item_key == item_key,
        )
    )
    if row is None:
        row = ItemInventoryRow(guild_id=guild_id, user_id=user_id, item_key=item_key, qty=0)
        session.add(row)
        await session.flush()
    return row


async def _list_inventory(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
) -> List[ItemInventoryRow]:
    res = await session.execute(
        select(ItemInventoryRow)
        .where(
            ItemInventoryRow.guild_id == guild_id,
            ItemInventoryRow.user_id == user_id,
            ItemInventoryRow.qty > 0,
        )
        .order_by(ItemInventoryRow.item_key.asc())
    )
    return list(res.scalars().all())


async def _cleanup_expired_effects(session: AsyncSession, *, guild_id: int, user_id: int) -> None:
    now = _utc_now()
    await session.execute(
        delete(ActiveEffectRow).where(
            ActiveEffectRow.guild_id == guild_id,
            ActiveEffectRow.user_id == user_id,
            ActiveEffectRow.expires_at.is_not(None),
            ActiveEffectRow.expires_at <= now,
        )
    )


async def _active_effects_for_user(session: AsyncSession, *, guild_id: int, user_id: int) -> List[ActiveEffectRow]:
    await _cleanup_expired_effects(session, guild_id=guild_id, user_id=user_id)
    res = await session.execute(
        select(ActiveEffectRow)
        .where(
            ActiveEffectRow.guild_id == guild_id,
            ActiveEffectRow.user_id == user_id,
        )
        .order_by(ActiveEffectRow.created_at.desc(), ActiveEffectRow.id.desc())
    )
    rows = [r for r in res.scalars().all()]
    now = _utc_now()
    return [r for r in rows if _is_effect_active(r, now)]


async def _get_or_create_stamina_row(session: AsyncSession, *, guild_id: int, user_id: int) -> StaminaRow:
    row = await session.scalar(
        select(StaminaRow).where(
            StaminaRow.guild_id == guild_id,
            StaminaRow.user_id == user_id,
        )
    )
    if row is None:
        row = StaminaRow(guild_id=guild_id, user_id=user_id, current_stamina=0, max_stamina=100)
        session.add(row)
        await session.flush()
    return row


@dataclass(frozen=True)
class _ApplyResult:
    ok: bool
    title: str
    detail: str


def _is_same_stacking(a: object, b: EffectStacking) -> bool:
    if a == b:
        return True
    if isinstance(a, str) and a.lower() == b.value.lower():
        return True
    return False


async def _apply_item_use(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    item: ItemDef,
    stamina_service: StaminaService,
) -> _ApplyResult:
    now = _utc_now()

    payload: Dict[str, Any] = dict(item.effect.payload or {})
    duration_seconds = item.effect.duration_seconds
    charges = item.effect.charges
    stacking = item.effect.stacking
    group_key = str(item.effect.group_key)
    effect_key = str(item.effect.effect_key)

    stamina_add = _payload_int(payload, "stamina_add", 0)
    silver_add = _payload_int(payload, "silver_add", 0)

    is_instant = (duration_seconds is None) and (charges is None) and (stamina_add != 0 or silver_add != 0)

    if is_instant:
        if stamina_add != 0:
            srow = await _get_or_create_stamina_row(session, guild_id=guild_id, user_id=user_id)

            max_stam = _safe_int(getattr(srow, "max_stamina", 100), 100)
            cur_stam = _safe_int(getattr(srow, "current_stamina", 0), 0)
            new_cur = clamp_int(cur_stam + int(stamina_add), 0, max_stam)

            srow.current_stamina = int(new_cur)
            srow.last_regen_at = now

        if silver_add != 0:
            # Intentionally not supporting silver_add right now unless you add it to your economy rules.
            # If you do want it, wire WalletRow here.
            pass

        _ = stamina_service  # keep param stable if you want to route through service later

        return _ApplyResult(ok=True, title=f"✅ Used {item.name}", detail="Instant effect applied.")

    if duration_seconds is None and charges is None:
        return _ApplyResult(
            ok=False,
            title="❌ Item misconfigured",
            detail="This item has no duration/charges and isn’t an instant item.",
        )

    expires_at: Optional[datetime] = None
    if duration_seconds is not None:
        expires_at = now + timedelta(seconds=int(duration_seconds))

    charges_remaining: Optional[int] = None
    if charges is not None:
        charges_remaining = int(charges)

    existing = await session.execute(
        select(ActiveEffectRow)
        .where(
            ActiveEffectRow.guild_id == guild_id,
            ActiveEffectRow.user_id == user_id,
            ActiveEffectRow.group_key == group_key,
        )
        .order_by(ActiveEffectRow.created_at.desc(), ActiveEffectRow.id.desc())
        .with_for_update()
    )
    group_rows = [r for r in existing.scalars().all() if _is_effect_active(r, now)]
    same_effect_rows = [r for r in group_rows if str(r.effect_key) == effect_key]

    if _is_same_stacking(stacking, EffectStacking.DENY):
        if group_rows:
            return _ApplyResult(
                ok=False,
                title="⛔ Buff blocked",
                detail=f"You already have an active **{group_key}** buff.",
            )

    if _is_same_stacking(stacking, EffectStacking.REPLACE):
        for r in group_rows:
            await session.delete(r)

    if _is_same_stacking(stacking, EffectStacking.REFRESH):
        if same_effect_rows:
            r = same_effect_rows[0]
            if expires_at is not None:
                r.expires_at = expires_at
            if charges_remaining is not None:
                r.charges_remaining = charges_remaining
            r.payload_json = dict(payload)
            return _ApplyResult(ok=True, title=f"✅ Refreshed {item.name}", detail="Buff refreshed.")

    if _is_same_stacking(stacking, EffectStacking.EXTEND):
        if same_effect_rows and duration_seconds is not None:
            r = same_effect_rows[0]
            cur_exp = getattr(r, "expires_at", None)
            if cur_exp is None:
                r.expires_at = expires_at
            else:
                if cur_exp.tzinfo is None:
                    cur_exp = cur_exp.replace(tzinfo=UTC)
                r.expires_at = cur_exp + timedelta(seconds=int(duration_seconds))
            if charges_remaining is not None and r.charges_remaining is None:
                r.charges_remaining = charges_remaining
            return _ApplyResult(ok=True, title=f"✅ Extended {item.name}", detail="Buff extended.")

    session.add(
        ActiveEffectRow(
            guild_id=guild_id,
            user_id=user_id,
            effect_key=effect_key,
            group_key=group_key,
            payload_json=dict(payload),
            expires_at=expires_at,
            charges_remaining=charges_remaining,
        )
    )
    return _ApplyResult(ok=True, title=f"✅ Used {item.name}", detail="Buff applied.")


class InventorySelect(discord.ui.Select):
    def __init__(self, *, options: List[discord.SelectOption]):
        super().__init__(
            placeholder="Pick an item…",
            min_values=1,
            max_values=1,
            options=options[:25],
        )

    async def callback(self, interaction: discord.Interaction):
        view: InventoryView = self.view  # type: ignore[assignment]
        view.selected_item_key = self.values[0]
        await view.cog._handle_refresh(interaction, view=view, include_effects=False)


class UseItemButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.primary, label="Use", emoji="⚡", row=1)

    async def callback(self, interaction: discord.Interaction):
        view: InventoryView = self.view  # type: ignore[assignment]
        await view.cog._handle_use(interaction, view=view)


class RefreshInvButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Refresh", emoji="🔄", row=1)

    async def callback(self, interaction: discord.Interaction):
        view: InventoryView = self.view  # type: ignore[assignment]
        await view.cog._handle_refresh(interaction, view=view, include_effects=False)


class ShowEffectsButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.success, label="Active Effects", emoji="✨", row=1)

    async def callback(self, interaction: discord.Interaction):
        view: InventoryView = self.view  # type: ignore[assignment]
        await view.cog._handle_refresh(interaction, view=view, include_effects=True)


class InventoryView(discord.ui.View):
    def __init__(self, *, cog: "InventoryCog", guild_id: int, inv_owner_id: int, viewer_id: int):
        super().__init__(timeout=VIEW_TIMEOUT_SECONDS)
        self.cog = cog
        self.guild_id = guild_id
        self.inv_owner_id = inv_owner_id
        self.viewer_id = viewer_id

        self.selected_item_key: Optional[str] = None
        self.select: Optional[InventorySelect] = None

        self.btn_use = UseItemButton()
        self.btn_refresh = RefreshInvButton()
        self.btn_effects = ShowEffectsButton()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user is None:
            return False
        if interaction.user.id != self.viewer_id:
            await interaction.response.send_message("This panel isn’t for you.", ephemeral=True)
            return False
        return True

    def set_select(self, options: List[discord.SelectOption]) -> None:
        if self.select:
            self.remove_item(self.select)
        self.select = InventorySelect(options=options)
        self.add_item(self.select)

        if self.btn_use not in self.children:
            self.add_item(self.btn_use)
        if self.btn_refresh not in self.children:
            self.add_item(self.btn_refresh)
        if self.btn_effects not in self.children:
            self.add_item(self.btn_effects)


def _inv_embed(
    *,
    owner: discord.abc.User,
    viewer: discord.abc.User,
    inv_rows: List[ItemInventoryRow],
    selected_key: Optional[str],
    effects_rows: Optional[List[ActiveEffectRow]],
) -> discord.Embed:
    is_self = int(owner.id) == int(viewer.id)

    title = f"🎒 Inventory • {owner}"
    desc = f"Owner: <@{owner.id}>"
    if not is_self:
        desc += f"\nViewer: <@{viewer.id}>"

    embed = discord.Embed(title=title, description=desc, color=discord.Color.blurple())

    if not inv_rows:
        embed.add_field(name="Items", value="Empty. Go shopping. 😈", inline=False)
        embed.set_footer(text="Use /shop to buy. Use /inventory to view.")
        return embed

    lines: List[str] = []
    for r in inv_rows[:12]:
        key = str(r.item_key)
        qty = _safe_int(getattr(r, "qty", 0), 0)
        it = ITEMS.get(key)
        if it is None:
            lines.append(f"📦 `{key}` × **{fmt_int(qty)}**")
        else:
            lines.append(f"{_rarity_emoji(it.rarity.value)} **{it.name}** × **{fmt_int(qty)}**")

    if len(inv_rows) > 12:
        lines.append(f"…and **{fmt_int(len(inv_rows) - 12)}** more")

    embed.add_field(name="Items", value="\n".join(lines), inline=False)

    if selected_key:
        it = ITEMS.get(selected_key)
        qty = 0
        for r in inv_rows:
            if str(r.item_key) == selected_key:
                qty = _safe_int(getattr(r, "qty", 0), 0)
                break

        if it is None:
            embed.add_field(name="Selected", value=f"`{selected_key}` × **{fmt_int(qty)}**", inline=False)
        else:
            embed.add_field(
                name=f"Selected • {_rarity_emoji(it.rarity.value)} {it.name} (x{fmt_int(qty)})",
                value=_effect_summary(it),
                inline=False,
            )

    if effects_rows is not None:
        if not effects_rows:
            embed.add_field(name="Active Effects", value="None.", inline=False)
        else:
            now = _utc_now()
            out: List[str] = []
            for r in effects_rows[:10]:
                ek = str(r.effect_key)
                gk = str(r.group_key)
                exp = getattr(r, "expires_at", None)
                ch = getattr(r, "charges_remaining", None)

                bits: List[str] = [f"**{ek}** (`{gk}`)"]
                if exp is not None:
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=UTC)
                    left = max(int((exp - now).total_seconds()), 0)
                    m = left // 60
                    h = m // 60
                    m2 = m % 60
                    bits.append(f"⏳ {h}h {m2:02d}m")
                if ch is not None:
                    bits.append(f"🎯 {fmt_int(int(ch))}")
                out.append(" • ".join(bits))

            if len(effects_rows) > 10:
                out.append(f"…and **{fmt_int(len(effects_rows) - 10)}** more")

            embed.add_field(name="Active Effects", value="\n".join(out), inline=False)

    embed.set_footer(text="Dropdown to select. Use button to consume. Effects button shows buffs.")
    avatar_url = getattr(getattr(owner, "display_avatar", None), "url", None)
    if avatar_url:
        embed.set_author(name=str(owner), icon_url=avatar_url)
    return embed


class InventoryCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self.stamina = StaminaService()

    async def _edit_panel(self, interaction: discord.Interaction, *, embed: discord.Embed, view: discord.ui.View) -> None:
        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(embed=embed, view=view)
                return
        except Exception:
            pass

        try:
            if interaction.message:
                await interaction.message.edit(embed=embed, view=view)
                return
        except Exception:
            pass

        # last resort
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    async def _build_panel(
        self,
        *,
        guild_id: int,
        inv_owner: discord.abc.User,
        viewer: discord.abc.User,
        selected_key: Optional[str],
        include_effects: bool,
    ) -> Tuple[discord.Embed, InventoryView]:
        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=inv_owner.id)
                await ensure_user_rows(session, guild_id=guild_id, user_id=viewer.id)

                inv_rows = await _list_inventory(session, guild_id=guild_id, user_id=inv_owner.id)

                if selected_key is None and inv_rows:
                    selected_key = str(inv_rows[0].item_key)

                effects_rows: Optional[List[ActiveEffectRow]] = None
                if include_effects:
                    effects_rows = await _active_effects_for_user(session, guild_id=guild_id, user_id=inv_owner.id)

        opts: List[discord.SelectOption] = []
        for r in inv_rows[:25]:
            key = str(r.item_key)
            qty = _safe_int(getattr(r, "qty", 0), 0)
            it = ITEMS.get(key)

            if it is None:
                label = f"{key} x{qty}"
                desc = "Unknown item (missing from catalog)"
                emoji = "📦"
            else:
                label = f"{it.name} x{qty}"
                desc = (it.rarity.value or "item").capitalize()
                emoji = _rarity_emoji(it.rarity.value)

            opts.append(
                discord.SelectOption(
                    label=label[:100],
                    value=key,
                    description=desc[:100],
                    emoji=emoji,
                    default=(selected_key == key),
                )
            )

        view = InventoryView(cog=self, guild_id=guild_id, inv_owner_id=int(inv_owner.id), viewer_id=int(viewer.id))
        view.selected_item_key = selected_key
        view.set_select(opts)

        view.btn_use.disabled = (selected_key is None) or (int(inv_owner.id) != int(viewer.id))

        embed = _inv_embed(
            owner=inv_owner,
            viewer=viewer,
            inv_rows=inv_rows,
            selected_key=selected_key,
            effects_rows=effects_rows,
        )
        return embed, view

    async def _handle_refresh(self, interaction: discord.Interaction, *, view: InventoryView, include_effects: bool) -> None:
        if interaction.guild is None:
            return

        try:
            owner = interaction.guild.get_member(view.inv_owner_id) or interaction.user
            embed, new_view = await self._build_panel(
                guild_id=interaction.guild.id,
                inv_owner=owner,
                viewer=interaction.user,
                selected_key=view.selected_item_key,
                include_effects=include_effects,
            )
            await self._edit_panel(interaction, embed=embed, view=new_view)
        except Exception as e:
            await interaction.followup.send(f"Inventory refresh crashed: `{type(e).__name__}`", ephemeral=True)

    async def _handle_use(self, interaction: discord.Interaction, *, view: InventoryView) -> None:
        if interaction.guild is None:
            return

        if int(interaction.user.id) != int(view.inv_owner_id):
            await interaction.response.send_message("You can’t use someone else’s items.", ephemeral=True)
            return

        item_key = view.selected_item_key
        if not item_key:
            await interaction.response.send_message("Pick an item first.", ephemeral=True)
            return

        try:
            if not interaction.response.is_done():
                await interaction.response.defer(thinking=True)

            it = ITEMS.get(item_key)
            if it is None:
                await interaction.followup.send("That item doesn’t exist in the catalog anymore.", ephemeral=True)
                return

            guild_id = interaction.guild.id
            user_id = interaction.user.id

            async with self.sessionmaker() as session:
                async with session.begin():
                    await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                    inv = await _get_or_create_inv_row(session, guild_id=guild_id, user_id=user_id, item_key=item_key)
                    if int(inv.qty) <= 0:
                        await interaction.followup.send("You don’t have that item.", ephemeral=True)
                        return

                    res = await _apply_item_use(
                        session,
                        guild_id=guild_id,
                        user_id=user_id,
                        item=it,
                        stamina_service=self.stamina,
                    )
                    if not res.ok:
                        await interaction.followup.send(f"{res.title}\n{res.detail}", ephemeral=True)
                        return

                    inv.qty = int(inv.qty) - 1
                    if inv.qty < 0:
                        inv.qty = 0

            await interaction.followup.send(f"{res.title}\n{res.detail}", ephemeral=True)

            owner = interaction.guild.get_member(user_id) or interaction.user
            embed, new_view = await self._build_panel(
                guild_id=guild_id,
                inv_owner=owner,
                viewer=interaction.user,
                selected_key=item_key,
                include_effects=False,
            )
            await self._edit_panel(interaction, embed=embed, view=new_view)

        except Exception as e:
            await interaction.followup.send(f"Use crashed: `{type(e).__name__}`", ephemeral=True)

    @app_commands.command(name="inventory", description="View an inventory and use your items (buttons).")
    @app_commands.describe(user="Whose inventory to view (defaults to you)")
    async def inventory_cmd(self, interaction: discord.Interaction, user: Optional[discord.Member] = None):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return

        await interaction.response.defer(thinking=True)

        owner = user or interaction.user
        try:
            embed, view = await self._build_panel(
                guild_id=interaction.guild.id,
                inv_owner=owner,
                viewer=interaction.user,
                selected_key=None,
                include_effects=False,
            )
            await interaction.followup.send(embed=embed, view=view)
        except Exception as e:
            await interaction.followup.send(f"Inventory panel crashed: `{type(e).__name__}`", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(InventoryCog(bot))