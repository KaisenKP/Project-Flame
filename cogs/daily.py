from __future__ import annotations

import asyncio
import json
import random
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select, text

from db.models import LootboxInventoryRow, StaminaRow, WalletRow, XpRow
from services.db import sessions
from services.users import ensure_user_rows
from services.vip import is_vip_member
from services.xp_award import award_xp


DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
DAILY_FILE = DATA_DIR / "daily_claims.json"


def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _cooldown_hms(seconds: int) -> str:
    s = max(int(seconds), 0)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    if h > 0:
        return f"{h}h {m:02d}m {sec:02d}s"
    if m > 0:
        return f"{m}m {sec:02d}s"
    return f"{sec}s"


def _utc_stamp(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(int(n), hi))


def _lerp(a: float, b: float, t: float) -> float:
    t2 = max(0.0, min(float(t), 1.0))
    return a + (b - a) * t2


class LootboxRarity(str, Enum):
    COMMON = "common"
    RARE = "rare"
    EPIC = "epic"
    LEGENDARY = "legendary"


class RemindMode(str, Enum):
    DM = "dm"
    CHANNEL = "channel"


def _rarity_emoji(r: LootboxRarity) -> str:
    if r == LootboxRarity.COMMON:
        return "📦"
    if r == LootboxRarity.RARE:
        return "🎁"
    if r == LootboxRarity.EPIC:
        return "🧰"
    return "👑"


def _rarity_color(r: LootboxRarity) -> discord.Color:
    if r == LootboxRarity.COMMON:
        return discord.Color.light_grey()
    if r == LootboxRarity.RARE:
        return discord.Color.blue()
    if r == LootboxRarity.EPIC:
        return discord.Color.purple()
    return discord.Color.gold()


def _mode_label(mode: RemindMode) -> str:
    return "DM" if mode == RemindMode.DM else "Channel"


def _mode_emoji(mode: RemindMode) -> str:
    return "📩" if mode == RemindMode.DM else "💬"


@dataclass(frozen=True)
class DailyReward:
    silver: int
    xp: int
    bonus_silver: int
    bonus_xp: int
    level_multiplier: int
    streak: int
    xp_mult: float
    milestone_hit: bool
    comeback_bonus_hit: bool
    jackpot_hit: bool
    lootbox_rarity: LootboxRarity | None
    lootbox_amount: int
    lootbox_write_failed: bool
    lootbox_write_error: str
    remind_enabled: bool
    remind_mode: RemindMode


class DailyReminderView(discord.ui.View):
    def __init__(
        self,
        *,
        cog: "DailyCog",
        guild_id: int,
        user_id: int,
        remind_enabled: bool,
        remind_mode: RemindMode,
        channel_id_hint: int | None,
        timeout: float,
    ):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = int(guild_id)
        self.user_id = int(user_id)
        self.remind_enabled = bool(remind_enabled)
        self.remind_mode = RemindMode(remind_mode)
        self.channel_id_hint = int(channel_id_hint) if channel_id_hint else None
        self._sync()

    def _sync(self) -> None:
        for item in self.children:
            if not isinstance(item, discord.ui.Button):
                continue

            if item.custom_id == "daily_remind_toggle":
                item.label = "🔔 Reminders: ON" if self.remind_enabled else "🔕 Reminders: OFF"
                item.style = discord.ButtonStyle.success if self.remind_enabled else discord.ButtonStyle.secondary

            if item.custom_id == "daily_remind_mode":
                item.label = f"{_mode_emoji(self.remind_mode)} Mode: {_mode_label(self.remind_mode)}"

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            try:
                await interaction.response.send_message("Server only.", ephemeral=True)
            except Exception:
                pass
            return False
        if int(interaction.guild.id) != self.guild_id:
            try:
                await interaction.response.send_message("Wrong server.", ephemeral=True)
            except Exception:
                pass
            return False
        if int(interaction.user.id) != self.user_id:
            try:
                await interaction.response.send_message("Not your button. Run `/daily status`.", ephemeral=True)
            except Exception:
                pass
            return False
        return True

    @discord.ui.button(
        label="🔕 Reminders: OFF",
        style=discord.ButtonStyle.secondary,
        custom_id="daily_remind_toggle",
    )
    async def toggle_reminders(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not (await self._guard(interaction)):
            return

        self.remind_enabled = not self.remind_enabled

        # update last/known channel
        ch_id = None
        if interaction.channel is not None:
            ch_id = int(interaction.channel.id)
            self.channel_id_hint = ch_id

        await self.cog._set_remind_pref(
            self.guild_id,
            self.user_id,
            enabled=self.remind_enabled,
            mode=self.remind_mode,
            channel_id=self.channel_id_hint,
            last_channel_id=ch_id,
        )

        if self.remind_enabled:
            await self.cog._schedule_or_refresh_reminder(self.guild_id, self.user_id)
        else:
            self.cog._cancel_reminder_task(self.guild_id, self.user_id)

        self._sync()

        try:
            if self.remind_enabled:
                await interaction.response.send_message(
                    f"✅ Reminders **ON**. Delivery: **{_mode_label(self.remind_mode)}**.",
                    ephemeral=True,
                )
            else:
                await interaction.response.send_message("✅ Reminders **OFF**. Silent mode.", ephemeral=True)
        except Exception:
            pass

        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass

    @discord.ui.button(
        label="📩 Mode: DM",
        style=discord.ButtonStyle.primary,
        custom_id="daily_remind_mode",
    )
    async def toggle_mode(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not (await self._guard(interaction)):
            return

        self.remind_mode = RemindMode.CHANNEL if self.remind_mode == RemindMode.DM else RemindMode.DM

        ch_id = None
        if interaction.channel is not None:
            ch_id = int(interaction.channel.id)
            self.channel_id_hint = ch_id

        await self.cog._set_remind_pref(
            self.guild_id,
            self.user_id,
            enabled=self.remind_enabled,
            mode=self.remind_mode,
            channel_id=self.channel_id_hint,
            last_channel_id=ch_id,
        )

        if self.remind_enabled:
            await self.cog._schedule_or_refresh_reminder(self.guild_id, self.user_id)

        self._sync()

        try:
            await interaction.response.send_message(
                f"✅ Mode set to **{_mode_label(self.remind_mode)}**.",
                ephemeral=True,
            )
        except Exception:
            pass

        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass


class DailyCog(commands.Cog):
    BASE_SILVER = 250
    BASE_XP = 100
    VIP_MULT = 2

    STREAK_MAX = 60
    STREAK_BREAK_SECONDS = 48 * 60 * 60
    COOLDOWN_SECONDS = 24 * 60 * 60

    STREAK_SILVER_BONUS = 10
    XP_STEP_BONUS = 0.2

    STREAK_MILESTONE_INTERVAL = 7
    STREAK_MILESTONE_SILVER = 450
    STREAK_MILESTONE_XP = 150

    COMEBACK_MIN_PREV_STREAK = 7
    COMEBACK_SILVER = 220
    COMEBACK_XP = 100

    JACKPOT_BP_MIN = 120
    JACKPOT_BP_MAX = 400
    JACKPOT_SILVER_MULTIPLIER = 0.40

    LOOTBOX_DROP_BP_MIN = 1800
    LOOTBOX_DROP_BP_MAX = 4200

    LOOTBOX_TRIPLE_BP_MIN = 150
    LOOTBOX_TRIPLE_BP_MAX = 900

    STAFF_ALERT_CHANNEL_ID = 1461115017662566631

    REMIND_BEFORE_SECONDS = 60 * 60  # 1h before streak break
    REMINDER_VIEW_TIMEOUT = 10 * 60  # 10m buttons
    REMINDER_BOOT_DELAY_SECONDS = 2

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self._lock = asyncio.Lock()
        self._state: dict[str, dict[str, int | str]] = self._load_state()
        self._reminder_tasks: dict[str, asyncio.Task] = {}

        # rebuild reminders after restart
        self.bot.loop.create_task(self._bootstrap_reminders())

    async def cog_unload(self):
        for t in list(self._reminder_tasks.values()):
            try:
                t.cancel()
            except Exception:
                pass
        self._reminder_tasks.clear()

    # -------------------------
    # State
    # -------------------------

    def _key(self, guild_id: int, user_id: int) -> str:
        return f"{int(guild_id)}:{int(user_id)}"

    def _load_state(self) -> dict[str, dict[str, int | str]]:
        if not DAILY_FILE.exists():
            return {}

        try:
            with DAILY_FILE.open("r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception:
            return {}

        out: dict[str, dict[str, int | str]] = {}
        try:
            for k, v in (raw or {}).items():
                if isinstance(v, dict):
                    last = int(v.get("last", 0))
                    streak = int(v.get("streak", 0))
                    remind = 1 if int(v.get("remind", 0)) else 0

                    mode_raw = (v.get("remind_mode") or "dm")
                    mode = "channel" if str(mode_raw).lower() == "channel" else "dm"

                    remind_channel = int(v.get("remind_channel", 0) or 0)
                    last_channel = int(v.get("last_channel", 0) or 0)

                    out[str(k)] = {
                        "last": max(last, 0),
                        "streak": max(streak, 0),
                        "remind": remind,
                        "remind_mode": mode,
                        "remind_channel": max(remind_channel, 0),
                        "last_channel": max(last_channel, 0),
                    }
                else:
                    # legacy format: "gid:uid": last_ts
                    out[str(k)] = {
                        "last": max(int(v), 0),
                        "streak": 0,
                        "remind": 0,
                        "remind_mode": "dm",
                        "remind_channel": 0,
                        "last_channel": 0,
                    }
        except Exception:
            return {}

        return out

    def _save_state(self) -> None:
        tmp = DAILY_FILE.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(self._state, f)
        tmp.replace(DAILY_FILE)

    async def _get_state(self, guild_id: int, user_id: int) -> tuple[int, int, bool, RemindMode, int, int]:
        k = self._key(guild_id, user_id)
        row = self._state.get(k)
        if not row:
            return 0, 0, False, RemindMode.DM, 0, 0

        last = int(row.get("last", 0) or 0)
        streak = int(row.get("streak", 0) or 0)
        remind_enabled = bool(int(row.get("remind", 0) or 0))

        mode_raw = str(row.get("remind_mode", "dm") or "dm").lower()
        mode = RemindMode.CHANNEL if mode_raw == "channel" else RemindMode.DM

        remind_channel = int(row.get("remind_channel", 0) or 0)
        last_channel = int(row.get("last_channel", 0) or 0)
        return last, streak, remind_enabled, mode, remind_channel, last_channel

    async def _set_state(self, guild_id: int, user_id: int, *, last_ts: int, streak: int) -> None:
        k = self._key(guild_id, user_id)
        async with self._lock:
            cur = self._state.get(k) or {}
            self._state[k] = {
                "last": int(last_ts),
                "streak": int(streak),
                "remind": int(cur.get("remind", 0) or 0),
                "remind_mode": str(cur.get("remind_mode", "dm") or "dm"),
                "remind_channel": int(cur.get("remind_channel", 0) or 0),
                "last_channel": int(cur.get("last_channel", 0) or 0),
            }
            self._save_state()

    async def _set_remind_pref(
        self,
        guild_id: int,
        user_id: int,
        *,
        enabled: bool,
        mode: RemindMode,
        channel_id: int | None,
        last_channel_id: int | None,
    ) -> None:
        k = self._key(guild_id, user_id)
        async with self._lock:
            cur = self._state.get(k) or {
                "last": 0,
                "streak": 0,
                "remind": 0,
                "remind_mode": "dm",
                "remind_channel": 0,
                "last_channel": 0,
            }

            # mode + enabled
            cur["remind"] = 1 if enabled else 0
            cur["remind_mode"] = "channel" if mode == RemindMode.CHANNEL else "dm"

            # when they interact, keep last_channel fresh
            if last_channel_id:
                cur["last_channel"] = int(last_channel_id)

            # if they use channel mode, store a channel hint (prefer the channel they clicked in)
            if channel_id:
                cur["remind_channel"] = int(channel_id)

            self._state[k] = {
                "last": int(cur.get("last", 0) or 0),
                "streak": int(cur.get("streak", 0) or 0),
                "remind": int(cur.get("remind", 0) or 0),
                "remind_mode": str(cur.get("remind_mode", "dm") or "dm"),
                "remind_channel": int(cur.get("remind_channel", 0) or 0),
                "last_channel": int(cur.get("last_channel", 0) or 0),
            }
            self._save_state()

    async def _touch_last_channel(self, guild_id: int, user_id: int, channel_id: int | None) -> None:
        if not channel_id:
            return
        k = self._key(guild_id, user_id)
        async with self._lock:
            cur = self._state.get(k)
            if not cur:
                self._state[k] = {
                    "last": 0,
                    "streak": 0,
                    "remind": 0,
                    "remind_mode": "dm",
                    "remind_channel": int(channel_id),
                    "last_channel": int(channel_id),
                }
            else:
                cur["last_channel"] = int(channel_id)
                # also keep remind_channel warm so fallback has a target
                if int(cur.get("remind_channel", 0) or 0) <= 0:
                    cur["remind_channel"] = int(channel_id)
            self._save_state()

    # -------------------------
    # Reminders (restart-safe)
    # -------------------------

    async def _bootstrap_reminders(self) -> None:
        await asyncio.sleep(float(self.REMINDER_BOOT_DELAY_SECONDS))
        for k, row in list(self._state.items()):
            try:
                remind_enabled = bool(int(row.get("remind", 0) or 0))
                if not remind_enabled:
                    continue
                last = int(row.get("last", 0) or 0)
                if last <= 0:
                    continue
                gid_s, uid_s = (str(k).split(":", 1) + ["0"])[:2]
                gid = int(gid_s)
                uid = int(uid_s)
                await self._schedule_or_refresh_reminder(gid, uid)
            except Exception:
                continue

    def _cancel_reminder_task(self, guild_id: int, user_id: int) -> None:
        k = self._key(guild_id, user_id)
        t = self._reminder_tasks.pop(k, None)
        if t is not None:
            try:
                t.cancel()
            except Exception:
                pass

    async def _schedule_or_refresh_reminder(self, guild_id: int, user_id: int) -> None:
        self._cancel_reminder_task(guild_id, user_id)

        last_ts, _streak, remind_enabled, _mode, _rch, _lch = await self._get_state(guild_id, user_id)
        if not remind_enabled:
            return
        if last_ts <= 0:
            return

        now = int(time.time())
        break_ts = int(last_ts) + int(self.STREAK_BREAK_SECONDS)
        remind_ts = int(break_ts) - int(self.REMIND_BEFORE_SECONDS)

        if now >= break_ts:
            return

        # if we boot late and should have reminded already, remind asap (but not instant spam)
        delay = int(remind_ts - now)
        if delay < 0:
            delay = 5
        else:
            delay = max(5, delay)

        task = self.bot.loop.create_task(self._reminder_worker(guild_id, user_id, expected_last_ts=int(last_ts), delay=delay))
        self._reminder_tasks[self._key(guild_id, user_id)] = task

    async def _reminder_worker(self, guild_id: int, user_id: int, *, expected_last_ts: int, delay: int) -> None:
        try:
            await asyncio.sleep(float(delay))
        except asyncio.CancelledError:
            return
        except Exception:
            return

        last_ts, _streak, remind_enabled, mode, remind_channel, last_channel = await self._get_state(guild_id, user_id)
        if not remind_enabled:
            return
        if int(last_ts) != int(expected_last_ts):
            return

        now = int(time.time())
        break_ts = int(last_ts) + int(self.STREAK_BREAK_SECONDS)
        if now >= break_ts:
            return

        remaining = int(break_ts - now)

        # build message
        title = "🔔 Daily Reminder"
        desc = (
            f"Your streak breaks in **{_cooldown_hms(remaining)}**.\n"
            "Run **/daily claim** to keep it alive."
        )
        embed = discord.Embed(title=title, description=desc, color=discord.Color.blurple())
        embed.set_footer(text="If you hate reminders, toggle them OFF next time you claim.")

        # attempt DM first if chosen
        if mode == RemindMode.DM:
            try:
                user = self.bot.get_user(int(user_id)) or await self.bot.fetch_user(int(user_id))
                if user is not None:
                    await user.send(embed=embed)
                    return
            except Exception:
                # DM failed -> requested: default to chat
                pass

        # channel mode OR DM fallback
        channel_id = int(remind_channel or 0) or int(last_channel or 0)
        if channel_id <= 0:
            return

        try:
            ch = self.bot.get_channel(channel_id)
            if ch is None:
                ch = await self.bot.fetch_channel(channel_id)

            if isinstance(ch, (discord.TextChannel, discord.Thread)):
                content = f"<@{int(user_id)}>"
                await ch.send(content=content, embed=embed)
                return
        except Exception:
            return

    # -------------------------
    # RNG / loot
    # -------------------------

    @staticmethod
    def _roll_bp(chance_bp: int) -> bool:
        bp = max(int(chance_bp), 0)
        if bp <= 0:
            return False
        if bp >= 10000:
            return True
        return random.randint(1, 10000) <= bp

    def _lootbox_drop_chance_bp(self, streak: int) -> int:
        s = _clamp(streak, 0, self.STREAK_MAX)
        t = s / float(self.STREAK_MAX) if self.STREAK_MAX > 0 else 0.0
        return int(round(_lerp(self.LOOTBOX_DROP_BP_MIN, self.LOOTBOX_DROP_BP_MAX, t)))

    def _lootbox_triple_chance_bp(self, streak: int) -> int:
        s = _clamp(streak, 0, self.STREAK_MAX)
        t = s / float(self.STREAK_MAX) if self.STREAK_MAX > 0 else 0.0
        return int(round(_lerp(self.LOOTBOX_TRIPLE_BP_MIN, self.LOOTBOX_TRIPLE_BP_MAX, t)))

    def _pick_lootbox_rarity(self, streak: int) -> LootboxRarity:
        # Legendary stays sub-1%.
        # At 0:  common 8200, rare 1600, epic 180, legendary 20 (0.20%)
        # At 60: common 5600, rare 3200, epic 1120, legendary 80 (0.80%)
        s = _clamp(streak, 0, self.STREAK_MAX)
        t = s / float(self.STREAK_MAX) if self.STREAK_MAX > 0 else 0.0

        w_common = int(round(_lerp(8200, 5600, t)))
        w_rare = int(round(_lerp(1600, 3200, t)))
        w_epic = int(round(_lerp(180, 1120, t)))
        w_legendary = int(round(_lerp(20, 80, t)))

        total = max(w_common + w_rare + w_epic + w_legendary, 1)
        r = random.randint(1, total)

        if r <= w_common:
            return LootboxRarity.COMMON
        r -= w_common
        if r <= w_rare:
            return LootboxRarity.RARE
        r -= w_rare
        if r <= w_epic:
            return LootboxRarity.EPIC
        return LootboxRarity.LEGENDARY

    # -------------------------
    # Lootbox DB (safe upsert)
    # -------------------------

    def _lootbox_table_name(self) -> str:
        try:
            return str(LootboxInventoryRow.__table__.name)
        except Exception:
            return "lootbox_inventory"

    def _lootbox_create_sql(self) -> str:
        tname = self._lootbox_table_name()
        return f"""
        CREATE TABLE IF NOT EXISTS `{tname}` (
            id INT NOT NULL AUTO_INCREMENT,
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            rarity VARCHAR(32) NOT NULL,
            amount INT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uq_lootbox_inventory_guild_user_rarity (guild_id, user_id, rarity),
            KEY ix_lootbox_inventory_guild_id (guild_id),
            KEY ix_lootbox_inventory_user_id (user_id)
        );
        """

    async def _ensure_lootbox_table(self) -> None:
        sql = self._lootbox_create_sql()
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(sql))

    async def _add_lootbox(self, *, guild_id: int, user_id: int, rarity: LootboxRarity, amount: int) -> None:
        amt = int(amount)
        if amt <= 0:
            return

        await self._ensure_lootbox_table()
        tname = self._lootbox_table_name()

        upsert_sql = text(f"""
            INSERT INTO `{tname}` (guild_id, user_id, rarity, amount)
            VALUES (:guild_id, :user_id, :rarity, :amount)
            ON DUPLICATE KEY UPDATE amount = amount + VALUES(amount)
        """)

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=int(guild_id), user_id=int(user_id))
                await session.execute(
                    upsert_sql,
                    {
                        "guild_id": int(guild_id),
                        "user_id": int(user_id),
                        "rarity": str(rarity.value),
                        "amount": int(amt),
                    },
                )

    async def _alert_staff(self, guild: discord.Guild, *, title: str, body: str) -> None:
        try:
            ch = guild.get_channel(self.STAFF_ALERT_CHANNEL_ID)
            if ch is None:
                ch = await self.bot.fetch_channel(self.STAFF_ALERT_CHANNEL_ID)
            if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                return
            embed = discord.Embed(title=title, description=body[:3500], color=discord.Color.red())
            await ch.send(content="@staff", embed=embed)
        except Exception:
            pass

    # -------------------------
    # Streak math
    # -------------------------

    def _compute_next_streak(self, *, last_ts: int, now_ts: int, prev_streak: int) -> int:
        if last_ts <= 0:
            return 1
        gap = int(now_ts - last_ts)
        if gap > self.STREAK_BREAK_SECONDS:
            return 1
        return _clamp(int(prev_streak) + 1, 1, self.STREAK_MAX)

    def _xp_multiplier(self, streak: int) -> float:
        s = _clamp(streak, 1, self.STREAK_MAX)
        return 1.0 + (self.XP_STEP_BONUS * float(s))

    def _jackpot_chance_bp(self, streak: int) -> int:
        s = _clamp(streak, 1, self.STREAK_MAX)
        t = s / float(self.STREAK_MAX) if self.STREAK_MAX > 0 else 0.0
        return int(round(_lerp(self.JACKPOT_BP_MIN, self.JACKPOT_BP_MAX, t)))

    @staticmethod
    def _progress_bar(value: int, total: int, *, width: int = 12) -> str:
        w = max(int(width), 3)
        if total <= 0:
            return "░" * w
        ratio = max(0.0, min(float(value) / float(total), 1.0))
        filled = int(round(ratio * w))
        return ("█" * filled) + ("░" * max(w - filled, 0))

    # -------------------------
    # UI helpers
    # -------------------------

    def _footer_times(self, *, last_ts: int) -> str:
        next_ts = int(last_ts) + int(self.COOLDOWN_SECONDS)
        break_ts = int(last_ts) + int(self.STREAK_BREAK_SECONDS)
        return f"Next claim: {_utc_stamp(next_ts)} | Streak breaks: {_utc_stamp(break_ts)}"

    def _make_view(
        self,
        *,
        guild_id: int,
        user_id: int,
        remind_enabled: bool,
        remind_mode: RemindMode,
        channel_id_hint: int | None,
    ) -> discord.ui.View:
        return DailyReminderView(
            cog=self,
            guild_id=guild_id,
            user_id=user_id,
            remind_enabled=remind_enabled,
            remind_mode=remind_mode,
            channel_id_hint=channel_id_hint,
            timeout=float(self.REMINDER_VIEW_TIMEOUT),
        )

    # -------------------------
    # Commands
    # -------------------------

    daily = app_commands.Group(name="daily", description="Daily rewards.")

    @daily.command(name="claim", description="Claim your daily reward.")
    async def daily_claim(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await interaction.response.defer(thinking=True)

        guild_id = int(interaction.guild.id)
        user_id = int(interaction.user.id)
        channel_id = int(interaction.channel.id) if interaction.channel else None

        await self._touch_last_channel(guild_id, user_id, channel_id)

        now = int(time.time())
        last_ts, prev_streak, remind_enabled, remind_mode, remind_channel, last_channel = await self._get_state(guild_id, user_id)

        if last_ts > 0 and (now - last_ts) < self.COOLDOWN_SECONDS:
            remaining = int(self.COOLDOWN_SECONDS - (now - last_ts))
            embed = discord.Embed(
                title="⏳ Daily already claimed",
                description=f"You’re early.\nCome back in **{_cooldown_hms(remaining)}**.",
                color=discord.Color.orange(),
            )
            embed.add_field(name="Last claim", value=_utc_stamp(last_ts), inline=False)
            embed.add_field(name="🔔 Reminders", value=("ON" if remind_enabled else "OFF"), inline=True)
            embed.add_field(name=f"{_mode_emoji(remind_mode)} Mode", value=_mode_label(remind_mode), inline=True)
            embed.set_footer(text=self._footer_times(last_ts=last_ts))

            view = self._make_view(
                guild_id=guild_id,
                user_id=user_id,
                remind_enabled=remind_enabled,
                remind_mode=remind_mode,
                channel_id_hint=(remind_channel or last_channel or channel_id or 0) or None,
            )
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            return

        next_streak = self._compute_next_streak(last_ts=last_ts, now_ts=now, prev_streak=prev_streak)
        xp_mult = self._xp_multiplier(next_streak)

        try:
            vip = bool(await is_vip_member(interaction))
        except Exception:
            vip = False

        vip_mult = self.VIP_MULT if vip else 1

        base_silver = int(self.BASE_SILVER + (self.STREAK_SILVER_BONUS * next_streak))
        base_xp = int(round(float(self.BASE_XP) * float(xp_mult)))

        milestone_hit = next_streak > 0 and (next_streak % self.STREAK_MILESTONE_INTERVAL == 0)
        milestone_silver = int(self.STREAK_MILESTONE_SILVER) if milestone_hit else 0
        milestone_xp = int(self.STREAK_MILESTONE_XP) if milestone_hit else 0

        streak_was_broken = last_ts > 0 and (now - last_ts) > self.STREAK_BREAK_SECONDS
        comeback_bonus_hit = bool(streak_was_broken and int(prev_streak) >= self.COMEBACK_MIN_PREV_STREAK)
        comeback_silver = int(self.COMEBACK_SILVER) if comeback_bonus_hit else 0
        comeback_xp = int(self.COMEBACK_XP) if comeback_bonus_hit else 0

        silver = int(base_silver + milestone_silver + comeback_silver)
        xp = int(base_xp + milestone_xp + comeback_xp)

        silver *= int(vip_mult)
        xp *= int(vip_mult)

        jackpot_bp = self._jackpot_chance_bp(next_streak)
        jackpot_hit = self._roll_bp(jackpot_bp)
        jackpot_silver = int(round(float(silver) * float(self.JACKPOT_SILVER_MULTIPLIER))) if jackpot_hit else 0
        silver += int(jackpot_silver)

        bonus_silver = int((milestone_silver + comeback_silver) * int(vip_mult)) + int(jackpot_silver)
        bonus_xp = int((milestone_xp + comeback_xp) * int(vip_mult))

        loot_rarity: LootboxRarity | None = None
        loot_amount = 0

        drop_bp = self._lootbox_drop_chance_bp(next_streak)
        if self._roll_bp(drop_bp):
            loot_rarity = self._pick_lootbox_rarity(next_streak)
            triple_bp = self._lootbox_triple_chance_bp(next_streak)
            loot_amount = 3 if self._roll_bp(triple_bp) else 1

        if milestone_hit and (loot_rarity is None or loot_amount <= 0):
            loot_rarity = LootboxRarity.RARE
            loot_amount = 1

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                wallet = await session.scalar(
                    select(WalletRow).where(
                        WalletRow.guild_id == guild_id,
                        WalletRow.user_id == user_id,
                    )
                )
                if wallet is None:
                    wallet = WalletRow(guild_id=guild_id, user_id=user_id, silver=0, diamonds=0)
                    session.add(wallet)
                    await session.flush()

                xp_row = await session.scalar(
                    select(XpRow).where(
                        XpRow.guild_id == guild_id,
                        XpRow.user_id == user_id,
                    )
                )
                level_multiplier = max(int(getattr(xp_row, "level_cached", 1) or 1), 1)

                silver *= int(level_multiplier)
                bonus_silver *= int(level_multiplier)

                wallet.silver += int(silver)
                if hasattr(wallet, "silver_earned"):
                    wallet.silver_earned += int(max(silver, 0))

                if xp > 0:
                    await award_xp(session, guild_id=guild_id, user_id=user_id, amount=int(xp))

                stamina_row = await session.scalar(
                    select(StaminaRow).where(
                        StaminaRow.guild_id == guild_id,
                        StaminaRow.user_id == user_id,
                    )
                )
                if stamina_row is None:
                    stamina_row = StaminaRow(
                        guild_id=guild_id,
                        user_id=user_id,
                        current_stamina=100,
                        max_stamina=100,
                    )
                    session.add(stamina_row)
                else:
                    stamina_row.max_stamina = max(int(getattr(stamina_row, "max_stamina", 100) or 100), 100)
                    stamina_row.current_stamina = 100

        loot_write_failed = False
        loot_write_error = ""

        if loot_rarity is not None and loot_amount > 0:
            try:
                await self._add_lootbox(
                    guild_id=guild_id,
                    user_id=user_id,
                    rarity=loot_rarity,
                    amount=loot_amount,
                )
            except Exception as e:
                loot_write_failed = True
                loot_write_error = f"{type(e).__name__}: {e}"
                print("Lootbox write failed:", repr(e))
                traceback.print_exc()
                try:
                    await self._alert_staff(
                        interaction.guild,
                        title="🚨 Lootbox write failed in /daily claim",
                        body=(
                            f"Guild: {guild_id}\n"
                            f"User: {user_id}\n"
                            f"Streak: {next_streak}\n"
                            f"Drop: {loot_rarity.value} x{loot_amount}\n"
                            f"Error: {loot_write_error}"
                        ),
                    )
                except Exception:
                    pass

                loot_rarity = None
                loot_amount = 0

        await self._set_state(guild_id, user_id, last_ts=now, streak=next_streak)

        # schedule reminder if enabled
        last_ts2, _streak2, remind_enabled2, remind_mode2, remind_channel2, last_channel2 = await self._get_state(guild_id, user_id)
        if remind_enabled2:
            await self._schedule_or_refresh_reminder(guild_id, user_id)

        reward = DailyReward(
            silver=silver,
            xp=xp,
            bonus_silver=bonus_silver,
            bonus_xp=bonus_xp,
            level_multiplier=level_multiplier,
            streak=next_streak,
            xp_mult=xp_mult,
            milestone_hit=milestone_hit,
            comeback_bonus_hit=comeback_bonus_hit,
            jackpot_hit=jackpot_hit,
            lootbox_rarity=loot_rarity,
            lootbox_amount=loot_amount,
            lootbox_write_failed=loot_write_failed,
            lootbox_write_error=loot_write_error,
            remind_enabled=remind_enabled2,
            remind_mode=remind_mode2,
        )

        embed = discord.Embed(
            title="✅ Daily claimed",
            description="Streak locked in. Rewards delivered.",
            color=discord.Color.green(),
        )

        streak_bar = self._progress_bar(reward.streak, self.STREAK_MAX)
        embed.description = f"{embed.description}\n`{streak_bar}` **{reward.streak}/{self.STREAK_MAX}**"

        embed.add_field(name="🔥 Streak", value=f"**{_fmt_int(reward.streak)}** / {self.STREAK_MAX}", inline=True)
        embed.add_field(name="💰 Silver", value=f"**+{_fmt_int(reward.silver)}**", inline=True)
        embed.add_field(name="🧠 XP", value=f"**+{_fmt_int(reward.xp)}**", inline=True)
        embed.add_field(name="⚡ Stamina", value="**100/100**", inline=True)

        embed.add_field(name="📈 XP Mult", value=f"**x{reward.xp_mult:.1f}**", inline=True)
        embed.add_field(name="🆙 Level Mult", value=f"**x{reward.level_multiplier}**", inline=True)
        embed.add_field(name="✨ VIP Bonus", value="**2x**" if vip else "—", inline=True)

        embed.add_field(
            name="🔔 Reminders",
            value=f"**ON** ({_mode_label(reward.remind_mode)})" if reward.remind_enabled else "**OFF** (silent)",
            inline=True,
        )

        if reward.bonus_silver > 0 or reward.bonus_xp > 0:
            bonus_lines: list[str] = []
            if reward.milestone_hit:
                bonus_lines.append("🏁 Milestone day bonus")
            if reward.comeback_bonus_hit:
                bonus_lines.append("🤝 Comeback protection bonus")
            if reward.jackpot_hit:
                bonus_lines.append(f"🎰 Streak jackpot (+{_fmt_int(jackpot_silver)} silver)")

            summary = "\n".join(bonus_lines) if bonus_lines else "Special bonus"
            summary += f"\nTotal extra: **+{_fmt_int(reward.bonus_silver)} silver**, **+{_fmt_int(reward.bonus_xp)} XP**"
            embed.add_field(name="🌟 Bonus events", value=summary, inline=False)

        if reward.lootbox_write_failed:
            embed.color = discord.Color.red()
            embed.add_field(
                name="🎁 Lootboxes",
                value="🚨 Drop rolled, but inventory write failed. Staff got pinged.",
                inline=False,
            )
            if reward.lootbox_write_error:
                embed.add_field(name="Debug", value=f"```{reward.lootbox_write_error[:900]}```", inline=False)
        elif reward.lootbox_rarity is None or reward.lootbox_amount <= 0:
            embed.add_field(
                name="🎁 Lootboxes",
                value=f"Nothing today.\nDrop chance: **{drop_bp / 100:.2f}%**",
                inline=False,
            )
        else:
            embed.color = _rarity_color(reward.lootbox_rarity)
            embed.add_field(
                name="🎁 Lootboxes",
                value=f"{_rarity_emoji(reward.lootbox_rarity)} **{reward.lootbox_rarity.value.upper()}** x **{_fmt_int(reward.lootbox_amount)}**",
                inline=False,
            )

        embed.set_footer(text=self._footer_times(last_ts=now))

        view = self._make_view(
            guild_id=guild_id,
            user_id=user_id,
            remind_enabled=reward.remind_enabled,
            remind_mode=reward.remind_mode,
            channel_id_hint=(remind_channel2 or last_channel2 or channel_id or 0) or None,
        )
        await interaction.followup.send(embed=embed, view=view)

    @daily.command(name="status", description="See when you can claim again and your streak.")
    async def daily_status(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        guild_id = int(interaction.guild.id)
        user_id = int(interaction.user.id)
        channel_id = int(interaction.channel.id) if interaction.channel else None

        await self._touch_last_channel(guild_id, user_id, channel_id)

        now = int(time.time())
        last_ts, streak, remind_enabled, remind_mode, remind_channel, last_channel = await self._get_state(guild_id, user_id)

        if last_ts <= 0:
            embed = discord.Embed(
                title="📅 Daily status",
                description="You haven’t claimed yet.\nUse **/daily claim**.",
                color=discord.Color.blurple(),
            )
            embed.add_field(
                name="🔔 Reminders",
                value=f"{'ON' if remind_enabled else 'OFF'} ({_mode_label(remind_mode)})",
                inline=False,
            )
            view = self._make_view(
                guild_id=guild_id,
                user_id=user_id,
                remind_enabled=remind_enabled,
                remind_mode=remind_mode,
                channel_id_hint=(remind_channel or last_channel or channel_id or 0) or None,
            )
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
            return

        remaining = int(self.COOLDOWN_SECONDS - (now - last_ts))
        can_claim = remaining <= 0

        gap = now - last_ts
        streak_alive = gap <= self.STREAK_BREAK_SECONDS
        shown_streak = int(streak) if streak_alive else 0
        next_streak = self._compute_next_streak(last_ts=last_ts, now_ts=now, prev_streak=shown_streak)
        next_milestone_at = ((shown_streak // self.STREAK_MILESTONE_INTERVAL) + 1) * self.STREAK_MILESTONE_INTERVAL
        until_milestone = max(next_milestone_at - shown_streak, 0)

        embed = discord.Embed(
            title="📅 Daily status",
            color=discord.Color.green() if can_claim else discord.Color.orange(),
        )
        embed.description = "✅ You can claim now.\nUse **/daily claim**." if can_claim else f"⏳ Next claim in **{_cooldown_hms(remaining)}**."
        embed.description += f"\n`{self._progress_bar(shown_streak, self.STREAK_MAX)}`"

        embed.add_field(name="🕒 Last claim", value=_utc_stamp(last_ts), inline=False)
        embed.add_field(name="🔥 Streak", value=f"**{_fmt_int(shown_streak)}** / {self.STREAK_MAX}", inline=True)
        embed.add_field(name="🧯 Break window", value="✅ alive" if streak_alive else "❌ broken", inline=True)
        embed.add_field(name="🔔 Reminders", value=f"{'ON' if remind_enabled else 'OFF'} ({_mode_label(remind_mode)})", inline=True)

        break_in = int(self.STREAK_BREAK_SECONDS - gap)
        embed.add_field(name="⏱️ Breaks in", value=_cooldown_hms(break_in) if break_in > 0 else "Already broken", inline=False)
        embed.add_field(
            name="🏁 Next milestone",
            value=(
                f"At streak **{_fmt_int(next_milestone_at)}**"
                f" (in **{_fmt_int(until_milestone)}** claim{'s' if until_milestone != 1 else ''})"
            ),
            inline=False,
        )
        embed.add_field(
            name="🎁 Next claim preview",
            value=(
                f"Streak after claim: **{_fmt_int(next_streak)}**\n"
                f"Lootbox chance: **{self._lootbox_drop_chance_bp(next_streak) / 100:.2f}%**\n"
                f"Jackpot chance: **{self._jackpot_chance_bp(next_streak) / 100:.2f}%**"
            ),
            inline=False,
        )

        embed.set_footer(text=self._footer_times(last_ts=last_ts))

        view = self._make_view(
            guild_id=guild_id,
            user_id=user_id,
            remind_enabled=remind_enabled,
            remind_mode=remind_mode,
            channel_id_hint=(remind_channel or last_channel or channel_id or 0) or None,
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @daily.command(name="debug_drop", description="Admin: simulate lootbox roll results for your current streak.")
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(trials="How many simulated rolls (default 2000, max 200000)")
    async def daily_debug_drop(self, interaction: discord.Interaction, trials: int = 2000):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        guild_id = int(interaction.guild.id)
        user_id = int(interaction.user.id)

        last_ts, prev_streak, _remind_enabled, _remind_mode, _rch, _lch = await self._get_state(guild_id, user_id)
        now = int(time.time())
        next_streak = self._compute_next_streak(last_ts=last_ts, now_ts=now, prev_streak=prev_streak)

        n = _clamp(int(trials or 2000), 1, 200000)
        drop_bp = self._lootbox_drop_chance_bp(next_streak)
        triple_bp = self._lootbox_triple_chance_bp(next_streak)

        counts = {
            "drops": 0,
            "common": 0,
            "rare": 0,
            "epic": 0,
            "legendary": 0,
            "triple": 0,
            "single": 0,
        }

        for _ in range(n):
            if not self._roll_bp(drop_bp):
                continue
            counts["drops"] += 1
            r = self._pick_lootbox_rarity(next_streak)
            counts[r.value] += 1
            if self._roll_bp(triple_bp):
                counts["triple"] += 1
            else:
                counts["single"] += 1

        drops = counts["drops"]

        def pct(x: int, denom: int) -> float:
            if denom <= 0:
                return 0.0
            return (float(x) / float(denom)) * 100.0

        embed = discord.Embed(
            title="🧪 Daily Lootbox Debug",
            color=discord.Color.blurple(),
            description=(
                f"Streak used: **{_fmt_int(next_streak)}**\n"
                f"Trials: **{_fmt_int(n)}**\n"
                f"Drop chance: **{drop_bp / 100:.2f}%**\n"
                f"Triple chance (given drop): **{triple_bp / 100:.2f}%**"
            ),
        )
        embed.add_field(name="✅ Drops", value=f"**{_fmt_int(drops)}** ({pct(drops, n):.2f}%)", inline=False)
        embed.add_field(
            name="🎲 Rarity split (of drops)",
            value=(
                f"📦 common: **{_fmt_int(counts['common'])}** ({pct(counts['common'], drops):.2f}%)\n"
                f"🎁 rare: **{_fmt_int(counts['rare'])}** ({pct(counts['rare'], drops):.2f}%)\n"
                f"🧰 epic: **{_fmt_int(counts['epic'])}** ({pct(counts['epic'], drops):.2f}%)\n"
                f"👑 legendary: **{_fmt_int(counts['legendary'])}** ({pct(counts['legendary'], drops):.2f}%)"
            ),
            inline=False,
        )
        embed.add_field(
            name="📦 Amount split (of drops)",
            value=(
                f"1x: **{_fmt_int(counts['single'])}** ({pct(counts['single'], drops):.2f}%)\n"
                f"3x: **{_fmt_int(counts['triple'])}** ({pct(counts['triple'], drops):.2f}%)"
            ),
            inline=False,
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(DailyCog(bot))
