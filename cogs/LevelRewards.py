# cogs/level_rewards.py
from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Dict, Optional

import discord
from discord.ext import commands
from sqlalchemy import select

from db.models import WalletRow, XpRow
from services.db import sessions
from services.users import ensure_user_rows


# ----------------------------
# Reward schedule (FINAL)
# ----------------------------
# 1-99 total = 50,000 exactly
# 100-109 = 10,000 each
# 110+ = 50,000 each
def reward_for_level(level: int) -> int:
    lv = int(level)
    if lv <= 0:
        return 0

    if lv in (10, 15, 20, 25, 30, 35, 40):
        return 200

    if 41 <= lv <= 59:
        return 200
    if 60 <= lv <= 75:
        return 600
    if 76 <= lv <= 85:
        return 1000
    if 86 <= lv <= 99:
        return 1800

    if 100 <= lv <= 109:
        return 10_000
    if lv >= 110:
        return 50_000

    return 0


def reward_between_levels(prev_level: int, new_level: int) -> int:
    a = int(prev_level or 0)
    b = int(new_level or 0)
    if b <= a:
        return 0
    total = 0
    for lv in range(a + 1, b + 1):
        total += reward_for_level(lv)
    return total


# ----------------------------
# File-backed marker store
# No DB changes needed.
# ----------------------------
@dataclass
class RewardMarker:
    last_rewarded_level: int = 0
    updated_at: float = 0.0


class RewardMarkerStore:
    def __init__(self, path: str):
        self.path = path
        self._lock = asyncio.Lock()
        self._loaded = False
        self._data: Dict[str, Dict[str, RewardMarker]] = {}

    def _ensure_dir(self) -> None:
        d = os.path.dirname(self.path)
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)

    async def _load(self) -> None:
        if self._loaded:
            return
        self._ensure_dir()
        if not os.path.exists(self.path):
            self._data = {}
            self._loaded = True
            return

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception:
            raw = {}

        parsed: Dict[str, Dict[str, RewardMarker]] = {}
        for gid, users in (raw or {}).items():
            if not isinstance(users, dict):
                continue
            parsed[gid] = {}
            for uid, info in users.items():
                try:
                    lvl = int((info or {}).get("last_rewarded_level", 0))
                except Exception:
                    lvl = 0
                try:
                    ts = float((info or {}).get("updated_at", 0.0))
                except Exception:
                    ts = 0.0
                parsed[gid][uid] = RewardMarker(last_rewarded_level=lvl, updated_at=ts)

        self._data = parsed
        self._loaded = True

    async def _save(self) -> None:
        self._ensure_dir()
        raw: Dict[str, Dict[str, Dict[str, object]]] = {}
        for gid, users in self._data.items():
            raw[gid] = {}
            for uid, marker in users.items():
                raw[gid][uid] = {
                    "last_rewarded_level": int(marker.last_rewarded_level),
                    "updated_at": float(marker.updated_at),
                }

        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    async def get_last(self, guild_id: int, user_id: int) -> int:
        async with self._lock:
            await self._load()
            g = self._data.get(str(int(guild_id)), {})
            m = g.get(str(int(user_id)))
            return int(m.last_rewarded_level) if m else 0

    async def set_last(self, guild_id: int, user_id: int, last_level: int) -> None:
        async with self._lock:
            await self._load()
            gid = str(int(guild_id))
            uid = str(int(user_id))
            if gid not in self._data:
                self._data[gid] = {}
            self._data[gid][uid] = RewardMarker(last_rewarded_level=int(last_level), updated_at=time.time())
            await self._save()


# ----------------------------
# Cog
# ----------------------------
class LevelRewardsCog(commands.Cog):
    # batch size for scanning XpRow on startup
    BATCH_SIZE = 800

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self.store = RewardMarkerStore("data/level_rewards_markers.json")

        self._startup_lock = asyncio.Lock()
        self._startup_ran = False

        if self.bot.is_ready():
            asyncio.create_task(self._run_startup_sync())

    async def cog_load(self) -> None:
        if self.bot.is_ready():
            asyncio.create_task(self._run_startup_sync())

    @commands.Cog.listener("on_ready")
    async def _on_ready(self) -> None:
        asyncio.create_task(self._run_startup_sync())

    async def _run_startup_sync(self) -> None:
        async with self._startup_lock:
            if self._startup_ran:
                return
            self._startup_ran = True

        await self.bot.wait_until_ready()
        t0 = time.time()

        total_users = 0
        total_paid = 0

        for guild in list(self.bot.guilds):
            try:
                u, p = await self._sync_guild_once(int(guild.id))
                total_users += u
                total_paid += p
            except Exception:
                pass

        dt = time.time() - t0
        print(f"[LevelRewards] startup sync done: users={total_users:,} paid={total_paid:,} in {dt:.2f}s")

    async def _sync_guild_once(self, guild_id: int) -> tuple[int, int]:
        processed = 0
        paid_total = 0
        offset = 0

        while True:
            async with self.sessionmaker() as session:
                rows = (
                    await session.execute(
                        select(XpRow.user_id, XpRow.level_cached)
                        .where(XpRow.guild_id == int(guild_id))
                        .order_by(XpRow.user_id.asc())
                        .offset(offset)
                        .limit(self.BATCH_SIZE)
                    )
                ).all()

            if not rows:
                break

            for (uid, lvl) in rows:
                processed += 1
                try:
                    paid = await self._apply_rewards_for_user(
                        guild_id=int(guild_id),
                        user_id=int(uid),
                        level_now=int(lvl or 0),
                    )
                    paid_total += int(paid)
                except Exception:
                    pass

            offset += len(rows)

        return processed, paid_total

    async def _apply_rewards_for_user(self, *, guild_id: int, user_id: int, level_now: int) -> int:
        last = await self.store.get_last(guild_id, user_id)
        owed = reward_between_levels(last, level_now)

        # always advance marker to prevent re-scans (even if owed=0)
        if owed <= 0:
            if level_now > last:
                await self.store.set_last(guild_id, user_id, level_now)
            return 0

        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=int(guild_id), user_id=int(user_id))

                wallet = await session.scalar(
                    select(WalletRow).where(
                        WalletRow.guild_id == int(guild_id),
                        WalletRow.user_id == int(user_id),
                    )
                )
                if wallet is None:
                    wallet = WalletRow(guild_id=int(guild_id), user_id=int(user_id), silver=0, diamonds=0)
                    session.add(wallet)
                    await session.flush()

                wallet.silver = int(wallet.silver) + int(owed)
                if hasattr(wallet, "silver_earned"):
                    wallet.silver_earned = int(wallet.silver_earned) + int(owed)

        await self.store.set_last(guild_id, user_id, level_now)
        return int(owed)


async def setup(bot: commands.Bot):
    await bot.add_cog(LevelRewardsCog(bot))
