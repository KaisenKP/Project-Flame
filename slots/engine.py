from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List

from sqlalchemy import select

from db.models import SlotPlayerStatsRow, WalletRow
from services.users import ensure_user_rows
from .events import chaos_modifier, flavor, jeff_event_roll, lucky_spin_roll, near_miss_bonus
from .jackpot import JackpotService
from .machines import MACHINES, machine_by_key
from .symbols import as_emoji


MASTERY_TITLES = {
    1: "Beginner Gambler",
    10: "Card Shark",
    25: "Casino King",
    50: "Slot Deity",
}


@dataclass
class SessionEffect:
    name: str
    payout_mult: float = 1.0
    luck_boost: float = 0.0
    spins_left: int = 1


@dataclass
class SessionState:
    machine_key: str = "classic"
    streak: int = 0
    best_streak: int = 0
    spins: int = 0
    total_spent: int = 0
    total_paid: int = 0
    pending_winnings: int = 0
    can_double: bool = False
    next_spin_mult: float = 1.0
    next_spin_luck_boost: float = 0.0
    effects: list[SessionEffect] = field(default_factory=list)


@dataclass
class SpinOutcome:
    reel_keys: List[str]
    payout: int
    category: str
    label: str
    flavor_text: str
    notes: list[str] = field(default_factory=list)
    jackpot_won: int = 0

    @property
    def reel_display(self) -> str:
        return " ".join(as_emoji(k) for k in self.reel_keys)


class SlotsEngine:
    def __init__(self, sessionmaker):
        self.sessionmaker = sessionmaker
        self.jackpot = JackpotService(sessionmaker)

    async def player_level(self, guild_id: int, user_id: int) -> tuple[int, int, str]:
        async with self.sessionmaker() as session:
            row = await session.scalar(select(SlotPlayerStatsRow).where(
                SlotPlayerStatsRow.guild_id == int(guild_id),
                SlotPlayerStatsRow.user_id == int(user_id),
            ))
            xp = int(row.slot_xp) if row else 0
            level = 1 + (xp // 250)
            title = "Beginner Gambler"
            for gate, gate_title in sorted(MASTERY_TITLES.items()):
                if level >= gate:
                    title = gate_title
            return xp, level, title

    async def _add_slot_xp(self, guild_id: int, user_id: int, amount: int = 15) -> tuple[int, int]:
        async with self.sessionmaker() as session:
            async with session.begin():
                row = await session.scalar(select(SlotPlayerStatsRow).where(
                    SlotPlayerStatsRow.guild_id == int(guild_id),
                    SlotPlayerStatsRow.user_id == int(user_id),
                ))
                if row is None:
                    row = SlotPlayerStatsRow(guild_id=int(guild_id), user_id=int(user_id), slot_xp=0, jackpots_won=0)
                    session.add(row)
                    await session.flush()
                row.slot_xp = int(row.slot_xp) + int(amount)
                row.best_session_streak = max(int(row.best_session_streak), 0)
                return int(row.slot_xp), 1 + (int(row.slot_xp) // 250)

    def machine_unlocked(self, machine_key: str, player_level: int) -> bool:
        return player_level >= machine_by_key(machine_key).unlock_level

    def _weighted_symbol(self, machine_key: str, bonus_luck: float) -> str:
        machine = machine_by_key(machine_key)
        keys = list(machine.weights.keys())
        weights = [float(machine.weights[k]) for k in keys]
        if bonus_luck > 0:
            for idx, key in enumerate(keys):
                if key in {"diamond", "crown", "seven", "fire", "jeff"}:
                    weights[idx] *= (1.0 + bonus_luck)
        return random.choices(keys, weights=weights, k=1)[0]

    def roll_reels(self, state: SessionState) -> list[str]:
        boost = state.next_spin_luck_boost + sum(e.luck_boost for e in state.effects)
        return [self._weighted_symbol(state.machine_key, boost) for _ in range(3)]

    def _classify(self, payout: int, bet: int, near_miss: bool, jackpot: bool) -> str:
        if jackpot:
            return "jackpot"
        if payout >= int(bet * 5):
            return "strong_win"
        if payout > 0:
            return "normal_win"
        if near_miss:
            return "near_miss"
        return "cold_spin"

    async def spin(self, guild_id: int, user_id: int, bet: int, state: SessionState) -> SpinOutcome:
        machine = machine_by_key(state.machine_key)
        reels = self.roll_reels(state)
        notes: list[str] = []

        is_triple = reels[0] == reels[1] == reels[2]
        has_pair = len(set(reels)) == 2
        near_miss = has_pair

        base_mult = 0.0
        if is_triple:
            base_mult = float(machine.triple_payouts.get(reels[0], 0.0))
        elif has_pair:
            base_mult = float(machine.pair_multiplier)

        if state.streak >= 2 and base_mult > 0:
            base_mult *= 1.5
            notes.append("🔥 Win Streak Bonus: +50% payout on 3rd consecutive win.")

        chain_mult = state.next_spin_mult
        if chain_mult > 1:
            notes.append(f"✨ Stored Multiplier consumed: {chain_mult}x")
        state.next_spin_mult = 1.0
        state.next_spin_luck_boost = 0.0

        for effect in list(state.effects):
            chain_mult *= effect.payout_mult
            effect.spins_left -= 1
            if effect.spins_left <= 0:
                state.effects.remove(effect)

        lucky_live, lucky_mult = lucky_spin_roll()
        if lucky_live:
            state.next_spin_mult = lucky_mult
            notes.append(f"✨ LUCKY SPIN ACTIVATED: next spin payout {lucky_mult}x")

        is_chaos_machine = state.machine_key == "chaos"
        chaos_live = False
        chaos_mult = 1.0
        if is_chaos_machine:
            chaos_live, chaos_mult, chaos_note = chaos_modifier()
            if chaos_live:
                notes.append(chaos_note)

        lucky_hour_active = datetime.now(timezone.utc).hour == 20
        if lucky_hour_active:
            chain_mult *= 1.25
            notes.append("🎰 Lucky Hour Active: +25% payouts.")

        if jeff_event_roll() and state.machine_key == "chaos":
            vault_steal = random.randint(int(bet * 2), int(bet * 8))
            notes.append(f"🐙 JEFF ATTACK: Jeff steals {vault_steal:,} silver from the vault for you.")
            state.pending_winnings += vault_steal

        payout = int(bet * base_mult * chain_mult * chaos_mult)

        jackpot_hit = 0
        if random.random() <= machine.jackpot_chance:
            jackpot_hit = await self.jackpot.claim(guild_id)
            if jackpot_hit > 0:
                payout += jackpot_hit

        if near_miss:
            bonus_type, refund_pct, next_mult = near_miss_bonus()
            if bonus_type == "refund":
                refund = int(bet * refund_pct)
                payout += refund
                notes.append(f"🧲 Near Miss refund: +{refund:,} silver")
            elif bonus_type == "next_mult":
                state.next_spin_mult = max(state.next_spin_mult, next_mult)
                notes.append(f"🧲 Near Miss bonus: next spin {next_mult}x")
            else:
                state.next_spin_luck_boost = max(state.next_spin_luck_boost, 0.25)
                notes.append("🧲 Near Miss bonus: luck boost next spin")

        if is_triple and reels[0] == "diamond":
            state.next_spin_mult = max(state.next_spin_mult, 2.5)
            notes.append("💎 Diamond Rush: next spin receives 2.5x")
        if is_triple and reels[0] == "fire":
            state.effects.append(SessionEffect(name="hot_streak", payout_mult=1.25, spins_left=3))
            notes.append("🔥 Hot Streak: +25% payout for next 3 spins")
        if is_triple and reels[0] == "clover":
            state.effects.append(SessionEffect(name="lucky_boost", luck_boost=0.35, spins_left=3))
            notes.append("🍀 Lucky Boost: improved rare symbol odds for 3 spins")

        if payout > 0:
            state.streak += 1
            state.best_streak = max(state.best_streak, state.streak)
        else:
            state.streak = 0
            await self.jackpot.add_loss(guild_id, bet)

        state.spins += 1
        state.total_spent += bet
        state.total_paid += payout
        state.pending_winnings += payout
        state.can_double = payout > 0

        category = self._classify(payout, bet, near_miss, jackpot_hit > 0)
        label = category.replace("_", " ").title()
        outcome = SpinOutcome(reel_keys=reels, payout=payout, category=category, label=label, flavor_text=flavor(category), notes=notes, jackpot_won=jackpot_hit)

        await self._add_slot_xp(guild_id, user_id)
        return outcome

    async def settle_collect(self, guild_id: int, user_id: int, amount: int) -> None:
        if amount <= 0:
            return
        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=int(guild_id), user_id=int(user_id))
                wallet = await session.scalar(select(WalletRow).where(
                    WalletRow.guild_id == int(guild_id),
                    WalletRow.user_id == int(user_id),
                ))
                if wallet is None:
                    wallet = WalletRow(guild_id=int(guild_id), user_id=int(user_id), silver=0, diamonds=0)
                    session.add(wallet)
                    await session.flush()
                wallet.silver = int(wallet.silver) + int(amount)
                wallet.silver_earned = int(wallet.silver_earned) + int(amount)

    async def charge_bet(self, guild_id: int, user_id: int, amount: int) -> bool:
        if amount <= 0:
            return False
        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=int(guild_id), user_id=int(user_id))
                wallet = await session.scalar(select(WalletRow).where(
                    WalletRow.guild_id == int(guild_id),
                    WalletRow.user_id == int(user_id),
                ))
                if wallet is None:
                    wallet = WalletRow(guild_id=int(guild_id), user_id=int(user_id), silver=0, diamonds=0)
                    session.add(wallet)
                    await session.flush()
                if int(wallet.silver) < int(amount):
                    return False
                wallet.silver = int(wallet.silver) - int(amount)
                wallet.silver_spent = int(wallet.silver_spent) + int(amount)
                return True

    async def get_wallet(self, guild_id: int, user_id: int) -> int:
        async with self.sessionmaker() as session:
            wallet = await session.scalar(select(WalletRow).where(WalletRow.guild_id == int(guild_id), WalletRow.user_id == int(user_id)))
            return int(wallet.silver) if wallet else 0

    async def register_jackpot_win(self, guild_id: int, user_id: int) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                row = await session.scalar(select(SlotPlayerStatsRow).where(SlotPlayerStatsRow.guild_id == int(guild_id), SlotPlayerStatsRow.user_id == int(user_id)))
                if row is None:
                    row = SlotPlayerStatsRow(guild_id=int(guild_id), user_id=int(user_id), slot_xp=0, jackpots_won=0)
                    session.add(row)
                    await session.flush()
                row.jackpots_won = int(row.jackpots_won) + 1
