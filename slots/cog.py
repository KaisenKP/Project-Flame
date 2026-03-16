from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from services.db import sessions
from .engine import SessionState, SlotsEngine
from .machines import MACHINES, machine_by_key

SESSION_TIMEOUT_SECONDS = 300
SPIN_COOLDOWN_SECONDS = 1.2
_ALLOWED_BETS = [25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]

_ACTIVE: Dict[Tuple[int, int], "SlotsSession"] = {}
_LOCKS: Dict[Tuple[int, int], asyncio.Lock] = {}
_ACTION_CD: Dict[Tuple[int, int], float] = {}


@dataclass
class SlotsSession:
    channel_id: int
    guild_id: int
    user_id: int
    created_at: float
    message_id: Optional[int] = None
    view_token: int = 0
    closed: bool = False
    last_action_at: float = 0.0
    bet: int = 25
    state: SessionState = None
    last_spin_text: str = "Press **Spin** to roll."
    last_reels: str = "🍒 🍋 🍇"

    def __post_init__(self):
        if self.state is None:
            self.state = SessionState()
        if not self.last_action_at:
            self.last_action_at = time.time()

    def touch(self) -> None:
        self.last_action_at = time.time()


def _fmt(v: int) -> str:
    return f"{int(v):,}"


def _net(s: SlotsSession) -> int:
    return int(s.state.total_paid) - int(s.state.total_spent)


def _lock(key: Tuple[int, int]) -> asyncio.Lock:
    if key not in _LOCKS:
        _LOCKS[key] = asyncio.Lock()
    return _LOCKS[key]


def _cooldown(key: Tuple[int, int]) -> Tuple[bool, int]:
    now = time.time()
    ready = _ACTION_CD.get(key, 0.0)
    if ready > now:
        return True, int(ready - now)
    _ACTION_CD[key] = now + SPIN_COOLDOWN_SECONDS
    return False, 0


class SlotsView(discord.ui.View):
    def __init__(self, cog: "SlotsCog", session: SlotsSession, token: int):
        super().__init__(timeout=SESSION_TIMEOUT_SECONDS)
        self.cog = cog
        self.session = session
        self.token = token

    def _fresh(self):
        s = _ACTIVE.get((self.session.channel_id, self.session.user_id))
        return bool(s and not s.closed and s.view_token == self.token)

    async def _deny(self, interaction: discord.Interaction, msg: str):
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != int(self.session.user_id):
            await self._deny(interaction, "Not your slot machine.")
            return False
        if not self._fresh():
            await self._deny(interaction, "This slot view is outdated.")
            return False
        return True

    async def on_timeout(self):
        await self.cog._timeout_close(self.session.channel_id, self.session.user_id, self.token)

    @discord.ui.button(label="Spin", emoji="🎲", style=discord.ButtonStyle.success, row=0)
    async def spin(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._spin(interaction, self.session.channel_id, self.session.user_id, self.token)

    @discord.ui.button(label="Collect", emoji="💰", style=discord.ButtonStyle.primary, row=0)
    async def collect(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._collect(interaction, self.session.channel_id, self.session.user_id, self.token)

    @discord.ui.button(label="Double or Nothing", emoji="🎯", style=discord.ButtonStyle.secondary, row=0)
    async def double(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._double(interaction, self.session.channel_id, self.session.user_id, self.token)

    @discord.ui.button(label="Classic", style=discord.ButtonStyle.secondary, row=1)
    async def classic(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._set_machine(interaction, self.session.channel_id, self.session.user_id, self.token, "classic")

    @discord.ui.button(label="Pirate", style=discord.ButtonStyle.secondary, row=1)
    async def pirate(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._set_machine(interaction, self.session.channel_id, self.session.user_id, self.token, "pirate")

    @discord.ui.button(label="High Roller", style=discord.ButtonStyle.secondary, row=1)
    async def high_roller(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._set_machine(interaction, self.session.channel_id, self.session.user_id, self.token, "high_roller")

    @discord.ui.button(label="Chaos", style=discord.ButtonStyle.secondary, row=1)
    async def chaos(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._set_machine(interaction, self.session.channel_id, self.session.user_id, self.token, "chaos")

    @discord.ui.button(label="Close", emoji="✖️", style=discord.ButtonStyle.danger, row=2)
    async def close(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._close(self.session.channel_id, self.session.user_id, self.token, "Ended by user")


class SlotsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self.engine = SlotsEngine(self.sessionmaker)

    def _key(self, channel_id: int, user_id: int) -> Tuple[int, int]:
        return int(channel_id), int(user_id)

    async def _render_embed(self, session: SlotsSession, guild: discord.Guild) -> discord.Embed:
        _, level, title = await self.engine.player_level(session.guild_id, session.user_id)
        jackpot = await self.engine.jackpot.get_pool(session.guild_id)
        machine = machine_by_key(session.state.machine_key)
        net = _net(session)

        e = discord.Embed(title="🎰 Slots", color=discord.Color.gold())
        e.description = (
            f"{session.last_reels}\n"
            f"{session.last_spin_text}\n\n"
            f"**Machine:** {machine.name} | **Bet:** {_fmt(session.bet)}\n"
            f"**Pending Winnings:** {_fmt(session.state.pending_winnings)}"
        )
        e.add_field(name="Win Streak", value=f"{session.state.streak} (best {session.state.best_streak})", inline=True)
        e.add_field(name="Net", value=f"{_fmt(net)} silver", inline=True)
        e.add_field(name="Mastery", value=f"Lvl {level} • {title}", inline=True)
        active_bonuses = []
        if session.state.next_spin_mult > 1:
            active_bonuses.append(f"Next mult: {session.state.next_spin_mult}x")
        if session.state.next_spin_luck_boost > 0:
            active_bonuses.append("Next luck boost")
        active_bonuses.extend(f"{fx.name} ({fx.spins_left})" for fx in session.state.effects)
        e.add_field(name="Active Bonuses", value="\n".join(active_bonuses) if active_bonuses else "None", inline=False)
        e.add_field(name="🎰 GLOBAL JACKPOT", value=f"Current Pool: {_fmt(jackpot)} Silver", inline=False)
        e.set_footer(text="Use Collect after wins, or risk Double or Nothing.")
        return e

    async def _edit_or_send(self, channel: discord.TextChannel, session: SlotsSession):
        session.view_token += 1
        view = SlotsView(self, session, session.view_token)
        embed = await self._render_embed(session, channel.guild)
        if session.message_id:
            try:
                await channel.get_partial_message(session.message_id).edit(embed=embed, view=view)
                return
            except Exception:
                session.message_id = None
        msg = await channel.send(embed=embed, view=view)
        session.message_id = msg.id

    async def _set_machine(self, interaction: discord.Interaction, channel_id: int, user_id: int, token: int, machine_key: str):
        key = self._key(channel_id, user_id)
        async with _lock(key):
            s = _ACTIVE.get(key)
            if not s or s.closed or s.view_token != token:
                return
            _, level, _ = await self.engine.player_level(s.guild_id, s.user_id)
            machine = machine_by_key(machine_key)
            if level < machine.unlock_level:
                await interaction.followup.send(f"Unlocks at Slot Mastery level {machine.unlock_level}.", ephemeral=True)
                return
            s.state.machine_key = machine_key
            s.bet = max(machine.min_bet, min(machine.max_bet, s.bet))
            s.touch()

        ch = self.bot.get_channel(channel_id)
        if isinstance(ch, discord.TextChannel):
            await self._edit_or_send(ch, s)

    async def _collect(self, interaction: discord.Interaction, channel_id: int, user_id: int, token: int):
        key = self._key(channel_id, user_id)
        async with _lock(key):
            s = _ACTIVE.get(key)
            if not s or s.closed or s.view_token != token:
                return
            amount = int(s.state.pending_winnings)
            s.state.pending_winnings = 0
            s.state.can_double = False
            s.last_spin_text = f"💰 Collected {_fmt(amount)} silver to your wallet."
        if amount > 0:
            await self.engine.settle_collect(s.guild_id, s.user_id, amount)
        ch = self.bot.get_channel(channel_id)
        if isinstance(ch, discord.TextChannel):
            await self._edit_or_send(ch, s)

    async def _double(self, interaction: discord.Interaction, channel_id: int, user_id: int, token: int):
        key = self._key(channel_id, user_id)
        async with _lock(key):
            s = _ACTIVE.get(key)
            if not s or s.closed or s.view_token != token or not s.state.can_double:
                return
            if s.state.pending_winnings <= 0:
                s.last_spin_text = "No winnings available to double."
            else:
                if random.random() < 0.5:
                    s.state.pending_winnings *= 2
                    s.last_spin_text = f"🎉 Double success! Pending now {_fmt(s.state.pending_winnings)} silver."
                else:
                    s.state.pending_winnings = 0
                    s.last_spin_text = "💥 Double failed. You lost the pending winnings."
                s.state.can_double = False
        ch = self.bot.get_channel(channel_id)
        if isinstance(ch, discord.TextChannel):
            await self._edit_or_send(ch, s)

    async def _spin(self, interaction: discord.Interaction, channel_id: int, user_id: int, token: int):
        key = self._key(channel_id, user_id)
        cd, left = _cooldown(key)
        if cd:
            await interaction.followup.send(f"Slow down: {left}s", ephemeral=True)
            return

        async with _lock(key):
            s = _ACTIVE.get(key)
            if not s or s.closed or s.view_token != token:
                return
            machine = machine_by_key(s.state.machine_key)
            s.bet = max(machine.min_bet, min(machine.max_bet, s.bet))
            ok = await self.engine.charge_bet(s.guild_id, s.user_id, s.bet)
            if not ok:
                await interaction.followup.send(f"Need {_fmt(s.bet)} silver.", ephemeral=True)
                return
            s.last_reels = "🎰 Spinning...\n🍒 🍋 ❔"
            s.last_spin_text = "The reels start turning..."

        ch = self.bot.get_channel(channel_id)
        if isinstance(ch, discord.TextChannel):
            await self._edit_or_send(ch, s)
            await asyncio.sleep(0.55)

        async with _lock(key):
            s = _ACTIVE.get(key)
            if not s or s.closed:
                return
            outcome = await self.engine.spin(s.guild_id, s.user_id, s.bet, s.state)
            s.last_reels = f"🎰 Spinning...\n{outcome.reel_display[:4]}❔\n{outcome.reel_display}"
            notes = "\n".join(outcome.notes[:5])
            s.last_spin_text = f"**{outcome.label}** — won {_fmt(outcome.payout)} silver\n_{outcome.flavor_text}_"
            if notes:
                s.last_spin_text += f"\n{notes}"
            if outcome.jackpot_won > 0:
                await self.engine.register_jackpot_win(s.guild_id, s.user_id)
                s.last_spin_text += f"\n💰 JACKPOT HIT: {_fmt(outcome.jackpot_won)} silver"
        if isinstance(ch, discord.TextChannel):
            await self._edit_or_send(ch, s)
            if outcome.jackpot_won > 0:
                await ch.send(f"💰 JACKPOT HIT\n<@{s.user_id}> won the GLOBAL JACKPOT: {_fmt(outcome.jackpot_won)} Silver")

    async def _close(self, channel_id: int, user_id: int, token: int, reason: str):
        key = self._key(channel_id, user_id)
        async with _lock(key):
            s = _ACTIVE.get(key)
            if not s or s.closed or s.view_token != token:
                return
            s.closed = True
        ch = self.bot.get_channel(channel_id)
        if isinstance(ch, discord.TextChannel) and s.message_id:
            try:
                await ch.get_partial_message(s.message_id).edit(view=None)
            except Exception:
                pass
            await ch.send(f"🎰 <@{user_id}> slots closed. {reason}")
        _ACTIVE.pop(key, None)
        _LOCKS.pop(key, None)
        _ACTION_CD.pop(key, None)

    async def _timeout_close(self, channel_id: int, user_id: int, token: int):
        key = self._key(channel_id, user_id)
        async with _lock(key):
            s = _ACTIVE.get(key)
            if not s or s.closed or s.view_token != token:
                return
            if time.time() - s.last_action_at < SESSION_TIMEOUT_SECONDS:
                return
        await self._close(channel_id, user_id, token, "Timed out due to inactivity")

    @app_commands.command(name="slots", description="Open an advanced slots machine")
    @app_commands.describe(bet="Initial bet", machine="classic/pirate/high_roller/chaos")
    async def slots_cmd(self, interaction: discord.Interaction, bet: Optional[int] = 25, machine: Optional[str] = "classic"):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        key = self._key(interaction.channel_id, interaction.user.id)
        if key in _ACTIVE and not _ACTIVE[key].closed:
            await interaction.response.send_message("You already have an open machine here.", ephemeral=True)
            return

        session = SlotsSession(channel_id=interaction.channel_id, guild_id=interaction.guild.id, user_id=interaction.user.id, created_at=time.time())
        _, level, _ = await self.engine.player_level(session.guild_id, session.user_id)
        if machine not in MACHINES or not self.engine.machine_unlocked(machine, level):
            machine = "classic"
        session.state.machine_key = machine
        mc = machine_by_key(machine)
        session.bet = max(mc.min_bet, min(mc.max_bet, int(bet or 25)))
        if session.bet not in _ALLOWED_BETS:
            session.bet = min(_ALLOWED_BETS, key=lambda x: abs(x - session.bet))

        _ACTIVE[key] = session
        await interaction.response.defer(ephemeral=True)
        ch = self.bot.get_channel(interaction.channel_id)
        if not isinstance(ch, discord.TextChannel):
            _ACTIVE.pop(key, None)
            await interaction.followup.send("Text channel only.", ephemeral=True)
            return
        await self._edit_or_send(ch, session)
        await interaction.followup.send("Slots opened in this channel.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(SlotsCog(bot))
