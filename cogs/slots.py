# cogs/slots.py
from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select

from db.models import WalletRow
from services.db import sessions
from services.users import ensure_user_rows


# =========================
# Config
# =========================

ALLOWED_BETS = [25, 50, 100, 1000]
MIN_BET = 25
MAX_BET = 1000

SPIN_COOLDOWN_SECONDS = 1.0
SESSION_TIMEOUT_SECONDS = 300  # auto-close after 5 minutes idle

# One session per (channel_id, user_id)
_ACTIVE: Dict[Tuple[int, int], "SlotsSession"] = {}
_LOCKS: Dict[Tuple[int, int], asyncio.Lock] = {}
_ACTION_CD: Dict[Tuple[int, int], float] = {}


# =========================
# Symbols / Virtual Reels
# =========================

SYM_CHERRY = "🍒"
SYM_LEMON = "🍋"
SYM_GRAPE = "🍇"
SYM_BELL = "🔔"
SYM_CLOVER = "🍀"
SYM_DIAMOND = "💎"
SYM_CROWN = "👑"
SYM_SEVEN = "7️⃣"

SYMBOLS = [
    SYM_CHERRY,
    SYM_LEMON,
    SYM_GRAPE,
    SYM_BELL,
    SYM_CLOVER,
    SYM_DIAMOND,
    SYM_CROWN,
    SYM_SEVEN,
]

# Virtual reel strip weights (sum 100). Higher = more common.
REEL_WEIGHTS = {
    SYM_CHERRY: 30,
    SYM_LEMON: 25,
    SYM_GRAPE: 18,
    SYM_BELL: 12,
    SYM_CLOVER: 8,
    SYM_DIAMOND: 4,
    SYM_CROWN: 2,
    SYM_SEVEN: 1,
}


def _build_reel_strip(weights: Dict[str, int]) -> List[str]:
    strip: List[str] = []
    for sym, w in weights.items():
        strip.extend([sym] * max(0, int(w)))
    if not strip:
        strip = [SYM_CHERRY] * 100
    random.shuffle(strip)
    return strip


REEL_STRIP = _build_reel_strip(REEL_WEIGHTS)


# =========================
# Helpers
# =========================

def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _get_lock(key: Tuple[int, int]) -> asyncio.Lock:
    lock = _LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _LOCKS[key] = lock
    return lock


def _cooldown(key: Tuple[int, int], seconds: float) -> Tuple[bool, int]:
    now = time.time()
    ready_at = float(_ACTION_CD.get(key, 0.0))
    if ready_at > now:
        left = int(max(ready_at - now, 0))
        return True, left
    _ACTION_CD[key] = now + float(seconds)
    return False, 0


def _nearest_allowed_bet(v: int) -> int:
    v = int(v)
    if v in ALLOWED_BETS:
        return v
    # pick closest; tie goes to lower
    return min(ALLOWED_BETS, key=lambda b: (abs(b - v), b))


def _roll_symbol() -> str:
    return random.choice(REEL_STRIP)


def _render_reels(reels: List[str]) -> str:
    # monospace-friendly look
    a, b, c = reels
    return f"`[ {a} | {b} | {c} ]`"


def _payout_multiplier(reels: List[str]) -> Tuple[int, str]:
    """
    Returns (multiplier, label)
    multiplier is total payout multiplier: 0, 2, 4, 10
    """
    a, b, c = reels
    is_triple = (a == b == c)
    is_pair = (a == b) or (a == c) or (b == c)

    if is_triple:
        if a in (SYM_SEVEN, SYM_CROWN):
            return 10, "FULL WIN (x10)"
        if a in (SYM_DIAMOND, SYM_CLOVER, SYM_BELL):
            return 4, "HALF WIN (x4)"
        if a in (SYM_GRAPE, SYM_CHERRY, SYM_LEMON):
            return 2, "QUARTER WIN (x2)"
        return 2, "QUARTER WIN (x2)"

    if is_pair:
        return 2, "QUARTER WIN (x2)"

    return 0, "NO WIN"


def _rules_text() -> str:
    return (
        "**Chatbox Slots Rules**\n"
        "Pick a bet, then press **Spin**.\n\n"
        "**Payouts**\n"
        "Quarter win: **x2**\n"
        "Half win: **x4**\n"
        "Full win: **x10**\n\n"
        "**Paytable style**\n"
        "Pairs = Quarter win\n"
        "Triple 🍇/🍒/🍋 = Quarter win\n"
        "Triple 🔔/🍀/💎 = Half win\n"
        "Triple 👑 or 7️⃣ = Full win\n\n"
        "Slots use virtual reels. Symbols are weighted.\n"
    )


# =========================
# State
# =========================

@dataclass
class SpinResult:
    reels: List[str]
    multiplier: int
    label: str
    payout: int


@dataclass
class SlotsSession:
    channel_id: int
    guild_id: int
    user_id: int
    created_at: float

    bet: int = 25
    view_token: int = 0
    message_id: Optional[int] = None

    spins: int = 0
    total_spent: int = 0
    total_paid: int = 0

    last_result: Optional[SpinResult] = None
    last_action_at: float = 0.0
    closed: bool = False

    def touch(self) -> None:
        self.last_action_at = time.time()


# =========================
# Embeds
# =========================

def _net(session: SlotsSession) -> int:
    return int(session.total_paid) - int(session.total_spent)


def _slots_embed(session: SlotsSession, guild: discord.Guild) -> discord.Embed:
    m = guild.get_member(int(session.user_id))
    name = m.display_name if m else str(session.user_id)

    embed = discord.Embed(
        title="🎰 Chatbox Slots",
        color=discord.Color.blurple(),
    )

    if session.last_result is None:
        embed.description = (
            f"Player: **{name}**\n\n"
            f"{_render_reels([SYM_CHERRY, SYM_LEMON, SYM_GRAPE])}\n"
            "Press **Spin** to roll."
        )
    else:
        r = session.last_result
        outcome_line = f"**{r.label}**"
        payout_line = f"Payout: **{_fmt_int(r.payout)} Silver**" if r.payout > 0 else "Payout: **0 Silver**"
        embed.description = (
            f"Player: **{name}**\n\n"
            f"{_render_reels(r.reels)}\n"
            f"{outcome_line}\n"
            f"{payout_line}"
        )

    embed.add_field(
        name="Bet",
        value=f"**{_fmt_int(session.bet)} Silver**",
        inline=True,
    )
    embed.add_field(
        name="Spins",
        value=f"**{_fmt_int(session.spins)}**",
        inline=True,
    )
    net = _net(session)
    net_s = f"+{_fmt_int(net)}" if net >= 0 else f"-{_fmt_int(abs(net))}"
    embed.add_field(
        name="Net",
        value=f"**{net_s} Silver**",
        inline=True,
    )

    embed.set_footer(text="Spin edits this message. Rules are in the Rules button.")
    return embed


# =========================
# Views
# =========================

class SlotsView(discord.ui.View):
    def __init__(self, cog: "SlotsCog", session: SlotsSession, token: int):
        super().__init__(timeout=SESSION_TIMEOUT_SECONDS)
        self.cog = cog
        self.session = session
        self.token = int(token)

        # Bet buttons, disable the selected one
        self.bet25.disabled = (session.bet == 25)
        self.bet50.disabled = (session.bet == 50)
        self.bet100.disabled = (session.bet == 100)
        self.bet1000.disabled = (session.bet == 1000)

    async def on_timeout(self):
        await self.cog._timeout_close(self.session.channel_id, self.session.user_id, token=self.token)

    def _key(self) -> Tuple[int, int]:
        return (int(self.session.channel_id), int(self.session.user_id))

    def _fresh(self) -> bool:
        s = _ACTIVE.get(self._key())
        if not s or s.closed:
            return False
        return int(s.view_token) == int(self.token)

    async def _deny(self, interaction: discord.Interaction, msg: str):
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass

    def _is_owner(self, interaction: discord.Interaction) -> bool:
        return int(interaction.user.id) == int(self.session.user_id)

    @discord.ui.button(label="Spin", style=discord.ButtonStyle.success, emoji="🎲")
    async def spin(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            return await self._deny(interaction, "Server only.")
        if not self._is_owner(interaction):
            return await self._deny(interaction, "Not your machine.")
        if not self._fresh():
            return await self._deny(interaction, "This machine is outdated.")

        key = self._key()
        cd, left = _cooldown(key, SPIN_COOLDOWN_SECONDS)
        if cd:
            return await self._deny(interaction, f"Slow down. **{left}s**")

        try:
            await interaction.response.defer()
        except Exception:
            pass

        await self.cog._spin(interaction, channel_id=self.session.channel_id, user_id=self.session.user_id, token=self.token)

    @discord.ui.button(label="Rules", style=discord.ButtonStyle.secondary, emoji="📜")
    async def rules(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._deny(interaction, _rules_text())

    @discord.ui.button(label="Close", style=discord.ButtonStyle.danger, emoji="✖️")
    async def close(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            return await self._deny(interaction, "Server only.")
        if not self._is_owner(interaction):
            return await self._deny(interaction, "Not your machine.")
        if not self._fresh():
            return await self._deny(interaction, "This machine is outdated.")

        try:
            await interaction.response.defer()
        except Exception:
            pass

        await self.cog._close(self.session.channel_id, self.session.user_id, token=self.token, reason="Ended by User.")

    @discord.ui.button(label="25", style=discord.ButtonStyle.primary)
    async def bet25(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog._set_bet(interaction, self.session.channel_id, self.session.user_id, 25, token=self.token)

    @discord.ui.button(label="50", style=discord.ButtonStyle.primary)
    async def bet50(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog._set_bet(interaction, self.session.channel_id, self.session.user_id, 50, token=self.token)

    @discord.ui.button(label="100", style=discord.ButtonStyle.primary)
    async def bet100(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog._set_bet(interaction, self.session.channel_id, self.session.user_id, 100, token=self.token)

    @discord.ui.button(label="1000", style=discord.ButtonStyle.primary)
    async def bet1000(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog._set_bet(interaction, self.session.channel_id, self.session.user_id, 1000, token=self.token)


# =========================
# Cog
# =========================

class SlotsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    def _key(self, channel_id: int, user_id: int) -> Tuple[int, int]:
        return (int(channel_id), int(user_id))

    async def _charge(self, guild_id: int, user_id: int, amount: int) -> bool:
        if amount <= 0:
            return False
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

                if int(wallet.silver) < int(amount):
                    return False

                wallet.silver = int(wallet.silver) - int(amount)
                if hasattr(wallet, "silver_spent"):
                    wallet.silver_spent = int(wallet.silver_spent) + int(amount)
        return True

    async def _pay(self, guild_id: int, user_id: int, amount: int) -> None:
        if amount <= 0:
            return
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

                wallet.silver = int(wallet.silver) + int(amount)
                if hasattr(wallet, "silver_earned"):
                    wallet.silver_earned = int(wallet.silver_earned) + int(amount)

    async def _edit_or_send(self, channel: discord.TextChannel, session: SlotsSession) -> Optional[int]:
        session.view_token += 1
        token = int(session.view_token)
        view = SlotsView(self, session, token=token)
        embed = _slots_embed(session, channel.guild)

        if session.message_id:
            try:
                pm = channel.get_partial_message(int(session.message_id))
                await pm.edit(embed=embed, view=view)
                return int(session.message_id)
            except Exception:
                session.message_id = None

        try:
            msg = await channel.send(embed=embed, view=view)
            session.message_id = int(msg.id)
            return int(msg.id)
        except Exception:
            return None

    async def _disable_view(self, channel: discord.TextChannel, message_id: Optional[int]) -> None:
        if not message_id:
            return
        try:
            pm = channel.get_partial_message(int(message_id))
            await pm.edit(view=None)
        except Exception:
            pass

    async def _set_bet(self, interaction: discord.Interaction, channel_id: int, user_id: int, bet: int, token: int) -> None:
        if interaction.guild is None:
            return
        if int(interaction.user.id) != int(user_id):
            try:
                await interaction.response.send_message("Not your machine.", ephemeral=True)
            except Exception:
                pass
            return

        key = self._key(channel_id, user_id)
        lock = _get_lock(key)

        async with lock:
            s = _ACTIVE.get(key)
            if not s or s.closed:
                try:
                    await interaction.response.send_message("Session is gone.", ephemeral=True)
                except Exception:
                    pass
                return
            if int(s.view_token) != int(token):
                try:
                    await interaction.response.send_message("This machine is outdated.", ephemeral=True)
                except Exception:
                    pass
                return

            if bet not in ALLOWED_BETS:
                bet = _nearest_allowed_bet(bet)

            s.bet = int(bet)
            s.touch()

        try:
            await interaction.response.defer()
        except Exception:
            pass

        ch = self.bot.get_channel(int(channel_id))
        if isinstance(ch, discord.TextChannel):
            await self._edit_or_send(ch, s)

    async def _spin(self, interaction: discord.Interaction, channel_id: int, user_id: int, token: int) -> None:
        if interaction.guild is None:
            return

        key = self._key(channel_id, user_id)
        lock = _get_lock(key)

        async with lock:
            s = _ACTIVE.get(key)
            if not s or s.closed:
                return
            if int(s.view_token) != int(token):
                return

            s.touch()
            bet = int(s.bet)
            if bet not in ALLOWED_BETS:
                bet = _nearest_allowed_bet(bet)
                s.bet = bet

            ok = await self._charge(int(s.guild_id), int(s.user_id), bet)
            if not ok:
                try:
                    await interaction.followup.send(f"You need **{_fmt_int(bet)} Silver** to spin.", ephemeral=True)
                except Exception:
                    pass
                return

            reels = [_roll_symbol(), _roll_symbol(), _roll_symbol()]
            mult, label = _payout_multiplier(reels)
            payout = int(bet) * int(mult)

            s.spins += 1
            s.total_spent += bet
            s.total_paid += payout
            s.last_result = SpinResult(reels=reels, multiplier=mult, label=label, payout=payout)

        if payout > 0:
            await self._pay(int(s.guild_id), int(s.user_id), payout)

        ch = self.bot.get_channel(int(channel_id))
        if isinstance(ch, discord.TextChannel):
            await self._edit_or_send(ch, s)

    async def _close(self, channel_id: int, user_id: int, token: int, reason: str) -> None:
        key = self._key(channel_id, user_id)
        lock = _get_lock(key)

        session: Optional[SlotsSession] = None
        async with lock:
            session = _ACTIVE.get(key)
            if not session:
                return
            if session.closed:
                return
            if int(session.view_token) != int(token):
                return
            session.closed = True

        ch = self.bot.get_channel(int(channel_id))
        if isinstance(ch, discord.TextChannel):
            await self._disable_view(ch, session.message_id)
            try:
                await ch.send(content=f"🎰 <@{user_id}> slots closed. {reason}")
            except Exception:
                pass

        _ACTIVE.pop(key, None)
        _LOCKS.pop(key, None)
        _ACTION_CD.pop(key, None)

    async def _timeout_close(self, channel_id: int, user_id: int, token: int) -> None:
        key = self._key(channel_id, user_id)
        lock = _get_lock(key)

        session: Optional[SlotsSession] = None
        async with lock:
            session = _ACTIVE.get(key)
            if not session or session.closed:
                return
            if int(session.view_token) != int(token):
                return

            now = time.time()
            last = float(session.last_action_at or session.created_at)
            if now - last < SESSION_TIMEOUT_SECONDS - 1:
                return

        await self._close(channel_id, user_id, token=token, reason="Timed out due to inactivity.")

    # =========================
    # Slash Command
    # =========================

    @app_commands.command(name="slots", description="Open a personal Chatbox slot machine.")
    @app_commands.describe(bet="Bet amount (25, 50, 100, 1000)")
    async def slots_cmd(self, interaction: discord.Interaction, bet: Optional[int] = None):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        channel_id = int(interaction.channel_id)
        user_id = int(interaction.user.id)
        key = self._key(channel_id, user_id)

        existing = _ACTIVE.get(key)
        if existing and not existing.closed:
            await interaction.response.send_message("You already have slots open in this channel. Use the buttons.", ephemeral=True)
            return

        b = int(bet) if bet is not None else 25
        if b not in ALLOWED_BETS:
            b = _nearest_allowed_bet(b)

        session = SlotsSession(
            channel_id=channel_id,
            guild_id=int(interaction.guild.id),
            user_id=user_id,
            created_at=time.time(),
            bet=b,
            last_action_at=time.time(),
        )
        _ACTIVE[key] = session

        try:
            await interaction.response.defer(ephemeral=True)
        except Exception:
            pass

        ch = self.bot.get_channel(channel_id)
        if not isinstance(ch, discord.TextChannel):
            _ACTIVE.pop(key, None)
            await interaction.followup.send("Text channels only.", ephemeral=True)
            return

        await self._edit_or_send(ch, session)
        try:
            await interaction.followup.send("Slots opened. Check the channel for your machine.", ephemeral=True)
        except Exception:
            pass


async def setup(bot: commands.Bot):
    await bot.add_cog(SlotsCog(bot))
