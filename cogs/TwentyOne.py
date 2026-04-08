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
# Static Assets (Project Flame)
# =========================

START_BANNER_URL = "https://cdn.discordapp.com/attachments/1464802631846330398/1465169440793428163/111388bb-2e71-4463-919f-c7e155f172df.png?ex=697820e0&is=6976cf60&hm=cb4bc80b60d2ea4e063f712ad36e8a3f4095a56258ef2a96c7b962bc9eac807f&"
ROUND_THUMB_URL = "https://cdn.discordapp.com/attachments/1464802631846330398/1465169554370986147/content.png?ex=697820fb&is=6976cf7b&hm=a36ac11e2d00563fa70edb8cbcf87f16324edce2131b968607393a067e6afe20&"
WINNER_IMAGE_URL = "https://cdn.discordapp.com/attachments/1464802631846330398/1465169440420266130/327c2971-4d06-441e-acf3-c7e155f172df.png?ex=697820e0&is=6976cf60&hm=b49692f2fbd25ec9af8e8dce3d734ef761419455b77e631535f8a5f0ecf36873&"


# =========================
# Config
# =========================

TURN_SECONDS = 45
BETWEEN_SECONDS = 45
LOBBY_SECONDS = 180

MIN_FEE = 10
MAX_FEE = 100_000

_ACTION_LOCKS: Dict[int, asyncio.Lock] = {}
_ACTION_CD: Dict[Tuple[int, int], float] = {}

_ACTIVE_GAMES: Dict[int, "TwentyOneGame"] = {}


def _fmt_int(n: int) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _clamp_int(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(v)))


def _get_lock(channel_id: int) -> asyncio.Lock:
    lock = _ACTION_LOCKS.get(int(channel_id))
    if lock is None:
        lock = asyncio.Lock()
        _ACTION_LOCKS[int(channel_id)] = lock
    return lock


def _cooldown(channel_id: int, user_id: int, seconds: float) -> Tuple[bool, int]:
    now = time.time()
    key = (int(channel_id), int(user_id))
    ready_at = float(_ACTION_CD.get(key, 0.0))
    if ready_at > now:
        left = int(max(ready_at - now, 0))
        return True, left
    _ACTION_CD[key] = now + float(seconds)
    return False, 0


# =========================
# Card Logic
# =========================

_RANKS = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
_SUITS = ["♠", "♥", "♦", "♣"]
_RANK_VALUE = {
    "A": 11,
    "2": 2,
    "3": 3,
    "4": 4,
    "5": 5,
    "6": 6,
    "7": 7,
    "8": 8,
    "9": 9,
    "10": 10,
    "J": 10,
    "Q": 10,
    "K": 10,
}


@dataclass(frozen=True)
class Card:
    rank: str
    suit: str

    def display(self) -> str:
        return f"{self.rank}{self.suit}"


def _new_shoe(decks: int = 4) -> List[Card]:
    shoe: List[Card] = []
    for _ in range(max(1, int(decks))):
        for s in _SUITS:
            for r in _RANKS:
                shoe.append(Card(rank=r, suit=s))
    random.shuffle(shoe)
    return shoe


def _hand_value(cards: List[Card]) -> int:
    total = 0
    aces = 0
    for c in cards:
        total += _RANK_VALUE[c.rank]
        if c.rank == "A":
            aces += 1
    while total > 21 and aces > 0:
        total -= 10
        aces -= 1
    return total


def _hand_str(cards: List[Card]) -> str:
    return " ".join(c.display() for c in cards)


# =========================
# Game State
# =========================

@dataclass
class PlayerState:
    user_id: int
    hand: List[Card]
    stood: bool = False
    busted: bool = False


@dataclass
class TwentyOneGame:
    channel_id: int
    guild_id: int
    host_id: int
    entry_fee: int
    created_at: float

    started: bool = False
    finished: bool = False
    phase: str = "lobby"  # lobby | round | results | between | done | cancelled

    pot: int = 0
    escrow: Dict[int, int] = None

    round_no: int = 0
    alive: List[int] = None
    turn_order: List[int] = None
    turn_index: int = 0
    states: Dict[int, PlayerState] = None

    shoe: List[Card] = None

    lobby_message_id: Optional[int] = None
    turn_message_id: Optional[int] = None
    results_message_id: Optional[int] = None
    between_message_id: Optional[int] = None

    view_token: int = 0  # increments whenever we post a new interactive state

    def __post_init__(self):
        if self.escrow is None:
            self.escrow = {}
        if self.alive is None:
            self.alive = []
        if self.turn_order is None:
            self.turn_order = []
        if self.states is None:
            self.states = {}
        if self.shoe is None:
            self.shoe = _new_shoe(decks=4)

    def draw(self) -> Card:
        if not self.shoe:
            self.shoe = _new_shoe(decks=4)
        return self.shoe.pop()

    def current_uid(self) -> Optional[int]:
        if not self.turn_order:
            return None
        if self.turn_index < 0 or self.turn_index >= len(self.turn_order):
            return None
        return self.turn_order[self.turn_index]


# =========================
# Embeds
# =========================

def _rules_text() -> str:
    return (
        "**Twenty One Rules**\n"
        "Goal: get closest to **21** without going over.\n"
        "On your turn: **Hit** (draw) or **Stand** (stop).\n"
        "A = 11 (or 1 if needed), J/Q/K = 10.\n"
        "Round ends when everyone stands or busts.\n"
        "Highest total **≤ 21** advances. Ties advance.\n"
        "If everyone busts, closest **over 21** advances.\n"
        f"Turn timer: **{TURN_SECONDS}s**. Timeout = auto-stand.\n"
    )


def _lobby_embed(game: TwentyOneGame, guild: discord.Guild) -> discord.Embed:
    host = guild.get_member(game.host_id)
    host_name = host.display_name if host else str(game.host_id)

    players = list(game.escrow.keys())
    lines: List[str] = []
    lines.append(f"Started by: **{host_name}**")
    lines.append(f"Entry Fee: **{_fmt_int(game.entry_fee)} Silver**")
    lines.append(f"Pot: **{_fmt_int(game.pot)} Silver**")
    lines.append("")
    if players:
        lines.append(f"Players Joined: **{len(players)}**")
        for uid in players[:25]:
            m = guild.get_member(uid)
            lines.append(f"• {m.mention if m else f'<@{uid}>'}")
    else:
        lines.append("No players yet. Hit **Join**.")

    embed = discord.Embed(
        title="🃏 Twenty One — Lobby",
        description="\n".join(lines),
        color=discord.Color.blurple(),
    )
    embed.set_footer(text="Closest to 21 advances • Ties advance • No dealer")
    return embed


def _start_banner_embed(game: TwentyOneGame, guild: discord.Guild) -> discord.Embed:
    host = guild.get_member(game.host_id)
    host_name = host.display_name if host else str(game.host_id)

    embed = discord.Embed(
        title="🃏 Twenty One — Game Start",
        description=f"Players locked in.\nStarted by: **{host_name}**\nShuffling and dealing…",
        color=discord.Color.blurple(),
    )
    embed.set_image(url=START_BANNER_URL)
    embed.set_footer(text="Round 1 begins shortly…")
    return embed


def _status_icon_public(st: PlayerState) -> str:
    # privacy: do not reveal bust / 21 mid-round
    return "🟡" if st.stood else "🟢"



def _turn_embed(game: TwentyOneGame, guild: discord.Guild, note: Optional[str] = None) -> discord.Embed:
    cur = game.current_uid()
    cur_m = guild.get_member(cur) if cur else None
    cur_mention = cur_m.mention if cur_m else (f"<@{cur}>" if cur else "(none)")

    embed = discord.Embed(
        title=f"🃏 Twenty One — Round {game.round_no}",
        color=discord.Color.dark_teal(),
    )
    embed.set_thumbnail(url=ROUND_THUMB_URL)

    if note:
        embed.description = note

    embed.add_field(
        name="🎯 Turn",
        value=f"{cur_mention}\n⏱️ **{TURN_SECONDS}s**",
        inline=False,
    )

    lines: List[str] = []
    for uid in game.turn_order[:25]:
        st = game.states.get(uid)
        m = guild.get_member(uid)
        name = m.mention if m else f"<@{uid}>"

        # Privacy rule: never reveal totals, busts, or 21 mid-round
        # Public only shows "Playing" vs "Done"
        if not st:
            lines.append(f"🟢 {name} — Playing")
            continue

        icon = "🟡" if st.stood else "🟢"
        label = "Done" if st.stood else "Playing"
        lines.append(f"{icon} {name} — {label}")

    embed.add_field(
        name="👥 Players",
        value="\n".join(lines) if lines else "None",
        inline=False,
    )

    embed.add_field(
        name="💰 Pot",
        value=f"**{_fmt_int(game.pot)} Silver**\nAlive: **{len(game.alive)}**",
        inline=False,
    )

    embed.set_footer(text="Peek Hand is private. Hit/Stand only work on your turn.")
    return embed



def _results_embed(game: TwentyOneGame, guild: discord.Guild, survivors: List[int], eliminated: List[int]) -> discord.Embed:
    embed = discord.Embed(
        title=f"🏁 Round {game.round_no} Results",
        color=discord.Color.blurple(),
    )

    finals: List[str] = []
    for uid in game.turn_order[:25]:
        st = game.states.get(uid)
        if not st:
            continue
        m = guild.get_member(uid)
        name = m.mention if m else f"<@{uid}>"
        v = _hand_value(st.hand)
        tag = " (BUST)" if st.busted else ""
        finals.append(f"{name} — **{v}**{tag}")

    embed.description = "\n".join(finals) if finals else "Round complete."

    surv_lines: List[str] = []
    for uid in survivors[:25]:
        m = guild.get_member(uid)
        surv_lines.append(m.mention if m else f"<@{uid}>")

    elim_lines: List[str] = []
    for uid in eliminated[:25]:
        m = guild.get_member(uid)
        elim_lines.append(m.mention if m else f"<@{uid}>")

    embed.add_field(name="✅ Survivors", value="\n".join(surv_lines) if surv_lines else "None", inline=False)
    embed.add_field(name="❌ Eliminated", value="\n".join(elim_lines) if elim_lines else "None", inline=False)
    embed.set_footer(text="Highest ≤ 21 advances • Ties advance • If all bust, closest over advances")
    return embed


def _between_embed(game: TwentyOneGame, guild: discord.Guild) -> discord.Embed:
    embed = discord.Embed(
        title="⏳ Next Round",
        description=f"Survivors: **{len(game.alive)}**\nNext round starting soon…",
        color=discord.Color.blurple(),
    )
    embed.set_footer(text=f"Host can start early • Auto-start in {BETWEEN_SECONDS}s")
    return embed


def _winner_embed(game: TwentyOneGame, guild: discord.Guild, winner_id: int) -> discord.Embed:
    m = guild.get_member(int(winner_id))
    winner_name = m.mention if m else f"<@{winner_id}>"

    embed = discord.Embed(
        title="🏆 Champion",
        description=f"{winner_name} takes the pot!",
        color=discord.Color.gold(),
    )
    embed.add_field(name="💰 Winnings", value=f"**{_fmt_int(game.pot)} Silver**", inline=False)
    embed.set_image(url=WINNER_IMAGE_URL)
    embed.set_footer(text="Twenty One • Project Flame")
    return embed


def _cancel_embed(reason: str) -> discord.Embed:
    embed = discord.Embed(
        title="🧹 Game Cancelled",
        description=reason,
        color=discord.Color.dark_grey(),
    )
    embed.set_footer(text="All entry fees refunded.")
    return embed


# =========================
# Views
# =========================

class LobbyView(discord.ui.View):
    def __init__(self, cog: "TwentyOneCog", game: TwentyOneGame, token: int):
        super().__init__(timeout=LOBBY_SECONDS)
        self.cog = cog
        self.game = game
        self.token = int(token)

    async def on_timeout(self):
        await self.cog._lobby_timeout(self.game.channel_id, token=self.token)

    async def _deny(self, interaction: discord.Interaction, msg: str):
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass

    def _is_fresh(self) -> bool:
        g = _ACTIVE_GAMES.get(int(self.game.channel_id))
        if not g:
            return False
        return (not g.finished) and (g.phase == "lobby") and (int(g.view_token) == int(self.token))

    @discord.ui.button(label="Join", style=discord.ButtonStyle.success, emoji="✅")
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            return await self._deny(interaction, "Server only.")
        if not self._is_fresh():
            return await self._deny(interaction, "This lobby is outdated.")
        await self.cog._lobby_join(interaction, token=self.token)

    @discord.ui.button(label="Leave", style=discord.ButtonStyle.secondary, emoji="🚪")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            return await self._deny(interaction, "Server only.")
        if not self._is_fresh():
            return await self._deny(interaction, "This lobby is outdated.")
        await self.cog._lobby_leave(interaction, token=self.token)

    @discord.ui.button(label="Rules", style=discord.ButtonStyle.secondary, emoji="📜")
    async def rules(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._deny(interaction, _rules_text())

    @discord.ui.button(label="Start", style=discord.ButtonStyle.primary, emoji="▶️")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            return await self._deny(interaction, "Server only.")
        if not self._is_fresh():
            return await self._deny(interaction, "This lobby is outdated.")
        await self.cog._lobby_start(interaction, token=self.token)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, emoji="✖️")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            return await self._deny(interaction, "Server only.")
        if not self._is_fresh():
            return await self._deny(interaction, "This lobby is outdated.")
        await self.cog._lobby_cancel(interaction, token=self.token)


class TurnView(discord.ui.View):
    def __init__(self, cog: "TwentyOneCog", game: TwentyOneGame, token: int):
        super().__init__(timeout=TURN_SECONDS)
        self.cog = cog
        self.game = game
        self.token = int(token)

    async def on_timeout(self):
        await self.cog._turn_timeout(self.game.channel_id, token=self.token)

    async def _deny(self, interaction: discord.Interaction, msg: str):
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass

    def _is_fresh(self) -> bool:
        g = _ACTIVE_GAMES.get(int(self.game.channel_id))
        if not g:
            return False
        return (not g.finished) and (g.phase == "round") and (int(g.view_token) == int(self.token))

    def _current_uid(self) -> Optional[int]:
        g = _ACTIVE_GAMES.get(int(self.game.channel_id))
        if not g:
            return None
        return g.current_uid()

    @discord.ui.button(label="Peek Hand", style=discord.ButtonStyle.secondary, emoji="👁️")
    async def peek(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            return await self._deny(interaction, "Server only.")
        g = _ACTIVE_GAMES.get(int(self.game.channel_id))
        if not g or g.finished:
            return await self._deny(interaction, "This game is finished.")
        st = g.states.get(int(interaction.user.id))
        if not st:
            return await self._deny(interaction, "You’re not in this match.")

        v = _hand_value(st.hand)
        cards = _hand_str(st.hand)
        status = "BUST" if st.busted else ("Stood" if st.stood else "Playing")
        if (not st.busted) and v == 21 and st.stood:
            status = "21"
        await self._deny(
            interaction,
            f"Your hand: `{cards}`\nTotal: **{v}**\nStatus: **{status}**",
        )

    @discord.ui.button(label="Rules", style=discord.ButtonStyle.secondary, emoji="📜")
    async def rules(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._deny(interaction, _rules_text())

    @discord.ui.button(label="Hit", style=discord.ButtonStyle.success, emoji="➕")
    async def hit(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            return await self._deny(interaction, "Server only.")
        if not self._is_fresh():
            return await self._deny(interaction, "This turn message is outdated.")
        cur = self._current_uid()
        if cur is None:
            return await self._deny(interaction, "Round is finishing.")
        if int(interaction.user.id) != int(cur):
            return await self._deny(interaction, "Not your turn.")

        cd, left = _cooldown(self.game.channel_id, interaction.user.id, seconds=1.0)
        if cd:
            return await self._deny(interaction, f"Slow down. **{left}s**")

        try:
            await interaction.response.defer()
        except Exception:
            pass

        await self.cog._turn_hit(interaction, token=self.token)

    @discord.ui.button(label="Stand", style=discord.ButtonStyle.primary, emoji="🛑")
    async def stand(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            return await self._deny(interaction, "Server only.")
        if not self._is_fresh():
            return await self._deny(interaction, "This turn message is outdated.")
        cur = self._current_uid()
        if cur is None:
            return await self._deny(interaction, "Round is finishing.")
        if int(interaction.user.id) != int(cur):
            return await self._deny(interaction, "Not your turn.")

        cd, left = _cooldown(self.game.channel_id, interaction.user.id, seconds=1.0)
        if cd:
            return await self._deny(interaction, f"Slow down. **{left}s**")

        try:
            await interaction.response.defer()
        except Exception:
            pass

        await self.cog._turn_stand(interaction, token=self.token)


class BetweenView(discord.ui.View):
    def __init__(self, cog: "TwentyOneCog", game: TwentyOneGame, token: int):
        super().__init__(timeout=BETWEEN_SECONDS)
        self.cog = cog
        self.game = game
        self.token = int(token)

    async def on_timeout(self):
        await self.cog._between_timeout(self.game.channel_id, token=self.token)

    async def _deny(self, interaction: discord.Interaction, msg: str):
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass

    def _is_fresh(self) -> bool:
        g = _ACTIVE_GAMES.get(int(self.game.channel_id))
        if not g:
            return False
        return (not g.finished) and (g.phase == "between") and (int(g.view_token) == int(self.token))

    @discord.ui.button(label="Next Round", style=discord.ButtonStyle.primary, emoji="⏭️")
    async def next_round(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            return await self._deny(interaction, "Server only.")
        if not self._is_fresh():
            return await self._deny(interaction, "This message is outdated.")
        if int(interaction.user.id) != int(self.game.host_id):
            return await self._deny(interaction, "Only the host can start the next round.")
        try:
            await interaction.response.defer()
        except Exception:
            pass
        await self.cog._start_next_round(self.game.channel_id, token=self.token)

    @discord.ui.button(label="Rules", style=discord.ButtonStyle.secondary, emoji="📜")
    async def rules(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._deny(interaction, _rules_text())

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, emoji="✖️")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            return await self._deny(interaction, "Server only.")
        if not self._is_fresh():
            return await self._deny(interaction, "This message is outdated.")
        if int(interaction.user.id) != int(self.game.host_id):
            return await self._deny(interaction, "Only the host can cancel.")
        await self.cog._cancel_game(self.game.channel_id, by_host_id=int(interaction.user.id), reason="🧹 Game cancelled by the host. All entry fees refunded.")


# =========================
# Cog
# =========================

class TwentyOneCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    # ---------- Economy ----------

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

    async def _refund(self, guild_id: int, user_id: int, amount: int) -> None:
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

    async def _pay_winner(self, guild_id: int, user_id: int, amount: int) -> None:
        await self._refund(guild_id, user_id, amount)

    # ---------- Message Utilities ----------

    async def _disable_view(self, channel: discord.TextChannel, message_id: Optional[int]) -> None:
        if not message_id:
            return
        try:
            pm = channel.get_partial_message(int(message_id))
            await pm.edit(view=None)
        except Exception:
            pass

    async def _delete_or_disable(self, channel: discord.TextChannel, message_id: Optional[int]) -> None:
        if not message_id:
            return
        try:
            pm = channel.get_partial_message(int(message_id))
            await pm.delete()
        except Exception:
            await self._disable_view(channel, message_id)

    async def _send_embed(
        self,
        channel: discord.TextChannel,
        *,
        embed: discord.Embed,
        view: Optional[discord.ui.View] = None,
    ) -> Optional[int]:
        try:
            msg = await channel.send(embed=embed, view=view)
            return int(msg.id)
        except Exception:
            return None

    async def _edit_embed(
        self,
        channel: discord.TextChannel,
        message_id: Optional[int],
        *,
        embed: discord.Embed,
        view: Optional[discord.ui.View] = None,
    ) -> Optional[int]:
        if not message_id:
            return None
        try:
            pm = channel.get_partial_message(int(message_id))
            await pm.edit(embed=embed, view=view)
            return int(message_id)
        except Exception:
            return None

    # ---------- Posting States ----------

    async def _post_lobby(self, channel_id: int) -> None:
        game = _ACTIVE_GAMES.get(int(channel_id))
        if not game or game.finished:
            return
        if game.phase != "lobby":
            return

        ch = self.bot.get_channel(int(channel_id))
        if not isinstance(ch, discord.TextChannel):
            return

        game.view_token += 1
        token = int(game.view_token)

        embed = _lobby_embed(game, ch.guild)
        view = LobbyView(self, game, token=token)

        mid = await self._edit_embed(ch, game.lobby_message_id, embed=embed, view=view)
        if mid is None:
            mid = await self._send_embed(ch, embed=embed, view=view)
        if mid is not None:
            game.lobby_message_id = int(mid)

    async def _post_turn(self, channel_id: int, note: Optional[str] = None) -> None:
        game = _ACTIVE_GAMES.get(int(channel_id))
        if not game or game.finished:
            return
        if game.phase != "round":
            return

        ch = self.bot.get_channel(int(channel_id))
        if not isinstance(ch, discord.TextChannel):
            return

        game.view_token += 1
        token = int(game.view_token)

        embed = _turn_embed(game, ch.guild, note=note)
        view = TurnView(self, game, token=token)

        prev_turn_id = game.turn_message_id
        mid = await self._send_embed(ch, embed=embed, view=view)
        if mid is not None:
            game.turn_message_id = int(mid)

        if prev_turn_id and prev_turn_id != game.turn_message_id:
            await self._delete_or_disable(ch, prev_turn_id)

    async def _post_results(self, channel_id: int, survivors: List[int], eliminated: List[int]) -> None:
        game = _ACTIVE_GAMES.get(int(channel_id))
        if not game or game.finished:
            return

        ch = self.bot.get_channel(int(channel_id))
        if not isinstance(ch, discord.TextChannel):
            return

        embed = _results_embed(game, ch.guild, survivors, eliminated)
        mid = await self._send_embed(ch, embed=embed, view=None)
        if mid is not None:
            game.results_message_id = int(mid)

    async def _post_between(self, channel_id: int) -> None:
        game = _ACTIVE_GAMES.get(int(channel_id))
        if not game or game.finished:
            return
        if game.phase != "between":
            return

        ch = self.bot.get_channel(int(channel_id))
        if not isinstance(ch, discord.TextChannel):
            return

        game.view_token += 1
        token = int(game.view_token)

        embed = _between_embed(game, ch.guild)
        view = BetweenView(self, game, token=token)

        prev_between = game.between_message_id
        mid = await self._send_embed(ch, embed=embed, view=view)
        if mid is not None:
            game.between_message_id = int(mid)

        if prev_between and prev_between != game.between_message_id:
            await self._delete_or_disable(ch, prev_between)

    # ---------- Lobby Actions ----------

    async def _lobby_join(self, interaction: discord.Interaction, token: int) -> None:
        channel_id = int(interaction.channel_id)
        lock = _get_lock(channel_id)
        async with lock:
            game = _ACTIVE_GAMES.get(channel_id)
            if not game or game.finished or game.phase != "lobby" or int(game.view_token) != int(token):
                try:
                    await interaction.followup.send("This lobby is outdated.", ephemeral=True)
                except Exception:
                    pass
                return

            uid = int(interaction.user.id)
            if uid in game.escrow:
                try:
                    await interaction.followup.send("You already joined.", ephemeral=True)
                except Exception:
                    pass
                return

            ok = await self._charge(game.guild_id, uid, int(game.entry_fee))
            if not ok:
                try:
                    await interaction.followup.send(f"You need **{_fmt_int(game.entry_fee)} Silver** to join.", ephemeral=True)
                except Exception:
                    pass
                return

            game.escrow[uid] = int(game.entry_fee)
            game.pot += int(game.entry_fee)

        await self._post_lobby(channel_id)

    async def _lobby_leave(self, interaction: discord.Interaction, token: int) -> None:
        channel_id = int(interaction.channel_id)
        lock = _get_lock(channel_id)
        async with lock:
            game = _ACTIVE_GAMES.get(channel_id)
            if not game or game.finished or game.phase != "lobby" or int(game.view_token) != int(token):
                try:
                    await interaction.followup.send("This lobby is outdated.", ephemeral=True)
                except Exception:
                    pass
                return

            uid = int(interaction.user.id)
            if uid == int(game.host_id):
                try:
                    await interaction.followup.send("Host can’t leave.", ephemeral=True)
                except Exception:
                    pass
                return

            paid = int(game.escrow.get(uid, 0))
            if paid <= 0:
                try:
                    await interaction.followup.send("You aren’t in the lobby.", ephemeral=True)
                except Exception:
                    pass
                return

            game.escrow.pop(uid, None)
            game.pot = max(int(game.pot) - paid, 0)

        await self._refund(game.guild_id, uid, paid)
        await self._post_lobby(channel_id)

    async def _lobby_start(self, interaction: discord.Interaction, token: int) -> None:
        channel_id = int(interaction.channel_id)
        lock = _get_lock(channel_id)

        async with lock:
            game = _ACTIVE_GAMES.get(channel_id)
            if not game or game.finished or game.phase != "lobby" or int(game.view_token) != int(token):
                try:
                    await interaction.followup.send("This lobby is outdated.", ephemeral=True)
                except Exception:
                    pass
                return

            if int(interaction.user.id) != int(game.host_id):
                try:
                    await interaction.followup.send("Only the host can start.", ephemeral=True)
                except Exception:
                    pass
                return

            if len(game.escrow) < 2:
                try:
                    await interaction.followup.send("Need at least **2** players.", ephemeral=True)
                except Exception:
                    pass
                return

            game.started = True
            game.phase = "results"  # temporary while we transition
            game.round_no = 0
            game.alive = list(game.escrow.keys())
            game.turn_order = []
            game.turn_index = 0
            game.states = {}

        ch = self.bot.get_channel(channel_id)
        if isinstance(ch, discord.TextChannel):
            await self._delete_or_disable(ch, game.lobby_message_id)

            embed = _start_banner_embed(game, ch.guild)
            await self._send_embed(ch, embed=embed, view=None)
            await asyncio.sleep(1.5)

        await self._start_round(channel_id)

    async def _lobby_cancel(self, interaction: discord.Interaction, token: int) -> None:
        channel_id = int(interaction.channel_id)
        lock = _get_lock(channel_id)
        async with lock:
            game = _ACTIVE_GAMES.get(channel_id)
            if not game or game.finished or game.phase != "lobby" or int(game.view_token) != int(token):
                try:
                    await interaction.followup.send("This lobby is outdated.", ephemeral=True)
                except Exception:
                    pass
                return

            if int(interaction.user.id) != int(game.host_id):
                try:
                    await interaction.followup.send("Only the host can cancel.", ephemeral=True)
                except Exception:
                    pass
                return

        await self._cancel_game(channel_id, by_host_id=int(interaction.user.id), reason="🧹 Game cancelled by the host. All entry fees refunded.")

    async def _lobby_timeout(self, channel_id: int, token: int) -> None:
        lock = _get_lock(channel_id)
        async with lock:
            game = _ACTIVE_GAMES.get(channel_id)
            if not game or game.finished:
                return
            if game.phase != "lobby":
                return
            if int(game.view_token) != int(token):
                return

        await self._cancel_game(channel_id, by_host_id=None, reason="🧹 Lobby timed out. All entry fees refunded.")

    # ---------- Round Flow ----------

    async def _start_round(self, channel_id: int) -> None:
        lock = _get_lock(channel_id)
        async with lock:
            game = _ACTIVE_GAMES.get(channel_id)
            if not game or game.finished:
                return

            game.round_no += 1
            game.phase = "round"
            game.turn_index = 0
            game.turn_order = list(game.alive)
            random.shuffle(game.turn_order)

            game.states = {}
            for uid in game.turn_order:
                hand = [game.draw(), game.draw()]
                st = PlayerState(user_id=uid, hand=hand, stood=False, busted=False)
                v = _hand_value(hand)
                if v == 21:
                    st.stood = True
                game.states[uid] = st

            await self._auto_advance_stood_locked(game)

        await self._post_turn(channel_id)

    async def _auto_advance_stood_locked(self, game: TwentyOneGame) -> None:
        while True:
            cur = game.current_uid()
            if cur is None:
                break
            st = game.states.get(cur)
            if st and st.stood:
                game.turn_index += 1
                continue
            break

    async def _turn_hit(self, interaction: discord.Interaction, token: int) -> None:
        channel_id = int(interaction.channel_id)
        lock = _get_lock(channel_id)

        async with lock:
            game = _ACTIVE_GAMES.get(channel_id)
            if not game or game.finished or game.phase != "round" or int(game.view_token) != int(token):
                return

            uid = int(interaction.user.id)
            cur = game.current_uid()
            if cur is None or int(cur) != int(uid):
                return

            st = game.states.get(uid)
            if not st or st.stood:
                return

            st.hand.append(game.draw())
            v = _hand_value(st.hand)
            if v >= 21:
                st.stood = True
                if v > 21:
                    st.busted = True

            await self._auto_advance_stood_locked(game)

            done = game.current_uid() is None

        if done:
            await self._finish_round(channel_id)
        else:
            await self._post_turn(channel_id)

    async def _turn_stand(self, interaction: discord.Interaction, token: int) -> None:
        channel_id = int(interaction.channel_id)
        lock = _get_lock(channel_id)

        async with lock:
            game = _ACTIVE_GAMES.get(channel_id)
            if not game or game.finished or game.phase != "round" or int(game.view_token) != int(token):
                return

            uid = int(interaction.user.id)
            cur = game.current_uid()
            if cur is None or int(cur) != int(uid):
                return

            st = game.states.get(uid)
            if not st or st.stood:
                return

            st.stood = True
            await self._auto_advance_stood_locked(game)
            done = game.current_uid() is None

        if done:
            await self._finish_round(channel_id)
        else:
            await self._post_turn(channel_id)

    async def _turn_timeout(self, channel_id: int, token: int) -> None:
        lock = _get_lock(channel_id)

        timed_uid: Optional[int] = None
        async with lock:
            game = _ACTIVE_GAMES.get(channel_id)
            if not game or game.finished:
                return
            if game.phase != "round":
                return
            if int(game.view_token) != int(token):
                return

            cur = game.current_uid()
            if cur is None:
                return

            st = game.states.get(int(cur))
            if not st or st.stood:
                await self._auto_advance_stood_locked(game)
            else:
                st.stood = True
                timed_uid = int(cur)
                await self._auto_advance_stood_locked(game)

            done = game.current_uid() is None

        if done:
            await self._finish_round(channel_id)
        else:
            note = None
            if timed_uid is not None:
                note = f"⏳ <@{timed_uid}> timed out, auto-stand applied."
            await self._post_turn(channel_id, note=note)

    async def _finish_round(self, channel_id: int) -> None:
        lock = _get_lock(channel_id)

        survivors: List[int] = []
        eliminated: List[int] = []
        winner_id: Optional[int] = None

        async with lock:
            game = _ACTIVE_GAMES.get(channel_id)
            if not game or game.finished:
                return
            if game.phase != "round":
                return

            game.phase = "results"
            game.view_token += 1  # invalidate all turn views immediately

            valid: List[Tuple[int, int]] = []
            busts: List[Tuple[int, int]] = []
            for uid in game.turn_order:
                st = game.states.get(uid)
                if not st:
                    continue
                v = _hand_value(st.hand)
                if st.busted or v > 21:
                    busts.append((uid, v - 21))
                else:
                    valid.append((uid, v))

            if valid:
                best = max(v for _, v in valid)
                survivors = [uid for uid, v in valid if v == best]
            else:
                best_over = min(over for _, over in busts) if busts else 10**9
                survivors = [uid for uid, over in busts if over == best_over]

            survivors = [int(x) for x in survivors]
            eliminated = [int(uid) for uid in game.alive if int(uid) not in set(survivors)]
            game.alive = list(survivors)

            if len(game.alive) == 1:
                winner_id = int(game.alive[0])

        await self._post_results(channel_id, survivors=survivors, eliminated=eliminated)

        if winner_id is not None:
            await self._finish_tournament(channel_id, winner_id)
        else:
            await self._enter_between(channel_id)

    async def _enter_between(self, channel_id: int) -> None:
        lock = _get_lock(channel_id)
        async with lock:
            game = _ACTIVE_GAMES.get(channel_id)
            if not game or game.finished:
                return
            game.phase = "between"
            game.view_token += 1  # invalidate old state

        await self._post_between(channel_id)

    async def _between_timeout(self, channel_id: int, token: int) -> None:
        await self._start_next_round(channel_id, token=token)

    async def _start_next_round(self, channel_id: int, token: int) -> None:
        lock = _get_lock(channel_id)
        async with lock:
            game = _ACTIVE_GAMES.get(channel_id)
            if not game or game.finished:
                return
            if game.phase != "between":
                return
            if int(game.view_token) != int(token):
                return
            game.phase = "results"
            game.view_token += 1

        await self._delete_between_message(channel_id)
        await self._start_round(channel_id)

    async def _delete_between_message(self, channel_id: int) -> None:
        game = _ACTIVE_GAMES.get(channel_id)
        if not game:
            return
        ch = self.bot.get_channel(int(channel_id))
        if not isinstance(ch, discord.TextChannel):
            return
        await self._delete_or_disable(ch, game.between_message_id)
        game.between_message_id = None

    # ---------- Finish / Cancel ----------

    async def _finish_tournament(self, channel_id: int, winner_id: int) -> None:
        lock = _get_lock(channel_id)

        pot: int = 0
        guild_id: int = 0
        async with lock:
            game = _ACTIVE_GAMES.get(channel_id)
            if not game or game.finished:
                return
            game.phase = "done"
            game.finished = True
            game.view_token += 1
            pot = int(game.pot)
            guild_id = int(game.guild_id)

        await self._pay_winner(guild_id, int(winner_id), pot)

        ch = self.bot.get_channel(int(channel_id))
        if isinstance(ch, discord.TextChannel):
            embed = _winner_embed(_ACTIVE_GAMES.get(channel_id) or TwentyOneGame(channel_id, guild_id, 0, 0, time.time()), ch.guild, winner_id)
            if _ACTIVE_GAMES.get(channel_id):
                embed = _winner_embed(_ACTIVE_GAMES[channel_id], ch.guild, winner_id)
            await self._send_embed(ch, embed=embed, view=None)

        _ACTIVE_GAMES.pop(channel_id, None)
        _ACTION_LOCKS.pop(channel_id, None)
        for (cid, uid) in list(_ACTION_CD.keys()):
            if int(cid) == int(channel_id):
                _ACTION_CD.pop((cid, uid), None)

    async def _cancel_game(self, channel_id: int, by_host_id: Optional[int], reason: str) -> None:
        lock = _get_lock(channel_id)

        game: Optional[TwentyOneGame] = None
        refunds: List[Tuple[int, int]] = []
        guild_id: int = 0
        host_id: int = 0

        async with lock:
            game = _ACTIVE_GAMES.pop(channel_id, None)
            if not game:
                return
            if game.finished:
                return

            game.finished = True
            game.phase = "cancelled"
            game.view_token += 1

            guild_id = int(game.guild_id)
            host_id = int(game.host_id)

            for uid, paid in list(game.escrow.items()):
                if int(paid) > 0:
                    refunds.append((int(uid), int(paid)))
            game.escrow = {}

        for uid, amt in refunds:
            try:
                await self._refund(guild_id, uid, amt)
            except Exception:
                pass

        ch = self.bot.get_channel(int(channel_id))
        if isinstance(ch, discord.TextChannel):
            await self._delete_or_disable(ch, game.lobby_message_id)
            await self._delete_or_disable(ch, game.turn_message_id)
            await self._delete_or_disable(ch, game.between_message_id)

            if by_host_id is not None:
                host = ch.guild.get_member(int(host_id))
                host_name = host.mention if host else f"<@{host_id}>"
                public_reason = f"🧹 Game cancelled by {host_name}. All entry fees refunded."
            else:
                public_reason = reason

            await self._send_embed(ch, embed=_cancel_embed(public_reason), view=None)

        _ACTION_LOCKS.pop(channel_id, None)
        for (cid, uid) in list(_ACTION_CD.keys()):
            if int(cid) == int(channel_id):
                _ACTION_CD.pop((cid, uid), None)

    # =========================
    # Slash Command
    # =========================

    @app_commands.command(name="twentyone", description="Start a Twenty One tournament lobby with an entry fee.")
    @app_commands.describe(entry_fee="Entry fee in Silver (10 to 100,000)")
    async def twentyone_cmd(self, interaction: discord.Interaction, entry_fee: int):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        channel_id = int(interaction.channel_id)
        if _ACTIVE_GAMES.get(channel_id) is not None:
            await interaction.response.send_message("There’s already an active Twenty One game in this channel.", ephemeral=True)
            return

        fee = _clamp_int(int(entry_fee), MIN_FEE, MAX_FEE)

        host_id = int(interaction.user.id)
        ok = await self._charge(int(interaction.guild.id), host_id, fee)
        if not ok:
            await interaction.response.send_message(f"You need **{_fmt_int(fee)} Silver** to start the table.", ephemeral=True)
            return

        game = TwentyOneGame(
            channel_id=channel_id,
            guild_id=int(interaction.guild.id),
            host_id=host_id,
            entry_fee=fee,
            created_at=time.time(),
        )
        game.escrow[host_id] = int(fee)
        game.pot = int(fee)

        _ACTIVE_GAMES[channel_id] = game

        try:
            await interaction.response.defer()
        except Exception:
            pass

        await self._post_lobby(channel_id)


async def setup(bot: commands.Bot):
    await bot.add_cog(TwentyOneCog(bot))
