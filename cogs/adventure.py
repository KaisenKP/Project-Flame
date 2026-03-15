from __future__ import annotations

import asyncio
import os
import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import select

from db.models import AdventureProfileRow, LootboxInventoryRow, WalletRow
from services.db import sessions
from services.items_catalog import ITEMS
from services.items_inventory import add_item
from services.stamina import StaminaService
from services.users import ensure_user_rows
from services.xp_award import award_xp

ADVENTURE_STAMINA_COST = 10
PARTY_MAX_SIZE = 4
PARTY_LOBBY_SECONDS = 30

PARTY_REWARD_BONUS_BP = {1: 0, 2: 1500, 3: 3000, 4: 5000}
PARTY_XP_BONUS_BP = {1: 0, 2: 1000, 3: 2000, 4: 3500}
PARTY_LOOTBOX_BONUS_BP = {1: 0, 2: 500, 3: 1000, 4: 1500}


class AdventureClass(str, Enum):
    DRAGON_SLAYER = "dragon_slayer"
    SHADOW_ASSASSIN = "shadow_assassin"
    ARCHMAGE = "archmage"
    STORM_KNIGHT = "storm_knight"
    TREASURE_HUNTER = "treasure_hunter"
    BEAST_TAMER = "beast_tamer"
    BERSERKER = "berserker"


CLASS_LABELS: dict[str, str] = {
    AdventureClass.DRAGON_SLAYER.value: "Dragon Slayer",
    AdventureClass.SHADOW_ASSASSIN.value: "Shadow Assassin",
    AdventureClass.ARCHMAGE.value: "Archmage",
    AdventureClass.STORM_KNIGHT.value: "Storm Knight",
    AdventureClass.TREASURE_HUNTER.value: "Treasure Hunter",
    AdventureClass.BEAST_TAMER.value: "Beast Tamer",
    AdventureClass.BERSERKER.value: "Berserker",
}


class AdventureMode(str, Enum):
    SOLO = "solo"
    PARTY = "party"


class StageTag(str, Enum):
    COMBAT = "combat"
    TREASURE = "treasure"
    MYSTIC = "mystic"
    TRAP = "trap"
    SOCIAL = "social"
    PUZZLE = "puzzle"
    BOSS = "boss"


@dataclass(frozen=True)
class StageTemplate:
    key: str
    title: str
    beats: list[str]
    choices: list[str]
    tag: StageTag
    party_only: bool = False
    min_adv_level: int = 1


@dataclass
class PlayerRuntime:
    user_id: int
    display_name: str
    class_key: str
    adventure_level: int


@dataclass
class AdventureRewards:
    silver: int = 0
    xp: int = 0
    adventure_xp: int = 0
    stamina_penalty: int = 0
    lootboxes: dict[str, int] = field(default_factory=dict)
    items: dict[str, int] = field(default_factory=dict)

    def merge(self, other: "AdventureRewards") -> None:
        self.silver += int(other.silver)
        self.xp += int(other.xp)
        self.adventure_xp += int(other.adventure_xp)
        self.stamina_penalty += int(other.stamina_penalty)

        for rarity, amt in other.lootboxes.items():
            self.lootboxes[rarity] = self.lootboxes.get(rarity, 0) + int(amt)

        for item_key, amt in other.items.items():
            self.items[item_key] = self.items.get(item_key, 0) + int(amt)


STAGE_POOL: list[StageTemplate] = [
    StageTemplate(
        key="offended_bear",
        title="The Offended Bush",
        beats=[
            "A nearby bush trembles like it's trying to hold in a secret.",
            "Someone pokes it with a stick. Regret arrives instantly.",
            "The bush explodes open and a massive bear lunges forward, deeply offended.",
        ],
        choices=["Fight the bear", "Climb a tree", "Run away"],
        tag=StageTag.COMBAT,
    ),
    StageTemplate(
        key="abandoned_campsite",
        title="Abandoned Campsite",
        beats=[
            "A dead campfire still smells like burnt stew.",
            "A dusty backpack sits in the dirt beside a half-buried knife.",
        ],
        choices=["Open the bag", "Check surroundings", "Ignore it"],
        tag=StageTag.TREASURE,
    ),
    StageTemplate(
        key="broken_wagon",
        title="Broken Wagon",
        beats=[
            "A merchant wagon blocks the road with one wheel snapped clean off.",
            "One crate is humming. That seems concerning.",
        ],
        choices=["Inspect the cargo", "Set up an ambush", "Take a detour"],
        tag=StageTag.SOCIAL,
    ),
    StageTemplate(
        key="ancient_shrine",
        title="Ancient Shrine",
        beats=[
            "Moss-covered pillars circle a glowing shrine.",
            "The carvings pulse as if reacting to your footsteps.",
        ],
        choices=["Study the carvings", "Offer silver", "Touch the altar"],
        tag=StageTag.MYSTIC,
    ),
    StageTemplate(
        key="bandit_ambush",
        title="Bandit Ambush",
        beats=[
            "Whistles cut through the trees.",
            "Bandits emerge wearing dramatic capes that probably cost more than your boots.",
        ],
        choices=["Charge them", "Bribe them", "Retreat through brush"],
        tag=StageTag.COMBAT,
        min_adv_level=5,
    ),
    StageTemplate(
        key="ruined_obelisk",
        title="Ruined Obelisk",
        beats=[
            "A black obelisk rises from cracked ruins.",
            "Symbols shift shape each time you blink.",
        ],
        choices=["Translate symbols", "Break a fragment", "Leave immediately"],
        tag=StageTag.MYSTIC,
        min_adv_level=10,
    ),
    StageTemplate(
        key="flooded_cave",
        title="Flooded Cave",
        beats=[
            "A cave entrance exhales cold air and dripping echoes.",
            "Fresh claw marks run along the stone walls.",
        ],
        choices=["Light torches and enter", "Set bait outside", "Mark it on map"],
        tag=StageTag.TRAP,
        min_adv_level=8,
    ),
    StageTemplate(
        key="vault_gate",
        title="Sealed Vault Gate",
        beats=[
            "An iron door the size of a house blocks the passage.",
            "Mechanisms spin behind the wall as if the vault is waking up.",
        ],
        choices=["Force it open", "Solve the puzzle", "Fall back"],
        tag=StageTag.PUZZLE,
        party_only=True,
        min_adv_level=6,
    ),
    StageTemplate(
        key="monster_horde",
        title="Monster Horde",
        beats=[
            "A roar rolls across the forest like thunder.",
            "A horde rushes downhill, eyes glowing and very motivated.",
        ],
        choices=["Hold formation", "Split and flank", "Full retreat"],
        tag=StageTag.BOSS,
        party_only=True,
        min_adv_level=11,
    ),
    StageTemplate(
        key="dungeon_entrance",
        title="Hidden Dungeon Entrance",
        beats=[
            "The ground collapses beneath old roots.",
            "A staircase descends into a sealed dungeon chamber.",
        ],
        choices=["Descend together", "Scout first", "Seal it and leave"],
        tag=StageTag.BOSS,
        party_only=True,
        min_adv_level=16,
    ),
]


def _fmt_int(n: int) -> str:
    return f"{int(n):,}"


def _bonus_bp(base: int, bp: int) -> int:
    return max((int(base) * (10_000 + int(bp))) // 10_000, 0)


def _roll_bp(chance_bp: int) -> bool:
    chance = max(0, int(chance_bp))
    if chance <= 0:
        return False
    if chance >= 10_000:
        return True
    return random.randint(1, 10_000) <= chance


class ClassSelect(discord.ui.Select):
    def __init__(self, cog: "AdventureCog", user_id: int):
        options = [
            discord.SelectOption(
                label=CLASS_LABELS[key],
                value=key,
                description=f"Choose {CLASS_LABELS[key]}",
            )
            for key in CLASS_LABELS
        ]
        super().__init__(
            placeholder="Choose your adventure class",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.cog = cog
        self.user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction) -> None:
        if int(interaction.user.id) != self.user_id:
            await interaction.response.send_message("This class selection is not for you.", ephemeral=True)
            return

        selected = str(self.values[0])
        await self.cog._set_class(guild_id=int(interaction.guild_id), user_id=self.user_id, class_key=selected)
        await interaction.response.send_message(
            f"🧭 Class selected: **{CLASS_LABELS.get(selected, selected)}**. You're ready for `/adventure`.",
            ephemeral=True,
        )


class ClassSelectView(discord.ui.View):
    def __init__(self, cog: "AdventureCog", user_id: int):
        super().__init__(timeout=120)
        self.add_item(ClassSelect(cog=cog, user_id=user_id))


class SetupView(discord.ui.View):
    def __init__(self, cog: "AdventureCog", owner_id: int):
        super().__init__(timeout=90)
        self.cog = cog
        self.owner_id = int(owner_id)

    async def _guard_owner(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.owner_id:
            await interaction.response.send_message("This setup belongs to someone else.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Start Solo Adventure", style=discord.ButtonStyle.success)
    async def start_solo(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._guard_owner(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        await self.cog._launch_direct(interaction=interaction, mode=AdventureMode.SOLO)

    @discord.ui.button(label="Create Party Adventure", style=discord.ButtonStyle.primary)
    async def create_party(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._guard_owner(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        await self.cog._create_lobby(interaction=interaction)


class LobbyView(discord.ui.View):
    def __init__(self, cog: "AdventureCog", guild_id: int, channel_id: int, leader_id: int):
        super().__init__(timeout=PARTY_LOBBY_SECONDS)
        self.cog = cog
        self.guild_id = int(guild_id)
        self.channel_id = int(channel_id)
        self.leader_id = int(leader_id)
        self.members: dict[int, str] = {}
        self.started = False
        self.message: Optional[discord.Message] = None

    def add_member(self, user: discord.abc.User) -> bool:
        uid = int(user.id)
        if uid in self.members or len(self.members) >= PARTY_MAX_SIZE:
            return False
        self.members[uid] = user.display_name
        return True

    def remove_member(self, user_id: int) -> bool:
        if int(user_id) == self.leader_id:
            return False
        return self.members.pop(int(user_id), None) is not None

    def build_embed(self) -> discord.Embed:
        em = discord.Embed(
            title="ADVENTURE PARTY FORMING",
            description="The unknown whispers your name. Join before the trail goes cold.",
            color=discord.Color.blurple(),
        )
        em.add_field(name="Party Leader", value=f"<@{self.leader_id}>", inline=False)
        members_line = "\n".join(f"• {name}" for name in self.members.values()) or "• None"
        em.add_field(name="Party Members", value=members_line, inline=False)
        em.add_field(name="Party Size", value=f"{len(self.members)} / {PARTY_MAX_SIZE}", inline=True)
        em.set_footer(text=f"Lobby duration: {PARTY_LOBBY_SECONDS}s • Leader can start early")
        return em

    async def refresh(self) -> None:
        if self.message:
            await self.message.edit(embed=self.build_embed(), view=self)

    async def on_timeout(self) -> None:
        if self.started:
            return
        self.started = True
        await self.cog._start_from_lobby(self)

    @discord.ui.button(label="Join Adventure", style=discord.ButtonStyle.success)
    async def join(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if int(interaction.channel_id) != self.channel_id or int(interaction.guild_id) != self.guild_id:
            await interaction.response.send_message("This is not your lobby.", ephemeral=True)
            return

        if len(self.members) >= PARTY_MAX_SIZE:
            await interaction.response.send_message("Party is full.", ephemeral=True)
            return

        ok, reason = await self.cog._can_join(guild_id=self.guild_id, user=interaction.user)
        if not ok:
            await interaction.response.send_message(reason, ephemeral=True)
            return

        if not self.add_member(interaction.user):
            await interaction.response.send_message("You're already in this party.", ephemeral=True)
            return

        await interaction.response.send_message("✅ You joined the adventure.", ephemeral=True)
        await self.refresh()

    @discord.ui.button(label="Leave Party", style=discord.ButtonStyle.secondary)
    async def leave(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if int(interaction.user.id) == self.leader_id:
            await interaction.response.send_message("Leader cannot leave their own lobby.", ephemeral=True)
            return
        if not self.remove_member(int(interaction.user.id)):
            await interaction.response.send_message("You're not in this party.", ephemeral=True)
            return
        await interaction.response.send_message("You left the party.", ephemeral=True)
        await self.refresh()

    @discord.ui.button(label="Start Adventure", style=discord.ButtonStyle.primary)
    async def start(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if int(interaction.user.id) != self.leader_id:
            await interaction.response.send_message("Only the leader can start.", ephemeral=True)
            return
        if self.started:
            await interaction.response.send_message("Adventure already started.", ephemeral=True)
            return
        self.started = True
        self.stop()
        await interaction.response.send_message("⚔️ Adventure launch initiated.", ephemeral=True)
        await self.cog._start_from_lobby(self)


class DecisionButton(discord.ui.Button):
    def __init__(self, idx: int, label: str):
        super().__init__(style=discord.ButtonStyle.secondary, label=label)
        self.idx = int(idx)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DecisionView):
            await interaction.response.send_message("This vote expired.", ephemeral=True)
            return
        await view.vote(interaction, self.idx)


class DecisionView(discord.ui.View):
    def __init__(self, players: list[PlayerRuntime], options: list[str], timeout: float):
        super().__init__(timeout=timeout)
        self.allowed_ids = {p.user_id for p in players}
        self.options = options[:3]
        self.ballots: dict[int, int] = {}
        for idx, label in enumerate(self.options):
            self.add_item(DecisionButton(idx=idx, label=label))

    async def vote(self, interaction: discord.Interaction, idx: int) -> None:
        uid = int(interaction.user.id)
        if uid not in self.allowed_ids:
            await interaction.response.send_message("You're not in this adventure.", ephemeral=True)
            return

        self.ballots[uid] = int(idx)
        await interaction.response.send_message(f"🗳️ Vote registered: **{self.options[idx]}**", ephemeral=True)

    def resolve(self) -> str:
        if not self.ballots:
            return random.choice(self.options)

        counts: dict[int, int] = {}
        for vote_idx in self.ballots.values():
            counts[vote_idx] = counts.get(vote_idx, 0) + 1

        top = max(counts.values())
        winners = [idx for idx, num in counts.items() if num == top]
        chosen = random.choice(winners)
        return self.options[chosen]


class AdventureCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self.stamina = StaminaService()

        self.active_adventure_channels: set[int] = set()
        self.active_lobbies: dict[int, LobbyView] = {}

    @app_commands.command(name="adventure", description="Begin a story-driven multiplayer adventure.")
    async def adventure(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.channel is None:
            await interaction.response.send_message("Server channels only.", ephemeral=True)
            return

        channel = interaction.channel
        channel_id = int(channel.id)

        if not self._is_adventure_channel(channel):
            await interaction.response.send_message(
                "`/adventure` only works in the designated adventure channel.",
                ephemeral=True,
            )
            return

        if channel_id in self.active_adventure_channels or channel_id in self.active_lobbies:
            await interaction.response.send_message("An adventure (or lobby) is already active in this channel.", ephemeral=True)
            return

        profile = await self._get_or_create_profile(guild_id=int(interaction.guild.id), user_id=int(interaction.user.id))

        if not profile.class_key:
            await interaction.response.send_message(
                "Choose your class before your first adventure.",
                view=ClassSelectView(self, int(interaction.user.id)),
                ephemeral=True,
            )
            return

        setup = SetupView(self, int(interaction.user.id))
        await interaction.response.send_message("### Adventure Setup\nChoose your run style:", view=setup, ephemeral=True)

    async def _create_lobby(self, interaction: discord.Interaction) -> None:
        assert interaction.guild is not None and interaction.channel is not None
        guild_id = int(interaction.guild.id)
        channel_id = int(interaction.channel.id)

        ok, reason = await self._can_join(guild_id=guild_id, user=interaction.user)
        if not ok:
            await interaction.followup.send(reason, ephemeral=True)
            return

        if channel_id in self.active_lobbies or channel_id in self.active_adventure_channels:
            await interaction.followup.send("This channel already has an active lobby/adventure.", ephemeral=True)
            return

        lobby = LobbyView(self, guild_id, channel_id, int(interaction.user.id))
        lobby.add_member(interaction.user)
        msg = await interaction.channel.send(embed=lobby.build_embed(), view=lobby)
        lobby.message = msg
        self.active_lobbies[channel_id] = lobby

        await interaction.followup.send("Party lobby created.", ephemeral=True)

    async def _launch_direct(self, interaction: discord.Interaction, mode: AdventureMode) -> None:
        assert interaction.guild is not None and interaction.channel is not None

        ok, reason = await self._can_join(guild_id=int(interaction.guild.id), user=interaction.user)
        if not ok:
            await interaction.followup.send(reason, ephemeral=True)
            return

        channel_id = int(interaction.channel.id)
        if channel_id in self.active_adventure_channels:
            await interaction.followup.send("Adventure already in progress here.", ephemeral=True)
            return

        player = await self._build_player(guild=interaction.guild, user=interaction.user)
        self.active_adventure_channels.add(channel_id)
        try:
            await interaction.channel.send(f"🧭 **{interaction.user.display_name}** sets out alone.")
            await self._run_adventure(
                guild=interaction.guild,
                channel=interaction.channel,
                players=[player],
                mode=mode,
            )
        finally:
            self.active_adventure_channels.discard(channel_id)

    async def _start_from_lobby(self, lobby: LobbyView) -> None:
        self.active_lobbies.pop(lobby.channel_id, None)

        channel = self.bot.get_channel(lobby.channel_id)
        guild = self.bot.get_guild(lobby.guild_id)
        if not isinstance(channel, discord.TextChannel) or guild is None:
            return

        if lobby.channel_id in self.active_adventure_channels:
            return

        players: list[PlayerRuntime] = []
        for uid in list(lobby.members.keys()):
            member = guild.get_member(uid)
            if member is None:
                continue
            ok, _reason = await self._can_join(guild_id=lobby.guild_id, user=member)
            if not ok:
                continue
            players.append(await self._build_player(guild=guild, user=member))

        if not players:
            await channel.send("Party collapsed. Nobody met stamina/class requirements.")
            return

        self.active_adventure_channels.add(lobby.channel_id)
        try:
            await channel.send(
                "⚔️ Party launched: " + ", ".join(f"**{p.display_name}**" for p in players)
            )
            mode = AdventureMode.PARTY if len(players) > 1 else AdventureMode.SOLO
            await self._run_adventure(guild=guild, channel=channel, players=players, mode=mode)
        finally:
            self.active_adventure_channels.discard(lobby.channel_id)

    async def _run_adventure(
        self,
        *,
        guild: discord.Guild,
        channel: discord.TextChannel,
        players: list[PlayerRuntime],
        mode: AdventureMode,
    ) -> None:
        await self._spend_entry_stamina(guild=guild, players=players)

        stage_total = random.randint(2, 3) if mode == AdventureMode.SOLO else random.randint(3, 5)
        rewards = AdventureRewards()

        await channel.send("The trail bends into the unknown...")
        await asyncio.sleep(0.8)

        for stage_number in range(1, stage_total + 1):
            lead_level = max(p.adventure_level for p in players)
            stage = self._pick_stage(mode=mode, party_size=len(players), adventure_level=lead_level)

            await channel.send(f"## Stage {stage_number}: {stage.title}")
            for beat in stage.beats:
                await channel.send(beat)
                await asyncio.sleep(0.9)

            decision_view = DecisionView(players=players, options=stage.choices, timeout=22.0)
            prompt = await channel.send("What do you do?", view=decision_view)
            await asyncio.sleep(20)
            decision_view.stop()

            try:
                await prompt.edit(view=None)
            except Exception:
                pass

            choice = decision_view.resolve()
            await channel.send(f"🗳️ Decision: **{choice}**")

            stage_rewards, aftermath = self._resolve_stage(
                stage=stage,
                chosen=choice,
                players=players,
                mode=mode,
            )
            rewards.merge(stage_rewards)

            for line in aftermath:
                await channel.send(line)
                await asyncio.sleep(0.8)

            if _roll_bp(self._rare_event_chance_bp(players=players, stage=stage)):
                rare_rewards, rare_lines = self._resolve_rare_event(players=players)
                rewards.merge(rare_rewards)
                for line in rare_lines:
                    await channel.send(line)
                    await asyncio.sleep(0.8)

        await self._grant_all_rewards(guild_id=int(guild.id), players=players, rewards=rewards)
        await channel.send(embed=self._summary_embed(players=players, rewards=rewards))

    def _resolve_stage(
        self,
        *,
        stage: StageTemplate,
        chosen: str,
        players: list[PlayerRuntime],
        mode: AdventureMode,
    ) -> tuple[AdventureRewards, list[str]]:
        party_size = len(players)
        adv_level = max(p.adventure_level for p in players)
        class_keys = [p.class_key for p in players]

        base_silver = random.randint(120, 260) + adv_level * random.randint(25, 45)
        base_xp = random.randint(40, 90) + adv_level * random.randint(10, 16)
        base_adv_xp = random.randint(35, 70) + adv_level * random.randint(8, 14)

        risk_bp = 1500
        if stage.tag in {StageTag.COMBAT, StageTag.BOSS, StageTag.TRAP}:
            risk_bp += 900
        if mode == AdventureMode.SOLO:
            risk_bp -= 450

        class_reward_bonus_bp = 0
        class_fail_reduce_bp = 0

        if AdventureClass.TREASURE_HUNTER.value in class_keys and stage.tag in {StageTag.TREASURE, StageTag.SOCIAL}:
            class_reward_bonus_bp += 1800
        if AdventureClass.ARCHMAGE.value in class_keys and stage.tag in {StageTag.MYSTIC, StageTag.PUZZLE}:
            class_reward_bonus_bp += 1600
        if AdventureClass.DRAGON_SLAYER.value in class_keys and stage.tag in {StageTag.COMBAT, StageTag.BOSS}:
            class_reward_bonus_bp += 1400
        if AdventureClass.BERSERKER.value in class_keys and stage.tag in {StageTag.COMBAT, StageTag.BOSS}:
            class_reward_bonus_bp += 2100
            risk_bp += 600
        if AdventureClass.STORM_KNIGHT.value in class_keys:
            class_fail_reduce_bp += 1200
        if AdventureClass.SHADOW_ASSASSIN.value in class_keys and stage.tag in {StageTag.TRAP, StageTag.SOCIAL}:
            class_fail_reduce_bp += 1000

        party_reward_bp = PARTY_REWARD_BONUS_BP.get(party_size, 0)
        party_xp_bp = PARTY_XP_BONUS_BP.get(party_size, 0)

        fail_chance_bp = max(600, risk_bp - class_fail_reduce_bp)
        partial_chance_bp = 2500

        rewards = AdventureRewards()
        lines: list[str] = []

        fail = _roll_bp(fail_chance_bp)
        partial = (not fail) and _roll_bp(partial_chance_bp)

        if fail:
            silver_loss = random.randint(20, 80)
            stamina_loss = random.randint(1, 4)
            if AdventureClass.STORM_KNIGHT.value in class_keys:
                silver_loss = max(5, silver_loss // 2)
                stamina_loss = max(1, stamina_loss - 1)

            rewards.silver -= silver_loss
            rewards.stamina_penalty += stamina_loss
            rewards.adventure_xp += max(6, base_adv_xp // 6)

            lines.append("💥 The plan implodes in spectacular fashion.")
            lines.append(f"You lose **{_fmt_int(silver_loss)} silver** and burn **{stamina_loss} stamina** escaping.")
            return rewards, lines

        silver_gain = _bonus_bp(base_silver, party_reward_bp + class_reward_bonus_bp)
        xp_gain = _bonus_bp(base_xp, party_xp_bp)
        adv_xp_gain = base_adv_xp

        if partial:
            silver_gain = max(20, silver_gain // 2)
            xp_gain = max(10, xp_gain // 2)
            adv_xp_gain = max(8, adv_xp_gain // 2)
            lines.append("⚠️ Chaotic success. You survive, but it gets messy.")
        else:
            lines.append("✅ The party executes the plan with questionable heroism.")

        # branching flavor by choice + stage
        lines.extend(self._branch_lines(stage=stage, chosen=chosen, class_keys=class_keys))

        rewards.silver += silver_gain
        rewards.xp += xp_gain
        rewards.adventure_xp += adv_xp_gain

        # item outcomes
        if stage.tag in {StageTag.TREASURE, StageTag.MYSTIC, StageTag.PUZZLE} and _roll_bp(3200):
            item_key = random.choice([
                "energy_drink",
                "protein_bar",
                "study_notes",
                "found_wallet",
                "training_manual",
                "caffeine_gum",
            ])
            rewards.items[item_key] = rewards.items.get(item_key, 0) + 1
            lines.append(f"📦 Found item: **{ITEMS[item_key].name}**.")

        # class-specific bonus moment
        if AdventureClass.BEAST_TAMER.value in class_keys and _roll_bp(1400):
            rewards.items["caffeine_gum"] = rewards.items.get("caffeine_gum", 0) + 1
            lines.append("🐺 A temporary animal companion drags bonus supplies to camp.")

        # lootboxes
        self._roll_lootbox(
            rewards=rewards,
            party_size=party_size,
            stage=stage,
            class_keys=class_keys,
        )

        lines.append(
            f"Rewards: **+{_fmt_int(silver_gain)} silver**, **+{_fmt_int(xp_gain)} XP**, **+{_fmt_int(adv_xp_gain)} Adventure XP**."
        )

        return rewards, lines

    def _branch_lines(self, *, stage: StageTemplate, chosen: str, class_keys: list[str]) -> list[str]:
        chosen_l = chosen.lower()

        if stage.key == "offended_bear":
            if "fight" in chosen_l:
                return [
                    "Steel clashes with claws. Someone yells 'hit it with the plan!'",
                    "The bear reconsiders its life choices and retreats into the trees.",
                ]
            if "climb" in chosen_l:
                return [
                    "Everyone scrambles up branches with zero dignity.",
                    "The bear huffs below, then steals your rations and leaves.",
                ]
            return [
                "You sprint as fast as fear allows.",
                "The bear gives chase, then gets distracted by a beehive.",
            ]

        if stage.tag == StageTag.MYSTIC:
            if AdventureClass.ARCHMAGE.value in class_keys:
                return ["Arcane glyphs unravel in your mind like a solved riddle."]
            return ["The strange symbols make sense... eventually."]

        if stage.tag == StageTag.PUZZLE:
            return ["Gears rotate, locks click, and the vault groans open a finger-width at a time."]

        if stage.tag == StageTag.BOSS:
            return ["The battlefield becomes pure chaos, but the party somehow holds the line."]

        return ["The situation shifts quickly, but luck stays on your side."]

    def _roll_lootbox(
        self,
        *,
        rewards: AdventureRewards,
        party_size: int,
        stage: StageTemplate,
        class_keys: list[str],
    ) -> None:
        chance_bp = 500 + PARTY_LOOTBOX_BONUS_BP.get(party_size, 0)
        if stage.tag in {StageTag.BOSS, StageTag.PUZZLE}:
            chance_bp += 700
        if AdventureClass.TREASURE_HUNTER.value in class_keys:
            chance_bp += 250

        if not _roll_bp(chance_bp):
            return

        rarity_roll = random.randint(1, 10_000)
        if rarity_roll <= 15:
            rarity = "legendary"
        elif rarity_roll <= 140:
            rarity = "epic"
        elif rarity_roll <= 2600:
            rarity = "rare"
        else:
            rarity = "common"

        rewards.lootboxes[rarity] = rewards.lootboxes.get(rarity, 0) + 1

    def _rare_event_chance_bp(self, *, players: list[PlayerRuntime], stage: StageTemplate) -> int:
        party_size = len(players)
        lead_level = max(p.adventure_level for p in players)
        chance = 450 + min(lead_level * 12, 500)
        if party_size > 1:
            chance += 200
        if stage.tag in {StageTag.BOSS, StageTag.MYSTIC}:
            chance += 180
        return chance

    def _resolve_rare_event(self, *, players: list[PlayerRuntime]) -> tuple[AdventureRewards, list[str]]:
        rewards = AdventureRewards()
        event = random.choice([
            "golden_merchant",
            "traveling_sage",
            "legendary_chest",
            "hidden_dungeon",
            "artifact_shard",
        ])

        if event == "golden_merchant":
            bonus = random.randint(400, 1000)
            rewards.silver += bonus
            rewards.adventure_xp += random.randint(20, 60)
            return rewards, [
                "✨ Rare Event: A Golden Merchant appears out of nowhere.",
                f"He nods once and tosses your party **{_fmt_int(bonus)} silver** for 'style points'.",
            ]

        if event == "traveling_sage":
            xp = random.randint(120, 260)
            rewards.xp += xp
            rewards.adventure_xp += random.randint(35, 75)
            return rewards, [
                "📜 Rare Event: A traveling sage interrupts your argument about directions.",
                f"After a cryptic lecture, your party gains **+{_fmt_int(xp)} XP** from newfound knowledge.",
            ]

        if event == "legendary_chest":
            rewards.lootboxes["legendary"] = rewards.lootboxes.get("legendary", 0) + 1
            return rewards, [
                "👑 Rare Event: You unearth a rune-locked chest beneath old ruins.",
                "Inside is a **Legendary Lootbox** pulsing with unstable light.",
            ]

        if event == "hidden_dungeon":
            silver = random.randint(700, 1700)
            advxp = random.randint(80, 170)
            rewards.silver += silver
            rewards.adventure_xp += advxp
            return rewards, [
                "🕳️ Rare Event: A hidden dungeon corridor opens behind a false wall.",
                f"You salvage relic fragments worth **{_fmt_int(silver)} silver** and **+{_fmt_int(advxp)} Adventure XP**.",
            ]

        item_key = random.choice(["training_manual", "study_sprint_timer", "adrenaline_patch"])
        rewards.items[item_key] = rewards.items.get(item_key, 0) + 1
        return rewards, [
            "🧿 Rare Event: An ancient artifact shard hums in your hands.",
            f"The shard stabilizes into **{ITEMS[item_key].name}**.",
        ]

    def _pick_stage(self, *, mode: AdventureMode, party_size: int, adventure_level: int) -> StageTemplate:
        pool = [
            s for s in STAGE_POOL
            if adventure_level >= int(s.min_adv_level)
            and (party_size > 1 or not s.party_only)
            and (mode == AdventureMode.PARTY or not s.party_only)
        ]
        if not pool:
            pool = [s for s in STAGE_POOL if not s.party_only]
        return random.choice(pool)

    async def _can_join(self, *, guild_id: int, user: discord.abc.User) -> tuple[bool, str]:
        profile = await self._get_or_create_profile(guild_id=guild_id, user_id=int(user.id))
        if not profile.class_key:
            return False, "Choose an Adventure Class first via `/adventure`."

        role_ids = {int(r.id) for r in getattr(user, "roles", [])}
        async with self.sessionmaker() as session:
            snap = await self.stamina.get_snapshot(
                session,
                guild_id=guild_id,
                user_id=int(user.id),
                role_ids=role_ids,
            )
            await session.commit()

        if int(snap.current) < ADVENTURE_STAMINA_COST:
            return False, f"Need **{ADVENTURE_STAMINA_COST} stamina** to join (you have {snap.current})."
        return True, "ok"

    async def _build_player(self, *, guild: discord.Guild, user: discord.abc.User) -> PlayerRuntime:
        profile = await self._get_or_create_profile(guild_id=int(guild.id), user_id=int(user.id))
        return PlayerRuntime(
            user_id=int(user.id),
            display_name=user.display_name,
            class_key=profile.class_key,
            adventure_level=int(profile.adventure_level),
        )

    async def _spend_entry_stamina(self, *, guild: discord.Guild, players: list[PlayerRuntime]) -> None:
        for player in players:
            member = guild.get_member(player.user_id)
            role_ids = {int(r.id) for r in getattr(member, "roles", [])} if member else set()
            async with self.sessionmaker() as session:
                await self.stamina.try_spend(
                    session,
                    guild_id=int(guild.id),
                    user_id=player.user_id,
                    cost=ADVENTURE_STAMINA_COST,
                    role_ids=role_ids,
                )
                await session.commit()

    async def _grant_all_rewards(self, *, guild_id: int, players: list[PlayerRuntime], rewards: AdventureRewards) -> None:
        for player in players:
            async with self.sessionmaker() as session:
                await ensure_user_rows(session, guild_id=guild_id, user_id=player.user_id)

                wallet = await session.scalar(
                    select(WalletRow).where(
                        WalletRow.guild_id == guild_id,
                        WalletRow.user_id == player.user_id,
                    )
                )
                if wallet is None:
                    wallet = WalletRow(guild_id=guild_id, user_id=player.user_id, silver=0, diamonds=0)
                    session.add(wallet)

                silver_delta = int(rewards.silver)
                if silver_delta >= 0:
                    wallet.silver += silver_delta
                    if hasattr(wallet, "silver_earned"):
                        wallet.silver_earned += silver_delta
                else:
                    wallet.silver = max(int(wallet.silver) + silver_delta, 0)

                if int(rewards.xp) > 0:
                    await award_xp(
                        session,
                        guild_id=guild_id,
                        user_id=player.user_id,
                        amount=int(rewards.xp),
                        apply_weekend_multiplier=False,
                    )

                profile = await session.scalar(
                    select(AdventureProfileRow).where(
                        AdventureProfileRow.guild_id == guild_id,
                        AdventureProfileRow.user_id == player.user_id,
                    )
                )
                if profile is None:
                    profile = AdventureProfileRow(guild_id=guild_id, user_id=player.user_id)
                    session.add(profile)
                    await session.flush()

                profile.adventure_xp += int(rewards.adventure_xp)
                profile.adventure_level = self._adventure_level_from_xp(int(profile.adventure_xp))
                profile.runs_completed += 1
                profile.total_stage_wins += 1

                for rarity, amount in rewards.lootboxes.items():
                    lb_row = await session.scalar(
                        select(LootboxInventoryRow).where(
                            LootboxInventoryRow.guild_id == guild_id,
                            LootboxInventoryRow.user_id == player.user_id,
                            LootboxInventoryRow.rarity == rarity,
                        )
                    )
                    if lb_row is None:
                        lb_row = LootboxInventoryRow(
                            guild_id=guild_id,
                            user_id=player.user_id,
                            rarity=rarity,
                            amount=0,
                        )
                        session.add(lb_row)
                    lb_row.amount += int(amount)

                for item_key, amount in rewards.items.items():
                    await add_item(
                        session,
                        guild_id=guild_id,
                        user_id=player.user_id,
                        item_key=item_key,
                        qty=int(amount),
                    )

                if int(rewards.stamina_penalty) > 0:
                    await self.stamina.try_spend(
                        session,
                        guild_id=guild_id,
                        user_id=player.user_id,
                        cost=int(rewards.stamina_penalty),
                    )

                await session.commit()

    async def _get_or_create_profile(self, *, guild_id: int, user_id: int) -> AdventureProfileRow:
        async with self.sessionmaker() as session:
            await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)
            row = await session.scalar(
                select(AdventureProfileRow).where(
                    AdventureProfileRow.guild_id == guild_id,
                    AdventureProfileRow.user_id == user_id,
                )
            )
            if row is None:
                row = AdventureProfileRow(guild_id=guild_id, user_id=user_id)
                session.add(row)
                await session.flush()
            await session.commit()
            return row

    async def _set_class(self, *, guild_id: int, user_id: int, class_key: str) -> None:
        async with self.sessionmaker() as session:
            row = await session.scalar(
                select(AdventureProfileRow).where(
                    AdventureProfileRow.guild_id == guild_id,
                    AdventureProfileRow.user_id == user_id,
                )
            )
            if row is None:
                row = AdventureProfileRow(guild_id=guild_id, user_id=user_id)
                session.add(row)
                await session.flush()
            row.class_key = class_key
            await session.commit()

    def _is_adventure_channel(self, channel: discord.abc.GuildChannel | discord.Thread) -> bool:
        configured = (os.getenv("ADVENTURE_CHANNEL_ID") or "").strip()
        if configured.isdigit():
            return int(channel.id) == int(configured)

        name = getattr(channel, "name", "")
        return str(name).lower() == "adventure"

    @staticmethod
    def _adventure_level_from_xp(xp_total: int) -> int:
        xp = max(int(xp_total), 0)
        level = 1
        need = 140
        while xp >= need and level < 100:
            xp -= need
            level += 1
            need = 140 + ((level - 1) * 45)
        return level

    def _summary_embed(self, *, players: list[PlayerRuntime], rewards: AdventureRewards) -> discord.Embed:
        party = ", ".join(p.display_name for p in players)
        em = discord.Embed(
            title="Adventure Complete",
            description=f"Party: **{party}**",
            color=discord.Color.green(),
        )
        em.add_field(name="Silver", value=f"{rewards.silver:+,}", inline=True)
        em.add_field(name="XP", value=f"+{_fmt_int(rewards.xp)}", inline=True)
        em.add_field(name="Adventure XP", value=f"+{_fmt_int(rewards.adventure_xp)}", inline=True)

        lootbox_text = "\n".join(f"• {rarity.title()}: {qty}" for rarity, qty in rewards.lootboxes.items()) or "None"
        items_text = "\n".join(f"• {ITEMS[item_key].name} x{qty}" for item_key, qty in rewards.items.items()) or "None"

        em.add_field(name="Lootboxes", value=lootbox_text, inline=False)
        em.add_field(name="Items", value=items_text, inline=False)
        if rewards.stamina_penalty > 0:
            em.set_footer(text=f"Extra stamina lost from failed outcomes: {rewards.stamina_penalty}")
        return em


async def setup(bot: commands.Bot):
    await bot.add_cog(AdventureCog(bot))
