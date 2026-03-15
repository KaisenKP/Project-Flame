from __future__ import annotations

import os
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from adventure.engine import AdventureEngine
from adventure.lobby_manager import LobbyView
from adventure.models.adventure_state import AdventureMode, CLASS_LABELS


class ClassSelect(discord.ui.Select):
    def __init__(self, cog: "AdventureCog", user_id: int):
        options = [
            discord.SelectOption(label=CLASS_LABELS[key], value=key, description=f"Choose {CLASS_LABELS[key]}")
            for key in CLASS_LABELS
        ]
        super().__init__(placeholder="Choose your adventure class", min_values=1, max_values=1, options=options)
        self.cog = cog
        self.user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction) -> None:
        if int(interaction.user.id) != self.user_id:
            await interaction.response.send_message("This class selection is not for you.", ephemeral=True)
            return
        selected = str(self.values[0])
        await self.cog.engine.set_class(guild_id=int(interaction.guild_id), user_id=self.user_id, class_key=selected)
        await interaction.response.send_message(f"🧭 Class selected: **{CLASS_LABELS.get(selected, selected)}**. You're ready for `/adventure`.", ephemeral=True)


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
        await self.cog.launch_direct(interaction, AdventureMode.SOLO)

    @discord.ui.button(label="Create Party Adventure", style=discord.ButtonStyle.primary)
    async def create_party(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if not await self._guard_owner(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        await self.cog.create_lobby(interaction)


class AdventureCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.engine = AdventureEngine()
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
            await interaction.response.send_message("`/adventure` only works in the designated adventure channel.", ephemeral=True)
            return
        if channel_id in self.active_adventure_channels or channel_id in self.active_lobbies:
            await interaction.response.send_message("An adventure (or lobby) is already active in this channel.", ephemeral=True)
            return

        profile = await self.engine.get_or_create_profile(guild_id=int(interaction.guild.id), user_id=int(interaction.user.id))
        if not profile.class_key:
            await interaction.response.send_message("Choose your class before your first adventure.", view=ClassSelectView(self, int(interaction.user.id)), ephemeral=True)
            return
        await interaction.response.send_message("### Adventure Setup\nChoose your run style:", view=SetupView(self, int(interaction.user.id)), ephemeral=True)

    async def create_lobby(self, interaction: discord.Interaction) -> None:
        assert interaction.guild is not None and interaction.channel is not None
        guild_id = int(interaction.guild.id)
        channel_id = int(interaction.channel.id)
        ok, reason = await self.engine.can_join(guild_id=guild_id, user=interaction.user)
        if not ok:
            await interaction.followup.send(reason, ephemeral=True)
            return
        if channel_id in self.active_lobbies or channel_id in self.active_adventure_channels:
            await interaction.followup.send("This channel already has an active lobby/adventure.", ephemeral=True)
            return
        lobby = LobbyView(guild_id=guild_id, channel_id=channel_id, leader_id=int(interaction.user.id), can_join=self._can_join_for_lobby, on_start=self.start_from_lobby)
        lobby.add_member(interaction.user)
        lobby.message = await interaction.channel.send(embed=lobby.build_embed(), view=lobby)
        self.active_lobbies[channel_id] = lobby
        await interaction.followup.send("Party lobby created.", ephemeral=True)

    async def launch_direct(self, interaction: discord.Interaction, mode: AdventureMode) -> None:
        assert interaction.guild is not None and interaction.channel is not None
        ok, reason = await self.engine.can_join(guild_id=int(interaction.guild.id), user=interaction.user)
        if not ok:
            await interaction.followup.send(reason, ephemeral=True)
            return
        channel_id = int(interaction.channel.id)
        if channel_id in self.active_adventure_channels:
            await interaction.followup.send("Adventure already in progress here.", ephemeral=True)
            return
        player = await self.engine.build_player(guild=interaction.guild, user=interaction.user)
        self.active_adventure_channels.add(channel_id)
        try:
            await interaction.channel.send(f"🧭 **{interaction.user.display_name}** sets out alone.")
            await self.engine.start_adventure(guild=interaction.guild, channel=interaction.channel, players=[player], mode=mode)
        finally:
            self.active_adventure_channels.discard(channel_id)

    async def _can_join_for_lobby(self, guild_id: int, user: discord.abc.User) -> tuple[bool, str]:
        return await self.engine.can_join(guild_id=guild_id, user=user)

    async def start_from_lobby(self, lobby: LobbyView) -> None:
        self.active_lobbies.pop(lobby.channel_id, None)
        channel = self.bot.get_channel(lobby.channel_id)
        guild = self.bot.get_guild(lobby.guild_id)
        if not isinstance(channel, discord.TextChannel) or guild is None or lobby.channel_id in self.active_adventure_channels:
            return
        players = []
        for uid in list(lobby.members.keys()):
            member = guild.get_member(uid)
            if member is None:
                continue
            ok, _ = await self.engine.can_join(guild_id=lobby.guild_id, user=member)
            if ok:
                players.append(await self.engine.build_player(guild=guild, user=member))
        if not players:
            await channel.send("Party collapsed. Nobody met stamina/class requirements.")
            return
        self.active_adventure_channels.add(lobby.channel_id)
        try:
            await channel.send("⚔️ Party launched: " + ", ".join(f"**{p.display_name}**" for p in players))
            mode = AdventureMode.PARTY if len(players) > 1 else AdventureMode.SOLO
            await self.engine.start_adventure(guild=guild, channel=channel, players=players, mode=mode)
        finally:
            self.active_adventure_channels.discard(lobby.channel_id)

    def _is_adventure_channel(self, channel: discord.abc.GuildChannel | discord.Thread) -> bool:
        configured = (os.getenv("ADVENTURE_CHANNEL_ID") or "").strip()
        if configured.isdigit():
            return int(channel.id) == int(configured)
        return str(getattr(channel, "name", "")).lower() == "adventure"


async def setup(bot: commands.Bot):
    await bot.add_cog(AdventureCog(bot))
