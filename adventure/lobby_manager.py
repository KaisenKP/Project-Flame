from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Optional

import discord

from adventure.models.adventure_state import PARTY_LOBBY_SECONDS, PARTY_MAX_SIZE


class LobbyView(discord.ui.View):
    def __init__(
        self,
        *,
        guild_id: int,
        channel_id: int,
        leader_id: int,
        can_join: Callable[[int, discord.abc.User], Awaitable[tuple[bool, str]]],
        on_start: Callable[["LobbyView"], Awaitable[None]],
    ):
        super().__init__(timeout=PARTY_LOBBY_SECONDS)
        self.guild_id = int(guild_id)
        self.channel_id = int(channel_id)
        self.leader_id = int(leader_id)
        self.members: dict[int, str] = {}
        self.started = False
        self.message: Optional[discord.Message] = None
        self._can_join = can_join
        self._on_start = on_start

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
        em = discord.Embed(title="ADVENTURE PARTY FORMING", description="The unknown whispers your name. Join before the trail goes cold.", color=discord.Color.blurple())
        em.add_field(name="Party Leader", value=f"<@{self.leader_id}>", inline=False)
        em.add_field(name="Party Members", value="\n".join(f"• {n}" for n in self.members.values()) or "• None", inline=False)
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
        await self._on_start(self)

    @discord.ui.button(label="Join Adventure", style=discord.ButtonStyle.success)
    async def join(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if int(interaction.channel_id) != self.channel_id or int(interaction.guild_id) != self.guild_id:
            await interaction.response.send_message("This is not your lobby.", ephemeral=True)
            return
        if len(self.members) >= PARTY_MAX_SIZE:
            await interaction.response.send_message("Party is full.", ephemeral=True)
            return
        ok, reason = await self._can_join(self.guild_id, interaction.user)
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
            await interaction.response.send_message("Only the leader can start the party.", ephemeral=True)
            return
        if self.started:
            await interaction.response.send_message("This lobby already started.", ephemeral=True)
            return
        self.started = True
        await interaction.response.defer(ephemeral=True)
        try:
            await interaction.message.edit(view=None)
        except Exception:
            pass
        await self._on_start(self)
