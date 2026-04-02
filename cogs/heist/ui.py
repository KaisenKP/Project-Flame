from __future__ import annotations

import discord


class HubView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=600)
        self.cog = cog

    @discord.ui.button(label="Quick Start", style=discord.ButtonStyle.success)
    async def quick(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_open_targets(interaction)

    @discord.ui.button(label="Browse Targets", style=discord.ButtonStyle.primary)
    async def browse(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_open_targets(interaction)

    @discord.ui.button(label="Reopen Crew/Run", style=discord.ButtonStyle.secondary)
    async def reopen(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.route_heist(interaction)

    @discord.ui.button(label="Help", style=discord.ButtonStyle.secondary)
    async def help_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_message("Build or join a crew, ready up, launch, and the run auto-resolves while you're away.", ephemeral=True)


class TargetBrowserView(discord.ui.View):
    def __init__(self, cog, index: int):
        super().__init__(timeout=600)
        self.cog = cog
        self.index = index

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def prev(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_target_page(interaction, self.index - 1)

    @discord.ui.button(label="Create Crew", style=discord.ButtonStyle.success)
    async def create(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_create_crew(interaction, self.index)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def nxt(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_target_page(interaction, self.index + 1)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.route_heist(interaction)


class LobbyView(discord.ui.View):
    def __init__(self, cog, *, can_launch: bool, is_leader: bool):
        super().__init__(timeout=600)
        self.cog = cog
        self.can_launch = can_launch
        self.is_leader = is_leader

    @discord.ui.button(label="Join Crew", style=discord.ButtonStyle.success)
    async def join(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_join_public(interaction)

    @discord.ui.button(label="Ready/Unready", style=discord.ButtonStyle.primary)
    async def ready(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_toggle_ready(interaction)

    @discord.ui.button(label="Leave", style=discord.ButtonStyle.secondary)
    async def leave(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_leave(interaction)

    @discord.ui.button(label="Launch", style=discord.ButtonStyle.danger)
    async def launch(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not self.is_leader:
            await interaction.response.send_message("Only the crew leader can launch.", ephemeral=True)
            return
        await self.cog.handle_launch(interaction)


class ActiveRunView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=600)
        self.cog = cog

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.primary)
    async def refresh(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.route_heist(interaction)


class ResultsView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=600)
        self.cog = cog

    @discord.ui.button(label="Collect Payout", style=discord.ButtonStyle.success)
    async def collect(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_collect(interaction)

    @discord.ui.button(label="Run It Back", style=discord.ButtonStyle.primary)
    async def run_it_back(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_open_targets(interaction)
