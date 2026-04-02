from __future__ import annotations

import discord


class OnboardingView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=300)
        self.cog = cog

    @discord.ui.button(label="How It Works", style=discord.ButtonStyle.primary)
    async def how(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_onboarding_how(interaction)

    @discord.ui.button(label="Find a Crew", style=discord.ButtonStyle.success)
    async def find(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_onboarding_complete(interaction)

    @discord.ui.button(label="Maybe Later", style=discord.ButtonStyle.secondary)
    async def later(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_onboarding_later(interaction)
