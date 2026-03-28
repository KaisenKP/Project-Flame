from __future__ import annotations

import os

import discord
from discord import app_commands
from discord.ext import commands

from services.vip_perks import get_category_title, get_vip_perks, iter_vip_perk_lines


VIP_EMBED_COLOR = discord.Color.from_rgb(155, 89, 182)
VIP_STORE_URL = (os.getenv("VIP_STORE_URL") or "").strip()


class VIPLinkView(discord.ui.View):
    def __init__(self, *, url: str):
        super().__init__(timeout=None)
        if url:
            self.add_item(
                discord.ui.Button(
                    label="Get VIP",
                    style=discord.ButtonStyle.link,
                    emoji="💎",
                    url=url,
                )
            )


class VIPEmbedCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    def _build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="💎 VIP Benefits",
            description="Unlock premium boosts across economy, jobs, and progression with one role.",
            color=VIP_EMBED_COLOR,
        )

        perks_map = get_vip_perks()
        if not perks_map:
            embed.add_field(
                name="No VIP Perks Configured",
                value="VIP benefits are currently being refreshed. Please check back soon.",
                inline=False,
            )
        else:
            for category_key, perks in perks_map.items():
                lines = iter_vip_perk_lines(perks)
                if not lines:
                    continue

                field_value = "\n".join(lines)
                embed.add_field(
                    name=get_category_title(category_key),
                    value=field_value,
                    inline=False,
                )
                embed.add_field(name="\u200b", value="\u200b", inline=False)

            if embed.fields and embed.fields[-1].name == "\u200b":
                embed.remove_field(len(embed.fields) - 1)

        embed.set_footer(text="Upgrade to VIP to unlock all perks")
        return embed

    @app_commands.command(name="vip", description="View all VIP benefits.")
    async def vip(self, interaction: discord.Interaction) -> None:
        embed = self._build_embed()
        view = VIPLinkView(url=VIP_STORE_URL) if VIP_STORE_URL else None
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(VIPEmbedCog(bot))
