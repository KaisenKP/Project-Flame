# What this file is: Discord UI views for the public self-role panel and private category menus.
# Last change: 2026-05-29 - Initial persistent buttons and ephemeral selects.

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import discord

from .config import RoleCategory
from .embeds import build_category_embed
from .errors import SelfRoleError

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .cog import SelfRolesCog


class SelfRolesPanelView(discord.ui.View):
    def __init__(self, cog: "SelfRolesCog") -> None:
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Platform", style=discord.ButtonStyle.primary, custom_id="selfroles:open:platform", emoji="🖥️")
    async def platform_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.open_category(interaction, "platform")

    @discord.ui.button(label="Age", style=discord.ButtonStyle.secondary, custom_id="selfroles:open:age", emoji="🔞")
    async def age_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.open_category(interaction, "age")

    @discord.ui.button(label="Pings", style=discord.ButtonStyle.secondary, custom_id="selfroles:open:pings", emoji="🔔")
    async def pings_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.open_category(interaction, "pings")

    @discord.ui.button(label="Fun Roles", style=discord.ButtonStyle.secondary, custom_id="selfroles:open:fun", emoji="✨")
    async def fun_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.open_category(interaction, "fun")


class CategoryRoleSelect(discord.ui.Select):
    def __init__(
        self,
        cog: "SelfRolesCog",
        category: RoleCategory,
        roles_by_key: dict[str, discord.Role],
        member: discord.Member,
    ) -> None:
        self.cog = cog
        self.category = category
        current_role_ids = {role.id for role in member.roles}
        options: list[discord.SelectOption] = []
        for role_def in category.roles:
            role = roles_by_key[role_def.key]
            options.append(
                discord.SelectOption(
                    label=role_def.name[:100],
                    value=role_def.key,
                    description=role_def.description[:100],
                    emoji=role_def.emoji,
                    default=role.id in current_role_ids,
                )
            )

        super().__init__(
            placeholder=category.placeholder,
            custom_id=category.select_custom_id,
            min_values=0 if category.selection_type == "multi" else 1,
            max_values=len(options) if category.selection_type == "multi" else 1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await _respond(interaction, "This can only be used in a server.")
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            result = await self.cog.service.apply_selection(
                guild=interaction.guild,
                member=interaction.user,
                category_key=self.category.key,
                selected_keys=set(self.values),
            )
        except SelfRoleError as exc:
            await interaction.followup.send(exc.user_message, ephemeral=True)
            return
        except Exception:
            log.exception("Unexpected self-role select failure")
            await interaction.followup.send("Something went wrong while updating your roles. Please try again.", ephemeral=True)
            return
        await interaction.followup.send(result.user_message(), ephemeral=True)


class CategoryRoleView(discord.ui.View):
    def __init__(
        self,
        cog: "SelfRolesCog",
        category: RoleCategory,
        roles_by_key: dict[str, discord.Role],
        member: discord.Member,
    ) -> None:
        super().__init__(timeout=180)
        self.cog = cog
        self.category = category
        self.add_item(CategoryRoleSelect(cog, category, roles_by_key, member))

        clear_button = discord.ui.Button(
            label="Clear",
            style=discord.ButtonStyle.danger,
            custom_id=category.clear_custom_id,
        )
        clear_button.callback = self.clear_callback
        self.add_item(clear_button)

    async def clear_callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await _respond(interaction, "This can only be used in a server.")
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            result = await self.cog.service.apply_selection(
                guild=interaction.guild,
                member=interaction.user,
                category_key=self.category.key,
                selected_keys=set(),
            )
        except SelfRoleError as exc:
            await interaction.followup.send(exc.user_message, ephemeral=True)
            return
        except Exception:
            log.exception("Unexpected self-role clear failure")
            await interaction.followup.send("Something went wrong while updating your roles. Please try again.", ephemeral=True)
            return
        await interaction.followup.send(result.user_message(), ephemeral=True)


async def send_category_menu(
    cog: "SelfRolesCog",
    interaction: discord.Interaction,
    category_key: str,
) -> None:
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        await interaction.followup.send("This can only be used in a server.", ephemeral=True)
        return
    try:
        category, roles_by_key = await cog.service.get_category_roles(interaction.guild, category_key)
    except SelfRoleError as exc:
        await interaction.followup.send(exc.user_message, ephemeral=True)
        return
    except Exception:
        log.exception("Unexpected self-role menu open failure")
        await interaction.followup.send("Something went wrong while opening that menu. Please try again.", ephemeral=True)
        return

    try:
        record = await cog.storage.get(interaction.guild.id)
    except SelfRoleError as exc:
        await interaction.followup.send(exc.user_message, ephemeral=True)
        return
    except Exception:
        log.exception("Unexpected self-role storage failure while opening menu")
        await interaction.followup.send("Something went wrong while opening that menu. Please try again.", ephemeral=True)
        return

    embed = build_category_embed(category, image_url=record.category_image_urls.get(category.key, ""))
    await interaction.followup.send(
        embed=embed,
        view=CategoryRoleView(cog, category, roles_by_key, interaction.user),
        ephemeral=True,
    )


async def _respond(interaction: discord.Interaction, message: str) -> None:
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)
