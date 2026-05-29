# What this file is: Discord cog orchestration for /setup_roles and self-role panel interactions.
# Last change: 2026-05-29 - Initial admin setup command and persistent panel registration.

from __future__ import annotations

import logging

import discord
from discord import app_commands
from discord.ext import commands

from .embeds import build_panel_embed, build_setup_summary_embed
from .errors import SelfRoleError, SelfRoleSetupError
from .service import SelfRolesService, SelfRoleSetupSummary
from .storage import SelfRolesGuildRecord, SelfRolesStorage
from .views import SelfRolesPanelView, send_category_menu

log = logging.getLogger(__name__)


class SelfRolesCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.storage = SelfRolesStorage()
        self.service = SelfRolesService(bot, self.storage)
        self._panel_view_registered = False

    async def cog_load(self) -> None:
        await self.storage.ensure_tables()
        self._register_persistent_panel_view()

    def _register_persistent_panel_view(self) -> None:
        if self._panel_view_registered:
            return
        try:
            self.bot.add_view(SelfRolesPanelView(self))
        except ValueError:
            pass
        self._panel_view_registered = True

    @app_commands.command(name="setup_roles", description="Admin: Create or refresh the self-role picker panel.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.guild_only()
    async def setup_roles(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel | None = None,
    ) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        if not (interaction.user.guild_permissions.manage_guild or interaction.user.guild_permissions.administrator):
            await interaction.response.send_message("You need Manage Server or Administrator to run `/setup_roles`.", ephemeral=True)
            return

        target_channel = channel
        if target_channel is None:
            if isinstance(interaction.channel, discord.TextChannel):
                target_channel = interaction.channel
            else:
                await interaction.response.send_message("Pick a text channel for the role panel.", ephemeral=True)
                return

        await interaction.response.defer(ephemeral=True, thinking=True)
        lock = self.service.setup_lock_for(interaction.guild.id)
        async with lock:
            try:
                summary, record = await self.service.resolve_configured_roles(interaction.guild, target_channel)
                message = await self._create_or_update_panel(interaction.guild, target_channel, record, summary)
                record.panel_channel_id = message.channel.id
                record.panel_message_id = message.id
                await self.storage.upsert(record)
                self._register_persistent_panel_view()
            except SelfRoleError as exc:
                await interaction.followup.send(exc.user_message, ephemeral=True)
                return
            except Exception:
                log.exception("Unexpected /setup_roles failure")
                await interaction.followup.send("Self-role setup failed unexpectedly. Check the bot logs for details.", ephemeral=True)
                return

        await interaction.followup.send(embed=build_setup_summary_embed(summary), ephemeral=True)

    async def _create_or_update_panel(
        self,
        guild: discord.Guild,
        target_channel: discord.TextChannel,
        record: SelfRolesGuildRecord,
        summary: SelfRoleSetupSummary,
    ) -> discord.Message:
        embed = build_panel_embed(
            image_url=record.panel_image_url,
            thumbnail_url=record.panel_thumbnail_url,
        )
        view = SelfRolesPanelView(self)

        if record.panel_channel_id and record.panel_message_id:
            saved_channel = guild.get_channel(record.panel_channel_id)
            if isinstance(saved_channel, discord.TextChannel):
                try:
                    saved_message = await saved_channel.fetch_message(record.panel_message_id)
                    await saved_message.edit(embed=embed, view=view)
                    summary.panel_action = "Panel updated."
                    summary.panel_channel_id = saved_channel.id
                    summary.panel_message_id = saved_message.id
                    return saved_message
                except discord.NotFound:
                    summary.warnings.append("Saved panel message was deleted, so I created a new panel.")
                except discord.Forbidden:
                    summary.warnings.append("I could not fetch or edit the saved panel, so I created a new one in the selected channel.")
                except discord.HTTPException as exc:
                    summary.warnings.append(f"Saved panel could not be edited due to a Discord error: {exc}")
            else:
                summary.warnings.append("Saved panel channel is missing, so I used the selected channel.")

        try:
            message = await target_channel.send(embed=embed, view=view)
        except discord.Forbidden as exc:
            raise SelfRoleSetupError("I cannot send the role picker panel in that channel.") from exc
        except discord.HTTPException as exc:
            raise SelfRoleSetupError("Discord rejected the role picker panel message.") from exc

        summary.panel_action = "Panel created."
        summary.panel_channel_id = target_channel.id
        summary.panel_message_id = message.id
        return message

    async def open_category(self, interaction: discord.Interaction, category_key: str) -> None:
        if interaction.response.is_done():
            await interaction.followup.send("Opening role menu...", ephemeral=True)
        else:
            await interaction.response.defer(ephemeral=True, thinking=True)
        await send_category_menu(self, interaction, category_key)

    @setup_roles.error
    async def setup_roles_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.errors.MissingPermissions):
            message = "You need Manage Server or Administrator to run `/setup_roles`."
        else:
            message = "I could not run `/setup_roles`. Please try again or check the bot logs."
            log.warning("setup_roles command error: %s", error)

        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
