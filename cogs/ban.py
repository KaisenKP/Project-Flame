from __future__ import annotations

from dataclasses import dataclass

import discord
from discord import app_commands
from discord.ext import commands


MAX_DELETE_DAYS = 7


@dataclass(slots=True)
class BanDraft:
    moderator_id: int
    target_id: int | None = None
    target_display: str = "Not selected"
    reason: str = "No reason provided"
    delete_days: int = 0


class BanReasonModal(discord.ui.Modal, title="Set ban reason"):
    reason = discord.ui.TextInput(
        label="Reason",
        placeholder="e.g., Spam, raids, harassment",
        required=True,
        max_length=300,
        style=discord.TextStyle.paragraph,
    )

    def __init__(self, view: "BanControlsView"):
        super().__init__()
        self.view = view

    async def on_submit(self, interaction: discord.Interaction) -> None:
        self.view.draft.reason = str(self.reason.value).strip() or "No reason provided"
        await self.view.refresh_message(interaction)


class BanDeleteDaysModal(discord.ui.Modal, title="Set message delete window"):
    days = discord.ui.TextInput(
        label=f"Delete messages from last N days (0-{MAX_DELETE_DAYS})",
        placeholder="0",
        required=True,
        max_length=1,
    )

    def __init__(self, view: "BanControlsView"):
        super().__init__()
        self.view = view

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = str(self.days.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("Please enter a whole number from 0 to 7.", ephemeral=True)
            return
        value = int(raw)
        if value < 0 or value > MAX_DELETE_DAYS:
            await interaction.response.send_message("Delete window must be between 0 and 7 days.", ephemeral=True)
            return
        self.view.draft.delete_days = value
        await self.view.refresh_message(interaction)


class BanTargetSelect(discord.ui.UserSelect):
    def __init__(self, owner_id: int):
        super().__init__(
            placeholder="Pick user to ban",
            min_values=1,
            max_values=1,
        )
        self.owner_id = owner_id

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, BanTargetPickerView):
            return

        if interaction.user is None or int(interaction.user.id) != self.owner_id:
            await interaction.response.send_message("Only the command author can pick a target.", ephemeral=True)
            return

        selected = self.values[0]
        view.parent_view.draft.target_id = int(selected.id)
        view.parent_view.draft.target_display = f"{selected.mention} (`{selected.id}`)"

        if interaction.response.is_done():
            await interaction.followup.send("Target updated.", ephemeral=True)
        else:
            await interaction.response.send_message("Target updated.", ephemeral=True)

        if view.parent_interaction is not None:
            await view.parent_view.refresh_message(view.parent_interaction)
        view.stop()


class BanTargetPickerView(discord.ui.View):
    def __init__(self, parent_view: "BanControlsView", owner_id: int, parent_interaction: discord.Interaction):
        super().__init__(timeout=120)
        self.parent_view = parent_view
        self.parent_interaction = parent_interaction
        self.add_item(BanTargetSelect(owner_id))


class BanControlsView(discord.ui.View):
    def __init__(self, *, moderator_id: int):
        super().__init__(timeout=300)
        self.draft = BanDraft(moderator_id=moderator_id)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user is None or int(interaction.user.id) != self.draft.moderator_id:
            await interaction.response.send_message("Only the command author can use these controls.", ephemeral=True)
            return False
        return True

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Ban Panel",
            description="Configure the ban, then press **Confirm Ban**.",
            color=discord.Color.red(),
        )
        embed.add_field(name="Target", value=self.draft.target_display, inline=False)
        embed.add_field(name="Reason", value=self.draft.reason, inline=False)
        embed.add_field(name="Delete Messages", value=f"{self.draft.delete_days} day(s)", inline=True)
        embed.set_footer(text="Only the staff member who opened this panel can interact.")
        return embed

    async def refresh_message(self, interaction: discord.Interaction) -> None:
        embed = self.build_embed()
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=self)
        else:
            await interaction.response.edit_message(embed=embed, view=self)

    async def disable_all(self, interaction: discord.Interaction, *, note: str) -> None:
        for child in self.children:
            if hasattr(child, "disabled"):
                child.disabled = True
        embed = self.build_embed()
        embed.add_field(name="Result", value=note, inline=False)
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=self)
        else:
            await interaction.response.edit_message(embed=embed, view=self)
        self.stop()

    @discord.ui.button(label="Pick User", style=discord.ButtonStyle.primary, emoji="👤", row=0)
    async def pick_user(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        picker = BanTargetPickerView(self, self.draft.moderator_id, interaction)
        await interaction.response.send_message(
            "Choose a user from the server list.",
            view=picker,
            ephemeral=True,
        )

    @discord.ui.button(label="Set Reason", style=discord.ButtonStyle.secondary, emoji="📝", row=0)
    async def set_reason(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(BanReasonModal(self))

    @discord.ui.button(label="Delete Days", style=discord.ButtonStyle.secondary, emoji="🧹", row=0)
    async def set_delete_days(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(BanDeleteDaysModal(self))

    @discord.ui.button(label="Preview", style=discord.ButtonStyle.secondary, emoji="🔄", row=1)
    async def preview(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.refresh_message(interaction)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="✖️", row=1)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.disable_all(interaction, note="Ban cancelled.")

    @discord.ui.button(label="Confirm Ban", style=discord.ButtonStyle.danger, emoji="⛔", row=1)
    async def confirm_ban(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return

        if self.draft.target_id is None:
            await interaction.response.send_message("Pick a user first.", ephemeral=True)
            return

        guild = interaction.guild
        moderator = guild.get_member(interaction.user.id)
        target_member = guild.get_member(self.draft.target_id)
        target_user: discord.abc.Snowflake = target_member or discord.Object(id=self.draft.target_id)

        if self.draft.target_id == interaction.user.id:
            await interaction.response.send_message("You cannot ban yourself.", ephemeral=True)
            return

        if self.draft.target_id == guild.owner_id:
            await interaction.response.send_message("You cannot ban the server owner.", ephemeral=True)
            return

        if target_member is not None and moderator is not None:
            if target_member.top_role >= moderator.top_role and interaction.user.id != guild.owner_id:
                await interaction.response.send_message("You cannot ban a member with an equal/higher role.", ephemeral=True)
                return

        me = guild.me
        if me is not None and not me.guild_permissions.ban_members:
            await interaction.response.send_message("I need the **Ban Members** permission to do that.", ephemeral=True)
            return

        if target_member is not None and me is not None and target_member.top_role >= me.top_role:
            await interaction.response.send_message("I cannot ban that user due to role hierarchy.", ephemeral=True)
            return

        delete_seconds = self.draft.delete_days * 24 * 60 * 60
        reason = f"{self.draft.reason} | by {interaction.user} ({interaction.user.id})"

        try:
            await guild.ban(target_user, reason=reason, delete_message_seconds=delete_seconds)
        except discord.Forbidden:
            await interaction.response.send_message("Ban failed: missing permissions or role hierarchy issue.", ephemeral=True)
            return
        except discord.HTTPException as exc:
            await interaction.response.send_message(f"Ban failed: {exc}", ephemeral=True)
            return

        await self.disable_all(interaction, note=f"✅ Banned <@{self.draft.target_id}>.")


class BanCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="ban", description="Open an easy interactive ban panel.")
    @app_commands.checks.has_permissions(ban_members=True)
    @app_commands.guild_only()
    async def ban(self, interaction: discord.Interaction) -> None:
        view = BanControlsView(moderator_id=interaction.user.id)
        await interaction.response.send_message(embed=view.build_embed(), view=view, ephemeral=True)

    @ban.error
    async def ban_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.errors.MissingPermissions):
            message = "You need the **Ban Members** permission to use this command."
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)
            return
        raise error


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(BanCog(bot))
