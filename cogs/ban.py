from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

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
    dm_enabled: bool = True
    dm_message: str = "You have been banned from this server."
    appeal_url: str = ""


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


class BanDmMessageModal(discord.ui.Modal, title="Set DM message"):
    message = discord.ui.TextInput(
        label="DM Message",
        placeholder="You have been banned from this server.",
        required=True,
        max_length=500,
        style=discord.TextStyle.paragraph,
    )

    def __init__(self, view: "BanControlsView"):
        super().__init__()
        self.view = view
        self.message.default = view.draft.dm_message

    async def on_submit(self, interaction: discord.Interaction) -> None:
        value = str(self.message.value).strip()
        if not value:
            await interaction.response.send_message("DM message cannot be empty.", ephemeral=True)
            return
        self.view.draft.dm_message = value
        await self.view.refresh_message(interaction)


class BanAppealModal(discord.ui.Modal, title="Set appeal URL"):
    appeal_url = discord.ui.TextInput(
        label="Appeal URL (optional)",
        placeholder="https://example.com/appeal",
        required=False,
        max_length=200,
    )

    def __init__(self, view: "BanControlsView"):
        super().__init__()
        self.view = view
        self.appeal_url.default = view.draft.appeal_url

    async def on_submit(self, interaction: discord.Interaction) -> None:
        value = str(self.appeal_url.value).strip()
        if value and not (value.startswith("https://") or value.startswith("http://")):
            await interaction.response.send_message("Appeal URL must start with http:// or https://", ephemeral=True)
            return
        self.view.draft.appeal_url = value
        await self.view.refresh_message(interaction)


class BanTargetIdModal(discord.ui.Modal, title="Set target by user ID"):
    user_id = discord.ui.TextInput(
        label="User ID",
        placeholder="123456789012345678",
        required=True,
        max_length=20,
    )

    def __init__(self, view: "BanControlsView"):
        super().__init__()
        self.view = view

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = str(self.user_id.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("Please enter a valid numeric Discord user ID.", ephemeral=True)
            return
        target_id = int(raw)
        self.view.draft.target_id = target_id
        self.view.draft.target_display = f"<@{target_id}> (`{target_id}`)"
        await self.view.refresh_message(interaction)


class BanConfirmModal(discord.ui.Modal, title="Confirm ban"):
    confirmation = discord.ui.TextInput(
        label="Type BAN to confirm",
        placeholder="BAN",
        required=True,
        max_length=3,
    )

    def __init__(self, view: "BanControlsView"):
        super().__init__()
        self.view = view

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if str(self.confirmation.value).strip().upper() != "BAN":
            await interaction.response.send_message("Ban confirmation failed. Type exactly `BAN`.", ephemeral=True)
            return
        await self.view.execute_ban(interaction)


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
    def __init__(self, *, moderator_id: int, bot: commands.Bot):
        super().__init__(timeout=300)
        self.bot = bot
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
        embed.add_field(name="DM User", value="Enabled ✅" if self.draft.dm_enabled else "Disabled ❌", inline=True)
        appeal_value = self.draft.appeal_url if self.draft.appeal_url else "Not set"
        embed.add_field(name="Appeal URL", value=appeal_value, inline=False)
        embed.add_field(name="DM Message Preview", value=self.draft.dm_message, inline=False)
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

    @discord.ui.button(label="Set User ID", style=discord.ButtonStyle.secondary, emoji="🆔", row=1)
    async def set_user_id(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(BanTargetIdModal(self))

    @discord.ui.button(label="DM Toggle", style=discord.ButtonStyle.secondary, emoji="📩", row=1)
    async def toggle_dm(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.draft.dm_enabled = not self.draft.dm_enabled
        await self.refresh_message(interaction)

    @discord.ui.button(label="Set DM", style=discord.ButtonStyle.secondary, emoji="💬", row=1)
    async def set_dm_message(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(BanDmMessageModal(self))

    @discord.ui.button(label="Set Appeal", style=discord.ButtonStyle.secondary, emoji="🪪", row=2)
    async def set_appeal(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(BanAppealModal(self))

    @discord.ui.button(label="Preview", style=discord.ButtonStyle.secondary, emoji="🔄", row=2)
    async def preview(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.refresh_message(interaction)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="✖️", row=2)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.disable_all(interaction, note="Ban cancelled.")

    @discord.ui.button(label="Confirm Ban", style=discord.ButtonStyle.danger, emoji="⛔", row=2)
    async def confirm_ban(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(BanConfirmModal(self))

    async def execute_ban(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return

        if self.draft.target_id is None:
            await interaction.response.send_message("Pick a user first.", ephemeral=True)
            return

        guild = interaction.guild
        moderator = guild.get_member(interaction.user.id)
        target_member = guild.get_member(self.draft.target_id)
        target_user: discord.abc.User | discord.Object | None = target_member

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

        if target_user is None:
            target_user = self.bot.get_user(self.draft.target_id)

        if target_user is None:
            try:
                target_user = await self.bot.fetch_user(self.draft.target_id)
            except discord.NotFound:
                await interaction.response.send_message("That user ID does not exist.", ephemeral=True)
                return
            except discord.HTTPException:
                target_user = discord.Object(id=self.draft.target_id)

        delete_seconds = self.draft.delete_days * 24 * 60 * 60
        reason = f"{self.draft.reason} | by {interaction.user} ({interaction.user.id})"
        dm_status = "Skipped"

        if self.draft.dm_enabled:
            dm_text = self.draft.dm_message
            if self.draft.appeal_url:
                dm_text = f"{dm_text}\nAppeal: {self.draft.appeal_url}"
            try:
                if isinstance(target_user, discord.User) or isinstance(target_user, discord.Member):
                    await target_user.send(dm_text)
                    dm_status = "Sent ✅"
                else:
                    dm_status = "Skipped (user object unavailable)"
            except discord.Forbidden:
                dm_status = "Failed (DMs closed)"
            except discord.HTTPException:
                dm_status = "Failed (HTTP error)"

        try:
            await guild.ban(target_user, reason=reason, delete_message_seconds=delete_seconds)
        except discord.Forbidden:
            await interaction.response.send_message("Ban failed: missing permissions or role hierarchy issue.", ephemeral=True)
            return
        except discord.HTTPException as exc:
            await interaction.response.send_message(f"Ban failed: {exc}", ephemeral=True)
            return

        await self.disable_all(interaction, note=f"✅ Banned <@{self.draft.target_id}>. DM: {dm_status}")


@dataclass(slots=True)
class PunishDraft:
    moderator_id: int
    target_id: int
    target_display: str
    reason: str = "No reason provided"
    timeout_minutes: int = 60
    delete_amount: int = 10
    ban_delete_days: int = 0


class PunishReasonModal(discord.ui.Modal, title="Set punishment reason"):
    reason = discord.ui.TextInput(
        label="Reason",
        placeholder="What happened?",
        required=True,
        max_length=300,
        style=discord.TextStyle.paragraph,
    )

    def __init__(self, view: "PunishHubView"):
        super().__init__()
        self.view = view
        self.reason.default = view.draft.reason

    async def on_submit(self, interaction: discord.Interaction) -> None:
        value = str(self.reason.value).strip()
        if not value:
            await interaction.response.send_message("Reason cannot be empty.", ephemeral=True)
            return
        self.view.draft.reason = value
        await self.view.refresh_message(interaction)


class TimeoutSettingsModal(discord.ui.Modal, title="Timeout settings"):
    minutes = discord.ui.TextInput(
        label="Timeout minutes (1-40320)",
        placeholder="60",
        required=True,
        max_length=5,
    )

    def __init__(self, view: "PunishHubView"):
        super().__init__()
        self.view = view
        self.minutes.default = str(view.draft.timeout_minutes)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = str(self.minutes.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("Please enter a whole number of minutes.", ephemeral=True)
            return
        minutes = int(raw)
        if minutes < 1 or minutes > 40320:
            await interaction.response.send_message("Timeout must be between 1 and 40320 minutes.", ephemeral=True)
            return
        self.view.draft.timeout_minutes = minutes
        await self.view.refresh_message(interaction)


class DeleteMessagesSettingsModal(discord.ui.Modal, title="Delete target messages"):
    amount = discord.ui.TextInput(
        label="Messages to delete (1-200)",
        placeholder="10",
        required=True,
        max_length=3,
    )

    def __init__(self, view: "PunishHubView"):
        super().__init__()
        self.view = view
        self.amount.default = str(view.draft.delete_amount)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = str(self.amount.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("Please enter a whole number.", ephemeral=True)
            return
        amount = int(raw)
        if amount < 1 or amount > 200:
            await interaction.response.send_message("Delete amount must be between 1 and 200.", ephemeral=True)
            return
        self.view.draft.delete_amount = amount
        await self.view.refresh_message(interaction)


class BanDeleteDaysSettingsModal(discord.ui.Modal, title="Ban delete window"):
    days = discord.ui.TextInput(
        label=f"Delete messages from last N days (0-{MAX_DELETE_DAYS})",
        placeholder="0",
        required=True,
        max_length=1,
    )

    def __init__(self, view: "PunishHubView"):
        super().__init__()
        self.view = view
        self.days.default = str(view.draft.ban_delete_days)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = str(self.days.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("Please enter a whole number from 0 to 7.", ephemeral=True)
            return
        value = int(raw)
        if value < 0 or value > MAX_DELETE_DAYS:
            await interaction.response.send_message("Delete window must be between 0 and 7 days.", ephemeral=True)
            return
        self.view.draft.ban_delete_days = value
        await self.view.refresh_message(interaction)


class PunishConfirmModal(discord.ui.Modal):
    confirm = discord.ui.TextInput(
        label="Type confirmation keyword",
        required=True,
        max_length=16,
    )

    def __init__(self, view: "PunishHubView", action: str, keyword: str):
        super().__init__(title=f"Confirm {action}")
        self.view = view
        self.action = action
        self.keyword = keyword
        self.confirm.label = f"Type {keyword} to confirm"
        self.confirm.placeholder = keyword
        self.confirm.max_length = len(keyword)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if str(self.confirm.value).strip().upper() != self.keyword:
            await interaction.response.send_message(f"Confirmation failed. Type exactly `{self.keyword}`.", ephemeral=True)
            return
        await self.view.execute_action(interaction, self.action)


class PunishHubView(discord.ui.View):
    def __init__(self, *, bot: commands.Bot, moderator_id: int, target: discord.Member):
        super().__init__(timeout=300)
        self.bot = bot
        self.draft = PunishDraft(
            moderator_id=moderator_id,
            target_id=target.id,
            target_display=f"{target.mention} (`{target.id}`)",
        )

    def _moderation_cog(self) -> commands.Cog | None:
        return self.bot.get_cog("ModerationCog")

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user is None or int(interaction.user.id) != self.draft.moderator_id:
            await interaction.response.send_message("Only the command author can use these controls.", ephemeral=True)
            return False
        return True

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Punishment Hub",
            description="Select a punishment path. This mirrors the interactive `/ban` moderation flow.",
            color=discord.Color.red(),
        )
        embed.add_field(name="Target", value=self.draft.target_display, inline=False)
        embed.add_field(name="Reason", value=self.draft.reason, inline=False)
        embed.add_field(name="Timeout", value=f"{self.draft.timeout_minutes} minute(s)", inline=True)
        embed.add_field(name="Delete Messages", value=f"{self.draft.delete_amount} message(s)", inline=True)
        embed.add_field(name="Ban Delete Days", value=f"{self.draft.ban_delete_days} day(s)", inline=True)
        embed.set_footer(text="Only the staff member who opened this panel can interact.")
        return embed

    async def refresh_message(self, interaction: discord.Interaction) -> None:
        embed = self.build_embed()
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=self)
        else:
            await interaction.response.edit_message(embed=embed, view=self)

    async def disable_all(self, interaction: discord.Interaction, *, result: str) -> None:
        for child in self.children:
            if hasattr(child, "disabled"):
                child.disabled = True
        embed = self.build_embed()
        embed.add_field(name="Result", value=result, inline=False)
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=self)
        else:
            await interaction.response.edit_message(embed=embed, view=self)
        self.stop()

    async def _permission_error(self, interaction: discord.Interaction, message: str) -> None:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)

    def _resolve_target(self, interaction: discord.Interaction) -> discord.Member | None:
        if interaction.guild is None:
            return None
        return interaction.guild.get_member(self.draft.target_id)

    async def _validate_member_action(
        self,
        interaction: discord.Interaction,
        *,
        target: discord.Member | None,
        required_permission: str,
    ) -> bool:
        if interaction.guild is None or interaction.user is None:
            await self._permission_error(interaction, "This can only be used in a server.")
            return False
        guild = interaction.guild
        moderator = guild.get_member(interaction.user.id)
        me = guild.me
        if target is None:
            await self._permission_error(interaction, "Target is no longer in this server.")
            return False
        if target.id == interaction.user.id:
            await self._permission_error(interaction, "You cannot punish yourself.")
            return False
        if target.id == guild.owner_id:
            await self._permission_error(interaction, "You cannot punish the server owner.")
            return False
        if moderator is not None and moderator.id != guild.owner_id and target.top_role >= moderator.top_role:
            await self._permission_error(interaction, "You cannot punish a member with an equal/higher role.")
            return False
        if me is None or not getattr(me.guild_permissions, required_permission):
            await self._permission_error(interaction, f"I need the **{required_permission.replace('_', ' ').title()}** permission.")
            return False
        if target.top_role >= me.top_role:
            await self._permission_error(interaction, "I cannot punish that user due to role hierarchy.")
            return False
        return True

    async def _log_action(self, guild: discord.Guild, *, title: str, description: str, color: discord.Color) -> None:
        moderation_cog = self._moderation_cog()
        if moderation_cog and hasattr(moderation_cog, "log_action"):
            await moderation_cog.log_action(guild, title=title, description=description, color=color)

    @discord.ui.button(label="Set Reason", style=discord.ButtonStyle.secondary, emoji="📝", row=0)
    async def set_reason(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(PunishReasonModal(self))

    @discord.ui.button(label="Timeout Config", style=discord.ButtonStyle.secondary, emoji="⏱️", row=0)
    async def timeout_config(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(TimeoutSettingsModal(self))

    @discord.ui.button(label="Delete Config", style=discord.ButtonStyle.secondary, emoji="🧹", row=0)
    async def delete_config(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(DeleteMessagesSettingsModal(self))

    @discord.ui.button(label="Ban Config", style=discord.ButtonStyle.secondary, emoji="⛔", row=1)
    async def ban_config(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(BanDeleteDaysSettingsModal(self))

    @discord.ui.button(label="Warn / Note", style=discord.ButtonStyle.primary, emoji="⚠️", row=1)
    async def warn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.execute_action(interaction, "warn")

    @discord.ui.button(label="Delete Messages", style=discord.ButtonStyle.primary, emoji="🗑️", row=1)
    async def delete_messages(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(PunishConfirmModal(self, "delete", "DELETE"))

    @discord.ui.button(label="Timeout", style=discord.ButtonStyle.primary, emoji="🔇", row=2)
    async def timeout_action(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(PunishConfirmModal(self, "timeout", "TIMEOUT"))

    @discord.ui.button(label="Kick", style=discord.ButtonStyle.danger, emoji="👢", row=2)
    async def kick(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(PunishConfirmModal(self, "kick", "KICK"))

    @discord.ui.button(label="Ban", style=discord.ButtonStyle.danger, emoji="⛔", row=2)
    async def ban(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(PunishConfirmModal(self, "ban", "BAN"))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="✖️", row=2)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.disable_all(interaction, result="Punishment flow cancelled.")

    async def execute_action(self, interaction: discord.Interaction, action: str) -> None:
        if action == "warn":
            await self._execute_warn(interaction)
        elif action == "delete":
            await self._execute_delete_messages(interaction)
        elif action == "timeout":
            await self._execute_timeout(interaction)
        elif action == "kick":
            await self._execute_kick(interaction)
        elif action == "ban":
            await self._execute_ban(interaction)

    async def _execute_warn(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.user is None:
            await self._permission_error(interaction, "This can only be used in a server.")
            return
        target = self._resolve_target(interaction)
        if target is None:
            await self._permission_error(interaction, "Target is no longer in this server.")
            return
        moderation_cog = self._moderation_cog()
        if moderation_cog is None or not hasattr(moderation_cog, "add_warning"):
            await self._permission_error(interaction, "Moderation warning storage is unavailable.")
            return
        await moderation_cog.add_warning(
            guild_id=interaction.guild.id,
            user_id=target.id,
            moderator_id=interaction.user.id,
            reason=self.draft.reason,
        )
        await self._log_action(
            interaction.guild,
            title="Punishment Hub • User Warned",
            description=f"{target.mention} warned by {interaction.user.mention}.\n**Reason:** {self.draft.reason}",
            color=discord.Color.orange(),
        )
        await self.disable_all(interaction, result=f"✅ Warning recorded for {target.mention}.")

    async def _execute_timeout(self, interaction: discord.Interaction) -> None:
        target = self._resolve_target(interaction)
        if not await self._validate_member_action(interaction, target=target, required_permission="moderate_members"):
            return
        assert target is not None and interaction.guild is not None and interaction.user is not None
        until = discord.utils.utcnow() + timedelta(minutes=self.draft.timeout_minutes)
        reason = f"{self.draft.reason} | by {interaction.user} ({interaction.user.id})"
        try:
            await target.timeout(until, reason=reason)
        except discord.Forbidden:
            await self._permission_error(interaction, "Timeout failed due to permissions or role hierarchy.")
            return
        except discord.HTTPException:
            await self._permission_error(interaction, "Timeout failed due to a Discord error.")
            return
        await self._log_action(
            interaction.guild,
            title="Punishment Hub • User Timed Out",
            description=f"{target.mention} timed out for **{self.draft.timeout_minutes}** minute(s) by {interaction.user.mention}.\n**Reason:** {self.draft.reason}",
            color=discord.Color.red(),
        )
        await self.disable_all(interaction, result=f"✅ Timed out {target.mention} for {self.draft.timeout_minutes} minute(s).")

    async def _execute_kick(self, interaction: discord.Interaction) -> None:
        target = self._resolve_target(interaction)
        if not await self._validate_member_action(interaction, target=target, required_permission="kick_members"):
            return
        assert target is not None and interaction.guild is not None and interaction.user is not None
        reason = f"{self.draft.reason} | by {interaction.user} ({interaction.user.id})"
        try:
            await interaction.guild.kick(target, reason=reason)
        except discord.Forbidden:
            await self._permission_error(interaction, "Kick failed due to permissions or role hierarchy.")
            return
        except discord.HTTPException:
            await self._permission_error(interaction, "Kick failed due to a Discord error.")
            return
        await self._log_action(
            interaction.guild,
            title="Punishment Hub • User Kicked",
            description=f"{target} kicked by {interaction.user.mention}.\n**Reason:** {self.draft.reason}",
            color=discord.Color.red(),
        )
        await self.disable_all(interaction, result=f"✅ Kicked {target}.")

    async def _execute_ban(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.user is None:
            await self._permission_error(interaction, "This can only be used in a server.")
            return
        guild = interaction.guild
        target_member = self._resolve_target(interaction)
        if target_member is not None:
            valid = await self._validate_member_action(interaction, target=target_member, required_permission="ban_members")
            if not valid:
                return
        else:
            me = guild.me
            if me is None or not me.guild_permissions.ban_members:
                await self._permission_error(interaction, "I need the **Ban Members** permission.")
                return
        target_user: discord.abc.User | discord.Object = target_member or discord.Object(id=self.draft.target_id)
        reason = f"{self.draft.reason} | by {interaction.user} ({interaction.user.id})"
        try:
            await guild.ban(target_user, reason=reason, delete_message_seconds=int(self.draft.ban_delete_days) * 86400)
        except discord.Forbidden:
            await self._permission_error(interaction, "Ban failed due to permissions or role hierarchy.")
            return
        except discord.HTTPException as exc:
            await self._permission_error(interaction, f"Ban failed: {exc}")
            return
        await self._log_action(
            guild,
            title="Punishment Hub • User Banned",
            description=f"<@{self.draft.target_id}> banned by {interaction.user.mention}.\n**Reason:** {self.draft.reason}\n**Delete Days:** {self.draft.ban_delete_days}",
            color=discord.Color.dark_red(),
        )
        await self.disable_all(interaction, result=f"✅ Banned <@{self.draft.target_id}>.")

    async def _execute_delete_messages(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.user is None:
            await self._permission_error(interaction, "This can only be used in a server.")
            return
        if interaction.channel is None or not isinstance(interaction.channel, (discord.TextChannel, discord.Thread)):
            await self._permission_error(interaction, "Message deletion currently works in text channels/threads only.")
            return
        channel = interaction.channel
        me = interaction.guild.me
        if me is None or not me.guild_permissions.manage_messages:
            await self._permission_error(interaction, "I need the **Manage Messages** permission.")
            return

        requested = int(self.draft.delete_amount)
        matching: list[discord.Message] = []
        scan_limit = min(max(requested * 15, 100), 2000)
        async for message in channel.history(limit=scan_limit):
            if message.author.id == self.draft.target_id:
                matching.append(message)
                if len(matching) >= requested:
                    break

        found = len(matching)
        if found == 0:
            await self.disable_all(interaction, result=f"No messages found for <@{self.draft.target_id}> in recent channel history.")
            return

        cutoff = discord.utils.utcnow() - timedelta(days=14)
        recent = [m for m in matching if m.created_at >= cutoff]
        old = [m for m in matching if m.created_at < cutoff]
        deleted = 0
        skipped = 0

        if recent:
            batches = [recent[i : i + 100] for i in range(0, len(recent), 100)]
            for batch in batches:
                try:
                    if len(batch) == 1:
                        await batch[0].delete(reason=f"Punish delete by {interaction.user.id} ({self.draft.reason})")
                        deleted += 1
                    else:
                        await channel.delete_messages(batch, reason=f"Punish delete by {interaction.user.id} ({self.draft.reason})")
                        deleted += len(batch)
                except discord.HTTPException:
                    skipped += len(batch)

        for message in old:
            try:
                await message.delete(reason=f"Punish delete by {interaction.user.id} ({self.draft.reason})")
                deleted += 1
            except discord.HTTPException:
                skipped += 1

        result = (
            "✅ Incident cleanup finished.\n"
            f"Requested: **{requested}**\n"
            f"Found: **{found}**\n"
            f"Deleted: **{deleted}**\n"
            f"Skipped: **{skipped}**\n"
            f"Messages older than 14 days encountered: **{len(old)}**"
        )
        await self._log_action(
            interaction.guild,
            title="Punishment Hub • Message Cleanup",
            description=(
                f"Cleanup run by {interaction.user.mention} for <@{self.draft.target_id}> in {channel.mention}.\n"
                f"Reason: {self.draft.reason}\nRequested: {requested} | Found: {found} | Deleted: {deleted} | Skipped: {skipped} | OlderThan14d: {len(old)}"
            ),
            color=discord.Color.blurple(),
        )
        await self.disable_all(interaction, result=result)


class BanCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="ban", description="Open an easy interactive ban panel.")
    @app_commands.checks.has_permissions(ban_members=True)
    @app_commands.guild_only()
    async def ban(self, interaction: discord.Interaction) -> None:
        view = BanControlsView(moderator_id=interaction.user.id, bot=self.bot)
        await interaction.response.send_message(embed=view.build_embed(), view=view, ephemeral=True)

    @app_commands.command(name="punish", description="Open the central punishment hub for a user.")
    @app_commands.guild_only()
    async def punish(self, interaction: discord.Interaction, user: discord.Member) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        perms = interaction.user.guild_permissions if isinstance(interaction.user, discord.Member) else discord.Permissions.none()
        if not (perms.moderate_members or perms.kick_members or perms.ban_members or perms.manage_messages):
            await interaction.response.send_message(
                "You need moderation permissions (Timeout/Kick/Ban/Manage Messages) to use `/punish`.",
                ephemeral=True,
            )
            return
        view = PunishHubView(bot=self.bot, moderator_id=interaction.user.id, target=user)
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

    @punish.error
    async def punish_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.errors.MissingPermissions):
            message = "You don't have permission to run moderation punishments."
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)
            return
        raise error


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(BanCog(bot))
