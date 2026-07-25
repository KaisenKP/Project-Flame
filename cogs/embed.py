# What this file is: Staff-only Discord embed studio with live previews and private image storage.
# Last change: 2026-07-25 - Initial Embed Studio command.

from __future__ import annotations

import asyncio
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from pathlib import PurePath
from typing import Any
from urllib.parse import urlparse

import discord
from discord import app_commands
from discord.ext import commands

log = logging.getLogger(__name__)

STORAGE_CHANNEL_NAME = "image-storage"
STORAGE_TOPIC = (
    "PRIVATE FlameBot Embed Studio asset vault. Do not delete, bulk-clean, or rename messages here: "
    "published embeds may use these image URLs."
)
IMAGE_UPLOAD_TIMEOUT = 120
SESSION_TIMEOUT = 15 * 60
HEX_COLOR_RE = re.compile(r"^#?(?P<value>[0-9a-fA-F]{6})$")
IMAGE_EXTENSIONS = {".apng", ".gif", ".jpeg", ".jpg", ".png", ".webp"}


@dataclass(slots=True)
class EmbedDraft:
    session_id: str
    guild_id: int
    user_id: int
    source_channel_id: int
    title: str = ""
    body: str = ""
    footer: str = ""
    url: str = ""
    color: int = 0x5865F2
    styles: set[str] = field(default_factory=set)
    image_url: str = ""
    image_storage_message_id: int | None = None
    awaiting_image: bool = False
    notice: str = ""
    builder_message: discord.Message | None = field(default=None, repr=False)
    builder_view: "EmbedStudioView | None" = field(default=None, repr=False)


def _safe_inline(value: str, limit: int = 200) -> str:
    cleaned = " ".join(value.replace("`", "'").split())
    return cleaned[:limit] or "(none)"


def _is_http_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _parse_hex_color(value: str) -> int | None:
    match = HEX_COLOR_RE.fullmatch(value.strip())
    return int(match.group("value"), 16) if match else None


def _is_image_attachment(attachment: discord.Attachment) -> bool:
    content_type = (attachment.content_type or "").lower()
    if content_type.startswith("image/"):
        return True
    return PurePath(attachment.filename).suffix.lower() in IMAGE_EXTENSIONS


def _is_staff(member: discord.Member) -> bool:
    permissions = member.guild_permissions
    return permissions.administrator or permissions.manage_guild or permissions.manage_messages


class EmbedContentModal(discord.ui.Modal):
    def __init__(self, cog: "EmbedStudioCog", draft: EmbedDraft) -> None:
        super().__init__(title="Embed Studio · Content")
        self.cog = cog
        self.draft = draft
        self.title_input = discord.ui.TextInput(
            label="Title",
            placeholder="What should the embed title be?",
            default=draft.title[:256] or None,
            max_length=256,
            required=True,
        )
        self.body_input = discord.ui.TextInput(
            label="Body",
            placeholder="Write the embed body here...",
            default=draft.body[:4000] or None,
            style=discord.TextStyle.paragraph,
            max_length=4000,
            required=False,
        )
        self.footer_input = discord.ui.TextInput(
            label="Footer (optional)",
            placeholder="A small note at the bottom",
            default=draft.footer[:2048] or None,
            max_length=2048,
            required=False,
        )
        self.url_input = discord.ui.TextInput(
            label="Link (optional)",
            placeholder="https://example.com",
            default=draft.url[:2048] or None,
            max_length=2048,
            required=False,
        )
        for item in (self.title_input, self.body_input, self.footer_input, self.url_input):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not self.cog._session_is_valid(interaction, self.draft):
            await interaction.response.send_message("This embed studio session has expired. Run `/embed` again.", ephemeral=True)
            return

        url = str(self.url_input.value or "").strip()
        if url and not _is_http_url(url):
            await interaction.response.send_message("The link must be a complete `http://` or `https://` URL.", ephemeral=True)
            return

        self.draft.title = str(self.title_input.value or "").strip()
        self.draft.body = str(self.body_input.value or "").strip()
        self.draft.footer = str(self.footer_input.value or "").strip()
        self.draft.url = url
        self.draft.notice = "Content updated."
        await self.cog.refresh_preview(interaction, self.draft)


class EmbedColorModal(discord.ui.Modal):
    def __init__(self, cog: "EmbedStudioCog", draft: EmbedDraft) -> None:
        super().__init__(title="Embed Studio · Color")
        self.cog = cog
        self.draft = draft
        self.color_input = discord.ui.TextInput(
            label="Accent color",
            placeholder="#5865F2",
            default=f"#{draft.color:06X}",
            min_length=6,
            max_length=7,
            required=True,
        )
        self.add_item(self.color_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not self.cog._session_is_valid(interaction, self.draft):
            await interaction.response.send_message("This embed studio session has expired. Run `/embed` again.", ephemeral=True)
            return

        color = _parse_hex_color(str(self.color_input.value or ""))
        if color is None:
            await interaction.response.send_message("Use a six-digit hex color such as `#5865F2`.", ephemeral=True)
            return

        self.draft.color = color
        self.draft.notice = "Color updated."
        await self.cog.refresh_preview(interaction, self.draft)


class EmbedStudioView(discord.ui.View):
    def __init__(self, cog: "EmbedStudioCog", draft: EmbedDraft) -> None:
        super().__init__(timeout=SESSION_TIMEOUT)
        self.cog = cog
        self.draft = draft
        self._build_items()

    def _build_items(self) -> None:
        self.clear_items()
        self.add_item(self._button("Edit content", discord.ButtonStyle.primary, 0, self.edit_content))
        self.add_item(self._button(self._style_label("bold", "Bold"), self._style_button_style("bold"), 0, self.toggle_bold))
        self.add_item(self._button(self._style_label("italic", "Italic"), self._style_button_style("italic"), 0, self.toggle_italic))
        self.add_item(self._button(self._style_label("underline", "Underline"), self._style_button_style("underline"), 0, self.toggle_underline))
        self.add_item(self._button(self._style_label("strike", "Strike"), self._style_button_style("strike"), 0, self.toggle_strike))
        self.add_item(self._button("Color", discord.ButtonStyle.secondary, 1, self.edit_color))
        image_label = "Remove image" if self.draft.image_url else "Add image"
        image_style = discord.ButtonStyle.danger if self.draft.image_url else discord.ButtonStyle.secondary
        self.add_item(self._button(image_label, image_style, 1, self.image_action))
        self.add_item(self._button("Send embed", discord.ButtonStyle.success, 1, self.send_embed))
        self.add_item(self._button("Cancel", discord.ButtonStyle.danger, 1, self.cancel))

    def _button(self, label: str, style: discord.ButtonStyle, row: int, callback: Any) -> discord.ui.Button:
        button = discord.ui.Button(label=label, style=style, row=row)
        button.callback = callback
        return button

    def _style_label(self, key: str, label: str) -> str:
        return f"{label}: {'On' if key in self.draft.styles else 'Off'}"

    def _style_button_style(self, key: str) -> discord.ButtonStyle:
        return discord.ButtonStyle.primary if key in self.draft.styles else discord.ButtonStyle.secondary

    async def _guard(self, interaction: discord.Interaction) -> bool:
        return await self.cog.guard_session(interaction, self.draft)

    async def edit_content(self, interaction: discord.Interaction) -> None:
        if await self._guard(interaction):
            await interaction.response.send_modal(EmbedContentModal(self.cog, self.draft))

    async def edit_color(self, interaction: discord.Interaction) -> None:
        if await self._guard(interaction):
            await interaction.response.send_modal(EmbedColorModal(self.cog, self.draft))

    async def _toggle_style(self, interaction: discord.Interaction, key: str) -> None:
        if not await self._guard(interaction):
            return
        if key in self.draft.styles:
            self.draft.styles.remove(key)
        else:
            self.draft.styles.add(key)
        self.draft.notice = f"{key.title()} formatting {'enabled' if key in self.draft.styles else 'disabled'}."
        self._build_items()
        await interaction.response.edit_message(
            content=self.cog.builder_text(self.draft),
            embed=self.cog.build_embed(self.draft),
            view=self,
        )

    async def toggle_bold(self, interaction: discord.Interaction) -> None:
        await self._toggle_style(interaction, "bold")

    async def toggle_italic(self, interaction: discord.Interaction) -> None:
        await self._toggle_style(interaction, "italic")

    async def toggle_underline(self, interaction: discord.Interaction) -> None:
        await self._toggle_style(interaction, "underline")

    async def toggle_strike(self, interaction: discord.Interaction) -> None:
        await self._toggle_style(interaction, "strike")

    async def image_action(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        if self.draft.image_url:
            self.draft.image_url = ""
            self.draft.image_storage_message_id = None
            self.draft.notice = "Image removed from this draft. The stored asset was kept safely."
            self._build_items()
            await interaction.response.edit_message(
                content=self.cog.builder_text(self.draft),
                embed=self.cog.build_embed(self.draft),
                view=self,
            )
            return
        await self.cog.start_image_upload(interaction, self.draft)

    async def send_embed(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        if not self.draft.title:
            await interaction.response.send_message("Add a title before sending the embed.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            message = await self.cog.publish(self.draft)
        except (discord.Forbidden, discord.HTTPException) as exc:
            log.warning("Embed publish failed for session %s: %s", self.draft.session_id, exc)
            await interaction.followup.send("I could not send the embed here. Check my Send Messages and Embed Links permissions.", ephemeral=True)
            return
        except Exception:
            log.exception("Unexpected embed publish failure")
            await interaction.followup.send("The embed could not be published. Check the bot logs for details.", ephemeral=True)
            return

        self.cog.close_session(self.draft)
        try:
            await interaction.message.edit(content="✅ Embed sent. This private editor is closed.", embed=self.cog.build_embed(self.draft), view=None)
        except discord.HTTPException:
            pass
        await interaction.followup.send(f"Embed sent successfully in <#{message.channel.id}>.", ephemeral=True)

    async def cancel(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        self.cog.close_session(self.draft)
        await interaction.response.edit_message(content="Embed draft cancelled.", embed=None, view=None)

    async def on_timeout(self) -> None:
        self.cog.close_session(self.draft)
        for child in self.children:
            child.disabled = True
        if self.draft.builder_message is not None:
            try:
                await self.draft.builder_message.edit(content="Embed studio session expired. Run `/embed` to start a new one.", view=self)
            except discord.HTTPException:
                pass


class EmbedStudioCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.sessions: dict[str, EmbedDraft] = {}
        self.image_waiters: dict[tuple[int, int, int], tuple[str, float]] = {}

    @app_commands.command(name="embed", description="Staff: Open the live Embed Studio.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.guild_only()
    async def embed(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        if not _is_staff(interaction.user):
            await interaction.response.send_message("Only server staff can use `/embed`.", ephemeral=True)
            return

        draft = EmbedDraft(
            session_id=uuid.uuid4().hex,
            guild_id=interaction.guild.id,
            user_id=interaction.user.id,
            source_channel_id=interaction.channel_id,
        )
        self.sessions[draft.session_id] = draft
        await interaction.response.send_modal(EmbedContentModal(self, draft))

    async def cog_unload(self) -> None:
        for draft in list(self.sessions.values()):
            self.close_session(draft)

    def _session_is_valid(self, interaction: discord.Interaction, draft: EmbedDraft) -> bool:
        return (
            self.sessions.get(draft.session_id) is draft
            and interaction.guild is not None
            and interaction.guild.id == draft.guild_id
            and interaction.user.id == draft.user_id
            and isinstance(interaction.user, discord.Member)
            and _is_staff(interaction.user)
        )

    async def guard_session(self, interaction: discord.Interaction, draft: EmbedDraft) -> bool:
        if self._session_is_valid(interaction, draft):
            return True
        message = "This embed studio session is no longer available. Run `/embed` again."
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
        return False

    def close_session(self, draft: EmbedDraft) -> None:
        self.sessions.pop(draft.session_id, None)
        key = (draft.guild_id, draft.user_id, draft.source_channel_id)
        waiter = self.image_waiters.get(key)
        if waiter and waiter[0] == draft.session_id:
            self.image_waiters.pop(key, None)
        draft.awaiting_image = False
        if draft.builder_view is not None:
            draft.builder_view.stop()

    def _format_body(self, draft: EmbedDraft) -> str:
        body = draft.body
        if not body:
            return body
        if "bold" in draft.styles:
            body = f"**{body}**"
        if "italic" in draft.styles:
            body = f"*{body}*"
        if "underline" in draft.styles:
            body = f"__{body}__"
        if "strike" in draft.styles:
            body = f"~~{body}~~"
        return body

    def build_embed(self, draft: EmbedDraft) -> discord.Embed:
        embed = discord.Embed(
            title=draft.title or None,
            description=self._format_body(draft) or None,
            url=draft.url or None,
            color=discord.Color(draft.color),
        )
        if draft.footer:
            embed.set_footer(text=draft.footer)
        if draft.image_url:
            embed.set_image(url=draft.image_url)
        return embed

    def builder_text(self, draft: EmbedDraft) -> str:
        image_state = "attached and stored in 🔒 image-storage" if draft.image_url else "none"
        status = f"\n> {draft.notice}" if draft.notice else ""
        return (
            "**Embed Studio** · staff-only draft\n"
            "Your preview is always shown below. Formatting toggles apply to the embed body.\n"
            f"Image: **{image_state}** · Session expires after 15 minutes.{status}"
        )

    async def refresh_preview(self, interaction: discord.Interaction, draft: EmbedDraft) -> None:
        if draft.builder_message is None:
            view = EmbedStudioView(self, draft)
            draft.builder_view = view
            await interaction.response.send_message(
                content=self.builder_text(draft),
                embed=self.build_embed(draft),
                view=view,
                ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            draft.builder_message = await interaction.original_response()
            return

        if draft.builder_view is not None:
            draft.builder_view.stop()
        view = EmbedStudioView(self, draft)
        draft.builder_view = view
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            await draft.builder_message.edit(
                content=self.builder_text(draft),
                embed=self.build_embed(draft),
                view=view,
            )
            await interaction.followup.send("Preview updated.", ephemeral=True)
        except discord.HTTPException:
            await interaction.followup.send("I could not refresh the preview. Run `/embed` again if it remains stuck.", ephemeral=True)

    async def start_image_upload(self, interaction: discord.Interaction, draft: EmbedDraft) -> None:
        key = (draft.guild_id, draft.user_id, draft.source_channel_id)
        draft.awaiting_image = True
        draft.notice = "Waiting for one image attachment in this channel..."
        self.image_waiters[key] = (draft.session_id, time.monotonic() + IMAGE_UPLOAD_TIMEOUT)
        await interaction.response.send_message(
            "Attach one PNG, JPG, GIF, or WebP image as your next message in this channel. "
            "I will move it into the private `image-storage` channel and use the stored asset in the preview. "
            "This upload window lasts two minutes.",
            ephemeral=True,
        )
        asyncio.create_task(self._expire_image_waiter(key, draft.session_id))

    async def _expire_image_waiter(self, key: tuple[int, int, int], session_id: str) -> None:
        await asyncio.sleep(IMAGE_UPLOAD_TIMEOUT)
        waiter = self.image_waiters.get(key)
        if waiter and waiter[0] == session_id:
            self.image_waiters.pop(key, None)
            draft = self.sessions.get(session_id)
            if draft is not None:
                draft.awaiting_image = False
                draft.notice = "Image upload window expired. Press Add image to try again."
                await self._edit_preview_from_event(draft)

    async def _edit_preview_from_event(self, draft: EmbedDraft) -> None:
        if draft.builder_message is None or draft.builder_view is None:
            return
        try:
            await draft.builder_message.edit(content=self.builder_text(draft), embed=self.build_embed(draft), view=draft.builder_view)
        except discord.HTTPException:
            log.debug("Could not update ephemeral embed preview for %s", draft.session_id, exc_info=True)

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or message.guild is None:
            return
        key = (message.guild.id, message.author.id, message.channel.id)
        waiter = self.image_waiters.get(key)
        if waiter is None:
            return
        session_id, expires_at = waiter
        draft = self.sessions.get(session_id)
        if draft is None or expires_at < time.monotonic():
            self.image_waiters.pop(key, None)
            return
        if not message.attachments:
            return
        attachment = next((item for item in message.attachments if _is_image_attachment(item)), None)
        if attachment is None:
            try:
                await message.channel.send("That is not a supported image. Attach a PNG, JPG, GIF, or WebP file.", delete_after=8)
            except discord.HTTPException:
                pass
            return

        self.image_waiters.pop(key, None)
        draft.awaiting_image = False
        try:
            storage_channel = await self.ensure_image_storage_channel(message.guild)
            stored_file = await attachment.to_file(use_cached=True)
            asset_id = uuid.uuid4().hex[:12]
            storage_message = await storage_channel.send(
                content=(
                    "🖼️ **Embed Studio image asset**\n"
                    f"Asset ID: `{asset_id}`\n"
                    f"Original file: `{_safe_inline(attachment.filename, 120)}`\n"
                    f"Uploaded by: <@{message.author.id}> (`{message.author.id}`)\n"
                    f"Source channel: <#{message.channel.id}>\n\n"
                    "⚠️ **Do not delete this message or its attachment.** Published embeds may depend on this stored image URL."
                ),
                file=stored_file,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            if not storage_message.attachments:
                raise RuntimeError("Discord did not return the stored image attachment.")
            draft.image_url = storage_message.attachments[0].url
            draft.image_storage_message_id = storage_message.id
            draft.notice = f"Image stored safely as `{asset_id}`."
            try:
                await message.delete()
            except discord.HTTPException:
                pass
            try:
                await message.channel.send("✅ Image stored privately and added to the preview.", delete_after=8)
            except discord.HTTPException:
                pass
            await self._edit_preview_from_event(draft)
        except (discord.Forbidden, discord.HTTPException) as exc:
            draft.notice = "I could not store that image. Check that I can manage channels and attach files."
            log.warning("Image storage failed for guild %s: %s", message.guild.id, exc)
            await self._edit_preview_from_event(draft)
        except Exception:
            draft.notice = "I could not store that image. Check the bot logs for details."
            log.exception("Unexpected image storage failure for guild %s", message.guild.id)
            await self._edit_preview_from_event(draft)

    def _storage_overwrites(self, guild: discord.Guild) -> dict[discord.abc.Snowflake, discord.PermissionOverwrite]:
        read_only = discord.PermissionOverwrite(
            view_channel=True,
            read_message_history=True,
            send_messages=False,
            add_reactions=False,
        )
        overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(
                view_channel=False,
                send_messages=False,
                read_message_history=False,
            )
        }
        for role in guild.roles:
            permissions = role.permissions
            if role.is_default() or role.managed:
                continue
            if permissions.administrator or permissions.manage_guild or permissions.manage_messages:
                overwrites[role] = read_only

        me = guild.me
        if me is not None:
            overwrites[me] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True,
                manage_messages=True,
            )
        return overwrites

    async def ensure_image_storage_channel(self, guild: discord.Guild) -> discord.TextChannel:
        channel = next((item for item in guild.text_channels if item.name == STORAGE_CHANNEL_NAME), None)
        overwrites = self._storage_overwrites(guild)
        if channel is None:
            try:
                channel = await guild.create_text_channel(
                    STORAGE_CHANNEL_NAME,
                    topic=STORAGE_TOPIC,
                    overwrites=overwrites,
                    reason="Create private Embed Studio image storage",
                )
            except (discord.Forbidden, discord.HTTPException) as exc:
                raise RuntimeError("The bot could not create the private image-storage channel.") from exc
            await self._seed_storage_channel(channel)
            return channel

        try:
            channel = await channel.edit(
                topic=STORAGE_TOPIC,
                overwrites=overwrites,
                reason="Keep Embed Studio image storage private and documented",
            )
        except (discord.Forbidden, discord.HTTPException) as exc:
            raise RuntimeError("The bot could not secure the existing image-storage channel.") from exc

        if STORAGE_TOPIC not in (channel.topic or ""):
            await self._seed_storage_channel(channel)
        return channel

    async def _seed_storage_channel(self, channel: discord.TextChannel) -> None:
        messages = (
            "🔒 **PRIVATE IMAGE STORAGE — DO NOT DELETE ASSETS**\n"
            "This channel stores the source attachments used by published `/embed` messages. "
            "Deleting an image message can break an embed that references its CDN URL.",
            "📚 **How storage works**\n"
            "Staff upload an image through Embed Studio; FlameBot copies it here, records an asset ID, "
            "and places the stored attachment URL in the draft preview. The original upload is removed when possible.",
            "🛡️ **Retention rule**\n"
            "Do not bulk-delete, prune, archive, or rename this channel. If an asset needs to be replaced, update the embed instead of deleting its stored message.",
        )
        for content in messages:
            try:
                stored = await channel.send(content, allowed_mentions=discord.AllowedMentions.none())
                if content.startswith("🔒"):
                    try:
                        await stored.pin(reason="Keep Embed Studio image-storage warning visible")
                    except discord.HTTPException:
                        log.debug("Could not pin image-storage warning", exc_info=True)
            except discord.HTTPException as exc:
                raise RuntimeError("The bot could not write the image-storage instructions.") from exc

    async def publish(self, draft: EmbedDraft) -> discord.Message:
        channel = self.bot.get_channel(draft.source_channel_id)
        if channel is None:
            channel = await self.bot.fetch_channel(draft.source_channel_id)
        if not hasattr(channel, "send"):
            raise RuntimeError("The original channel is no longer available.")
        return await channel.send(
            embed=self.build_embed(draft),
            allowed_mentions=discord.AllowedMentions.none(),
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(EmbedStudioCog(bot))

