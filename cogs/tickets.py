from __future__ import annotations

import asyncio
import html
import io
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import text

from services.db import sessions


DEFAULT_OPEN_CATEGORY_ID = 1491451868407271545
DEFAULT_ARCHIVE_CATEGORY_ID = 1495166803486310642
DEFAULT_PANEL_CHANNEL_ID = 1491451689528594472
DEFAULT_PANEL_MESSAGE_ID = 1481820566015971500
TARGET_TICKET_GUILD_ID = 1479503568095412325
TARGET_SUPPORT_PANEL_CHANNEL_ID = 1491451689528594472

DEFAULT_STAFF_ROLE_ID = 1476620136114028544
DEFAULT_HEAD_MOD_ROLE_ID = 1467394498878373979
CLOSED_TICKET_TRANSCRIPTS_CHANNEL_ID = 1490729352072269975
DEFAULT_CLOSED_TRANSCRIPTS_CHANNEL_NAME = "ticket-transcripts"

DEFAULT_PANEL_TITLE = "Support Center"
DEFAULT_PANEL_DESCRIPTION = (
    "Need help? Pick the option that fits your issue best.\n"
    "A private ticket will be opened for you and our staff will handle it as fast as possible."
)
DEFAULT_PANEL_IMAGE_URL = (
    "https://base44.app/api/apps/69acbce85ee689a96f4dd42f/files/public/"
    "69acbce85ee689a96f4dd42f/2ad3540fd_tickets.png"
)
DEFAULT_BLUE = 0x2F6BFF
PUBLIC_TICKET_CLAIM_MESSAGE = "✅ This ticket is now being handled by {user_mention}."
PRIVATE_TICKET_OVERRIDE_DENIED_MESSAGE = (
    "This ticket is already assigned to {claimer_mention}. "
    "Only Head Mod can override the current handler."
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ts() -> int:
    return int(_utc_now().timestamp())


def _clean_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9_\-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:32] or "general"


def _clean_channel_fragment(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9\-]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:70] or "ticket"


def _safe_json_load(s: str | None, default: Any) -> Any:
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _style_from_name(name: str | None) -> discord.ButtonStyle:
    n = (name or "").strip().lower()
    if n == "primary":
        return discord.ButtonStyle.primary
    if n == "success":
        return discord.ButtonStyle.success
    if n == "danger":
        return discord.ButtonStyle.danger
    return discord.ButtonStyle.secondary


def _style_to_name(style_value: int) -> str:
    mapping = {
        int(discord.ButtonStyle.primary): "primary",
        int(discord.ButtonStyle.secondary): "secondary",
        int(discord.ButtonStyle.success): "success",
        int(discord.ButtonStyle.danger): "danger",
    }
    return mapping.get(int(style_value), "secondary")


def _fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return "Unknown"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return f"<t:{int(dt.timestamp())}:f>"


def _chunk_lines(lines: list[str], max_len: int = 1000) -> list[str]:
    out: list[str] = []
    buf = ""
    for line in lines:
        if len(buf) + len(line) + 1 > max_len:
            if buf:
                out.append(buf)
            buf = line
        else:
            buf = f"{buf}\n{line}" if buf else line
    if buf:
        out.append(buf)
    return out or ["None"]


def _member_has_role(member: discord.Member, role_id: int | None) -> bool:
    if not role_id:
        return False
    return any(r.id == int(role_id) for r in member.roles)


@dataclass
class TicketConfig:
    guild_id: int
    category_id: int | None
    archive_category_id: int | None
    log_channel_id: int | None
    support_role_id: int | None
    admin_role_id: int | None
    head_mod_role_id: int | None
    panel_channel_id: int | None
    panel_message_id: int | None
    transcript_channel_id: int | None
    panel_title: str
    panel_description: str
    panel_image_url: str | None
    max_open_per_user: int
    transcript_enabled: bool
    close_cooldown_s: int


@dataclass
class TicketTypeRow:
    id: int
    guild_id: int
    type_key: str
    label: str
    emoji: str | None
    button_style: int
    category_id: int | None
    staff_role_id: int | None
    questions_json: str | None
    sort_order: int
    enabled: bool


@dataclass
class TicketRow:
    id: int
    guild_id: int
    channel_id: int
    creator_id: int
    claimed_by_id: int | None
    type_key: str
    type_label: str
    status: str
    created_at: datetime | None
    closed_at: datetime | None
    close_reason: str | None
    intake_answers_json: str | None


class TicketOpenModal(discord.ui.Modal, title="Open Ticket"):
    def __init__(
        self,
        cog: "TicketsCog",
        ticket_type: TicketTypeRow,
        *,
        prefilled_answers: list[dict[str, str]] | None = None,
        skip_first_question: bool = False,
        allow_empty_form: bool = False,
    ):
        super().__init__(timeout=300)
        self.cog = cog
        self.ticket_type = ticket_type
        self.prefilled_answers = list(prefilled_answers or [])
        self.qdefs = _safe_json_load(ticket_type.questions_json, [])
        if skip_first_question and self.qdefs:
            self.qdefs = self.qdefs[1:]

        normalized: list[dict[str, Any]] = []
        for q in self.qdefs[:5]:
            if not isinstance(q, dict):
                continue
            normalized.append(
                {
                    "label": str(q.get("label") or "Question")[:45],
                    "placeholder": str(q.get("placeholder") or "")[:100],
                    "required": bool(q.get("required", True)),
                    "style": str(q.get("style") or "paragraph").lower(),
                    "max_length": max(1, min(int(q.get("max_length", 400)), 4000)),
                }
            )

        if not normalized and not allow_empty_form:
            normalized = [
                {
                    "label": "What do you need help with?",
                    "placeholder": "Describe the issue clearly",
                    "required": True,
                    "style": "paragraph",
                    "max_length": 1200,
                }
            ]

        self._inputs: list[discord.ui.TextInput] = []
        for q in normalized:
            ti = discord.ui.TextInput(
                label=q["label"],
                placeholder=q["placeholder"],
                required=q["required"],
                style=discord.TextStyle.paragraph if q["style"] == "paragraph" else discord.TextStyle.short,
                max_length=q["max_length"],
            )
            self._inputs.append(ti)
            self.add_item(ti)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        answers = list(self.prefilled_answers)
        answers.extend({"label": ti.label, "value": ti.value} for ti in self._inputs)
        await self.cog.create_ticket_from_modal(
            interaction=interaction,
            ticket_type=self.ticket_type,
            answers=answers,
        )


class ReportUserSelectView(discord.ui.View):
    def __init__(self, cog: "TicketsCog", opener_id: int, ticket_type: TicketTypeRow):
        super().__init__(timeout=180)
        self.cog = cog
        self.opener_id = int(opener_id)
        self.ticket_type = ticket_type

        user_select = discord.ui.UserSelect(
            placeholder="Pick the user you want to report",
            min_values=1,
            max_values=1,
        )

        async def user_select_callback(interaction: discord.Interaction) -> None:
            if interaction.user is None or int(interaction.user.id) != self.opener_id:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Only the user opening this report can select a target.", ephemeral=True)
                return

            selected = user_select.values[0]
            if int(selected.id) == self.opener_id:
                if not interaction.response.is_done():
                    await interaction.response.send_message("You can only report another user.", ephemeral=True)
                return

            prefilled_answers = [
                {
                    "label": "Who are you reporting?",
                    "value": f"{selected.mention} (`{selected.id}`)",
                }
            ]
            await interaction.response.send_modal(
                TicketOpenModal(
                    self.cog,
                    self.ticket_type,
                    prefilled_answers=prefilled_answers,
                    skip_first_question=True,
                    allow_empty_form=True,
                )
            )
            self.stop()

        user_select.callback = user_select_callback
        self.add_item(user_select)


class TicketPanelView(discord.ui.View):
    def __init__(self, cog: "TicketsCog", guild_id: int, items: list[TicketTypeRow]):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id

        for item in items[:25]:
            custom_id = f"ticket_open:{guild_id}:{item.type_key}"
            button = discord.ui.Button(
                label=item.label[:80],
                style=_style_from_name(_style_to_name(item.button_style)),
                emoji=item.emoji or None,
                custom_id=custom_id,
            )
            button.callback = self._make_open_callback(item)
            self.add_item(button)

    def _make_open_callback(self, item: TicketTypeRow):
        async def callback(interaction: discord.Interaction) -> None:
            if interaction.guild is None or interaction.user is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Server only.", ephemeral=True)
                return

            current = await self.cog.fetch_ticket_type(interaction.guild.id, item.type_key)
            if current is None or not current.enabled:
                if not interaction.response.is_done():
                    await interaction.response.send_message("That ticket option is disabled right now.", ephemeral=True)
                return

            if current.type_key == "report":
                embed = discord.Embed(
                    title="Report Ticket",
                    description="Pick the user you want to report using the selector below.",
                    color=discord.Color.red(),
                    timestamp=_utc_now(),
                )
                embed.set_footer(text="After selecting a user, you'll fill out the report details.")
                await interaction.response.send_message(
                    embed=embed,
                    view=ReportUserSelectView(self.cog, interaction.user.id, current),
                    ephemeral=True,
                )
                return

            await interaction.response.send_modal(TicketOpenModal(self.cog, current))

        return callback


class TicketChannelView(discord.ui.View):
    def __init__(self, cog: "TicketsCog", *, is_closed: bool):
        super().__init__(timeout=None)
        self.cog = cog
        self.is_closed = is_closed

        if not is_closed:
            claim_btn = discord.ui.Button(
                label="Claim",
                style=discord.ButtonStyle.primary,
                custom_id="ticket_claim_open",
            )
            unclaim_btn = discord.ui.Button(
                label="Unclaim",
                style=discord.ButtonStyle.secondary,
                custom_id="ticket_unclaim_open",
            )
            close_btn = discord.ui.Button(
                label="Close",
                style=discord.ButtonStyle.danger,
                custom_id="ticket_close_open",
            )
            reopen_btn = discord.ui.Button(
                label="Reopen",
                style=discord.ButtonStyle.success,
                custom_id="ticket_reopen_open",
                disabled=True,
            )

            claim_btn.callback = self._claim_callback
            unclaim_btn.callback = self._unclaim_callback
            close_btn.callback = self._close_callback
            reopen_btn.callback = self._reopen_callback

            self.add_item(claim_btn)
            self.add_item(unclaim_btn)
            self.add_item(close_btn)
            self.add_item(reopen_btn)
        else:
            reopen_btn = discord.ui.Button(
                label="Reopen",
                style=discord.ButtonStyle.success,
                custom_id="ticket_reopen_closed",
            )
            transcript_btn = discord.ui.Button(
                label="Save Transcript",
                style=discord.ButtonStyle.secondary,
                custom_id="ticket_save_transcript_closed",
            )

            reopen_btn.callback = self._reopen_callback
            transcript_btn.callback = self._transcript_callback

            self.add_item(reopen_btn)
            self.add_item(transcript_btn)

    async def _claim_callback(self, interaction: discord.Interaction) -> None:
        await self.cog.handle_claim(interaction)

    async def _close_callback(self, interaction: discord.Interaction) -> None:
        await self.cog.handle_close(interaction)

    async def _unclaim_callback(self, interaction: discord.Interaction) -> None:
        await self.cog.handle_unclaim(interaction)

    async def _reopen_callback(self, interaction: discord.Interaction) -> None:
        await self.cog.handle_reopen(interaction)

    async def _transcript_callback(self, interaction: discord.Interaction) -> None:
        await self.cog.handle_transcript(interaction)


class LegacyClosedTranscriptView(discord.ui.View):
    """Compatibility view for already-sent closed-ticket messages."""

    def __init__(self, cog: "TicketsCog"):
        super().__init__(timeout=None)
        self.cog = cog
        transcript_btn = discord.ui.Button(
            label="Transcript",
            style=discord.ButtonStyle.secondary,
            custom_id="ticket_transcript_closed",
        )
        transcript_btn.callback = self._transcript_callback
        self.add_item(transcript_btn)

    async def _transcript_callback(self, interaction: discord.Interaction) -> None:
        await self.cog.handle_transcript(interaction)


class CloseReasonModal(discord.ui.Modal, title="Close Ticket"):
    reason = discord.ui.TextInput(
        label="Reason",
        placeholder="Optional close reason",
        required=False,
        style=discord.TextStyle.paragraph,
        max_length=500,
    )

    def __init__(self, cog: "TicketsCog"):
        super().__init__(timeout=300)
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.close_ticket_from_interaction(interaction, str(self.reason.value or "").strip())


class TicketsCog(commands.Cog):
    TABLE_CONFIG = "ticket_config"
    TABLE_TYPES = "ticket_types"
    TABLE_TICKETS = "ticket_tickets"
    TABLE_MEMBERS = "ticket_members"

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self._lock = asyncio.Lock()
        self._booted = False
        self._base_views_registered = False
        self._panel_view_signatures: dict[int, tuple[str, ...]] = {}

    def _register_base_persistent_views(self) -> None:
        if self._base_views_registered:
            return
        for view in (
            TicketChannelView(self, is_closed=False),
            TicketChannelView(self, is_closed=True),
            LegacyClosedTranscriptView(self),
        ):
            try:
                self.bot.add_view(view)
            except ValueError:
                continue
        self._base_views_registered = True

    def _register_panel_view(self, guild_id: int, items: list[TicketTypeRow]) -> None:
        gid = int(guild_id)
        if not items:
            return
        signature = tuple(item.type_key for item in items[:25])
        if self._panel_view_signatures.get(gid) == signature:
            return
        try:
            self.bot.add_view(TicketPanelView(self, gid, items))
        except ValueError:
            pass
        self._panel_view_signatures[gid] = signature

    async def cog_load(self) -> None:
        await self._ensure_tables()
        await self._ensure_new_columns()
        self._register_base_persistent_views()
        await self._restore_panel_views()

    @commands.Cog.listener("on_ready")
    async def _on_ready(self) -> None:
        if self._booted:
            return
        self._booted = True
        await self._ensure_tables()
        await self._ensure_new_columns()
        self._register_base_persistent_views()
        await self._auto_seed_defaults_for_all_guilds()
        await self._restore_panel_views()
        await self._ensure_default_panel_messages()

    async def _ensure_tables(self) -> None:
        sql_config = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_CONFIG} (
            guild_id BIGINT NOT NULL,
            category_id BIGINT NULL,
            archive_category_id BIGINT NULL,
            log_channel_id BIGINT NULL,
            support_role_id BIGINT NULL,
            admin_role_id BIGINT NULL,
            panel_channel_id BIGINT NULL,
            panel_message_id BIGINT NULL,
            transcript_channel_id BIGINT NULL,
            panel_title VARCHAR(150) NOT NULL,
            panel_description TEXT NOT NULL,
            max_open_per_user INT NOT NULL DEFAULT 1,
            transcript_enabled TINYINT(1) NOT NULL DEFAULT 1,
            close_cooldown_s INT NOT NULL DEFAULT 5,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id)
        );
        """

        sql_types = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_TYPES} (
            id BIGINT NOT NULL AUTO_INCREMENT,
            guild_id BIGINT NOT NULL,
            type_key VARCHAR(32) NOT NULL,
            label VARCHAR(80) NOT NULL,
            emoji VARCHAR(32) NULL,
            button_style INT NOT NULL DEFAULT 2,
            category_id BIGINT NULL,
            staff_role_id BIGINT NULL,
            questions_json LONGTEXT NULL,
            sort_order INT NOT NULL DEFAULT 0,
            enabled TINYINT(1) NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uq_ticket_type_guild_key (guild_id, type_key),
            KEY ix_ticket_type_guild (guild_id),
            KEY ix_ticket_type_sort (guild_id, sort_order)
        );
        """

        sql_tickets = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_TICKETS} (
            id BIGINT NOT NULL AUTO_INCREMENT,
            guild_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            creator_id BIGINT NOT NULL,
            claimed_by_id BIGINT NULL,
            type_key VARCHAR(32) NOT NULL,
            type_label VARCHAR(80) NOT NULL,
            status VARCHAR(16) NOT NULL DEFAULT 'open',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            closed_at TIMESTAMP NULL DEFAULT NULL,
            close_reason TEXT NULL,
            intake_answers_json LONGTEXT NULL,
            panel_message_id BIGINT NULL,
            initial_message_id BIGINT NULL,
            PRIMARY KEY (id),
            UNIQUE KEY uq_ticket_channel (channel_id),
            KEY ix_ticket_guild_status (guild_id, status),
            KEY ix_ticket_creator_status (guild_id, creator_id, status),
            KEY ix_ticket_channel (channel_id)
        );
        """

        sql_members = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_MEMBERS} (
            ticket_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            added_by_id BIGINT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticket_id, user_id),
            KEY ix_ticket_members_user (user_id)
        );
        """

        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(sql_config))
                await session.execute(text(sql_types))
                await session.execute(text(sql_tickets))
                await session.execute(text(sql_members))

    async def _ensure_new_columns(self) -> None:
        statements = [
            f"ALTER TABLE {self.TABLE_CONFIG} ADD COLUMN head_mod_role_id BIGINT NULL",
            f"ALTER TABLE {self.TABLE_CONFIG} ADD COLUMN panel_image_url TEXT NULL",
            f"ALTER TABLE {self.TABLE_CONFIG} ADD COLUMN transcript_channel_id BIGINT NULL",
            f"ALTER TABLE {self.TABLE_TICKETS} ADD COLUMN intake_answers_json LONGTEXT NULL",
        ]
        async with self.sessionmaker() as session:
            async with session.begin():
                for stmt in statements:
                    try:
                        await session.execute(text(stmt))
                    except Exception:
                        pass

    async def _ensure_category(
        self,
        guild: discord.Guild,
        *,
        preferred_id: int | None,
        fallback_name: str,
    ) -> int | None:
        if preferred_id:
            existing = guild.get_channel(int(preferred_id))
            if isinstance(existing, discord.CategoryChannel):
                return int(existing.id)
            try:
                fetched = await self.bot.fetch_channel(int(preferred_id))
                if isinstance(fetched, discord.CategoryChannel) and fetched.guild.id == guild.id:
                    return int(fetched.id)
            except Exception:
                pass

        by_name = discord.utils.get(guild.categories, name=fallback_name)
        if by_name:
            return int(by_name.id)

        try:
            created = await guild.create_category(fallback_name, reason="Ticket system bootstrap")
            return int(created.id)
        except Exception:
            return None

    async def _ensure_transcript_channel(
        self,
        guild: discord.Guild,
        *,
        preferred_id: int | None,
        archive_category_id: int | None,
    ) -> int | None:
        if preferred_id:
            existing = guild.get_channel(int(preferred_id))
            if isinstance(existing, discord.TextChannel) and existing.guild.id == guild.id:
                return int(existing.id)
            try:
                fetched = await self.bot.fetch_channel(int(preferred_id))
                if isinstance(fetched, discord.TextChannel) and fetched.guild.id == guild.id:
                    return int(fetched.id)
            except Exception:
                pass

        if int(guild.id) == TARGET_TICKET_GUILD_ID:
            legacy = guild.get_channel(int(CLOSED_TICKET_TRANSCRIPTS_CHANNEL_ID))
            if isinstance(legacy, discord.TextChannel):
                return int(legacy.id)
            try:
                fetched = await self.bot.fetch_channel(int(CLOSED_TICKET_TRANSCRIPTS_CHANNEL_ID))
                if isinstance(fetched, discord.TextChannel) and fetched.guild.id == guild.id:
                    return int(fetched.id)
            except Exception:
                pass

        archive_category = None
        if archive_category_id:
            maybe_archive = guild.get_channel(int(archive_category_id))
            if isinstance(maybe_archive, discord.CategoryChannel):
                archive_category = maybe_archive

        for channel in guild.text_channels:
            if channel.category_id == (archive_category.id if archive_category else None) and channel.name == DEFAULT_CLOSED_TRANSCRIPTS_CHANNEL_NAME:
                return int(channel.id)

        overwrites = None
        if archive_category is not None:
            overwrites = archive_category.overwrites

        try:
            created = await guild.create_text_channel(
                DEFAULT_CLOSED_TRANSCRIPTS_CHANNEL_NAME,
                category=archive_category,
                overwrites=overwrites,
                reason="Ticket system bootstrap: transcript storage channel",
            )
            return int(created.id)
        except Exception:
            return None

    async def _auto_seed_defaults_for_all_guilds(self) -> None:
        for guild in self.bot.guilds:
            try:
                await self._auto_seed_defaults_for_guild(guild)
            except Exception:
                pass

    async def _auto_seed_defaults_for_guild(self, guild: discord.Guild) -> None:
        cfg = await self.fetch_config(guild.id)

        desired_panel_channel_id = (
            TARGET_SUPPORT_PANEL_CHANNEL_ID
            if int(guild.id) == TARGET_TICKET_GUILD_ID
            else (cfg.panel_channel_id if cfg else None) or DEFAULT_PANEL_CHANNEL_ID
        )

        open_category_id = await self._ensure_category(
            guild,
            preferred_id=(cfg.category_id if cfg else None) or DEFAULT_OPEN_CATEGORY_ID,
            fallback_name="Open Tickets",
        )
        archive_category_id = await self._ensure_category(
            guild,
            preferred_id=(cfg.archive_category_id if cfg else None) or DEFAULT_ARCHIVE_CATEGORY_ID,
            fallback_name="Closed Tickets",
        )
        transcript_channel_id = await self._ensure_transcript_channel(
            guild,
            preferred_id=cfg.transcript_channel_id if cfg else None,
            archive_category_id=archive_category_id or (cfg.archive_category_id if cfg else None),
        )

        if cfg is None:
            await self.upsert_config(
                guild.id,
                category_id=open_category_id or DEFAULT_OPEN_CATEGORY_ID,
                archive_category_id=archive_category_id or DEFAULT_ARCHIVE_CATEGORY_ID,
                log_channel_id=None,
                support_role_id=DEFAULT_STAFF_ROLE_ID,
                admin_role_id=DEFAULT_STAFF_ROLE_ID,
                head_mod_role_id=DEFAULT_HEAD_MOD_ROLE_ID,
                panel_channel_id=desired_panel_channel_id,
                panel_message_id=DEFAULT_PANEL_MESSAGE_ID,
                transcript_channel_id=transcript_channel_id,
                panel_title=DEFAULT_PANEL_TITLE,
                panel_description=DEFAULT_PANEL_DESCRIPTION,
                panel_image_url=DEFAULT_PANEL_IMAGE_URL,
                max_open_per_user=1,
                transcript_enabled=True,
                close_cooldown_s=5,
            )
        else:
            await self.upsert_config(
                guild.id,
                category_id=open_category_id or cfg.category_id or DEFAULT_OPEN_CATEGORY_ID,
                archive_category_id=archive_category_id or cfg.archive_category_id or DEFAULT_ARCHIVE_CATEGORY_ID,
                log_channel_id=cfg.log_channel_id,
                support_role_id=cfg.support_role_id or DEFAULT_STAFF_ROLE_ID,
                admin_role_id=cfg.admin_role_id or DEFAULT_STAFF_ROLE_ID,
                head_mod_role_id=cfg.head_mod_role_id or DEFAULT_HEAD_MOD_ROLE_ID,
                panel_channel_id=desired_panel_channel_id,
                panel_message_id=cfg.panel_message_id or DEFAULT_PANEL_MESSAGE_ID,
                transcript_channel_id=transcript_channel_id or cfg.transcript_channel_id,
                panel_title=cfg.panel_title or DEFAULT_PANEL_TITLE,
                panel_description=cfg.panel_description or DEFAULT_PANEL_DESCRIPTION,
                panel_image_url=cfg.panel_image_url or DEFAULT_PANEL_IMAGE_URL,
                max_open_per_user=cfg.max_open_per_user or 1,
                transcript_enabled=cfg.transcript_enabled,
                close_cooldown_s=cfg.close_cooldown_s or 5,
            )

        await self._ensure_default_types(guild.id)

    async def _ensure_default_types(self, guild_id: int) -> None:
        defaults = [
            {
                "type_key": "general",
                "label": "General",
                "emoji": "💬",
                "button_style": int(discord.ButtonStyle.primary),
                "questions": [
                    {
                        "label": "What do you need help with?",
                        "placeholder": "Ask your question or share your suggestion",
                        "required": True,
                        "style": "paragraph",
                        "max_length": 1200,
                    }
                ],
                "sort_order": 0,
            },
            {
                "type_key": "report",
                "label": "Report",
                "emoji": "🚨",
                "button_style": int(discord.ButtonStyle.danger),
                "questions": [
                    {
                        "label": "Who are you reporting?",
                        "placeholder": "User or staff name / ID",
                        "required": True,
                        "style": "short",
                        "max_length": 200,
                    },
                    {
                        "label": "What happened?",
                        "placeholder": "Explain clearly what happened",
                        "required": True,
                        "style": "paragraph",
                        "max_length": 1500,
                    }
                ],
                "sort_order": 1,
            },
            {
                "type_key": "application",
                "label": "Application",
                "emoji": "📝",
                "button_style": int(discord.ButtonStyle.success),
                "questions": [
                    {
                        "label": "Why do you want staff?",
                        "placeholder": "Tell us why you'd be a good fit",
                        "required": True,
                        "style": "paragraph",
                        "max_length": 1200,
                    },
                    {
                        "label": "What experience do you have?",
                        "placeholder": "Moderation / community experience",
                        "required": True,
                        "style": "paragraph",
                        "max_length": 1200,
                    }
                ],
                "sort_order": 2,
            },
            {
                "type_key": "donate",
                "label": "Donate",
                "emoji": "💙",
                "button_style": int(discord.ButtonStyle.primary),
                "questions": [
                    {
                        "label": "What do you need help with?",
                        "placeholder": "Billing or donation issue",
                        "required": True,
                        "style": "paragraph",
                        "max_length": 1200,
                    }
                ],
                "sort_order": 3,
            },
        ]

        for item in defaults:
            existing = await self.fetch_ticket_type(guild_id, item["type_key"])
            if existing is None:
                await self.upsert_ticket_type(
                    guild_id,
                    type_key=item["type_key"],
                    label=item["label"],
                    emoji=item["emoji"],
                    button_style=item["button_style"],
                    category_id=None,
                    staff_role_id=DEFAULT_STAFF_ROLE_ID,
                    questions_json=json.dumps(item["questions"]),
                    sort_order=item["sort_order"],
                    enabled=True,
                )

        partnership = await self.fetch_ticket_type(guild_id, "partnership")
        if partnership and partnership.enabled:
            await self.upsert_ticket_type(
                guild_id,
                type_key=partnership.type_key,
                label=partnership.label,
                emoji=partnership.emoji,
                button_style=partnership.button_style,
                category_id=partnership.category_id,
                staff_role_id=partnership.staff_role_id,
                questions_json=partnership.questions_json,
                sort_order=partnership.sort_order,
                enabled=False,
            )

    async def _restore_panel_views(self) -> None:
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    text(
                        f"""
                        SELECT guild_id
                        FROM {self.TABLE_CONFIG}
                        WHERE panel_message_id IS NOT NULL
                        """
                    )
                )
            ).all()

        for (guild_id,) in rows:
            try:
                items = await self.fetch_ticket_types(int(guild_id), enabled_only=True)
                self._register_panel_view(int(guild_id), items)
            except Exception:
                pass

    async def _ensure_default_panel_messages(self) -> None:
        for guild in self.bot.guilds:
            try:
                cfg = await self.fetch_config(guild.id)
                if cfg is None:
                    continue
                if not cfg.panel_message_id:
                    await self.upsert_config(
                        guild.id,
                        category_id=cfg.category_id,
                        archive_category_id=cfg.archive_category_id,
                        log_channel_id=cfg.log_channel_id,
                        support_role_id=cfg.support_role_id,
                        admin_role_id=cfg.admin_role_id,
                        head_mod_role_id=cfg.head_mod_role_id,
                        panel_channel_id=cfg.panel_channel_id or DEFAULT_PANEL_CHANNEL_ID,
                        panel_message_id=None,
                        panel_title=cfg.panel_title,
                        panel_description=cfg.panel_description,
                        panel_image_url=cfg.panel_image_url,
                        max_open_per_user=cfg.max_open_per_user,
                        transcript_enabled=cfg.transcript_enabled,
                        close_cooldown_s=cfg.close_cooldown_s,
                    )
                await self._refresh_panel_message(guild, only_create_if_missing=True)
            except Exception:
                pass

    async def fetch_config(self, guild_id: int) -> TicketConfig | None:
        async with self.sessionmaker() as session:
            row = (
                await session.execute(
                    text(
                        f"""
                        SELECT
                            guild_id,
                            category_id,
                            archive_category_id,
                            log_channel_id,
                            support_role_id,
                            admin_role_id,
                            head_mod_role_id,
                            panel_channel_id,
                            panel_message_id,
                            transcript_channel_id,
                            panel_title,
                            panel_description,
                            panel_image_url,
                            max_open_per_user,
                            transcript_enabled,
                            close_cooldown_s
                        FROM {self.TABLE_CONFIG}
                        WHERE guild_id=:gid
                        """
                    ),
                    {"gid": int(guild_id)},
                )
            ).first()

        if not row:
            return None

        return TicketConfig(
            guild_id=int(row[0]),
            category_id=int(row[1]) if row[1] is not None else None,
            archive_category_id=int(row[2]) if row[2] is not None else None,
            log_channel_id=int(row[3]) if row[3] is not None else None,
            support_role_id=int(row[4]) if row[4] is not None else None,
            admin_role_id=int(row[5]) if row[5] is not None else None,
            head_mod_role_id=int(row[6]) if row[6] is not None else None,
            panel_channel_id=int(row[7]) if row[7] is not None else None,
            panel_message_id=int(row[8]) if row[8] is not None else None,
            transcript_channel_id=int(row[9]) if row[9] is not None else None,
            panel_title=str(row[10] or DEFAULT_PANEL_TITLE),
            panel_description=str(row[11] or DEFAULT_PANEL_DESCRIPTION),
            panel_image_url=str(row[12]) if row[12] else None,
            max_open_per_user=int(row[13] or 1),
            transcript_enabled=bool(int(row[14] or 0)),
            close_cooldown_s=max(0, int(row[15] or 5)),
        )

    async def upsert_config(
        self,
        guild_id: int,
        *,
        category_id: int | None,
        archive_category_id: int | None,
        log_channel_id: int | None,
        support_role_id: int | None,
        admin_role_id: int | None,
        head_mod_role_id: int | None,
        panel_channel_id: int | None = None,
        panel_message_id: int | None = None,
        transcript_channel_id: int | None = None,
        panel_title: str | None = None,
        panel_description: str | None = None,
        panel_image_url: str | None = None,
        max_open_per_user: int | None = None,
        transcript_enabled: bool | None = None,
        close_cooldown_s: int | None = None,
    ) -> None:
        existing = await self.fetch_config(guild_id)

        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"""
                        INSERT INTO {self.TABLE_CONFIG} (
                            guild_id,
                            category_id,
                            archive_category_id,
                            log_channel_id,
                            support_role_id,
                            admin_role_id,
                            head_mod_role_id,
                            panel_channel_id,
                            panel_message_id,
                            transcript_channel_id,
                            panel_title,
                            panel_description,
                            panel_image_url,
                            max_open_per_user,
                            transcript_enabled,
                            close_cooldown_s
                        )
                        VALUES (
                            :guild_id,
                            :category_id,
                            :archive_category_id,
                            :log_channel_id,
                            :support_role_id,
                            :admin_role_id,
                            :head_mod_role_id,
                            :panel_channel_id,
                            :panel_message_id,
                            :transcript_channel_id,
                            :panel_title,
                            :panel_description,
                            :panel_image_url,
                            :max_open_per_user,
                            :transcript_enabled,
                            :close_cooldown_s
                        )
                        ON DUPLICATE KEY UPDATE
                            category_id=VALUES(category_id),
                            archive_category_id=VALUES(archive_category_id),
                            log_channel_id=VALUES(log_channel_id),
                            support_role_id=VALUES(support_role_id),
                            admin_role_id=VALUES(admin_role_id),
                            head_mod_role_id=VALUES(head_mod_role_id),
                            panel_channel_id=COALESCE(VALUES(panel_channel_id), panel_channel_id),
                            panel_message_id=COALESCE(VALUES(panel_message_id), panel_message_id),
                            transcript_channel_id=COALESCE(VALUES(transcript_channel_id), transcript_channel_id),
                            panel_title=VALUES(panel_title),
                            panel_description=VALUES(panel_description),
                            panel_image_url=VALUES(panel_image_url),
                            max_open_per_user=VALUES(max_open_per_user),
                            transcript_enabled=VALUES(transcript_enabled),
                            close_cooldown_s=VALUES(close_cooldown_s)
                        """
                    ),
                    {
                        "guild_id": int(guild_id),
                        "category_id": int(category_id) if category_id is not None else None,
                        "archive_category_id": int(archive_category_id) if archive_category_id is not None else None,
                        "log_channel_id": int(log_channel_id) if log_channel_id is not None else None,
                        "support_role_id": int(support_role_id) if support_role_id is not None else None,
                        "admin_role_id": int(admin_role_id) if admin_role_id is not None else None,
                        "head_mod_role_id": int(head_mod_role_id) if head_mod_role_id is not None else None,
                        "panel_channel_id": int(panel_channel_id) if panel_channel_id is not None else None,
                        "panel_message_id": int(panel_message_id) if panel_message_id is not None else None,
                        "transcript_channel_id": int(transcript_channel_id) if transcript_channel_id is not None else None,
                        "panel_title": str(panel_title or (existing.panel_title if existing else DEFAULT_PANEL_TITLE)),
                        "panel_description": str(
                            panel_description or (existing.panel_description if existing else DEFAULT_PANEL_DESCRIPTION)
                        ),
                        "panel_image_url": str(panel_image_url or (existing.panel_image_url if existing else DEFAULT_PANEL_IMAGE_URL)),
                        "max_open_per_user": int(
                            max_open_per_user if max_open_per_user is not None else (existing.max_open_per_user if existing else 1)
                        ),
                        "transcript_enabled": 1 if bool(
                            transcript_enabled if transcript_enabled is not None else (existing.transcript_enabled if existing else True)
                        ) else 0,
                        "close_cooldown_s": int(
                            close_cooldown_s if close_cooldown_s is not None else (existing.close_cooldown_s if existing else 5)
                        ),
                    },
                )

    async def fetch_ticket_types(self, guild_id: int, *, enabled_only: bool) -> list[TicketTypeRow]:
        where_enabled = "AND enabled=1" if enabled_only else ""
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    text(
                        f"""
                        SELECT
                            id,
                            guild_id,
                            type_key,
                            label,
                            emoji,
                            button_style,
                            category_id,
                            staff_role_id,
                            questions_json,
                            sort_order,
                            enabled
                        FROM {self.TABLE_TYPES}
                        WHERE guild_id=:gid
                        {where_enabled}
                        ORDER BY sort_order ASC, id ASC
                        """
                    ),
                    {"gid": int(guild_id)},
                )
            ).all()

        return [
            TicketTypeRow(
                id=int(r[0]),
                guild_id=int(r[1]),
                type_key=str(r[2]),
                label=str(r[3]),
                emoji=str(r[4]) if r[4] is not None else None,
                button_style=int(r[5]),
                category_id=int(r[6]) if r[6] is not None else None,
                staff_role_id=int(r[7]) if r[7] is not None else None,
                questions_json=str(r[8]) if r[8] is not None else None,
                sort_order=int(r[9]),
                enabled=bool(int(r[10])),
            )
            for r in rows
        ]

    async def fetch_ticket_type(self, guild_id: int, type_key: str) -> TicketTypeRow | None:
        async with self.sessionmaker() as session:
            row = (
                await session.execute(
                    text(
                        f"""
                        SELECT
                            id,
                            guild_id,
                            type_key,
                            label,
                            emoji,
                            button_style,
                            category_id,
                            staff_role_id,
                            questions_json,
                            sort_order,
                            enabled
                        FROM {self.TABLE_TYPES}
                        WHERE guild_id=:gid AND type_key=:type_key
                        LIMIT 1
                        """
                    ),
                    {"gid": int(guild_id), "type_key": str(type_key)},
                )
            ).first()

        if not row:
            return None

        return TicketTypeRow(
            id=int(row[0]),
            guild_id=int(row[1]),
            type_key=str(row[2]),
            label=str(row[3]),
            emoji=str(row[4]) if row[4] is not None else None,
            button_style=int(row[5]),
            category_id=int(row[6]) if row[6] is not None else None,
            staff_role_id=int(row[7]) if row[7] is not None else None,
            questions_json=str(row[8]) if row[8] is not None else None,
            sort_order=int(row[9]),
            enabled=bool(int(row[10])),
        )

    async def upsert_ticket_type(
        self,
        guild_id: int,
        *,
        type_key: str,
        label: str,
        emoji: str | None,
        button_style: int,
        category_id: int | None,
        staff_role_id: int | None,
        questions_json: str | None,
        sort_order: int,
        enabled: bool,
    ) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"""
                        INSERT INTO {self.TABLE_TYPES} (
                            guild_id,
                            type_key,
                            label,
                            emoji,
                            button_style,
                            category_id,
                            staff_role_id,
                            questions_json,
                            sort_order,
                            enabled
                        )
                        VALUES (
                            :guild_id,
                            :type_key,
                            :label,
                            :emoji,
                            :button_style,
                            :category_id,
                            :staff_role_id,
                            :questions_json,
                            :sort_order,
                            :enabled
                        )
                        ON DUPLICATE KEY UPDATE
                            label=VALUES(label),
                            emoji=VALUES(emoji),
                            button_style=VALUES(button_style),
                            category_id=VALUES(category_id),
                            staff_role_id=VALUES(staff_role_id),
                            questions_json=VALUES(questions_json),
                            sort_order=VALUES(sort_order),
                            enabled=VALUES(enabled)
                        """
                    ),
                    {
                        "guild_id": int(guild_id),
                        "type_key": str(type_key),
                        "label": str(label)[:80],
                        "emoji": str(emoji)[:32] if emoji else None,
                        "button_style": int(button_style),
                        "category_id": int(category_id) if category_id is not None else None,
                        "staff_role_id": int(staff_role_id) if staff_role_id is not None else None,
                        "questions_json": questions_json,
                        "sort_order": int(sort_order),
                        "enabled": 1 if enabled else 0,
                    },
                )

    async def delete_ticket_type(self, guild_id: int, type_key: str) -> bool:
        async with self.sessionmaker() as session:
            async with session.begin():
                result = await session.execute(
                    text(
                        f"""
                        DELETE FROM {self.TABLE_TYPES}
                        WHERE guild_id=:gid AND type_key=:type_key
                        """
                    ),
                    {"gid": int(guild_id), "type_key": str(type_key)},
                )
                return bool(result.rowcount)

    async def fetch_ticket_by_channel(self, channel_id: int) -> TicketRow | None:
        async with self.sessionmaker() as session:
            row = (
                await session.execute(
                    text(
                        f"""
                        SELECT
                            id,
                            guild_id,
                            channel_id,
                            creator_id,
                            claimed_by_id,
                            type_key,
                            type_label,
                            status,
                            created_at,
                            closed_at,
                            close_reason,
                            intake_answers_json
                        FROM {self.TABLE_TICKETS}
                        WHERE channel_id=:cid
                        LIMIT 1
                        """
                    ),
                    {"cid": int(channel_id)},
                )
            ).first()

        if not row:
            return None

        return TicketRow(
            id=int(row[0]),
            guild_id=int(row[1]),
            channel_id=int(row[2]),
            creator_id=int(row[3]),
            claimed_by_id=int(row[4]) if row[4] is not None else None,
            type_key=str(row[5]),
            type_label=str(row[6]),
            status=str(row[7]),
            created_at=row[8],
            closed_at=row[9],
            close_reason=str(row[10]) if row[10] is not None else None,
            intake_answers_json=str(row[11]) if row[11] is not None else None,
        )

    async def fetch_ticket_by_id(self, ticket_id: int) -> TicketRow | None:
        async with self.sessionmaker() as session:
            row = (
                await session.execute(
                    text(
                        f"""
                        SELECT
                            id,
                            guild_id,
                            channel_id,
                            creator_id,
                            claimed_by_id,
                            type_key,
                            type_label,
                            status,
                            created_at,
                            closed_at,
                            close_reason,
                            intake_answers_json
                        FROM {self.TABLE_TICKETS}
                        WHERE id=:tid
                        LIMIT 1
                        """
                    ),
                    {"tid": int(ticket_id)},
                )
            ).first()

        if not row:
            return None

        return TicketRow(
            id=int(row[0]),
            guild_id=int(row[1]),
            channel_id=int(row[2]),
            creator_id=int(row[3]),
            claimed_by_id=int(row[4]) if row[4] is not None else None,
            type_key=str(row[5]),
            type_label=str(row[6]),
            status=str(row[7]),
            created_at=row[8],
            closed_at=row[9],
            close_reason=str(row[10]) if row[10] is not None else None,
            intake_answers_json=str(row[11]) if row[11] is not None else None,
        )

    async def fetch_open_tickets_for_user(self, guild_id: int, user_id: int, *, limit: int = 10) -> list[TicketRow]:
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    text(
                        f"""
                        SELECT
                            id,
                            guild_id,
                            channel_id,
                            creator_id,
                            claimed_by_id,
                            type_key,
                            type_label,
                            status,
                            created_at,
                            closed_at,
                            close_reason,
                            intake_answers_json
                        FROM {self.TABLE_TICKETS}
                        WHERE guild_id=:gid
                          AND creator_id=:uid
                          AND status='open'
                        ORDER BY created_at ASC
                        LIMIT :row_limit
                        """
                    ),
                    {"gid": int(guild_id), "uid": int(user_id), "row_limit": int(limit)},
                )
            ).all()

        return [
            TicketRow(
                id=int(r[0]),
                guild_id=int(r[1]),
                channel_id=int(r[2]),
                creator_id=int(r[3]),
                claimed_by_id=int(r[4]) if r[4] is not None else None,
                type_key=str(r[5]),
                type_label=str(r[6]),
                status=str(r[7]),
                created_at=r[8],
                closed_at=r[9],
                close_reason=str(r[10]) if r[10] is not None else None,
                intake_answers_json=str(r[11]) if r[11] is not None else None,
            )
            for r in rows
        ]

    async def fetch_open_tickets_for_guild(self, guild_id: int, *, limit: int = 25) -> list[TicketRow]:
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    text(
                        f"""
                        SELECT
                            id,
                            guild_id,
                            channel_id,
                            creator_id,
                            claimed_by_id,
                            type_key,
                            type_label,
                            status,
                            created_at,
                            closed_at,
                            close_reason,
                            intake_answers_json
                        FROM {self.TABLE_TICKETS}
                        WHERE guild_id=:gid
                          AND status='open'
                        ORDER BY created_at ASC
                        LIMIT :row_limit
                        """
                    ),
                    {"gid": int(guild_id), "row_limit": int(limit)},
                )
            ).all()

        return [
            TicketRow(
                id=int(r[0]),
                guild_id=int(r[1]),
                channel_id=int(r[2]),
                creator_id=int(r[3]),
                claimed_by_id=int(r[4]) if r[4] is not None else None,
                type_key=str(r[5]),
                type_label=str(r[6]),
                status=str(r[7]),
                created_at=r[8],
                closed_at=r[9],
                close_reason=str(r[10]) if r[10] is not None else None,
                intake_answers_json=str(r[11]) if r[11] is not None else None,
            )
            for r in rows
        ]

    async def count_open_tickets_for_user(self, guild_id: int, user_id: int) -> int:
        async with self.sessionmaker() as session:
            row = (
                await session.execute(
                    text(
                        f"""
                        SELECT COUNT(*)
                        FROM {self.TABLE_TICKETS}
                        WHERE guild_id=:gid
                          AND creator_id=:uid
                          AND status='open'
                        """
                    ),
                    {"gid": int(guild_id), "uid": int(user_id)},
                )
            ).first()
        return int(row[0] or 0)

    async def create_ticket_row(
        self,
        guild_id: int,
        channel_id: int,
        creator_id: int,
        type_key: str,
        type_label: str,
        intake_answers: list[dict[str, str]] | None = None,
    ) -> int:
        intake_answers_json: str | None = None
        if intake_answers:
            normalized_answers = [
                {
                    "label": str(item.get("label") or "Question")[:256],
                    "value": str(item.get("value") or "No response")[:4000],
                }
                for item in intake_answers[:5]
                if isinstance(item, dict)
            ]
            if normalized_answers:
                intake_answers_json = json.dumps(normalized_answers)

        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"""
                        INSERT INTO {self.TABLE_TICKETS} (
                            guild_id,
                            channel_id,
                            creator_id,
                            type_key,
                            type_label,
                            status,
                            intake_answers_json
                        )
                        VALUES (
                            :gid,
                            :cid,
                            :uid,
                            :type_key,
                            :type_label,
                            'open',
                            :intake_answers_json
                        )
                        """
                    ),
                    {
                        "gid": int(guild_id),
                        "cid": int(channel_id),
                        "uid": int(creator_id),
                        "type_key": str(type_key),
                        "type_label": str(type_label),
                        "intake_answers_json": intake_answers_json,
                    },
                )
                row = (await session.execute(text("SELECT LAST_INSERT_ID()"))).first()
        return int(row[0])

    async def set_ticket_initial_message(self, ticket_id: int, message_id: int) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"""
                        UPDATE {self.TABLE_TICKETS}
                        SET initial_message_id=:mid
                        WHERE id=:tid
                        """
                    ),
                    {"mid": int(message_id), "tid": int(ticket_id)},
                )

    async def set_ticket_claim(self, ticket_id: int, claimed_by_id: int | None) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"""
                        UPDATE {self.TABLE_TICKETS}
                        SET claimed_by_id=:uid
                        WHERE id=:tid
                        """
                    ),
                    {"uid": int(claimed_by_id) if claimed_by_id is not None else None, "tid": int(ticket_id)},
                )

    async def close_ticket_db(self, ticket_id: int, reason: str | None) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"""
                        UPDATE {self.TABLE_TICKETS}
                        SET status='closed',
                            closed_at=UTC_TIMESTAMP(),
                            close_reason=:reason
                        WHERE id=:tid
                        """
                    ),
                    {"tid": int(ticket_id), "reason": str(reason) if reason else None},
                )

    async def reopen_ticket_db(self, ticket_id: int) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"""
                        UPDATE {self.TABLE_TICKETS}
                        SET status='open',
                            closed_at=NULL,
                            close_reason=NULL
                        WHERE id=:tid
                        """
                    ),
                    {"tid": int(ticket_id)},
                )

    async def add_ticket_member(self, ticket_id: int, user_id: int, added_by_id: int | None) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"""
                        INSERT INTO {self.TABLE_MEMBERS} (
                            ticket_id,
                            user_id,
                            added_by_id
                        )
                        VALUES (
                            :tid,
                            :uid,
                            :added_by
                        )
                        ON DUPLICATE KEY UPDATE
                            added_by_id=VALUES(added_by_id)
                        """
                    ),
                    {
                        "tid": int(ticket_id),
                        "uid": int(user_id),
                        "added_by": int(added_by_id) if added_by_id is not None else None,
                    },
                )

    async def remove_ticket_member(self, ticket_id: int, user_id: int) -> None:
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"""
                        DELETE FROM {self.TABLE_MEMBERS}
                        WHERE ticket_id=:tid AND user_id=:uid
                        """
                    ),
                    {"tid": int(ticket_id), "uid": int(user_id)},
                )

    async def fetch_ticket_members(self, ticket_id: int) -> list[int]:
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    text(
                        f"""
                        SELECT user_id
                        FROM {self.TABLE_MEMBERS}
                        WHERE ticket_id=:tid
                        ORDER BY created_at ASC
                        """
                    ),
                    {"tid": int(ticket_id)},
                )
            ).all()
        return [int(r[0]) for r in rows]

    def _is_head_mod(self, member: discord.Member, cfg: TicketConfig) -> bool:
        return _member_has_role(member, cfg.head_mod_role_id)

    def _is_staff(self, member: discord.Member, cfg: TicketConfig, ttype: TicketTypeRow | None) -> bool:
        if member.guild_permissions.administrator or member.guild_permissions.manage_guild:
            return True
        if _member_has_role(member, cfg.support_role_id):
            return True
        if _member_has_role(member, cfg.admin_role_id):
            return True
        if _member_has_role(member, cfg.head_mod_role_id):
            return True
        if ttype and _member_has_role(member, ttype.staff_role_id):
            return True
        return False

    def _can_manage_ticket(
        self,
        member: discord.Member,
        cfg: TicketConfig,
        ttype: TicketTypeRow | None,
        ticket: TicketRow,
    ) -> bool:
        if member.guild_permissions.administrator or member.guild_permissions.manage_guild:
            return True
        if member.id == ticket.creator_id:
            return True
        return self._is_staff(member, cfg, ttype)

    def _ticket_view_for_status(self, status: str) -> TicketChannelView:
        return TicketChannelView(self, is_closed=(status == "closed"))

    async def _build_panel_embed(self, guild: discord.Guild, cfg: TicketConfig) -> discord.Embed:
        items = await self.fetch_ticket_types(guild.id, enabled_only=True)

        e = discord.Embed(
            title=cfg.panel_title,
            description=cfg.panel_description,
            color=discord.Color(DEFAULT_BLUE),
            timestamp=_utc_now(),
        )
        if guild.icon:
            e.set_thumbnail(url=guild.icon.url)
        if cfg.panel_image_url:
            e.set_image(url=cfg.panel_image_url)

        summaries = {
            "general": "Any general questions or suggestions",
            "report": "Report a user and/or staff",
            "application": "Apply for staff",
            "donate": "Help with billing and donations",
        }

        if items:
            lines = []
            for item in items[:25]:
                emoji = f"{item.emoji} " if item.emoji else ""
                desc = summaries.get(item.type_key, "Open a private ticket")
                lines.append(f"{emoji}**{item.label}**\n> {desc}")
            for idx, chunk in enumerate(_chunk_lines(lines, 950), start=1):
                e.add_field(
                    name="Ticket Options" if idx == 1 else f"More Options {idx}",
                    value=chunk,
                    inline=False,
                )
        else:
            e.add_field(
                name="No ticket types available",
                value="An admin needs to add ticket types.",
                inline=False,
            )

        e.set_footer(text="Pick the option that matches your issue.")
        return e

    async def _build_ticket_header_embed(
        self,
        guild: discord.Guild,
        ticket: TicketRow,
        cfg: TicketConfig,
        ttype: TicketTypeRow | None,
        opener: discord.abc.User | discord.Member | None,
        answers: list[dict[str, str]] | None = None,
    ) -> discord.Embed:
        e = discord.Embed(
            title=f"{ticket.type_label} Ticket #{ticket.id}",
            color=discord.Color(DEFAULT_BLUE) if ticket.status == "open" else discord.Color.red(),
            timestamp=_utc_now(),
        )
        if guild.icon:
            e.set_thumbnail(url=guild.icon.url)

        creator_line = f"<@{ticket.creator_id}>"
        claimer_line = f"<@{ticket.claimed_by_id}>" if ticket.claimed_by_id else "Nobody yet"

        e.add_field(name="Opened By", value=creator_line, inline=True)
        e.add_field(name="Claimed By", value=claimer_line, inline=True)
        e.add_field(name="Status", value=ticket.status.title(), inline=True)

        staff_mentions = []
        for rid in {cfg.support_role_id, cfg.admin_role_id, cfg.head_mod_role_id, (ttype.staff_role_id if ttype else None)}:
            if rid:
                staff_mentions.append(f"<@&{rid}>")
        e.add_field(name="Staff Access", value=" ".join(staff_mentions) if staff_mentions else "None", inline=False)

        if answers:
            for pair in answers[:5]:
                label = str(pair.get("label") or "Question")[:256]
                value = str(pair.get("value") or "No response")[:1024]
                e.add_field(name=label, value=value, inline=False)
        elif ticket.intake_answers_json:
            stored_answers = _safe_json_load(ticket.intake_answers_json, [])
            if isinstance(stored_answers, list):
                for pair in stored_answers[:5]:
                    if not isinstance(pair, dict):
                        continue
                    label = str(pair.get("label") or "Question")[:256]
                    value = str(pair.get("value") or "No response")[:1024]
                    e.add_field(name=label, value=value, inline=False)

        if ticket.close_reason:
            e.add_field(name="Close Reason", value=ticket.close_reason[:1024], inline=False)

        opener_name = opener.display_name if isinstance(opener, discord.Member) else (opener.name if opener else f"User {ticket.creator_id}")
        e.set_footer(text=f"{opener_name} • Ticket ID {ticket.id}")
        return e

    async def _resolve_category(self, guild: discord.Guild, cfg: TicketConfig, ttype: TicketTypeRow) -> discord.CategoryChannel | None:
        cid = ttype.category_id or cfg.category_id
        if not cid:
            return None
        ch = guild.get_channel(int(cid))
        if isinstance(ch, discord.CategoryChannel):
            return ch
        try:
            fetched = await self.bot.fetch_channel(int(cid))
            if isinstance(fetched, discord.CategoryChannel) and fetched.guild.id == guild.id:
                return fetched
        except Exception:
            return None
        return None

    async def _resolve_archive_category(self, guild: discord.Guild, cfg: TicketConfig) -> discord.CategoryChannel | None:
        if not cfg.archive_category_id:
            return None
        ch = guild.get_channel(int(cfg.archive_category_id))
        if isinstance(ch, discord.CategoryChannel):
            return ch
        try:
            fetched = await self.bot.fetch_channel(int(cfg.archive_category_id))
            if isinstance(fetched, discord.CategoryChannel) and fetched.guild.id == guild.id:
                return fetched
        except Exception:
            return None
        return None

    async def _resolve_panel_channel(self, guild: discord.Guild, cfg: TicketConfig) -> discord.TextChannel | None:
        if not cfg.panel_channel_id:
            return None
        ch = guild.get_channel(int(cfg.panel_channel_id))
        if isinstance(ch, discord.TextChannel):
            return ch
        try:
            fetched = await self.bot.fetch_channel(int(cfg.panel_channel_id))
            if isinstance(fetched, discord.TextChannel) and fetched.guild.id == guild.id:
                return fetched
        except Exception:
            return None
        return None

    async def _resolve_log_channel(self, guild: discord.Guild, cfg: TicketConfig) -> discord.TextChannel | None:
        if not cfg.log_channel_id:
            return None
        ch = guild.get_channel(int(cfg.log_channel_id))
        if isinstance(ch, discord.TextChannel):
            return ch
        try:
            fetched = await self.bot.fetch_channel(int(cfg.log_channel_id))
            if isinstance(fetched, discord.TextChannel) and fetched.guild.id == guild.id:
                return fetched
        except Exception:
            return None
        return None

    async def _resolve_closed_transcript_channel(self, guild: discord.Guild, cfg: TicketConfig) -> discord.TextChannel | None:
        channel_id = int(cfg.transcript_channel_id or CLOSED_TICKET_TRANSCRIPTS_CHANNEL_ID)
        ch = guild.get_channel(channel_id)
        if isinstance(ch, discord.TextChannel):
            return ch
        try:
            fetched = await self.bot.fetch_channel(channel_id)
            if isinstance(fetched, discord.TextChannel) and fetched.guild.id == guild.id:
                return fetched
        except Exception:
            pass

        ensured_channel_id = await self._ensure_transcript_channel(
            guild,
            preferred_id=cfg.transcript_channel_id,
            archive_category_id=cfg.archive_category_id,
        )
        if ensured_channel_id is None:
            return None

        if cfg.transcript_channel_id != ensured_channel_id:
            await self.upsert_config(
                guild.id,
                category_id=cfg.category_id,
                archive_category_id=cfg.archive_category_id,
                log_channel_id=cfg.log_channel_id,
                support_role_id=cfg.support_role_id,
                admin_role_id=cfg.admin_role_id,
                head_mod_role_id=cfg.head_mod_role_id,
                panel_channel_id=cfg.panel_channel_id,
                panel_message_id=cfg.panel_message_id,
                transcript_channel_id=ensured_channel_id,
                panel_title=cfg.panel_title,
                panel_description=cfg.panel_description,
                panel_image_url=cfg.panel_image_url,
                max_open_per_user=cfg.max_open_per_user,
                transcript_enabled=cfg.transcript_enabled,
                close_cooldown_s=cfg.close_cooldown_s,
            )

        final_ch = guild.get_channel(int(ensured_channel_id))
        return final_ch if isinstance(final_ch, discord.TextChannel) else None

    async def _send_log(self, guild: discord.Guild, cfg: TicketConfig, *, embed: discord.Embed, file: discord.File | None = None) -> None:
        ch = await self._resolve_log_channel(guild, cfg)
        if ch is None:
            return
        try:
            await ch.send(embed=embed, file=file)
        except Exception:
            pass

    async def _refresh_panel_message(self, guild: discord.Guild, *, only_create_if_missing: bool = False) -> tuple[bool, str]:
        cfg = await self.fetch_config(guild.id)
        if cfg is None:
            return False, "Ticket system is not configured."

        panel_channel = await self._resolve_panel_channel(guild, cfg)
        if panel_channel is None:
            return False, "Panel channel is missing."

        items = await self.fetch_ticket_types(guild.id, enabled_only=True)
        view = TicketPanelView(self, guild.id, items)
        embed = await self._build_panel_embed(guild, cfg)

        msg: discord.Message | None = None
        target_message_id = cfg.panel_message_id or DEFAULT_PANEL_MESSAGE_ID

        try:
            msg = await panel_channel.fetch_message(int(target_message_id))
        except Exception:
            msg = None

        if msg is None:
            msg = await panel_channel.send(embed=embed, view=view)
            await self.upsert_config(
                guild.id,
                category_id=cfg.category_id,
                archive_category_id=cfg.archive_category_id,
                log_channel_id=cfg.log_channel_id,
                support_role_id=cfg.support_role_id,
                admin_role_id=cfg.admin_role_id,
                head_mod_role_id=cfg.head_mod_role_id,
                panel_channel_id=panel_channel.id,
                panel_message_id=msg.id,
                panel_title=cfg.panel_title,
                panel_description=cfg.panel_description,
                panel_image_url=cfg.panel_image_url,
                max_open_per_user=cfg.max_open_per_user,
                transcript_enabled=cfg.transcript_enabled,
                close_cooldown_s=cfg.close_cooldown_s,
            )
        elif not only_create_if_missing:
            await msg.edit(embed=embed, view=view)
            if cfg.panel_message_id != msg.id:
                await self.upsert_config(
                    guild.id,
                    category_id=cfg.category_id,
                    archive_category_id=cfg.archive_category_id,
                    log_channel_id=cfg.log_channel_id,
                    support_role_id=cfg.support_role_id,
                    admin_role_id=cfg.admin_role_id,
                    head_mod_role_id=cfg.head_mod_role_id,
                    panel_channel_id=panel_channel.id,
                    panel_message_id=msg.id,
                    panel_title=cfg.panel_title,
                    panel_description=cfg.panel_description,
                    panel_image_url=cfg.panel_image_url,
                    max_open_per_user=cfg.max_open_per_user,
                    transcript_enabled=cfg.transcript_enabled,
                    close_cooldown_s=cfg.close_cooldown_s,
                )

        self._register_panel_view(guild.id, items)
        return True, f"Panel is live in {panel_channel.mention}."

    async def _apply_claim_lock(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
        cfg: TicketConfig,
        ttype: TicketTypeRow | None,
        claimer_id: int,
    ) -> None:
        claimer = guild.get_member(int(claimer_id))
        if claimer:
            await channel.set_permissions(
                claimer,
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True,
                add_reactions=True,
                reason=f"Ticket claimed by {claimer_id}",
            )

        claim_locked_roles = {cfg.support_role_id, cfg.admin_role_id}
        if ttype and ttype.staff_role_id:
            claim_locked_roles.add(ttype.staff_role_id)

        for rid in claim_locked_roles:
            if not rid or rid == cfg.head_mod_role_id:
                continue
            role = guild.get_role(int(rid))
            if role is None:
                continue
            await channel.set_permissions(
                role,
                view_channel=True,
                send_messages=False,
                read_message_history=True,
                attach_files=False,
                embed_links=False,
                add_reactions=False,
                reason=f"Claim lock applied by {claimer_id}",
            )

        if cfg.head_mod_role_id:
            hm = guild.get_role(int(cfg.head_mod_role_id))
            if hm:
                await channel.set_permissions(
                    hm,
                    view_channel=True,
                    send_messages=True,
                    read_message_history=True,
                    attach_files=True,
                    embed_links=True,
                    add_reactions=True,
                    reason="Head mods keep full access",
                )

    async def _clear_claim_lock(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
        cfg: TicketConfig,
        ttype: TicketTypeRow | None,
    ) -> None:
        unlocked_roles = {cfg.support_role_id, cfg.admin_role_id, cfg.head_mod_role_id}
        if ttype and ttype.staff_role_id:
            unlocked_roles.add(ttype.staff_role_id)

        for rid in unlocked_roles:
            if not rid:
                continue
            role = guild.get_role(int(rid))
            if role is None:
                continue
            await channel.set_permissions(
                role,
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True,
                add_reactions=True,
                reason="Claim lock cleared",
            )

    async def _clear_user_ticket_override(
        self,
        channel: discord.TextChannel,
        user_id: int,
        *,
        reason: str,
    ) -> None:
        try:
            await channel.set_permissions(
                discord.Object(id=int(user_id)),
                overwrite=None,
                reason=reason,
            )
        except discord.HTTPException:
            pass

    async def create_ticket_from_modal(
        self,
        interaction: discord.Interaction,
        ticket_type: TicketTypeRow,
        answers: list[dict[str, str]],
    ) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            if not interaction.response.is_done():
                await interaction.response.send_message("Server only.", ephemeral=True)
            return

        async with self._lock:
            cfg = await self.fetch_config(interaction.guild.id)
            if cfg is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Ticket system is not configured yet.", ephemeral=True)
                return

            open_count = await self.count_open_tickets_for_user(interaction.guild.id, interaction.user.id)
            if open_count >= cfg.max_open_per_user:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"You already have the max number of open tickets: {cfg.max_open_per_user}.",
                        ephemeral=True,
                    )
                return

            category = await self._resolve_category(interaction.guild, cfg, ticket_type)
            if category is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Open tickets category is missing.", ephemeral=True)
                return

            me = interaction.guild.me
            if me is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Bot member not ready yet.", ephemeral=True)
                return

            overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
                interaction.guild.default_role: discord.PermissionOverwrite(view_channel=False),
                interaction.user: discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    read_message_history=True,
                    attach_files=True,
                    embed_links=True,
                    add_reactions=True,
                ),
                me: discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    manage_channels=True,
                    manage_messages=True,
                    read_message_history=True,
                    attach_files=True,
                    embed_links=True,
                    add_reactions=True,
                ),
            }

            access_roles = {cfg.support_role_id, cfg.admin_role_id, cfg.head_mod_role_id, ticket_type.staff_role_id}
            for rid in access_roles:
                if not rid:
                    continue
                role = interaction.guild.get_role(int(rid))
                if role:
                    overwrites[role] = discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=True,
                        read_message_history=True,
                        attach_files=True,
                        embed_links=True,
                        add_reactions=True,
                    )

            channel_name = f"ticket-{ticket_type.type_key}-{interaction.user.name}-{str(interaction.user.id)[-4:]}"
            channel_name = _clean_channel_fragment(channel_name)

            try:
                channel = await interaction.guild.create_text_channel(
                    name=channel_name,
                    category=category,
                    overwrites=overwrites,
                    topic=f"ticket_type={ticket_type.type_key} | opener={interaction.user.id} | created_at={_ts()}",
                    reason=f"Ticket opened by {interaction.user} ({interaction.user.id})",
                )
            except discord.Forbidden:
                if not interaction.response.is_done():
                    await interaction.response.send_message("I don't have permission to create ticket channels.", ephemeral=True)
                return
            except Exception:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Ticket creation failed.", ephemeral=True)
                return

            ticket_id = await self.create_ticket_row(
                interaction.guild.id,
                channel.id,
                interaction.user.id,
                ticket_type.type_key,
                ticket_type.label,
                intake_answers=answers,
            )

            ticket = await self.fetch_ticket_by_id(ticket_id)
            if ticket is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Ticket was created but DB lookup failed.", ephemeral=True)
                return

            header_embed = await self._build_ticket_header_embed(
                interaction.guild,
                ticket,
                cfg,
                ticket_type,
                interaction.user,
                answers=answers,
            )

            ping_roles = {cfg.support_role_id, ticket_type.staff_role_id, cfg.head_mod_role_id}
            staff_ping = " ".join(f"<@&{rid}>" for rid in ping_roles if rid)

            sent = await channel.send(
                content=f"{interaction.user.mention} {staff_ping}".strip(),
                embed=header_embed,
                view=self._ticket_view_for_status(ticket.status),
            )
            await self.set_ticket_initial_message(ticket_id, sent.id)

            log = discord.Embed(
                title="Ticket Opened",
                color=discord.Color.green(),
                timestamp=_utc_now(),
                description=f"{interaction.user.mention} opened **{ticket_type.label}** in {channel.mention}",
            )
            log.add_field(name="Ticket ID", value=str(ticket_id), inline=True)
            log.add_field(name="Type Key", value=ticket_type.type_key, inline=True)
            log.add_field(name="Creator", value=f"{interaction.user} (`{interaction.user.id}`)", inline=False)
            await self._send_log(interaction.guild, cfg, embed=log)

        if not interaction.response.is_done():
            await interaction.response.send_message(f"Your ticket has been opened: {channel.mention}", ephemeral=True)

    async def _regen_ticket_header(self, guild: discord.Guild, channel: discord.TextChannel, ticket: TicketRow) -> None:
        cfg = await self.fetch_config(guild.id)
        if cfg is None:
            return
        ttype = await self.fetch_ticket_type(guild.id, ticket.type_key)
        opener = guild.get_member(ticket.creator_id) or self.bot.get_user(ticket.creator_id)
        embed = await self._build_ticket_header_embed(guild, ticket, cfg, ttype, opener, answers=None)
        view = self._ticket_view_for_status(ticket.status)

        try:
            async for msg in channel.history(limit=10, oldest_first=True):
                if msg.author.id == self.bot.user.id and msg.embeds:
                    await msg.edit(embed=embed, view=view)
                    return
        except Exception:
            return

    async def _build_transcript_file(self, channel: discord.TextChannel, ticket: TicketRow) -> discord.File:
        rows: list[str] = []
        async for msg in channel.history(limit=None, oldest_first=True):
            created = msg.created_at.replace(tzinfo=timezone.utc) if msg.created_at.tzinfo is None else msg.created_at
            author = html.escape(f"{msg.author} ({msg.author.id})")
            content = html.escape(msg.content or "").replace("\n", "<br>")

            attachments = ""
            if msg.attachments:
                links = [
                    f'<a href="{html.escape(a.url)}" target="_blank">{html.escape(a.filename)}</a>'
                    for a in msg.attachments
                ]
                attachments = "<div class='attachments'>Attachments: " + " | ".join(links) + "</div>"

            embeds_text = ""
            if msg.embeds:
                parts = []
                for e in msg.embeds:
                    title = html.escape(e.title or "")
                    desc = html.escape(e.description or "").replace("\n", "<br>")
                    if title or desc:
                        parts.append(f"<div class='embed'><strong>{title}</strong><div>{desc}</div></div>")
                embeds_text = "".join(parts)

            rows.append(
                f"""
                <div class="msg">
                    <div class="meta">
                        <span class="author">{author}</span>
                        <span class="time">{html.escape(created.strftime("%Y-%m-%d %H:%M:%S UTC"))}</span>
                    </div>
                    <div class="content">{content or "<i>[no text]</i>"}</div>
                    {attachments}
                    {embeds_text}
                </div>
                """
            )

        body = "\n".join(rows)
        doc = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Transcript Ticket #{ticket.id}</title>
<style>
body {{
    background: #0f1115;
    color: #e8eaf0;
    font-family: Arial, Helvetica, sans-serif;
    margin: 0;
    padding: 24px;
}}
.wrapper {{
    max-width: 1100px;
    margin: 0 auto;
}}
.header {{
    background: #171a21;
    border: 1px solid #292f3a;
    border-radius: 12px;
    padding: 18px;
    margin-bottom: 18px;
}}
.msg {{
    background: #171a21;
    border: 1px solid #292f3a;
    border-radius: 12px;
    padding: 14px;
    margin-bottom: 12px;
}}
.meta {{
    display: flex;
    justify-content: space-between;
    gap: 16px;
    margin-bottom: 8px;
    color: #9aa4b2;
    font-size: 13px;
}}
.author {{
    color: #ffffff;
    font-weight: bold;
}}
.content {{
    line-height: 1.45;
}}
.attachments {{
    margin-top: 8px;
    font-size: 13px;
}}
.embed {{
    margin-top: 10px;
    padding: 10px;
    border-left: 4px solid #2F6BFF;
    background: #11141b;
    border-radius: 8px;
}}
a {{
    color: #7ab7ff;
}}
</style>
</head>
<body>
<div class="wrapper">
    <div class="header">
        <h1>Transcript for Ticket #{ticket.id}</h1>
        <p><strong>Channel:</strong> #{html.escape(channel.name)}</p>
        <p><strong>Opened by:</strong> {ticket.creator_id}</p>
        <p><strong>Type:</strong> {html.escape(ticket.type_label)}</p>
        <p><strong>Status:</strong> {html.escape(ticket.status)}</p>
        <p><strong>Created:</strong> {html.escape(ticket.created_at.strftime("%Y-%m-%d %H:%M:%S UTC") if ticket.created_at else "Unknown")}</p>
        <p><strong>Closed:</strong> {html.escape(ticket.closed_at.strftime("%Y-%m-%d %H:%M:%S UTC") if ticket.closed_at else "Not closed")}</p>
        <p><strong>Reason:</strong> {html.escape(ticket.close_reason or "None")}</p>
    </div>
    {body}
</div>
</body>
</html>
"""
        fp = io.BytesIO(doc.encode("utf-8"))
        return discord.File(fp=fp, filename=f"ticket-{ticket.id}-transcript.html")

    async def handle_claim(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.channel is None or not isinstance(interaction.user, discord.Member):
            if not interaction.response.is_done():
                await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            if not interaction.response.is_done():
                await interaction.response.send_message("This is not a text channel.", ephemeral=True)
            return

        async with self._lock:
            ticket = await self.fetch_ticket_by_channel(interaction.channel.id)
            if ticket is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message("This channel is not a tracked ticket.", ephemeral=True)
                return

            cfg = await self.fetch_config(interaction.guild.id)
            ttype = await self.fetch_ticket_type(interaction.guild.id, ticket.type_key)
            if cfg is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Ticket config is missing.", ephemeral=True)
                return

            if not self._is_staff(interaction.user, cfg, ttype):
                if not interaction.response.is_done():
                    await interaction.response.send_message("Only staff can claim tickets.", ephemeral=True)
                return

            previous_claimer_id: int | None = None
            if ticket.claimed_by_id and ticket.claimed_by_id != interaction.user.id:
                if not self._is_head_mod(interaction.user, cfg):
                    if not interaction.response.is_done():
                        await interaction.response.send_message(
                            PRIVATE_TICKET_OVERRIDE_DENIED_MESSAGE.format(
                                claimer_mention=f"<@{ticket.claimed_by_id}>"
                            ),
                            ephemeral=True,
                        )
                    return
                previous_claimer_id = int(ticket.claimed_by_id)

            if ticket.claimed_by_id == interaction.user.id:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "You already claimed this ticket.",
                        ephemeral=True,
                    )
                return

            if previous_claimer_id:
                await self._clear_user_ticket_override(
                    interaction.channel,
                    previous_claimer_id,
                    reason=f"Ticket claim overridden by {interaction.user.id}",
                )

            await self.set_ticket_claim(ticket.id, interaction.user.id)
            await self._apply_claim_lock(interaction.guild, interaction.channel, cfg, ttype, interaction.user.id)

            fresh = await self.fetch_ticket_by_id(ticket.id)
            if fresh:
                await self._regen_ticket_header(interaction.guild, interaction.channel, fresh)

            if not interaction.response.is_done():
                await interaction.response.send_message(
                    PUBLIC_TICKET_CLAIM_MESSAGE.format(user_mention=interaction.user.mention),
                    ephemeral=False,
                )

    async def handle_unclaim(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.channel is None or not isinstance(interaction.user, discord.Member):
            if not interaction.response.is_done():
                await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            if not interaction.response.is_done():
                await interaction.response.send_message("This is not a text channel.", ephemeral=True)
            return

        async with self._lock:
            ticket = await self.fetch_ticket_by_channel(interaction.channel.id)
            if ticket is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message("This channel is not a tracked ticket.", ephemeral=True)
                return

            cfg = await self.fetch_config(interaction.guild.id)
            ttype = await self.fetch_ticket_type(interaction.guild.id, ticket.type_key)
            if cfg is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Ticket config is missing.", ephemeral=True)
                return

            if ticket.status != "open":
                if not interaction.response.is_done():
                    await interaction.response.send_message("Only open tickets can be unclaimed.", ephemeral=True)
                return

            if ticket.claimed_by_id is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message("This ticket is not currently claimed.", ephemeral=True)
                return

            can_force_unclaim = (
                interaction.user.guild_permissions.administrator
                or interaction.user.guild_permissions.manage_guild
                or self._is_head_mod(interaction.user, cfg)
            )
            if int(ticket.claimed_by_id) != interaction.user.id and not can_force_unclaim:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "Only the current claimer or Head Mod+ can unclaim this ticket.",
                        ephemeral=True,
                    )
                return

            previous_claimer_id = int(ticket.claimed_by_id)
            await self.set_ticket_claim(ticket.id, None)
            await self._clear_user_ticket_override(
                interaction.channel,
                previous_claimer_id,
                reason=f"Ticket unclaimed by {interaction.user.id}",
            )
            await self._clear_claim_lock(interaction.guild, interaction.channel, cfg, ttype)

            fresh = await self.fetch_ticket_by_id(ticket.id)
            if fresh:
                await self._regen_ticket_header(interaction.guild, interaction.channel, fresh)

            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"✅ Ticket unclaimed by {interaction.user.mention}.",
                    ephemeral=False,
                )

    async def handle_close(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.channel is None or not isinstance(interaction.user, discord.Member):
            if not interaction.response.is_done():
                await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            if not interaction.response.is_done():
                await interaction.response.send_message("This is not a text channel.", ephemeral=True)
            return

        ticket = await self.fetch_ticket_by_channel(interaction.channel.id)
        if ticket is None:
            if not interaction.response.is_done():
                await interaction.response.send_message("This channel is not a tracked ticket.", ephemeral=True)
            return

        cfg = await self.fetch_config(interaction.guild.id)
        ttype = await self.fetch_ticket_type(interaction.guild.id, ticket.type_key)
        if cfg is None:
            if not interaction.response.is_done():
                await interaction.response.send_message("Ticket config is missing.", ephemeral=True)
            return

        if not self._can_manage_ticket(interaction.user, cfg, ttype, ticket):
            if not interaction.response.is_done():
                await interaction.response.send_message("You can't close this ticket.", ephemeral=True)
            return

        await interaction.response.send_modal(CloseReasonModal(self))

    async def close_ticket_from_interaction(self, interaction: discord.Interaction, reason: str | None) -> None:
        if interaction.guild is None or interaction.channel is None:
            if not interaction.response.is_done():
                await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            if not interaction.response.is_done():
                await interaction.response.send_message("This is not a text channel.", ephemeral=True)
            return

        await self._close_ticket(
            guild=interaction.guild,
            channel=interaction.channel,
            actor=interaction.user if isinstance(interaction.user, discord.Member) else None,
            reason=reason,
            interaction=interaction,
        )

    async def _close_ticket(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
        actor: discord.Member | None,
        reason: str | None,
        interaction: discord.Interaction | None = None,
    ) -> None:
        async with self._lock:
            ticket = await self.fetch_ticket_by_channel(channel.id)
            if ticket is None:
                if interaction and not interaction.response.is_done():
                    await interaction.response.send_message("This channel is not a tracked ticket.", ephemeral=True)
                return
            if ticket.status == "closed":
                if interaction and not interaction.response.is_done():
                    await interaction.response.send_message("This ticket is already closed.", ephemeral=True)
                return

            cfg = await self.fetch_config(guild.id)
            ttype = await self.fetch_ticket_type(guild.id, ticket.type_key)
            if cfg is None:
                if interaction and not interaction.response.is_done():
                    await interaction.response.send_message("Ticket config is missing.", ephemeral=True)
                return

            transcript_file: discord.File | None = None
            if cfg.transcript_enabled:
                try:
                    transcript_file = await self._build_transcript_file(channel, ticket)
                except Exception:
                    transcript_file = None

            await self.close_ticket_db(ticket.id, reason)
            fresh = await self.fetch_ticket_by_id(ticket.id)
            if fresh is None:
                fresh = ticket
                fresh.status = "closed"
                fresh.close_reason = reason
                fresh.closed_at = _utc_now()

            archive_cat = await self._resolve_archive_category(guild, cfg)

            overwrites = channel.overwrites
            opener = guild.get_member(ticket.creator_id)
            if opener in overwrites:
                overwrites[opener] = discord.PermissionOverwrite(view_channel=True, send_messages=False, read_message_history=True)

            try:
                await channel.edit(
                    name=f"closed-{ticket.id}-{_clean_channel_fragment(ticket.type_key)}",
                    category=archive_cat or channel.category,
                    overwrites=overwrites,
                    reason=f"Ticket #{ticket.id} closed",
                )
            except Exception:
                pass

            await self._regen_ticket_header(guild, channel, fresh)

            close_embed = discord.Embed(
                title=f"Ticket #{ticket.id} Closed",
                color=discord.Color.red(),
                timestamp=_utc_now(),
                description=f"Closed by {actor.mention if actor else 'Unknown'}",
            )
            close_embed.add_field(name="Reason", value=(reason or "No reason provided")[:1024], inline=False)
            await channel.send(embed=close_embed)

            log = discord.Embed(
                title="Ticket Closed",
                color=discord.Color.red(),
                timestamp=_utc_now(),
                description=f"Ticket #{ticket.id} was closed in {channel.mention}",
            )
            log.add_field(name="Type", value=ticket.type_label, inline=True)
            log.add_field(name="Creator", value=f"<@{ticket.creator_id}>", inline=True)
            log.add_field(name="Closed By", value=actor.mention if actor else "Unknown", inline=True)
            log.add_field(name="Reason", value=(reason or "No reason provided")[:1024], inline=False)
            await self._send_log(guild, cfg, embed=log, file=transcript_file)

        if interaction and not interaction.response.is_done():
            await interaction.response.send_message("✅ Ticket closed.", ephemeral=True)

    async def handle_reopen(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.channel is None or not isinstance(interaction.user, discord.Member):
            if not interaction.response.is_done():
                await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            if not interaction.response.is_done():
                await interaction.response.send_message("This is not a text channel.", ephemeral=True)
            return

        async with self._lock:
            ticket = await self.fetch_ticket_by_channel(interaction.channel.id)
            if ticket is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message("This channel is not a tracked ticket.", ephemeral=True)
                return

            cfg = await self.fetch_config(interaction.guild.id)
            ttype = await self.fetch_ticket_type(interaction.guild.id, ticket.type_key)
            if cfg is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Ticket config is missing.", ephemeral=True)
                return

            if not self._is_staff(interaction.user, cfg, ttype):
                if not interaction.response.is_done():
                    await interaction.response.send_message("Only staff can reopen tickets.", ephemeral=True)
                return

            if ticket.status == "open":
                if not interaction.response.is_done():
                    await interaction.response.send_message("This ticket is already open.", ephemeral=True)
                return

            await self.reopen_ticket_db(ticket.id)
            fresh = await self.fetch_ticket_by_id(ticket.id)
            if fresh is None:
                if not interaction.response.is_done():
                    await interaction.response.send_message("Reopen succeeded but reload failed.", ephemeral=True)
                return

            category = await self._resolve_category(interaction.guild, cfg, ttype) if ttype else None
            overwrites = interaction.channel.overwrites
            opener = interaction.guild.get_member(ticket.creator_id)
            if opener in overwrites:
                overwrites[opener] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    read_message_history=True,
                    attach_files=True,
                    embed_links=True,
                    add_reactions=True,
                )

            try:
                await interaction.channel.edit(
                    name=f"ticket-{ticket.id}-{_clean_channel_fragment(ticket.type_key)}",
                    category=category or interaction.channel.category,
                    overwrites=overwrites,
                    reason=f"Ticket #{ticket.id} reopened",
                )
            except Exception:
                pass

            await self._clear_claim_lock(interaction.guild, interaction.channel, cfg, ttype)
            await self.set_ticket_claim(ticket.id, None)
            fresh = await self.fetch_ticket_by_id(ticket.id) or fresh
            await self._regen_ticket_header(interaction.guild, interaction.channel, fresh)

            if not interaction.response.is_done():
                await interaction.response.send_message("✅ Ticket reopened.", ephemeral=False)

    async def handle_transcript(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.channel is None or not isinstance(interaction.user, discord.Member):
            if not interaction.response.is_done():
                await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            if not interaction.response.is_done():
                await interaction.response.send_message("This is not a text channel.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        async with self._lock:
            ticket = await self.fetch_ticket_by_channel(interaction.channel.id)
            if ticket is None:
                await interaction.followup.send("This channel is not a tracked ticket.", ephemeral=True)
                return

            if ticket.status != "closed":
                await interaction.followup.send("Transcript can only be saved after the ticket is closed.", ephemeral=True)
                return

            cfg = await self.fetch_config(interaction.guild.id)
            ttype = await self.fetch_ticket_type(interaction.guild.id, ticket.type_key)
            if cfg is None:
                await interaction.followup.send("Ticket config is missing.", ephemeral=True)
                return

            if not self._can_manage_ticket(interaction.user, cfg, ttype, ticket):
                await interaction.followup.send("You can't save this transcript.", ephemeral=True)
                return

            transcript_channel = await self._resolve_closed_transcript_channel(interaction.guild, cfg)
            if transcript_channel is None:
                target_channel_id = int(cfg.transcript_channel_id or CLOSED_TICKET_TRANSCRIPTS_CHANNEL_ID)
                await interaction.followup.send(
                    f"Closed transcript channel <#{target_channel_id}> is missing or inaccessible.",
                    ephemeral=True,
                )
                return

            try:
                file = await self._build_transcript_file(interaction.channel, ticket)
            except Exception:
                await interaction.followup.send("Transcript generation failed.", ephemeral=True)
                return

            actor_mention = interaction.user.mention
            embed = discord.Embed(
                title=f"Closed Ticket Transcript #{ticket.id}",
                color=discord.Color.dark_grey(),
                timestamp=_utc_now(),
            )
            embed.add_field(name="Ticket", value=f"`#{ticket.id}` • {ticket.type_label}", inline=False)
            embed.add_field(name="Opened By", value=f"<@{ticket.creator_id}>", inline=True)
            embed.add_field(name="Saved By", value=actor_mention, inline=True)
            embed.add_field(name="Closed At", value=_fmt_dt(ticket.closed_at), inline=True)
            embed.add_field(name="Reason", value=(ticket.close_reason or "No reason provided")[:1024], inline=False)

            try:
                await transcript_channel.send(embed=embed, file=file)
            except Exception:
                target_channel_id = int(cfg.transcript_channel_id or CLOSED_TICKET_TRANSCRIPTS_CHANNEL_ID)
                await interaction.followup.send(
                    f"Could not send transcript to <#{target_channel_id}>.",
                    ephemeral=True,
                )
                return

            await interaction.followup.send(
                f"✅ Transcript saved in {transcript_channel.mention}. Deleting this closed ticket channel...",
                ephemeral=True,
            )

            try:
                await interaction.channel.delete(reason=f"Ticket #{ticket.id} transcript saved by {interaction.user.id}")
            except Exception:
                await interaction.followup.send(
                    "Transcript was saved, but I couldn't delete this ticket channel. Please delete it manually.",
                    ephemeral=True,
                )

    tickets = app_commands.Group(name="tickets", description="Ticket system commands.")

    @tickets.command(name="setup_defaults", description="Seed default ticket settings, panel, and types.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def tickets_setup_defaults(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await self._auto_seed_defaults_for_guild(interaction.guild)
        ok, msg = await self._refresh_panel_message(interaction.guild)
        await interaction.response.send_message(
            f"{'✅' if ok else '❌'} Defaults seeded.\n{msg}",
            ephemeral=True,
        )

    @tickets.command(name="panel_refresh", description="Refresh the permanent panel if needed.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def tickets_panel_refresh(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        ok, msg = await self._refresh_panel_message(interaction.guild)
        await interaction.response.send_message(("✅ " if ok else "❌ ") + msg, ephemeral=True)

    @tickets.command(name="panel_text", description="Update panel title, description, and image.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def tickets_panel_text(
        self,
        interaction: discord.Interaction,
        title: str,
        description: str,
        image_url: str | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        cfg = await self.fetch_config(interaction.guild.id)
        if cfg is None:
            await interaction.response.send_message("Run `/tickets setup_defaults` first.", ephemeral=True)
            return

        await self.upsert_config(
            interaction.guild.id,
            category_id=cfg.category_id,
            archive_category_id=cfg.archive_category_id,
            log_channel_id=cfg.log_channel_id,
            support_role_id=cfg.support_role_id,
            admin_role_id=cfg.admin_role_id,
            head_mod_role_id=cfg.head_mod_role_id,
            panel_channel_id=cfg.panel_channel_id,
            panel_message_id=cfg.panel_message_id or DEFAULT_PANEL_MESSAGE_ID,
            panel_title=title[:150],
            panel_description=description[:3500],
            panel_image_url=image_url or cfg.panel_image_url,
            max_open_per_user=cfg.max_open_per_user,
            transcript_enabled=cfg.transcript_enabled,
            close_cooldown_s=cfg.close_cooldown_s,
        )
        ok, msg = await self._refresh_panel_message(interaction.guild)
        await interaction.response.send_message(("✅ " if ok else "❌ ") + msg, ephemeral=True)

    @tickets.command(name="set_categories", description="Change default open and closed ticket categories.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def tickets_set_categories(
        self,
        interaction: discord.Interaction,
        open_category: discord.CategoryChannel,
        closed_category: discord.CategoryChannel,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if cfg is None:
            await interaction.response.send_message("Run `/tickets setup_defaults` first.", ephemeral=True)
            return

        await self.upsert_config(
            interaction.guild.id,
            category_id=open_category.id,
            archive_category_id=closed_category.id,
            log_channel_id=cfg.log_channel_id,
            support_role_id=cfg.support_role_id,
            admin_role_id=cfg.admin_role_id,
            head_mod_role_id=cfg.head_mod_role_id,
            panel_channel_id=cfg.panel_channel_id,
            panel_message_id=cfg.panel_message_id or DEFAULT_PANEL_MESSAGE_ID,
            panel_title=cfg.panel_title,
            panel_description=cfg.panel_description,
            panel_image_url=cfg.panel_image_url,
            max_open_per_user=cfg.max_open_per_user,
            transcript_enabled=cfg.transcript_enabled,
            close_cooldown_s=cfg.close_cooldown_s,
        )
        await interaction.response.send_message("✅ Default categories updated.", ephemeral=True)

    @tickets.command(name="set_panel_channel", description="Change the permanent panel channel.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def tickets_set_panel_channel(self, interaction: discord.Interaction, channel: discord.TextChannel) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if cfg is None:
            await interaction.response.send_message("Run `/tickets setup_defaults` first.", ephemeral=True)
            return

        await self.upsert_config(
            interaction.guild.id,
            category_id=cfg.category_id,
            archive_category_id=cfg.archive_category_id,
            log_channel_id=cfg.log_channel_id,
            support_role_id=cfg.support_role_id,
            admin_role_id=cfg.admin_role_id,
            head_mod_role_id=cfg.head_mod_role_id,
            panel_channel_id=channel.id,
            panel_message_id=cfg.panel_message_id or DEFAULT_PANEL_MESSAGE_ID,
            panel_title=cfg.panel_title,
            panel_description=cfg.panel_description,
            panel_image_url=cfg.panel_image_url,
            max_open_per_user=cfg.max_open_per_user,
            transcript_enabled=cfg.transcript_enabled,
            close_cooldown_s=cfg.close_cooldown_s,
        )
        ok, msg = await self._refresh_panel_message(interaction.guild)
        await interaction.response.send_message(("✅ " if ok else "❌ ") + msg, ephemeral=True)

    @tickets.command(name="set_roles", description="Update staff and head mod roles.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def tickets_set_roles(
        self,
        interaction: discord.Interaction,
        staff_role: discord.Role,
        head_mod_role: discord.Role,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if cfg is None:
            await interaction.response.send_message("Run `/tickets setup_defaults` first.", ephemeral=True)
            return

        await self.upsert_config(
            interaction.guild.id,
            category_id=cfg.category_id,
            archive_category_id=cfg.archive_category_id,
            log_channel_id=cfg.log_channel_id,
            support_role_id=staff_role.id,
            admin_role_id=staff_role.id,
            head_mod_role_id=head_mod_role.id,
            panel_channel_id=cfg.panel_channel_id,
            panel_message_id=cfg.panel_message_id or DEFAULT_PANEL_MESSAGE_ID,
            panel_title=cfg.panel_title,
            panel_description=cfg.panel_description,
            panel_image_url=cfg.panel_image_url,
            max_open_per_user=cfg.max_open_per_user,
            transcript_enabled=cfg.transcript_enabled,
            close_cooldown_s=cfg.close_cooldown_s,
        )
        await interaction.response.send_message("✅ Staff roles updated.", ephemeral=True)

    @tickets.command(name="settings", description="View current ticket settings.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def tickets_settings(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        cfg = await self.fetch_config(interaction.guild.id)
        if cfg is None:
            await interaction.response.send_message("Ticket system is not configured.", ephemeral=True)
            return

        types = await self.fetch_ticket_types(interaction.guild.id, enabled_only=False)

        e = discord.Embed(title="Ticket Settings", color=discord.Color(DEFAULT_BLUE), timestamp=_utc_now())
        e.add_field(name="Open Category", value=f"<#{cfg.category_id}>" if cfg.category_id else "None", inline=True)
        e.add_field(name="Closed Category", value=f"<#{cfg.archive_category_id}>" if cfg.archive_category_id else "None", inline=True)
        e.add_field(name="Panel Channel", value=f"<#{cfg.panel_channel_id}>" if cfg.panel_channel_id else "None", inline=True)
        e.add_field(
            name="Transcript Channel",
            value=f"<#{cfg.transcript_channel_id}>" if cfg.transcript_channel_id else "Auto/None",
            inline=True,
        )
        e.add_field(name="Log Channel", value=f"<#{cfg.log_channel_id}>" if cfg.log_channel_id else "None", inline=True)
        e.add_field(name="Staff Role", value=f"<@&{cfg.support_role_id}>" if cfg.support_role_id else "None", inline=True)
        e.add_field(name="Head Mod Role", value=f"<@&{cfg.head_mod_role_id}>" if cfg.head_mod_role_id else "None", inline=True)
        e.add_field(name="Panel Message ID", value=str(cfg.panel_message_id) if cfg.panel_message_id else "None", inline=True)
        e.add_field(name="Max Open Per User", value=str(cfg.max_open_per_user), inline=True)
        e.add_field(name="Transcripts", value="Enabled" if cfg.transcript_enabled else "Disabled", inline=True)

        if types:
            lines = [f"• `{t.type_key}` -> **{t.label}** [{'on' if t.enabled else 'off'}]" for t in types]
            for idx, chunk in enumerate(_chunk_lines(lines, 900), start=1):
                e.add_field(name="Ticket Types" if idx == 1 else f"Ticket Types {idx}", value=chunk, inline=False)

        await interaction.response.send_message(embed=e, ephemeral=True)

    @tickets.command(name="limit", description="Set max open tickets per user.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def tickets_limit(self, interaction: discord.Interaction, amount: app_commands.Range[int, 1, 10]) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if cfg is None:
            await interaction.response.send_message("Run `/tickets setup_defaults` first.", ephemeral=True)
            return

        await self.upsert_config(
            interaction.guild.id,
            category_id=cfg.category_id,
            archive_category_id=cfg.archive_category_id,
            log_channel_id=cfg.log_channel_id,
            support_role_id=cfg.support_role_id,
            admin_role_id=cfg.admin_role_id,
            head_mod_role_id=cfg.head_mod_role_id,
            panel_channel_id=cfg.panel_channel_id,
            panel_message_id=cfg.panel_message_id or DEFAULT_PANEL_MESSAGE_ID,
            panel_title=cfg.panel_title,
            panel_description=cfg.panel_description,
            panel_image_url=cfg.panel_image_url,
            max_open_per_user=int(amount),
            transcript_enabled=cfg.transcript_enabled,
            close_cooldown_s=cfg.close_cooldown_s,
        )
        await interaction.response.send_message(f"✅ Max open tickets per user set to **{amount}**.", ephemeral=True)

    @tickets.command(name="transcripts", description="Enable or disable transcript generation.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def tickets_transcripts(self, interaction: discord.Interaction, enabled: bool) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if cfg is None:
            await interaction.response.send_message("Run `/tickets setup_defaults` first.", ephemeral=True)
            return

        await self.upsert_config(
            interaction.guild.id,
            category_id=cfg.category_id,
            archive_category_id=cfg.archive_category_id,
            log_channel_id=cfg.log_channel_id,
            support_role_id=cfg.support_role_id,
            admin_role_id=cfg.admin_role_id,
            head_mod_role_id=cfg.head_mod_role_id,
            panel_channel_id=cfg.panel_channel_id,
            panel_message_id=cfg.panel_message_id or DEFAULT_PANEL_MESSAGE_ID,
            panel_title=cfg.panel_title,
            panel_description=cfg.panel_description,
            panel_image_url=cfg.panel_image_url,
            max_open_per_user=cfg.max_open_per_user,
            transcript_enabled=enabled,
            close_cooldown_s=cfg.close_cooldown_s,
        )
        await interaction.response.send_message(f"✅ Transcripts {'enabled' if enabled else 'disabled'}.", ephemeral=True)

    @tickets.command(name="type_add", description="Add or update a ticket type.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def tickets_type_add(
        self,
        interaction: discord.Interaction,
        key: str,
        label: str,
        emoji: str | None = None,
        button_style: str | None = "primary",
        category: discord.CategoryChannel | None = None,
        staff_role: discord.Role | None = None,
        question_1: str | None = None,
        question_2: str | None = None,
        question_3: str | None = None,
        sort_order: int = 0,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        cfg = await self.fetch_config(interaction.guild.id)
        if cfg is None:
            await interaction.response.send_message("Run `/tickets setup_defaults` first.", ephemeral=True)
            return

        ckey = _clean_key(key)
        qs = []
        for q in [question_1, question_2, question_3]:
            if q and q.strip():
                qs.append(
                    {
                        "label": q.strip()[:45],
                        "placeholder": "Answer here",
                        "required": True,
                        "style": "paragraph",
                        "max_length": 1000,
                    }
                )

        await self.upsert_ticket_type(
            interaction.guild.id,
            type_key=ckey,
            label=label[:80],
            emoji=emoji,
            button_style=int(_style_from_name(button_style).value),
            category_id=category.id if category else None,
            staff_role_id=staff_role.id if staff_role else cfg.support_role_id,
            questions_json=json.dumps(qs) if qs else None,
            sort_order=int(sort_order),
            enabled=True,
        )

        ok, msg = await self._refresh_panel_message(interaction.guild)
        await interaction.response.send_message(
            f"✅ Ticket type `{ckey}` saved.\n{msg if ok else 'Panel refresh failed.'}",
            ephemeral=True,
        )

    @tickets.command(name="type_toggle", description="Enable or disable a ticket type.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def tickets_type_toggle(self, interaction: discord.Interaction, key: str, enabled: bool) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        t = await self.fetch_ticket_type(interaction.guild.id, _clean_key(key))
        if t is None:
            await interaction.response.send_message("Ticket type not found.", ephemeral=True)
            return

        await self.upsert_ticket_type(
            interaction.guild.id,
            type_key=t.type_key,
            label=t.label,
            emoji=t.emoji,
            button_style=t.button_style,
            category_id=t.category_id,
            staff_role_id=t.staff_role_id,
            questions_json=t.questions_json,
            sort_order=t.sort_order,
            enabled=enabled,
        )
        ok, msg = await self._refresh_panel_message(interaction.guild)
        await interaction.response.send_message(
            f"✅ Ticket type `{t.type_key}` is now {'enabled' if enabled else 'disabled'}.\n{msg if ok else 'Panel refresh failed.'}",
            ephemeral=True,
        )

    @tickets.command(name="type_remove", description="Remove a ticket type.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def tickets_type_remove(self, interaction: discord.Interaction, key: str) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        ok = await self.delete_ticket_type(interaction.guild.id, _clean_key(key))
        if not ok:
            await interaction.response.send_message("Ticket type not found.", ephemeral=True)
            return

        panel_ok, msg = await self._refresh_panel_message(interaction.guild)
        await interaction.response.send_message(
            f"✅ Ticket type removed.\n{msg if panel_ok else 'Panel refresh failed.'}",
            ephemeral=True,
        )

    @tickets.command(name="close", description="Close the current ticket.")
    async def tickets_close(self, interaction: discord.Interaction, reason: str | None = None) -> None:
        if interaction.guild is None or interaction.channel is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("This is not a text channel.", ephemeral=True)
            return

        ticket = await self.fetch_ticket_by_channel(interaction.channel.id)
        if ticket is None:
            await interaction.response.send_message("This channel is not a tracked ticket.", ephemeral=True)
            return

        cfg = await self.fetch_config(interaction.guild.id)
        ttype = await self.fetch_ticket_type(interaction.guild.id, ticket.type_key)
        if cfg is None:
            await interaction.response.send_message("Ticket config is missing.", ephemeral=True)
            return

        if not self._can_manage_ticket(interaction.user, cfg, ttype, ticket):
            await interaction.response.send_message("You can't close this ticket.", ephemeral=True)
            return

        await self._close_ticket(
            guild=interaction.guild,
            channel=interaction.channel,
            actor=interaction.user,
            reason=reason,
            interaction=interaction,
        )

    @tickets.command(name="reopen", description="Reopen the current ticket.")
    async def tickets_reopen(self, interaction: discord.Interaction) -> None:
        await self.handle_reopen(interaction)

    @tickets.command(name="claim", description="Claim the current ticket.")
    async def tickets_claim(self, interaction: discord.Interaction) -> None:
        await self.handle_claim(interaction)

    @tickets.command(name="unclaim", description="Unclaim the current ticket.")
    async def tickets_unclaim(self, interaction: discord.Interaction) -> None:
        await self.handle_unclaim(interaction)

    @tickets.command(name="transcript", description="Export the current ticket transcript.")
    async def tickets_transcript(self, interaction: discord.Interaction) -> None:
        await self.handle_transcript(interaction)

    @tickets.command(name="add", description="Add a user to the current ticket.")
    async def tickets_add(self, interaction: discord.Interaction, user: discord.Member) -> None:
        if interaction.guild is None or interaction.channel is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("This is not a text channel.", ephemeral=True)
            return

        ticket = await self.fetch_ticket_by_channel(interaction.channel.id)
        if ticket is None:
            await interaction.response.send_message("This channel is not a tracked ticket.", ephemeral=True)
            return

        cfg = await self.fetch_config(interaction.guild.id)
        ttype = await self.fetch_ticket_type(interaction.guild.id, ticket.type_key)
        if cfg is None:
            await interaction.response.send_message("Ticket config is missing.", ephemeral=True)
            return

        if not self._is_staff(interaction.user, cfg, ttype):
            await interaction.response.send_message("Only staff can add users to tickets.", ephemeral=True)
            return

        ow = interaction.channel.overwrites_for(user)
        ow.view_channel = True
        ow.send_messages = True
        ow.read_message_history = True
        ow.attach_files = True
        ow.embed_links = True

        try:
            await interaction.channel.set_permissions(user, overwrite=ow, reason=f"Added to ticket #{ticket.id}")
        except Exception:
            await interaction.response.send_message("Failed to add that user.", ephemeral=True)
            return

        await self.add_ticket_member(ticket.id, user.id, interaction.user.id)
        await interaction.response.send_message(f"✅ Added {user.mention} to this ticket.", ephemeral=False)

    @tickets.command(name="remove", description="Remove a user from the current ticket.")
    async def tickets_remove(self, interaction: discord.Interaction, user: discord.Member) -> None:
        if interaction.guild is None or interaction.channel is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("This is not a text channel.", ephemeral=True)
            return

        ticket = await self.fetch_ticket_by_channel(interaction.channel.id)
        if ticket is None:
            await interaction.response.send_message("This channel is not a tracked ticket.", ephemeral=True)
            return

        cfg = await self.fetch_config(interaction.guild.id)
        ttype = await self.fetch_ticket_type(interaction.guild.id, ticket.type_key)
        if cfg is None:
            await interaction.response.send_message("Ticket config is missing.", ephemeral=True)
            return

        if user.id == ticket.creator_id:
            await interaction.response.send_message("You can't remove the ticket opener.", ephemeral=True)
            return

        if not self._is_staff(interaction.user, cfg, ttype):
            await interaction.response.send_message("Only staff can remove users from tickets.", ephemeral=True)
            return

        try:
            await interaction.channel.set_permissions(user, overwrite=None, reason=f"Removed from ticket #{ticket.id}")
        except Exception:
            await interaction.response.send_message("Failed to remove that user.", ephemeral=True)
            return

        await self.remove_ticket_member(ticket.id, user.id)
        await interaction.response.send_message(f"✅ Removed {user.mention} from this ticket.", ephemeral=False)

    @tickets.command(name="rename", description="Rename the current ticket channel.")
    async def tickets_rename(self, interaction: discord.Interaction, name: str) -> None:
        if interaction.guild is None or interaction.channel is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("This is not a text channel.", ephemeral=True)
            return

        ticket = await self.fetch_ticket_by_channel(interaction.channel.id)
        if ticket is None:
            await interaction.response.send_message("This channel is not a tracked ticket.", ephemeral=True)
            return

        cfg = await self.fetch_config(interaction.guild.id)
        ttype = await self.fetch_ticket_type(interaction.guild.id, ticket.type_key)
        if cfg is None:
            await interaction.response.send_message("Ticket config is missing.", ephemeral=True)
            return

        if not self._is_staff(interaction.user, cfg, ttype):
            await interaction.response.send_message("Only staff can rename tickets.", ephemeral=True)
            return

        new_name = _clean_channel_fragment(name)
        try:
            await interaction.channel.edit(name=new_name, reason=f"Ticket #{ticket.id} renamed")
        except Exception:
            await interaction.response.send_message("Rename failed.", ephemeral=True)
            return

        await interaction.response.send_message(f"✅ Ticket renamed to `{new_name}`.", ephemeral=False)

    @tickets.command(name="info", description="Show info for the current ticket.")
    async def tickets_info(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.channel is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("This is not a text channel.", ephemeral=True)
            return

        ticket = await self.fetch_ticket_by_channel(interaction.channel.id)
        if ticket is None:
            await interaction.response.send_message("This channel is not a tracked ticket.", ephemeral=True)
            return

        cfg = await self.fetch_config(interaction.guild.id)
        ttype = await self.fetch_ticket_type(interaction.guild.id, ticket.type_key)
        opener = interaction.guild.get_member(ticket.creator_id) or self.bot.get_user(ticket.creator_id)
        if cfg is None:
            await interaction.response.send_message("Ticket config is missing.", ephemeral=True)
            return

        e = await self._build_ticket_header_embed(interaction.guild, ticket, cfg, ttype, opener, answers=None)

        members = await self.fetch_ticket_members(ticket.id)
        if members:
            lines = [f"<@{uid}>" for uid in members]
            for idx, chunk in enumerate(_chunk_lines(lines, 900), start=1):
                e.add_field(name="Added Users" if idx == 1 else f"Added Users {idx}", value=chunk, inline=False)

        e.add_field(name="Created", value=_fmt_dt(ticket.created_at), inline=True)
        e.add_field(name="Closed", value=_fmt_dt(ticket.closed_at) if ticket.closed_at else "Not closed", inline=True)
        e.add_field(name="Channel", value=interaction.channel.mention, inline=True)

        await interaction.response.send_message(embed=e, ephemeral=True)

    @tickets.command(name="my_open", description="List your currently open tickets.")
    async def tickets_my_open(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        rows = await self.fetch_open_tickets_for_user(interaction.guild.id, interaction.user.id, limit=10)
        if not rows:
            await interaction.response.send_message("You have no open tickets right now.", ephemeral=True)
            return

        e = discord.Embed(
            title="Your Open Tickets",
            color=discord.Color(DEFAULT_BLUE),
            timestamp=_utc_now(),
        )
        lines: list[str] = []
        for t in rows:
            channel = interaction.guild.get_channel(t.channel_id)
            channel_ref = channel.mention if isinstance(channel, discord.TextChannel) else f"`deleted-channel:{t.channel_id}`"
            claimed = f"<@{t.claimed_by_id}>" if t.claimed_by_id else "Unclaimed"
            lines.append(
                f"• `#{t.id}` {channel_ref} • **{t.type_label}** • {claimed} • Opened {_fmt_dt(t.created_at)}"
            )

        for idx, chunk in enumerate(_chunk_lines(lines, 900), start=1):
            e.add_field(name="Tickets" if idx == 1 else f"Tickets {idx}", value=chunk, inline=False)

        await interaction.response.send_message(embed=e, ephemeral=True)

    @tickets.command(name="queue", description="Show currently open tickets for staff triage.")
    async def tickets_queue(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        cfg = await self.fetch_config(interaction.guild.id)
        if cfg is None:
            await interaction.response.send_message("Ticket system is not configured.", ephemeral=True)
            return

        if not self._is_staff(interaction.user, cfg, None):
            await interaction.response.send_message("Only staff can view the queue.", ephemeral=True)
            return

        rows = await self.fetch_open_tickets_for_guild(interaction.guild.id, limit=25)
        if not rows:
            await interaction.response.send_message("No open tickets in queue.", ephemeral=True)
            return

        e = discord.Embed(
            title="Open Ticket Queue",
            color=discord.Color.orange(),
            timestamp=_utc_now(),
            description="Oldest tickets are listed first.",
        )

        lines: list[str] = []
        for t in rows:
            channel = interaction.guild.get_channel(t.channel_id)
            channel_ref = channel.mention if isinstance(channel, discord.TextChannel) else f"`deleted-channel:{t.channel_id}`"
            claimer = f"<@{t.claimed_by_id}>" if t.claimed_by_id else "**Unclaimed**"
            lines.append(
                f"• `#{t.id}` {channel_ref} • {claimer} • Opened {_fmt_dt(t.created_at)} • by <@{t.creator_id}> • `{t.type_key}`"
            )

        for idx, chunk in enumerate(_chunk_lines(lines, 900), start=1):
            e.add_field(name="Queue" if idx == 1 else f"Queue {idx}", value=chunk, inline=False)

        await interaction.response.send_message(embed=e, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(TicketsCog(bot))
