from __future__ import annotations

import asyncio
import json
import logging
import re
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import text

from services.db import sessions

log = logging.getLogger(__name__)


YOUTUBE_POLL_SECONDS = 300


@dataclass(slots=True)
class GuildClientConfig:
    guild_id: int
    welcome_channel_id: int | None
    welcome_message: str | None
    autorole_ids_json: str | None
    modlog_channel_id: int | None
    mute_role_id: int | None
    youtube_source_channel_id: str | None
    youtube_target_channel_id: int | None
    youtube_ping_mode: str
    youtube_ping_role_id: int | None
    youtube_last_video_id: str | None

    @property
    def autorole_ids(self) -> list[int]:
        try:
            raw = json.loads(self.autorole_ids_json or "[]")
            return [int(x) for x in raw if str(x).isdigit()]
        except Exception:
            return []


@dataclass(slots=True)
class SelfRolePanel:
    message_id: int
    guild_id: int
    channel_id: int
    title: str
    description: str
    role_ids_json: str

    @property
    def role_ids(self) -> list[int]:
        try:
            raw = json.loads(self.role_ids_json or "[]")
            return [int(x) for x in raw if str(x).isdigit()]
        except Exception:
            return []


@dataclass(slots=True)
class YoutubeVideo:
    video_id: str
    title: str
    url: str
    published: str
    description: str


class SelfRoleSelect(discord.ui.Select):
    def __init__(self, cog: "ClientCoreCog", panel_message_id: int, role_ids: list[int], guild: discord.Guild):
        self.cog = cog
        self.panel_message_id = panel_message_id
        options: list[discord.SelectOption] = []
        for rid in role_ids:
            role = guild.get_role(rid)
            if role is None:
                continue
            options.append(discord.SelectOption(label=role.name[:100], value=str(role.id)))

        super().__init__(
            placeholder="Select roles",
            min_values=0,
            max_values=max(1, len(options)),
            options=options[:25],
            custom_id=f"selfrole_select:{panel_message_id}",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        panel = await self.cog.fetch_panel(self.panel_message_id)
        if panel is None:
            await interaction.response.send_message("This role panel is no longer configured.", ephemeral=True)
            return

        selectable_ids = set(panel.role_ids)
        selected_ids = {int(v) for v in self.values if v.isdigit()}

        current_selectable = {r.id for r in interaction.user.roles if r.id in selectable_ids}
        to_add_ids = selected_ids - current_selectable
        to_remove_ids = current_selectable - selected_ids

        add_roles = [interaction.guild.get_role(rid) for rid in to_add_ids]
        remove_roles = [interaction.guild.get_role(rid) for rid in to_remove_ids]
        add_roles = [r for r in add_roles if r is not None]
        remove_roles = [r for r in remove_roles if r is not None]

        try:
            if add_roles:
                await interaction.user.add_roles(*add_roles, reason=f"Self-role panel {self.panel_message_id}")
            if remove_roles:
                await interaction.user.remove_roles(*remove_roles, reason=f"Self-role panel {self.panel_message_id}")
        except discord.Forbidden:
            await interaction.response.send_message("I cannot edit one or more of those roles due to permissions.", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message("Role update failed. Please try again.", ephemeral=True)
            return

        await interaction.response.send_message("✅ Your roles were updated.", ephemeral=True)


class SelfRoleView(discord.ui.View):
    def __init__(self, cog: "ClientCoreCog", panel: SelfRolePanel, guild: discord.Guild):
        super().__init__(timeout=None)
        self.add_item(SelfRoleSelect(cog, panel.message_id, panel.role_ids, guild))


class ClientCoreCog(commands.Cog):
    client = app_commands.Group(name="client", description="Client configuration commands.")
    mod = app_commands.Group(name="mod", description="Moderation commands.")
    selfroles = app_commands.Group(name="selfroles", description="Self role panel commands.")
    youtube = app_commands.Group(name="youtube", description="YouTube notifications.")

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self._youtube_task: asyncio.Task | None = None
        self._yt_lock = asyncio.Lock()

    async def cog_load(self) -> None:
        await self._ensure_tables()
        await self._register_persistent_selfrole_views()
        self._youtube_task = asyncio.create_task(self._youtube_loop(), name="client.youtube.loop")

    async def cog_unload(self) -> None:
        if self._youtube_task:
            self._youtube_task.cancel()

    async def _ensure_tables(self) -> None:
        sql_config = """
        CREATE TABLE IF NOT EXISTS client_config (
            guild_id BIGINT PRIMARY KEY,
            welcome_channel_id BIGINT NULL,
            welcome_message TEXT NULL,
            autorole_ids_json TEXT NULL,
            modlog_channel_id BIGINT NULL,
            mute_role_id BIGINT NULL,
            youtube_source_channel_id VARCHAR(64) NULL,
            youtube_target_channel_id BIGINT NULL,
            youtube_ping_mode VARCHAR(16) NOT NULL DEFAULT 'none',
            youtube_ping_role_id BIGINT NULL,
            youtube_last_video_id VARCHAR(64) NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        sql_warns = """
        CREATE TABLE IF NOT EXISTS mod_warnings (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            moderator_id BIGINT NOT NULL,
            reason TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY ix_warns_guild_user (guild_id, user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        sql_panels = """
        CREATE TABLE IF NOT EXISTS selfrole_panels (
            message_id BIGINT PRIMARY KEY,
            guild_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            title VARCHAR(120) NOT NULL,
            description TEXT NOT NULL,
            role_ids_json TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY ix_selfrole_guild (guild_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(sql_config))
                await session.execute(text(sql_warns))
                await session.execute(text(sql_panels))

    async def fetch_config(self, guild_id: int) -> GuildClientConfig:
        async with self.sessionmaker() as session:
            row = (
                await session.execute(
                    text("SELECT * FROM client_config WHERE guild_id=:gid"),
                    {"gid": int(guild_id)},
                )
            ).mappings().first()
            if row is None:
                await session.execute(text("INSERT INTO client_config (guild_id, autorole_ids_json) VALUES (:gid, '[]')"), {"gid": int(guild_id)})
                await session.commit()
                return GuildClientConfig(
                    guild_id=int(guild_id),
                    welcome_channel_id=None,
                    welcome_message=None,
                    autorole_ids_json="[]",
                    modlog_channel_id=None,
                    mute_role_id=None,
                    youtube_source_channel_id=None,
                    youtube_target_channel_id=None,
                    youtube_ping_mode="none",
                    youtube_ping_role_id=None,
                    youtube_last_video_id=None,
                )
            return GuildClientConfig(**row)

    async def update_config(self, guild_id: int, **fields: Any) -> None:
        if not fields:
            return
        parts = [f"{k}=:{k}" for k in fields]
        payload = {k: v for k, v in fields.items()}
        payload["gid"] = int(guild_id)
        sql = f"UPDATE client_config SET {', '.join(parts)} WHERE guild_id=:gid"
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(sql), payload)

    async def _register_persistent_selfrole_views(self) -> None:
        async with self.sessionmaker() as session:
            rows = (await session.execute(text("SELECT * FROM selfrole_panels"))).mappings().all()
        for row in rows:
            panel = SelfRolePanel(**row)
            guild = self.bot.get_guild(panel.guild_id)
            if guild is None:
                continue
            self.bot.add_view(SelfRoleView(self, panel, guild), message_id=panel.message_id)

    async def fetch_panel(self, message_id: int) -> SelfRolePanel | None:
        async with self.sessionmaker() as session:
            row = (await session.execute(text("SELECT * FROM selfrole_panels WHERE message_id=:mid"), {"mid": int(message_id)})).mappings().first()
            return SelfRolePanel(**row) if row else None

    async def _send_modlog(self, guild: discord.Guild, cfg: GuildClientConfig, title: str, description: str) -> None:
        if not cfg.modlog_channel_id:
            return
        ch = guild.get_channel(int(cfg.modlog_channel_id))
        if not isinstance(ch, discord.TextChannel):
            return
        e = discord.Embed(title=title, description=description, color=discord.Color.orange(), timestamp=datetime.now(timezone.utc))
        try:
            await ch.send(embed=e)
        except Exception:
            pass

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member) -> None:
        cfg = await self.fetch_config(member.guild.id)

        roles_to_add = []
        missing = []
        for rid in cfg.autorole_ids:
            role = member.guild.get_role(rid)
            if role is None:
                missing.append(str(rid))
                continue
            roles_to_add.append(role)

        if roles_to_add:
            try:
                await member.add_roles(*roles_to_add, reason="Configured auto role on join")
            except Exception:
                await self._send_modlog(member.guild, cfg, "Auto-role failure", f"Failed adding roles to {member.mention}.")

        if missing:
            await self._send_modlog(member.guild, cfg, "Auto-role missing roles", f"Configured auto-role IDs missing: {', '.join(missing)}")

        if cfg.welcome_channel_id:
            ch = member.guild.get_channel(int(cfg.welcome_channel_id))
            if isinstance(ch, discord.TextChannel):
                message = cfg.welcome_message or "Welcome to **{server}**, {user}!"
                rendered = message.replace("{user}", member.mention).replace("{server}", member.guild.name)
                try:
                    await ch.send(rendered)
                except Exception:
                    await self._send_modlog(member.guild, cfg, "Welcome message failure", f"Could not send welcome message for {member.mention}.")

    @mod.command(name="warn", description="Warn a member.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def mod_warn(self, interaction: discord.Interaction, member: discord.Member, reason: str) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text("INSERT INTO mod_warnings (guild_id,user_id,moderator_id,reason) VALUES (:g,:u,:m,:r)"),
                    {"g": interaction.guild.id, "u": member.id, "m": interaction.user.id, "r": reason[:1000]},
                )
        cfg = await self.fetch_config(interaction.guild.id)
        await self._send_modlog(interaction.guild, cfg, "Member Warned", f"{member.mention} warned by {interaction.user.mention}\nReason: {reason}")
        await interaction.response.send_message(f"✅ Warned {member.mention}.", ephemeral=True)

    @mod.command(name="warnings", description="View warnings for a member.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def mod_warnings(self, interaction: discord.Interaction, member: discord.Member) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    text("SELECT id, moderator_id, reason, created_at FROM mod_warnings WHERE guild_id=:g AND user_id=:u ORDER BY id DESC LIMIT 10"),
                    {"g": interaction.guild.id, "u": member.id},
                )
            ).mappings().all()
        if not rows:
            await interaction.response.send_message("No warnings for that user.", ephemeral=True)
            return
        lines = [f"`#{r['id']}` by <@{r['moderator_id']}> • {r['reason'][:120]}" for r in rows]
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @mod.command(name="purge", description="Delete recent messages.")
    @app_commands.checks.has_permissions(manage_messages=True)
    async def mod_purge(self, interaction: discord.Interaction, amount: app_commands.Range[int, 1, 100]) -> None:
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("Text channels only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        deleted = await interaction.channel.purge(limit=int(amount))
        await interaction.followup.send(f"✅ Deleted {len(deleted)} messages.", ephemeral=True)

    @mod.command(name="timeout", description="Timeout a member.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def mod_timeout(self, interaction: discord.Interaction, member: discord.Member, minutes: app_commands.Range[int, 1, 40320], reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        until = discord.utils.utcnow() + timedelta(minutes=int(minutes))
        try:
            await member.timeout(until, reason=f"{reason} | by {interaction.user.id}")
        except Exception as exc:
            await interaction.response.send_message(f"Timeout failed: {exc}", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        await self._send_modlog(interaction.guild, cfg, "Member Timed Out", f"{member.mention} for {minutes}m\nReason: {reason}")
        await interaction.response.send_message(f"✅ Timed out {member.mention} for {minutes} minute(s).", ephemeral=True)

    @mod.command(name="mute", description="Apply configured mute role.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def mod_mute(self, interaction: discord.Interaction, member: discord.Member, reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if not cfg.mute_role_id:
            await interaction.response.send_message("No mute role configured. Use `/client set_mute_role`.", ephemeral=True)
            return
        role = interaction.guild.get_role(int(cfg.mute_role_id))
        if role is None:
            await interaction.response.send_message("Configured mute role no longer exists.", ephemeral=True)
            return
        try:
            await member.add_roles(role, reason=f"Mute by {interaction.user.id}: {reason}")
        except Exception:
            await interaction.response.send_message("Mute failed due to permissions.", ephemeral=True)
            return
        await self._send_modlog(interaction.guild, cfg, "Member Muted", f"{member.mention}\nReason: {reason}")
        await interaction.response.send_message(f"✅ Muted {member.mention}.", ephemeral=True)

    @mod.command(name="unmute", description="Remove configured mute role.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def mod_unmute(self, interaction: discord.Interaction, member: discord.Member) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if not cfg.mute_role_id:
            await interaction.response.send_message("No mute role configured.", ephemeral=True)
            return
        role = interaction.guild.get_role(int(cfg.mute_role_id))
        if role is None:
            await interaction.response.send_message("Configured mute role no longer exists.", ephemeral=True)
            return
        try:
            await member.remove_roles(role, reason=f"Unmute by {interaction.user.id}")
        except Exception:
            await interaction.response.send_message("Unmute failed due to permissions.", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Unmuted {member.mention}.", ephemeral=True)

    @mod.command(name="kick", description="Kick a member.")
    @app_commands.checks.has_permissions(kick_members=True)
    async def mod_kick(self, interaction: discord.Interaction, member: discord.Member, reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        try:
            await member.kick(reason=f"{reason} | by {interaction.user.id}")
        except Exception:
            await interaction.response.send_message("Kick failed due to permissions.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        await self._send_modlog(interaction.guild, cfg, "Member Kicked", f"{member}\nReason: {reason}")
        await interaction.response.send_message(f"✅ Kicked {member}.", ephemeral=True)

    @mod.command(name="ban", description="Ban a member.")
    @app_commands.checks.has_permissions(ban_members=True)
    async def mod_ban(self, interaction: discord.Interaction, member: discord.Member, reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        try:
            await interaction.guild.ban(member, reason=f"{reason} | by {interaction.user.id}")
        except Exception:
            await interaction.response.send_message("Ban failed due to permissions.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        await self._send_modlog(interaction.guild, cfg, "Member Banned", f"{member}\nReason: {reason}")
        await interaction.response.send_message(f"✅ Banned {member}.", ephemeral=True)

    @mod.command(name="unban", description="Unban a user by ID.")
    @app_commands.checks.has_permissions(ban_members=True)
    async def mod_unban(self, interaction: discord.Interaction, user_id: str, reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not user_id.isdigit():
            await interaction.response.send_message("Provide a numeric user ID.", ephemeral=True)
            return
        try:
            await interaction.guild.unban(discord.Object(id=int(user_id)), reason=f"{reason} | by {interaction.user.id}")
        except Exception:
            await interaction.response.send_message("Unban failed. Ensure the user is banned and ID is correct.", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Unbanned `{user_id}`.", ephemeral=True)

    @client.command(name="welcome_set", description="Configure welcome channel and template.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def client_welcome_set(self, interaction: discord.Interaction, channel: discord.TextChannel, message: str) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await self.fetch_config(interaction.guild.id)
        await self.update_config(interaction.guild.id, welcome_channel_id=channel.id, welcome_message=message[:1800])
        await interaction.response.send_message("✅ Welcome system updated. Supports `{user}` and `{server}` placeholders.", ephemeral=True)

    @client.command(name="welcome_disable", description="Disable welcome messages.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def client_welcome_disable(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await self.fetch_config(interaction.guild.id)
        await self.update_config(interaction.guild.id, welcome_channel_id=None)
        await interaction.response.send_message("✅ Welcome messages disabled.", ephemeral=True)

    @client.command(name="autorole_add", description="Add an automatic role for new members.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def client_autorole_add(self, interaction: discord.Interaction, role: discord.Role) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        ids = set(cfg.autorole_ids)
        ids.add(role.id)
        await self.update_config(interaction.guild.id, autorole_ids_json=json.dumps(sorted(ids)))
        await interaction.response.send_message(f"✅ Added {role.mention} to auto-role list.", ephemeral=True)

    @client.command(name="autorole_remove", description="Remove an automatic role.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def client_autorole_remove(self, interaction: discord.Interaction, role: discord.Role) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        ids = [rid for rid in cfg.autorole_ids if rid != role.id]
        await self.update_config(interaction.guild.id, autorole_ids_json=json.dumps(ids))
        await interaction.response.send_message(f"✅ Removed {role.mention} from auto-role list.", ephemeral=True)

    @client.command(name="autoroles", description="List automatic roles.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def client_autoroles(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if not cfg.autorole_ids:
            await interaction.response.send_message("No auto-roles configured.", ephemeral=True)
            return
        roles = [interaction.guild.get_role(rid) for rid in cfg.autorole_ids]
        roles = [r.mention for r in roles if r is not None]
        await interaction.response.send_message("Auto-roles: " + ", ".join(roles), ephemeral=True)

    @client.command(name="modlog_set", description="Set moderation log channel.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def client_modlog_set(self, interaction: discord.Interaction, channel: discord.TextChannel) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await self.fetch_config(interaction.guild.id)
        await self.update_config(interaction.guild.id, modlog_channel_id=channel.id)
        await interaction.response.send_message(f"✅ Modlog set to {channel.mention}.", ephemeral=True)

    @client.command(name="set_mute_role", description="Set role used by /mod mute.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def client_set_mute_role(self, interaction: discord.Interaction, role: discord.Role) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await self.fetch_config(interaction.guild.id)
        await self.update_config(interaction.guild.id, mute_role_id=role.id)
        await interaction.response.send_message(f"✅ Mute role set to {role.mention}.", ephemeral=True)

    @client.command(name="post", description="Post a simple message as the bot.")
    @app_commands.checks.has_permissions(manage_messages=True)
    async def client_post(self, interaction: discord.Interaction, channel: discord.TextChannel, message: str) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not message.strip():
            await interaction.response.send_message("Message cannot be empty.", ephemeral=True)
            return
        try:
            await channel.send(message[:1900])
        except Exception as exc:
            await interaction.response.send_message(f"Failed to post message: {exc}", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Message posted in {channel.mention}.", ephemeral=True)

    @selfroles.command(name="create", description="Create a self-role panel.")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def selfroles_create(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        title: str,
        description: str,
        role_1: discord.Role,
        role_2: discord.Role | None = None,
        role_3: discord.Role | None = None,
        role_4: discord.Role | None = None,
        role_5: discord.Role | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        role_ids = [r.id for r in [role_1, role_2, role_3, role_4, role_5] if r is not None]
        role_ids = sorted(set(role_ids))[:25]

        embed = discord.Embed(title=title[:120], description=description[:3000], color=discord.Color.blurple())
        panel = SelfRolePanel(message_id=0, guild_id=interaction.guild.id, channel_id=channel.id, title=title[:120], description=description[:3000], role_ids_json=json.dumps(role_ids))
        view = SelfRoleView(self, panel, interaction.guild)
        sent = await channel.send(embed=embed, view=view)

        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text("INSERT INTO selfrole_panels (message_id,guild_id,channel_id,title,description,role_ids_json) VALUES (:m,:g,:c,:t,:d,:r)"),
                    {"m": sent.id, "g": interaction.guild.id, "c": channel.id, "t": panel.title, "d": panel.description, "r": panel.role_ids_json},
                )

        self.bot.add_view(SelfRoleView(self, SelfRolePanel(sent.id, panel.guild_id, panel.channel_id, panel.title, panel.description, panel.role_ids_json), interaction.guild), message_id=sent.id)
        await interaction.response.send_message(f"✅ Self-role panel created in {channel.mention}.", ephemeral=True)

    @youtube.command(name="configure", description="Configure YouTube source + target notification channel.")
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(source_channel_id="YouTube channel ID", ping_mode="none/everyone/role")
    async def youtube_configure(
        self,
        interaction: discord.Interaction,
        source_channel_id: str,
        target_channel: discord.TextChannel,
        ping_mode: str = "none",
        ping_role: discord.Role | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not re.fullmatch(r"[A-Za-z0-9_-]{10,40}", source_channel_id):
            await interaction.response.send_message("That does not look like a valid YouTube channel ID.", ephemeral=True)
            return
        ping_mode = ping_mode.lower()
        if ping_mode not in {"none", "everyone", "role"}:
            await interaction.response.send_message("Ping mode must be one of: none, everyone, role.", ephemeral=True)
            return
        if ping_mode == "role" and ping_role is None:
            await interaction.response.send_message("Pick a role when ping mode is `role`.", ephemeral=True)
            return

        await self.fetch_config(interaction.guild.id)
        latest = await asyncio.to_thread(self._fetch_latest_video, source_channel_id)
        await self.update_config(
            interaction.guild.id,
            youtube_source_channel_id=source_channel_id,
            youtube_target_channel_id=target_channel.id,
            youtube_ping_mode=ping_mode,
            youtube_ping_role_id=ping_role.id if ping_role else None,
            youtube_last_video_id=latest.video_id if latest else None,
        )
        await interaction.response.send_message(
            f"✅ YouTube notifications configured for `{source_channel_id}` in {target_channel.mention}.\n"
            "Baseline saved from latest video to avoid reposting old uploads.",
            ephemeral=True,
        )

    @youtube.command(name="check_now", description="Poll YouTube feeds immediately.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def youtube_check_now(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        posted = await self._run_youtube_scan(target_guild_id=interaction.guild.id)
        await interaction.followup.send(f"✅ Check complete. New posts sent: {posted}.", ephemeral=True)

    @youtube.command(name="disable", description="Disable YouTube notifications.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def youtube_disable(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await self.fetch_config(interaction.guild.id)
        await self.update_config(
            interaction.guild.id,
            youtube_source_channel_id=None,
            youtube_target_channel_id=None,
            youtube_ping_mode="none",
            youtube_ping_role_id=None,
        )
        await interaction.response.send_message("✅ YouTube notifications disabled.", ephemeral=True)

    @youtube.command(name="status", description="Show YouTube notification settings.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def youtube_status(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        target = f"<#{cfg.youtube_target_channel_id}>" if cfg.youtube_target_channel_id else "None"
        src = cfg.youtube_source_channel_id or "None"
        ping = cfg.youtube_ping_mode
        if cfg.youtube_ping_mode == "role" and cfg.youtube_ping_role_id:
            ping += f" (<@&{cfg.youtube_ping_role_id}>)"
        await interaction.response.send_message(
            f"Source: `{src}`\nTarget: {target}\nPing: {ping}\nLast video ID: `{cfg.youtube_last_video_id or 'None'}`",
            ephemeral=True,
        )

    async def _youtube_loop(self) -> None:
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                await self._run_youtube_scan()
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("YouTube loop tick failed")
            await asyncio.sleep(YOUTUBE_POLL_SECONDS)

    async def _run_youtube_scan(self, target_guild_id: int | None = None) -> int:
        posted = 0
        async with self._yt_lock:
            async with self.sessionmaker() as session:
                rows = (await session.execute(text("SELECT * FROM client_config WHERE youtube_source_channel_id IS NOT NULL AND youtube_target_channel_id IS NOT NULL"))).mappings().all()

            for row in rows:
                cfg = GuildClientConfig(**row)
                if target_guild_id and cfg.guild_id != target_guild_id:
                    continue
                guild = self.bot.get_guild(cfg.guild_id)
                if guild is None:
                    continue
                target = guild.get_channel(int(cfg.youtube_target_channel_id or 0))
                if not isinstance(target, discord.TextChannel):
                    continue

                video = await asyncio.to_thread(self._fetch_latest_video, str(cfg.youtube_source_channel_id))
                if video is None:
                    continue
                if cfg.youtube_last_video_id == video.video_id:
                    continue

                prefix = ""
                if cfg.youtube_ping_mode == "everyone":
                    prefix = "@everyone "
                elif cfg.youtube_ping_mode == "role" and cfg.youtube_ping_role_id:
                    prefix = f"<@&{cfg.youtube_ping_role_id}> "

                description_excerpt = (video.description or "").strip()
                if description_excerpt:
                    description_excerpt = description_excerpt[:300]
                embed = discord.Embed(
                    title=video.title[:250],
                    description=(description_excerpt + "\n\n" if description_excerpt else "") + f"[Watch on YouTube]({video.url})",
                    color=discord.Color.red(),
                    timestamp=datetime.now(timezone.utc),
                )
                embed.set_footer(text="New YouTube upload")

                try:
                    await target.send(content=(prefix + "📺 New upload!"), embed=embed, allowed_mentions=discord.AllowedMentions(everyone=True, roles=True))
                except Exception:
                    continue

                await self.update_config(cfg.guild_id, youtube_last_video_id=video.video_id)
                posted += 1
        return posted

    def _fetch_latest_video(self, source_channel_id: str) -> YoutubeVideo | None:
        url = f"https://www.youtube.com/feeds/videos.xml?channel_id={source_channel_id}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 FlameBot"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read()
        except Exception:
            return None

        try:
            root = ET.fromstring(raw)
        except Exception:
            return None

        ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "yt": "http://www.youtube.com/xml/schemas/2015",
            "media": "http://search.yahoo.com/mrss/",
        }
        entry = root.find("atom:entry", ns)
        if entry is None:
            return None
        vid = (entry.findtext("yt:videoId", default="", namespaces=ns) or "").strip()
        title = (entry.findtext("atom:title", default="", namespaces=ns) or "New Video").strip()
        link_node = entry.find("atom:link", ns)
        link = link_node.attrib.get("href") if link_node is not None else ""
        if not link and vid:
            link = f"https://youtu.be/{vid}"
        published = (entry.findtext("atom:published", default="", namespaces=ns) or "").strip()
        description = (entry.findtext("media:group/media:description", default="", namespaces=ns) or "").strip()
        if not vid:
            return None
        return YoutubeVideo(video_id=vid, title=title, url=link, published=published, description=description)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ClientCoreCog(bot))
