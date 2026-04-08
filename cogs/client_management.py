from __future__ import annotations

import asyncio
import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
from sqlalchemy import text

from services.db import sessions


YOUTUBE_POLL_SECONDS = 300


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_json_load(raw: str | None, default):
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _fmt_error(exc: Exception) -> str:
    return f"{exc.__class__.__name__}: {exc}"


def _extract_first_link(text_value: str | None) -> str | None:
    if not text_value:
        return None
    m = re.search(r"https?://\S+", text_value)
    return m.group(0) if m else None


@dataclass
class GuildConfig:
    guild_id: int
    welcome_channel_id: int | None
    welcome_message: str
    autoroles_json: str | None
    modlog_channel_id: int | None
    mute_role_id: int | None
    selfrole_channel_id: int | None
    selfrole_message_id: int | None
    youtube_source_channel_id: str | None
    youtube_target_channel_id: int | None
    youtube_ping_mode: str
    youtube_ping_role_id: int | None
    youtube_last_video_id: str | None


@dataclass
class SelfRoleOption:
    guild_id: int
    role_id: int
    label: str
    emoji: str | None
    sort_order: int
    enabled: bool


class SelfRoleSelect(discord.ui.Select):
    def __init__(self, cog: "ClientManagementCog", guild_id: int, options: list[SelfRoleOption]):
        self.cog = cog
        self.guild_id = int(guild_id)

        select_options: list[discord.SelectOption] = []
        for item in options[:25]:
            select_options.append(
                discord.SelectOption(
                    label=item.label[:100],
                    value=str(item.role_id),
                    emoji=item.emoji or None,
                )
            )

        super().__init__(
            custom_id=f"selfroles:select:{self.guild_id}",
            placeholder="Pick your roles",
            min_values=0,
            max_values=len(select_options) if select_options else 1,
            options=select_options or [discord.SelectOption(label="No roles configured", value="none")],
            disabled=not bool(select_options),
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        configured = await self.cog.fetch_selfrole_options(interaction.guild.id, enabled_only=True)
        allowed_ids = {int(x.role_id) for x in configured}
        requested_ids = {int(v) for v in self.values if v.isdigit() and int(v) in allowed_ids}

        add_roles: list[discord.Role] = []
        remove_roles: list[discord.Role] = []
        for role_id in allowed_ids:
            role = interaction.guild.get_role(role_id)
            if role is None:
                continue
            has_role = any(r.id == role_id for r in interaction.user.roles)
            if role_id in requested_ids and not has_role:
                add_roles.append(role)
            elif role_id not in requested_ids and has_role:
                remove_roles.append(role)

        if not add_roles and not remove_roles:
            await interaction.response.send_message("No role changes were needed.", ephemeral=True)
            return

        try:
            if add_roles:
                await interaction.user.add_roles(*add_roles, reason="Self-role selection")
            if remove_roles:
                await interaction.user.remove_roles(*remove_roles, reason="Self-role selection")
        except discord.Forbidden:
            await interaction.response.send_message("I can't update your roles due to role hierarchy/permissions.", ephemeral=True)
            return
        except discord.HTTPException as exc:
            await interaction.response.send_message(f"Role update failed: {exc}", ephemeral=True)
            return

        add_txt = ", ".join(r.mention for r in add_roles) if add_roles else "None"
        rem_txt = ", ".join(r.mention for r in remove_roles) if remove_roles else "None"
        await interaction.response.send_message(f"✅ Updated roles. Added: {add_txt} | Removed: {rem_txt}", ephemeral=True)


class SelfRoleView(discord.ui.View):
    def __init__(self, cog: "ClientManagementCog", guild_id: int, options: list[SelfRoleOption]):
        super().__init__(timeout=None)
        self.add_item(SelfRoleSelect(cog, guild_id, options))


class ClientManagementCog(commands.Cog):
    TABLE_CONFIG = "client_guild_config"
    TABLE_SELFROLES = "client_selfroles"
    TABLE_WARNINGS = "client_warnings"

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self.http: aiohttp.ClientSession | None = None
        self._booted = False
        self._guild_locks: dict[int, asyncio.Lock] = {}

    async def cog_load(self) -> None:
        await self._ensure_tables()
        self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        await self._restore_selfrole_views()
        if not self.youtube_poller.is_running():
            self.youtube_poller.start()

    async def cog_unload(self) -> None:
        if self.youtube_poller.is_running():
            self.youtube_poller.cancel()
        if self.http and not self.http.closed:
            await self.http.close()

    @commands.Cog.listener("on_ready")
    async def _on_ready(self) -> None:
        if self._booted:
            return
        self._booted = True
        await self._ensure_tables()
        await self._restore_selfrole_views()

    async def _ensure_tables(self) -> None:
        sql_config = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_CONFIG} (
            guild_id BIGINT NOT NULL,
            welcome_channel_id BIGINT NULL,
            welcome_message TEXT NULL,
            autoroles_json LONGTEXT NULL,
            modlog_channel_id BIGINT NULL,
            mute_role_id BIGINT NULL,
            selfrole_channel_id BIGINT NULL,
            selfrole_message_id BIGINT NULL,
            youtube_source_channel_id VARCHAR(64) NULL,
            youtube_target_channel_id BIGINT NULL,
            youtube_ping_mode VARCHAR(16) NOT NULL DEFAULT 'none',
            youtube_ping_role_id BIGINT NULL,
            youtube_last_video_id VARCHAR(64) NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id)
        );
        """

        sql_selfroles = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_SELFROLES} (
            guild_id BIGINT NOT NULL,
            role_id BIGINT NOT NULL,
            label VARCHAR(100) NOT NULL,
            emoji VARCHAR(32) NULL,
            sort_order INT NOT NULL DEFAULT 0,
            enabled TINYINT(1) NOT NULL DEFAULT 1,
            PRIMARY KEY (guild_id, role_id)
        );
        """

        sql_warnings = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_WARNINGS} (
            id BIGINT NOT NULL AUTO_INCREMENT,
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            moderator_id BIGINT NOT NULL,
            reason TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            KEY ix_warning_guild_user (guild_id, user_id)
        );
        """

        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(sql_config))
                await session.execute(text(sql_selfroles))
                await session.execute(text(sql_warnings))

    async def fetch_config(self, guild_id: int) -> GuildConfig:
        async with self.sessionmaker() as session:
            row = (
                await session.execute(
                    text(
                        f"""
                        SELECT
                            guild_id,
                            welcome_channel_id,
                            welcome_message,
                            autoroles_json,
                            modlog_channel_id,
                            mute_role_id,
                            selfrole_channel_id,
                            selfrole_message_id,
                            youtube_source_channel_id,
                            youtube_target_channel_id,
                            youtube_ping_mode,
                            youtube_ping_role_id,
                            youtube_last_video_id
                        FROM {self.TABLE_CONFIG}
                        WHERE guild_id = :gid
                        """
                    ),
                    {"gid": int(guild_id)},
                )
            ).first()

        if row is None:
            await self.upsert_config(guild_id)
            return GuildConfig(int(guild_id), None, "Welcome {user} to **{server}**!", "[]", None, None, None, None, None, None, "none", None, None)

        return GuildConfig(
            guild_id=int(row[0]),
            welcome_channel_id=int(row[1]) if row[1] is not None else None,
            welcome_message=str(row[2] or "Welcome {user} to **{server}**!"),
            autoroles_json=str(row[3]) if row[3] is not None else "[]",
            modlog_channel_id=int(row[4]) if row[4] is not None else None,
            mute_role_id=int(row[5]) if row[5] is not None else None,
            selfrole_channel_id=int(row[6]) if row[6] is not None else None,
            selfrole_message_id=int(row[7]) if row[7] is not None else None,
            youtube_source_channel_id=str(row[8]) if row[8] else None,
            youtube_target_channel_id=int(row[9]) if row[9] is not None else None,
            youtube_ping_mode=str(row[10] or "none"),
            youtube_ping_role_id=int(row[11]) if row[11] is not None else None,
            youtube_last_video_id=str(row[12]) if row[12] else None,
        )

    async def upsert_config(self, guild_id: int, **updates) -> None:
        cfg = updates
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"""
                        INSERT INTO {self.TABLE_CONFIG} (
                            guild_id,
                            welcome_channel_id,
                            welcome_message,
                            autoroles_json,
                            modlog_channel_id,
                            mute_role_id,
                            selfrole_channel_id,
                            selfrole_message_id,
                            youtube_source_channel_id,
                            youtube_target_channel_id,
                            youtube_ping_mode,
                            youtube_ping_role_id,
                            youtube_last_video_id
                        ) VALUES (
                            :guild_id,
                            :welcome_channel_id,
                            :welcome_message,
                            :autoroles_json,
                            :modlog_channel_id,
                            :mute_role_id,
                            :selfrole_channel_id,
                            :selfrole_message_id,
                            :youtube_source_channel_id,
                            :youtube_target_channel_id,
                            :youtube_ping_mode,
                            :youtube_ping_role_id,
                            :youtube_last_video_id
                        )
                        ON DUPLICATE KEY UPDATE
                            welcome_channel_id=VALUES(welcome_channel_id),
                            welcome_message=VALUES(welcome_message),
                            autoroles_json=VALUES(autoroles_json),
                            modlog_channel_id=VALUES(modlog_channel_id),
                            mute_role_id=VALUES(mute_role_id),
                            selfrole_channel_id=VALUES(selfrole_channel_id),
                            selfrole_message_id=VALUES(selfrole_message_id),
                            youtube_source_channel_id=VALUES(youtube_source_channel_id),
                            youtube_target_channel_id=VALUES(youtube_target_channel_id),
                            youtube_ping_mode=VALUES(youtube_ping_mode),
                            youtube_ping_role_id=VALUES(youtube_ping_role_id),
                            youtube_last_video_id=VALUES(youtube_last_video_id)
                        """
                    ),
                    {
                        "guild_id": int(guild_id),
                        "welcome_channel_id": cfg.get("welcome_channel_id"),
                        "welcome_message": cfg.get("welcome_message", "Welcome {user} to **{server}**!"),
                        "autoroles_json": cfg.get("autoroles_json", "[]"),
                        "modlog_channel_id": cfg.get("modlog_channel_id"),
                        "mute_role_id": cfg.get("mute_role_id"),
                        "selfrole_channel_id": cfg.get("selfrole_channel_id"),
                        "selfrole_message_id": cfg.get("selfrole_message_id"),
                        "youtube_source_channel_id": cfg.get("youtube_source_channel_id"),
                        "youtube_target_channel_id": cfg.get("youtube_target_channel_id"),
                        "youtube_ping_mode": cfg.get("youtube_ping_mode", "none"),
                        "youtube_ping_role_id": cfg.get("youtube_ping_role_id"),
                        "youtube_last_video_id": cfg.get("youtube_last_video_id"),
                    },
                )

    async def fetch_selfrole_options(self, guild_id: int, *, enabled_only: bool) -> list[SelfRoleOption]:
        where = "AND enabled=1" if enabled_only else ""
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    text(
                        f"""
                        SELECT guild_id, role_id, label, emoji, sort_order, enabled
                        FROM {self.TABLE_SELFROLES}
                        WHERE guild_id=:gid {where}
                        ORDER BY sort_order ASC, role_id ASC
                        """
                    ),
                    {"gid": int(guild_id)},
                )
            ).all()
        return [SelfRoleOption(int(r[0]), int(r[1]), str(r[2]), str(r[3]) if r[3] else None, int(r[4]), bool(r[5])) for r in rows]

    async def _restore_selfrole_views(self) -> None:
        for guild in self.bot.guilds:
            options = await self.fetch_selfrole_options(guild.id, enabled_only=True)
            if options:
                self.bot.add_view(SelfRoleView(self, guild.id, options))

    async def _send_modlog(self, guild: discord.Guild, *, title: str, description: str) -> None:
        cfg = await self.fetch_config(guild.id)
        if not cfg.modlog_channel_id:
            return
        channel = guild.get_channel(cfg.modlog_channel_id)
        if not isinstance(channel, discord.TextChannel):
            return
        embed = discord.Embed(title=title, description=description, color=discord.Color.orange(), timestamp=_utc_now())
        try:
            await channel.send(embed=embed)
        except Exception:
            return

    @commands.Cog.listener("on_member_join")
    async def _on_member_join(self, member: discord.Member) -> None:
        cfg = await self.fetch_config(member.guild.id)

        autoroles = [int(rid) for rid in _safe_json_load(cfg.autoroles_json, []) if str(rid).isdigit()]
        role_objects: list[discord.Role] = []
        missing_roles: list[int] = []
        for rid in autoroles:
            role = member.guild.get_role(rid)
            if role is None:
                missing_roles.append(rid)
                continue
            role_objects.append(role)

        if role_objects:
            try:
                await member.add_roles(*role_objects, reason="Configured auto-role on join")
            except Exception as exc:
                await self._send_modlog(member.guild, title="Auto-role failed", description=f"Member: {member.mention}\nError: `{_fmt_error(exc)}`")

        if missing_roles:
            await self._send_modlog(member.guild, title="Auto-role warning", description=f"Missing role IDs in config: `{', '.join(str(x) for x in missing_roles)}`")

        if cfg.welcome_channel_id:
            channel = member.guild.get_channel(cfg.welcome_channel_id)
            if isinstance(channel, discord.TextChannel):
                content = (cfg.welcome_message or "Welcome {user} to **{server}**!").format(
                    user=member.mention,
                    username=member.display_name,
                    server=member.guild.name,
                )
                try:
                    await channel.send(content)
                except Exception as exc:
                    await self._send_modlog(member.guild, title="Welcome message failed", description=f"Channel: <#{cfg.welcome_channel_id}>\nError: `{_fmt_error(exc)}`")

    @tasks.loop(seconds=YOUTUBE_POLL_SECONDS)
    async def youtube_poller(self) -> None:
        await self.bot.wait_until_ready()
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    text(
                        f"""
                        SELECT guild_id
                        FROM {self.TABLE_CONFIG}
                        WHERE youtube_source_channel_id IS NOT NULL
                          AND youtube_target_channel_id IS NOT NULL
                        """
                    )
                )
            ).all()

        for row in rows:
            guild_id = int(row[0])
            lock = self._guild_locks.setdefault(guild_id, asyncio.Lock())
            if lock.locked():
                continue
            async with lock:
                guild = self.bot.get_guild(guild_id)
                if guild is None:
                    continue
                await self._run_youtube_check_for_guild(guild)

    async def _run_youtube_check_for_guild(self, guild: discord.Guild) -> None:
        cfg = await self.fetch_config(guild.id)
        if not cfg.youtube_source_channel_id or not cfg.youtube_target_channel_id or self.http is None:
            return

        url = f"https://www.youtube.com/feeds/videos.xml?channel_id={cfg.youtube_source_channel_id.strip()}"
        try:
            async with self.http.get(url) as resp:
                if resp.status != 200:
                    await self._send_modlog(guild, title="YouTube poll failed", description=f"HTTP `{resp.status}` for `{url}`")
                    return
                body = await resp.text()
        except Exception as exc:
            await self._send_modlog(guild, title="YouTube poll failed", description=f"{_fmt_error(exc)}")
            return

        try:
            root = ET.fromstring(body)
        except ET.ParseError as exc:
            await self._send_modlog(guild, title="YouTube parse failed", description=f"{exc}")
            return

        ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "yt": "http://www.youtube.com/xml/schemas/2015",
            "media": "http://search.yahoo.com/mrss/",
        }
        entry = root.find("atom:entry", ns)
        if entry is None:
            return

        video_id = (entry.findtext("yt:videoId", default="", namespaces=ns) or "").strip()
        title = (entry.findtext("atom:title", default="New upload", namespaces=ns) or "New upload").strip()
        link = entry.find("atom:link", ns)
        video_url = link.attrib.get("href") if link is not None else f"https://youtu.be/{video_id}"
        description = entry.findtext("media:group/media:description", default="", namespaces=ns) or ""
        first_link = _extract_first_link(description)

        if not video_id:
            return

        if not cfg.youtube_last_video_id:
            await self.upsert_config(guild.id, **(cfg.__dict__ | {"youtube_last_video_id": video_id}))
            return

        if cfg.youtube_last_video_id == video_id:
            return

        channel = guild.get_channel(cfg.youtube_target_channel_id)
        if not isinstance(channel, discord.TextChannel):
            await self._send_modlog(guild, title="YouTube target channel missing", description=f"Configured channel `{cfg.youtube_target_channel_id}` was not found.")
            return

        ping_text = ""
        if cfg.youtube_ping_mode == "everyone":
            ping_text = "@everyone"
        elif cfg.youtube_ping_mode == "role" and cfg.youtube_ping_role_id:
            ping_text = f"<@&{cfg.youtube_ping_role_id}>"

        embed = discord.Embed(
            title="🎬 New YouTube Upload",
            description=f"**{title}**\n{video_url}",
            color=discord.Color.red(),
            timestamp=_utc_now(),
        )
        if description:
            embed.add_field(name="Description", value=description[:1000], inline=False)
        if first_link:
            embed.add_field(name="Link found in description", value=first_link[:1024], inline=False)

        try:
            await channel.send(content=ping_text or None, embed=embed, allowed_mentions=discord.AllowedMentions(everyone=True, roles=True))
        except Exception as exc:
            await self._send_modlog(guild, title="YouTube notification failed", description=f"{_fmt_error(exc)}")
            return

        await self.upsert_config(guild.id, **(cfg.__dict__ | {"youtube_last_video_id": video_id}))

    admin_group = app_commands.Group(name="server", description="Client server management setup.")
    mod_group = app_commands.Group(name="mod", description="Practical moderation tools.")

    @admin_group.command(name="welcome_set", description="Set welcome channel + message (message should include {user}).")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def welcome_set(self, interaction: discord.Interaction, channel: discord.TextChannel, message: str) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        await self.upsert_config(interaction.guild.id, **(cfg.__dict__ | {"welcome_channel_id": channel.id, "welcome_message": message[:1800]}))
        await interaction.response.send_message(f"✅ Welcome is set in {channel.mention}.", ephemeral=True)

    @admin_group.command(name="autorole_add", description="Add an automatic join role.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def autorole_add(self, interaction: discord.Interaction, role: discord.Role) -> None:
        if interaction.guild is None:
            return
        cfg = await self.fetch_config(interaction.guild.id)
        ids = [int(x) for x in _safe_json_load(cfg.autoroles_json, []) if str(x).isdigit()]
        if role.id in ids:
            await interaction.response.send_message("That role is already in auto-role list.", ephemeral=True)
            return
        ids.append(role.id)
        await self.upsert_config(interaction.guild.id, **(cfg.__dict__ | {"autoroles_json": json.dumps(ids)}))
        await interaction.response.send_message(f"✅ Added {role.mention} to auto-role list.", ephemeral=True)

    @admin_group.command(name="autorole_remove", description="Remove an automatic join role.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def autorole_remove(self, interaction: discord.Interaction, role: discord.Role) -> None:
        if interaction.guild is None:
            return
        cfg = await self.fetch_config(interaction.guild.id)
        ids = [int(x) for x in _safe_json_load(cfg.autoroles_json, []) if str(x).isdigit()]
        ids = [x for x in ids if x != role.id]
        await self.upsert_config(interaction.guild.id, **(cfg.__dict__ | {"autoroles_json": json.dumps(ids)}))
        await interaction.response.send_message(f"✅ Removed {role.mention} from auto-role list.", ephemeral=True)

    @admin_group.command(name="botpost", description="Have the bot send a simple message in any channel.")
    @app_commands.checks.has_permissions(manage_messages=True)
    async def botpost(self, interaction: discord.Interaction, channel: discord.TextChannel, content: str) -> None:
        if not content.strip():
            await interaction.response.send_message("Message cannot be empty.", ephemeral=True)
            return
        try:
            await channel.send(content[:1900])
        except Exception as exc:
            await interaction.response.send_message(f"Failed to post: {_fmt_error(exc)}", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Posted in {channel.mention}.", ephemeral=True)

    @admin_group.command(name="modlog_set", description="Set moderation log channel.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def modlog_set(self, interaction: discord.Interaction, channel: discord.TextChannel) -> None:
        if interaction.guild is None:
            return
        cfg = await self.fetch_config(interaction.guild.id)
        await self.upsert_config(interaction.guild.id, **(cfg.__dict__ | {"modlog_channel_id": channel.id}))
        await interaction.response.send_message(f"✅ Mod log channel set to {channel.mention}.", ephemeral=True)

    @admin_group.command(name="mute_role_set", description="Set mute role used by /mod mute.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def mute_role_set(self, interaction: discord.Interaction, role: discord.Role) -> None:
        if interaction.guild is None:
            return
        cfg = await self.fetch_config(interaction.guild.id)
        await self.upsert_config(interaction.guild.id, **(cfg.__dict__ | {"mute_role_id": role.id}))
        await interaction.response.send_message(f"✅ Mute role set to {role.mention}.", ephemeral=True)

    @admin_group.command(name="selfrole_add", description="Add a role to the self-role picker.")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def selfrole_add(self, interaction: discord.Interaction, role: discord.Role, label: str | None = None, emoji: str | None = None) -> None:
        if interaction.guild is None:
            return
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"""
                        INSERT INTO {self.TABLE_SELFROLES} (guild_id, role_id, label, emoji, sort_order, enabled)
                        VALUES (:gid, :rid, :label, :emoji, :sort_order, 1)
                        ON DUPLICATE KEY UPDATE
                            label=VALUES(label), emoji=VALUES(emoji), enabled=1
                        """
                    ),
                    {"gid": interaction.guild.id, "rid": role.id, "label": (label or role.name)[:100], "emoji": emoji, "sort_order": role.position},
                )

        options = await self.fetch_selfrole_options(interaction.guild.id, enabled_only=True)
        self.bot.add_view(SelfRoleView(self, interaction.guild.id, options))
        await interaction.response.send_message(f"✅ Added {role.mention} to self-role options.", ephemeral=True)

    @admin_group.command(name="selfrole_remove", description="Remove a role from the self-role picker.")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def selfrole_remove(self, interaction: discord.Interaction, role: discord.Role) -> None:
        if interaction.guild is None:
            return
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(f"DELETE FROM {self.TABLE_SELFROLES} WHERE guild_id=:gid AND role_id=:rid"),
                    {"gid": interaction.guild.id, "rid": role.id},
                )
        await interaction.response.send_message(f"✅ Removed {role.mention} from self-role options.", ephemeral=True)

    @admin_group.command(name="selfrole_list", description="List configured self-role options.")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def selfrole_list(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            return
        options = await self.fetch_selfrole_options(interaction.guild.id, enabled_only=True)
        if not options:
            await interaction.response.send_message("No self-role options configured.", ephemeral=True)
            return
        lines = []
        for item in options:
            lines.append(f"• <@&{item.role_id}> — `{item.label}`")
        await interaction.response.send_message("\n".join(lines[:25]), ephemeral=True)

    @admin_group.command(name="selfrole_panel", description="Post/refresh the self-role panel in a channel.")
    @app_commands.checks.has_permissions(manage_roles=True)
    async def selfrole_panel(self, interaction: discord.Interaction, channel: discord.TextChannel, title: str = "Choose your roles") -> None:
        if interaction.guild is None:
            return
        options = await self.fetch_selfrole_options(interaction.guild.id, enabled_only=True)
        if not options:
            await interaction.response.send_message("Add at least one self-role first with `/server selfrole_add`.", ephemeral=True)
            return

        view = SelfRoleView(self, interaction.guild.id, options)
        embed = discord.Embed(title=title[:200], description="Use the dropdown below to add/remove your roles.", color=discord.Color.blurple())
        sent = await channel.send(embed=embed, view=view)
        self.bot.add_view(view)

        cfg = await self.fetch_config(interaction.guild.id)
        await self.upsert_config(interaction.guild.id, **(cfg.__dict__ | {"selfrole_channel_id": channel.id, "selfrole_message_id": sent.id}))
        await interaction.response.send_message(f"✅ Self-role panel posted in {channel.mention}.", ephemeral=True)

    @admin_group.command(name="youtube_set", description="Configure YouTube upload notifications.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def youtube_set(
        self,
        interaction: discord.Interaction,
        youtube_channel_id: str,
        target_channel: discord.TextChannel,
        ping_mode: Literal["none", "everyone", "role"],
        ping_role: discord.Role | None = None,
    ) -> None:
        if interaction.guild is None:
            return
        if ping_mode == "role" and ping_role is None:
            await interaction.response.send_message("Pick a role when ping mode is `role`.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        await self.upsert_config(
            interaction.guild.id,
            **(
                cfg.__dict__
                | {
                    "youtube_source_channel_id": youtube_channel_id.strip(),
                    "youtube_target_channel_id": target_channel.id,
                    "youtube_ping_mode": ping_mode,
                    "youtube_ping_role_id": ping_role.id if ping_role else None,
                    "youtube_last_video_id": None,
                }
            ),
        )
        await interaction.response.send_message(f"✅ YouTube notifications configured in {target_channel.mention}.", ephemeral=True)

    @admin_group.command(name="youtube_check_now", description="Run YouTube check immediately.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def youtube_check_now(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            return
        await interaction.response.defer(ephemeral=True)
        await self._run_youtube_check_for_guild(interaction.guild)
        await interaction.followup.send("✅ YouTube check completed.", ephemeral=True)

    @mod_group.command(name="warn", description="Warn a user and log it.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def mod_warn(self, interaction: discord.Interaction, member: discord.Member, reason: str) -> None:
        if interaction.guild is None:
            return
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    text(
                        f"INSERT INTO {self.TABLE_WARNINGS} (guild_id, user_id, moderator_id, reason) VALUES (:gid, :uid, :mid, :reason)"
                    ),
                    {"gid": interaction.guild.id, "uid": member.id, "mid": interaction.user.id, "reason": reason[:1000]},
                )
        await self._send_modlog(interaction.guild, title="Member Warned", description=f"User: {member.mention}\nModerator: {interaction.user.mention}\nReason: {reason}")
        await interaction.response.send_message(f"✅ Warned {member.mention}.", ephemeral=True)

    @mod_group.command(name="warnings", description="Show warning count for a user.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def mod_warnings(self, interaction: discord.Interaction, member: discord.Member) -> None:
        if interaction.guild is None:
            return
        async with self.sessionmaker() as session:
            count = (
                await session.execute(
                    text(f"SELECT COUNT(*) FROM {self.TABLE_WARNINGS} WHERE guild_id=:gid AND user_id=:uid"),
                    {"gid": interaction.guild.id, "uid": member.id},
                )
            ).scalar_one()
        await interaction.response.send_message(f"{member.mention} has **{int(count)}** warning(s).", ephemeral=True)

    @mod_group.command(name="timeout", description="Timeout a member.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def mod_timeout(self, interaction: discord.Interaction, member: discord.Member, minutes: app_commands.Range[int, 1, 40320], reason: str = "No reason provided") -> None:
        until = discord.utils.utcnow() + timedelta(minutes=int(minutes))
        try:
            await member.timeout(until, reason=f"{reason} | by {interaction.user}")
        except Exception as exc:
            await interaction.response.send_message(f"Timeout failed: {_fmt_error(exc)}", ephemeral=True)
            return
        if interaction.guild:
            await self._send_modlog(interaction.guild, title="Member Timed Out", description=f"User: {member.mention}\nDuration: {minutes}m\nReason: {reason}")
        await interaction.response.send_message(f"✅ Timed out {member.mention} for {minutes} minute(s).", ephemeral=True)

    @mod_group.command(name="untimeout", description="Remove timeout from a member.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def mod_untimeout(self, interaction: discord.Interaction, member: discord.Member) -> None:
        try:
            await member.timeout(None, reason=f"Timeout removed by {interaction.user}")
        except Exception as exc:
            await interaction.response.send_message(f"Failed: {_fmt_error(exc)}", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Removed timeout for {member.mention}.", ephemeral=True)

    @mod_group.command(name="mute", description="Apply configured mute role to a member.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def mod_mute(self, interaction: discord.Interaction, member: discord.Member, reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if not cfg.mute_role_id:
            await interaction.response.send_message("Set a mute role first with `/server mute_role_set`.", ephemeral=True)
            return
        role = interaction.guild.get_role(cfg.mute_role_id)
        if role is None:
            await interaction.response.send_message("Configured mute role no longer exists.", ephemeral=True)
            return
        try:
            await member.add_roles(role, reason=f"Mute by {interaction.user}: {reason}")
        except Exception as exc:
            await interaction.response.send_message(f"Mute failed: {_fmt_error(exc)}", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Muted {member.mention}.", ephemeral=True)

    @mod_group.command(name="unmute", description="Remove configured mute role from a member.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def mod_unmute(self, interaction: discord.Interaction, member: discord.Member) -> None:
        if interaction.guild is None:
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if not cfg.mute_role_id:
            await interaction.response.send_message("Set a mute role first with `/server mute_role_set`.", ephemeral=True)
            return
        role = interaction.guild.get_role(cfg.mute_role_id)
        if role is None:
            await interaction.response.send_message("Configured mute role no longer exists.", ephemeral=True)
            return
        try:
            await member.remove_roles(role, reason=f"Unmute by {interaction.user}")
        except Exception as exc:
            await interaction.response.send_message(f"Unmute failed: {_fmt_error(exc)}", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Unmuted {member.mention}.", ephemeral=True)

    @mod_group.command(name="kick", description="Kick a member.")
    @app_commands.checks.has_permissions(kick_members=True)
    async def mod_kick(self, interaction: discord.Interaction, member: discord.Member, reason: str = "No reason provided") -> None:
        try:
            await member.kick(reason=f"{reason} | by {interaction.user}")
        except Exception as exc:
            await interaction.response.send_message(f"Kick failed: {_fmt_error(exc)}", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Kicked {member.mention}.", ephemeral=True)

    @mod_group.command(name="ban", description="Ban a member quickly.")
    @app_commands.checks.has_permissions(ban_members=True)
    async def mod_ban(self, interaction: discord.Interaction, member: discord.Member, reason: str = "No reason provided", delete_days: app_commands.Range[int, 0, 7] = 0) -> None:
        try:
            await interaction.guild.ban(member, reason=f"{reason} | by {interaction.user}", delete_message_seconds=int(delete_days) * 86400)
        except Exception as exc:
            await interaction.response.send_message(f"Ban failed: {_fmt_error(exc)}", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Banned {member.mention}.", ephemeral=True)

    @mod_group.command(name="unban", description="Unban by user ID.")
    @app_commands.checks.has_permissions(ban_members=True)
    async def mod_unban(self, interaction: discord.Interaction, user_id: str, reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            return
        if not user_id.isdigit():
            await interaction.response.send_message("Provide a valid numeric user ID.", ephemeral=True)
            return
        user = discord.Object(id=int(user_id))
        try:
            await interaction.guild.unban(user, reason=f"{reason} | by {interaction.user}")
        except Exception as exc:
            await interaction.response.send_message(f"Unban failed: {_fmt_error(exc)}", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Unbanned `{user_id}`.", ephemeral=True)

    @mod_group.command(name="purge", description="Delete a number of recent messages.")
    @app_commands.checks.has_permissions(manage_messages=True)
    async def mod_purge(self, interaction: discord.Interaction, amount: app_commands.Range[int, 1, 200]) -> None:
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("This only works in text channels.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            deleted = await interaction.channel.purge(limit=int(amount), reason=f"Purge by {interaction.user}")
        except Exception as exc:
            await interaction.followup.send(f"Purge failed: {_fmt_error(exc)}", ephemeral=True)
            return
        await interaction.followup.send(f"✅ Deleted {len(deleted)} messages.", ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ClientManagementCog(bot))
