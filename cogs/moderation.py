from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import text

from services.db import sessions
from services.mod_warnings import add_warning as store_warning
from services.mod_warnings import ensure_warning_table

log = logging.getLogger(__name__)
UTC = timezone.utc


@dataclass
class ModConfig:
    guild_id: int
    log_channel_id: int | None
    mute_role_id: int | None


class ModerationCog(commands.Cog):
    WARN_TABLE = "mod_warnings"
    CONFIG_TABLE = "mod_config"

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

    async def cog_load(self) -> None:
        await self._ensure_tables()

    async def _ensure_tables(self) -> None:
        await ensure_warning_table()
        sql_cfg = f"""
        CREATE TABLE IF NOT EXISTS {self.CONFIG_TABLE} (
            guild_id BIGINT NOT NULL,
            log_channel_id BIGINT NULL,
            mute_role_id BIGINT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id)
        );
        """
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(sql_cfg))

    async def fetch_config(self, guild_id: int) -> ModConfig:
        sql = text(f"SELECT guild_id, log_channel_id, mute_role_id FROM {self.CONFIG_TABLE} WHERE guild_id = :g LIMIT 1")
        async with self.sessionmaker() as session:
            row = (await session.execute(sql, {"g": int(guild_id)})).mappings().first()
        if not row:
            return ModConfig(guild_id=guild_id, log_channel_id=None, mute_role_id=None)
        return ModConfig(
            guild_id=int(row["guild_id"]),
            log_channel_id=int(row["log_channel_id"]) if row["log_channel_id"] else None,
            mute_role_id=int(row["mute_role_id"]) if row["mute_role_id"] else None,
        )

    async def upsert_config(self, cfg: ModConfig) -> None:
        sql = text(
            f"""
            INSERT INTO {self.CONFIG_TABLE} (guild_id, log_channel_id, mute_role_id)
            VALUES (:guild_id, :log_channel_id, :mute_role_id)
            ON DUPLICATE KEY UPDATE
                log_channel_id = VALUES(log_channel_id),
                mute_role_id = VALUES(mute_role_id)
            """
        )
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(sql, {"guild_id": cfg.guild_id, "log_channel_id": cfg.log_channel_id, "mute_role_id": cfg.mute_role_id})

    async def _send_mod_log(self, guild: discord.Guild, cfg: ModConfig, *, title: str, description: str, color: discord.Color) -> None:
        if not cfg.log_channel_id:
            return
        ch = guild.get_channel(cfg.log_channel_id)
        if not isinstance(ch, discord.TextChannel):
            return
        embed = discord.Embed(title=title, description=description, color=color, timestamp=datetime.now(tz=UTC))
        try:
            await ch.send(embed=embed)
        except Exception:
            log.warning("Failed to send moderation log in guild %s", guild.id)

    async def log_action(self, guild: discord.Guild, *, title: str, description: str, color: discord.Color) -> None:
        cfg = await self.fetch_config(guild.id)
        await self._send_mod_log(guild, cfg, title=title, description=description, color=color)

    async def add_warning(self, *, guild_id: int, user_id: int, moderator_id: int, reason: str) -> None:
        await store_warning(guild_id=guild_id, user_id=user_id, moderator_id=moderator_id, reason=reason)

    mod = app_commands.Group(name="mod", description="Practical moderation commands.")

    @mod.command(name="config", description="Configure mod logs and mute role.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def mod_config(self, interaction: discord.Interaction, log_channel: discord.TextChannel | None = None, mute_role: discord.Role | None = None) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if log_channel is not None:
            cfg.log_channel_id = log_channel.id
        if mute_role is not None:
            cfg.mute_role_id = mute_role.id
        await self.upsert_config(cfg)
        await interaction.response.send_message("✅ Moderation config updated.", ephemeral=True)

    @mod.command(name="warn", description="Warn a user and save the warning.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def warn(self, interaction: discord.Interaction, user: discord.Member, reason: str) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if user.id == interaction.user.id:
            await interaction.response.send_message("You can't warn yourself.", ephemeral=True)
            return

        await self.add_warning(guild_id=interaction.guild.id, user_id=user.id, moderator_id=interaction.user.id, reason=reason)

        await self.log_action(
            interaction.guild,
            title="User Warned",
            description=f"{user.mention} was warned by {interaction.user.mention}.\n**Reason:** {reason}",
            color=discord.Color.orange(),
        )
        await interaction.response.send_message(f"✅ Warned {user.mention}.", ephemeral=False)

    @mod.command(name="warnings", description="View a user's recent warnings.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def warnings(self, interaction: discord.Interaction, user: discord.Member) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        sql = text(
            f"""
            SELECT id, moderator_id, reason, created_at
            FROM {self.WARN_TABLE}
            WHERE guild_id = :g AND user_id = :u
            ORDER BY id DESC
            LIMIT 10
            """
        )
        async with self.sessionmaker() as session:
            rows = (await session.execute(sql, {"g": interaction.guild.id, "u": user.id})).mappings().all()

        if not rows:
            await interaction.response.send_message(f"{user.mention} has no warnings.", ephemeral=True)
            return

        lines = []
        for row in rows:
            ts = int(row["created_at"].replace(tzinfo=UTC).timestamp()) if row["created_at"] else 0
            lines.append(f"`#{row['id']}` by <@{int(row['moderator_id'])}> • <t:{ts}:R>\n{str(row['reason'])}")

        embed = discord.Embed(title=f"Warnings for {user}", description="\n\n".join(lines), color=discord.Color.orange())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @mod.command(name="clear_warnings", description="Clear all warnings for a user.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def clear_warnings(self, interaction: discord.Interaction, user: discord.Member) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        sql = text(f"DELETE FROM {self.WARN_TABLE} WHERE guild_id = :g AND user_id = :u")
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(sql, {"g": interaction.guild.id, "u": user.id})
        await interaction.response.send_message(f"✅ Cleared warnings for {user.mention}.", ephemeral=True)

    @mod.command(name="timeout", description="Timeout a user for N minutes.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def timeout(self, interaction: discord.Interaction, user: discord.Member, minutes: app_commands.Range[int, 1, 40320], reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        until = discord.utils.utcnow() + timedelta(minutes=int(minutes))
        try:
            await user.timeout(until, reason=f"{reason} | by {interaction.user} ({interaction.user.id})")
        except discord.Forbidden:
            await interaction.response.send_message("I can't timeout that user (permissions/role hierarchy).", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message("Timeout failed due to a Discord error.", ephemeral=True)
            return

        cfg = await self.fetch_config(interaction.guild.id)
        await self._send_mod_log(interaction.guild, cfg, title="User Timed Out", description=f"{user.mention} timed out for **{minutes}** minutes.\n**Reason:** {reason}", color=discord.Color.red())
        await interaction.response.send_message(f"✅ Timed out {user.mention} for {minutes} minute(s).", ephemeral=False)

    @mod.command(name="untimeout", description="Remove timeout from a user.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def untimeout(self, interaction: discord.Interaction, user: discord.Member, reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        try:
            await user.timeout(None, reason=f"{reason} | by {interaction.user} ({interaction.user.id})")
        except discord.Forbidden:
            await interaction.response.send_message("I can't remove timeout for that user.", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message("Failed to remove timeout due to a Discord error.", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Removed timeout for {user.mention}.", ephemeral=False)

    @mod.command(name="mute", description="Mute a user using the configured mute role.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def mute(self, interaction: discord.Interaction, user: discord.Member, reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if not cfg.mute_role_id:
            await interaction.response.send_message("No mute role configured. Use `/mod config`.", ephemeral=True)
            return
        role = interaction.guild.get_role(cfg.mute_role_id)
        if role is None:
            await interaction.response.send_message("Configured mute role no longer exists.", ephemeral=True)
            return
        try:
            await user.add_roles(role, reason=f"Mute | {reason} | by {interaction.user.id}")
        except discord.Forbidden:
            await interaction.response.send_message("I can't apply that mute role.", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message("Mute failed due to a Discord error.", ephemeral=True)
            return
        await self._send_mod_log(interaction.guild, cfg, title="User Muted", description=f"{user.mention} muted.\n**Reason:** {reason}", color=discord.Color.red())
        await interaction.response.send_message(f"✅ Muted {user.mention}.", ephemeral=False)

    @mod.command(name="unmute", description="Unmute a user by removing the configured mute role.")
    @app_commands.checks.has_permissions(moderate_members=True)
    async def unmute(self, interaction: discord.Interaction, user: discord.Member, reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if not cfg.mute_role_id:
            await interaction.response.send_message("No mute role configured. Use `/mod config`.", ephemeral=True)
            return
        role = interaction.guild.get_role(cfg.mute_role_id)
        if role is None:
            await interaction.response.send_message("Configured mute role no longer exists.", ephemeral=True)
            return
        try:
            await user.remove_roles(role, reason=f"Unmute | {reason} | by {interaction.user.id}")
        except discord.Forbidden:
            await interaction.response.send_message("I can't remove that mute role.", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message("Unmute failed due to a Discord error.", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Unmuted {user.mention}.", ephemeral=False)

    @mod.command(name="kick", description="Kick a user from the server.")
    @app_commands.checks.has_permissions(kick_members=True)
    async def kick(self, interaction: discord.Interaction, user: discord.Member, reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        try:
            await interaction.guild.kick(user, reason=f"{reason} | by {interaction.user.id}")
        except discord.Forbidden:
            await interaction.response.send_message("I can't kick that user (permissions/role hierarchy).", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message("Kick failed due to a Discord error.", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Kicked {user}.", ephemeral=False)

    @mod.command(name="ban", description="Quick-ban a user.")
    @app_commands.checks.has_permissions(ban_members=True)
    async def ban(self, interaction: discord.Interaction, user: discord.User, delete_days: app_commands.Range[int, 0, 7] = 0, reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        try:
            await interaction.guild.ban(user, reason=f"{reason} | by {interaction.user.id}", delete_message_seconds=int(delete_days) * 86400)
        except discord.Forbidden:
            await interaction.response.send_message("I can't ban that user (permissions/role hierarchy).", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message("Ban failed due to a Discord error.", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Banned {user}.", ephemeral=False)

    @mod.command(name="unban", description="Unban a user by ID.")
    @app_commands.checks.has_permissions(ban_members=True)
    async def unban(self, interaction: discord.Interaction, user_id: str, reason: str = "No reason provided") -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if not user_id.isdigit():
            await interaction.response.send_message("Provide a valid numeric Discord user ID.", ephemeral=True)
            return
        uid = int(user_id)
        try:
            await interaction.guild.unban(discord.Object(id=uid), reason=f"{reason} | by {interaction.user.id}")
        except discord.NotFound:
            await interaction.response.send_message("That user is not currently banned.", ephemeral=True)
            return
        except discord.Forbidden:
            await interaction.response.send_message("I can't unban users in this server.", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message("Unban failed due to a Discord error.", ephemeral=True)
            return
        await interaction.response.send_message(f"✅ Unbanned <@{uid}>.", ephemeral=False)

    @mod.command(name="purge", description="Delete a number of recent messages in this channel.")
    @app_commands.checks.has_permissions(manage_messages=True)
    async def purge(self, interaction: discord.Interaction, amount: app_commands.Range[int, 1, 100]) -> None:
        if interaction.channel is None or not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("This command only works in text channels.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            deleted = await interaction.channel.purge(limit=int(amount), reason=f"Purge by {interaction.user.id}")
        except discord.Forbidden:
            await interaction.followup.send("I don't have permission to delete messages here.", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.followup.send("Purge failed due to a Discord error.", ephemeral=True)
            return
        await interaction.followup.send(f"✅ Deleted {len(deleted)} message(s).", ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ModerationCog(bot))
