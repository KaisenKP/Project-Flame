from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import text

from services.db import sessions

log = logging.getLogger(__name__)

DEFAULT_WELCOME_MESSAGE = "Welcome {user_mention} to **{server_name}**!"


def _safe_json_load(raw: str | None, fallback: list[int]) -> list[int]:
    if not raw:
        return fallback
    try:
        data = json.loads(raw)
    except Exception:
        return fallback
    if not isinstance(data, list):
        return fallback
    out: list[int] = []
    for item in data:
        try:
            out.append(int(item))
        except Exception:
            continue
    return out


@dataclass
class CommunityConfig:
    guild_id: int
    welcome_channel_id: int | None
    welcome_message: str
    auto_role_ids: list[int]
    self_roles_channel_id: int | None
    self_roles_message_id: int | None
    self_role_ids: list[int]


class SelfRoleLaunchView(discord.ui.View):
    def __init__(self, cog: "CommunityToolsCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Choose Roles", style=discord.ButtonStyle.primary, custom_id="selfroles:open", emoji="🎭")
    async def open_picker(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return

        cfg = await self.cog.fetch_config(interaction.guild.id)
        if cfg is None or not cfg.self_role_ids:
            await interaction.response.send_message("Self-roles are not configured yet.", ephemeral=True)
            return

        valid_roles = [r for role_id in cfg.self_role_ids if (r := interaction.guild.get_role(role_id)) is not None]
        if not valid_roles:
            await interaction.response.send_message("Configured self-roles were not found. Ask staff to update setup.", ephemeral=True)
            return

        picker = SelfRolePickerView(valid_roles)
        await interaction.response.send_message(
            "Pick the roles you want. Roles not selected from the configured list will be removed.",
            view=picker,
            ephemeral=True,
        )


class SelfRolePicker(discord.ui.Select):
    def __init__(self, roles: list[discord.Role]):
        options = [discord.SelectOption(label=role.name[:100], value=str(role.id)) for role in roles[:25]]
        super().__init__(
            placeholder="Select your roles",
            min_values=0,
            max_values=len(options),
            options=options,
        )
        self.role_ids = {int(option.value) for option in options}

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        chosen = {int(v) for v in self.values}
        to_add: list[discord.Role] = []
        to_remove: list[discord.Role] = []

        for role_id in self.role_ids:
            role = interaction.guild.get_role(role_id)
            if role is None:
                continue
            has_role = role in interaction.user.roles
            if role_id in chosen and not has_role:
                to_add.append(role)
            elif role_id not in chosen and has_role:
                to_remove.append(role)

        if not to_add and not to_remove:
            await interaction.response.send_message("No role changes needed.", ephemeral=True)
            return

        try:
            if to_add:
                await interaction.user.add_roles(*to_add, reason="Self-role picker")
            if to_remove:
                await interaction.user.remove_roles(*to_remove, reason="Self-role picker")
        except discord.Forbidden:
            await interaction.response.send_message("I don't have permission to update one or more selected roles.", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message("Role update failed due to a Discord error. Please try again.", ephemeral=True)
            return

        parts: list[str] = []
        if to_add:
            parts.append("Added: " + ", ".join(role.mention for role in to_add))
        if to_remove:
            parts.append("Removed: " + ", ".join(role.mention for role in to_remove))
        await interaction.response.send_message("✅ " + "\n".join(parts), ephemeral=True)


class SelfRolePickerView(discord.ui.View):
    def __init__(self, roles: list[discord.Role]):
        super().__init__(timeout=180)
        self.add_item(SelfRolePicker(roles))


class CommunityToolsCog(commands.Cog):
    TABLE = "community_config"

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self.launch_view = SelfRoleLaunchView(self)

    async def cog_load(self) -> None:
        await self._ensure_tables()
        self.bot.add_view(self.launch_view)

    async def _ensure_tables(self) -> None:
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE} (
            guild_id BIGINT NOT NULL,
            welcome_channel_id BIGINT NULL,
            welcome_message TEXT NULL,
            auto_role_ids_json LONGTEXT NULL,
            self_roles_channel_id BIGINT NULL,
            self_roles_message_id BIGINT NULL,
            self_role_ids_json LONGTEXT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id)
        );
        """
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(sql))

    async def fetch_config(self, guild_id: int) -> CommunityConfig | None:
        sql = text(
            f"""
            SELECT guild_id, welcome_channel_id, welcome_message, auto_role_ids_json,
                   self_roles_channel_id, self_roles_message_id, self_role_ids_json
            FROM {self.TABLE}
            WHERE guild_id = :guild_id
            LIMIT 1
            """
        )
        async with self.sessionmaker() as session:
            row = (await session.execute(sql, {"guild_id": int(guild_id)})).mappings().first()

        if not row:
            return None
        return CommunityConfig(
            guild_id=int(row["guild_id"]),
            welcome_channel_id=int(row["welcome_channel_id"]) if row["welcome_channel_id"] else None,
            welcome_message=str(row["welcome_message"] or DEFAULT_WELCOME_MESSAGE),
            auto_role_ids=_safe_json_load(row["auto_role_ids_json"], []),
            self_roles_channel_id=int(row["self_roles_channel_id"]) if row["self_roles_channel_id"] else None,
            self_roles_message_id=int(row["self_roles_message_id"]) if row["self_roles_message_id"] else None,
            self_role_ids=_safe_json_load(row["self_role_ids_json"], []),
        )

    async def upsert_config(self, cfg: CommunityConfig) -> None:
        sql = text(
            f"""
            INSERT INTO {self.TABLE}
                (guild_id, welcome_channel_id, welcome_message, auto_role_ids_json, self_roles_channel_id, self_roles_message_id, self_role_ids_json)
            VALUES
                (:guild_id, :welcome_channel_id, :welcome_message, :auto_role_ids_json, :self_roles_channel_id, :self_roles_message_id, :self_role_ids_json)
            ON DUPLICATE KEY UPDATE
                welcome_channel_id = VALUES(welcome_channel_id),
                welcome_message = VALUES(welcome_message),
                auto_role_ids_json = VALUES(auto_role_ids_json),
                self_roles_channel_id = VALUES(self_roles_channel_id),
                self_roles_message_id = VALUES(self_roles_message_id),
                self_role_ids_json = VALUES(self_role_ids_json)
            """
        )
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    sql,
                    {
                        "guild_id": cfg.guild_id,
                        "welcome_channel_id": cfg.welcome_channel_id,
                        "welcome_message": cfg.welcome_message,
                        "auto_role_ids_json": json.dumps(cfg.auto_role_ids),
                        "self_roles_channel_id": cfg.self_roles_channel_id,
                        "self_roles_message_id": cfg.self_roles_message_id,
                        "self_role_ids_json": json.dumps(cfg.self_role_ids),
                    },
                )

    async def _require_cfg(self, guild_id: int) -> CommunityConfig:
        cfg = await self.fetch_config(guild_id)
        if cfg:
            return cfg
        cfg = CommunityConfig(
            guild_id=guild_id,
            welcome_channel_id=None,
            welcome_message=DEFAULT_WELCOME_MESSAGE,
            auto_role_ids=[],
            self_roles_channel_id=None,
            self_roles_message_id=None,
            self_role_ids=[],
        )
        await self.upsert_config(cfg)
        return cfg

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member) -> None:
        if member.guild is None or member.bot:
            return

        cfg = await self.fetch_config(member.guild.id)
        if not cfg:
            return

        if cfg.auto_role_ids:
            added: list[discord.Role] = []
            missing: list[int] = []
            for role_id in cfg.auto_role_ids:
                role = member.guild.get_role(role_id)
                if role is None:
                    missing.append(role_id)
                    continue
                added.append(role)

            if added:
                try:
                    await member.add_roles(*added, reason="Configured auto-role on join")
                except discord.Forbidden:
                    log.warning("Missing permissions to auto-assign roles in guild %s", member.guild.id)
                except discord.HTTPException:
                    log.warning("HTTP failure assigning auto-roles in guild %s", member.guild.id)

            if missing:
                log.warning("Missing configured auto-role IDs in guild %s: %s", member.guild.id, missing)

        if cfg.welcome_channel_id:
            channel = member.guild.get_channel(cfg.welcome_channel_id)
            if isinstance(channel, discord.TextChannel):
                message = cfg.welcome_message or DEFAULT_WELCOME_MESSAGE
                rendered = (
                    message
                    .replace("{user_mention}", member.mention)
                    .replace("{user_name}", member.display_name)
                    .replace("{server_name}", member.guild.name)
                )
                try:
                    await channel.send(rendered)
                except discord.HTTPException:
                    log.warning("Welcome message failed in guild %s", member.guild.id)

    community = app_commands.Group(name="community", description="Welcome, auto-role, role picker, and bot posting tools.")

    @community.command(name="welcome_set", description="Set the welcome channel and template message.")
    @app_commands.default_permissions(manage_guild=True)
    async def welcome_set(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        message: str,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self._require_cfg(interaction.guild.id)
        cfg.welcome_channel_id = channel.id
        cfg.welcome_message = message[:1500]
        await self.upsert_config(cfg)
        await interaction.response.send_message(
            "✅ Welcome system updated. Supported placeholders: `{user_mention}`, `{user_name}`, `{server_name}`.",
            ephemeral=True,
        )

    @community.command(name="welcome_test", description="Preview the configured welcome message in the welcome channel.")
    @app_commands.default_permissions(manage_guild=True)
    async def welcome_test(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if not cfg or not cfg.welcome_channel_id:
            await interaction.response.send_message("Welcome system is not configured yet.", ephemeral=True)
            return
        channel = interaction.guild.get_channel(cfg.welcome_channel_id)
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("Welcome channel is missing. Set it again.", ephemeral=True)
            return

        rendered = (
            cfg.welcome_message
            .replace("{user_mention}", interaction.user.mention)
            .replace("{user_name}", interaction.user.display_name)
            .replace("{server_name}", interaction.guild.name)
        )
        await channel.send(rendered)
        await interaction.response.send_message(f"✅ Sent a welcome preview in {channel.mention}.", ephemeral=True)

    @community.command(name="autorole_set", description="Set roles that should be auto-assigned when users join.")
    @app_commands.default_permissions(manage_guild=True)
    async def autorole_set(
        self,
        interaction: discord.Interaction,
        role_1: discord.Role,
        role_2: discord.Role | None = None,
        role_3: discord.Role | None = None,
        role_4: discord.Role | None = None,
        role_5: discord.Role | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self._require_cfg(interaction.guild.id)
        picked = [r for r in [role_1, role_2, role_3, role_4, role_5] if r is not None]
        cfg.auto_role_ids = [role.id for role in picked]
        await self.upsert_config(cfg)
        await interaction.response.send_message(
            "✅ Auto-roles updated: " + ", ".join(role.mention for role in picked),
            ephemeral=True,
        )

    @community.command(name="selfroles_set", description="Set self-selectable roles.")
    @app_commands.default_permissions(manage_guild=True)
    async def selfroles_set(
        self,
        interaction: discord.Interaction,
        role_1: discord.Role,
        role_2: discord.Role | None = None,
        role_3: discord.Role | None = None,
        role_4: discord.Role | None = None,
        role_5: discord.Role | None = None,
        role_6: discord.Role | None = None,
        role_7: discord.Role | None = None,
        role_8: discord.Role | None = None,
        role_9: discord.Role | None = None,
        role_10: discord.Role | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self._require_cfg(interaction.guild.id)
        picked = [
            r
            for r in [role_1, role_2, role_3, role_4, role_5, role_6, role_7, role_8, role_9, role_10]
            if r is not None
        ]
        cfg.self_role_ids = [role.id for role in picked][:25]
        await self.upsert_config(cfg)
        await interaction.response.send_message(
            "✅ Self-roles updated: " + ", ".join(role.mention for role in picked),
            ephemeral=True,
        )

    @community.command(name="selfroles_panel", description="Post or refresh the self-role panel in a channel.")
    @app_commands.default_permissions(manage_guild=True)
    async def selfroles_panel(self, interaction: discord.Interaction, channel: discord.TextChannel) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self._require_cfg(interaction.guild.id)
        if not cfg.self_role_ids:
            await interaction.response.send_message("Set self-roles first with `/community selfroles_set`.", ephemeral=True)
            return

        embed = discord.Embed(
            title="Pick Your Roles",
            description="Tap **Choose Roles** to pick or remove your server roles.",
            color=discord.Color.blurple(),
        )
        msg = await channel.send(embed=embed, view=self.launch_view)
        cfg.self_roles_channel_id = channel.id
        cfg.self_roles_message_id = msg.id
        await self.upsert_config(cfg)
        await interaction.response.send_message(f"✅ Self-role panel posted in {channel.mention}.", ephemeral=True)

    @community.command(name="botsay", description="Send a simple bot message to a selected channel.")
    @app_commands.default_permissions(manage_messages=True)
    async def botsay(self, interaction: discord.Interaction, channel: discord.TextChannel, message: str) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        content = message.strip()
        if not content:
            await interaction.response.send_message("Message cannot be empty.", ephemeral=True)
            return
        if len(content) > 1900:
            await interaction.response.send_message("Message is too long (max 1900 chars).", ephemeral=True)
            return

        try:
            await channel.send(content)
        except discord.Forbidden:
            await interaction.response.send_message("I can't send messages to that channel.", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message("Failed to send message due to a Discord error.", ephemeral=True)
            return

        await interaction.response.send_message(f"✅ Sent message in {channel.mention}.", ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(CommunityToolsCog(bot))
