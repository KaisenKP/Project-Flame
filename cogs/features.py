from __future__ import annotations

from datetime import UTC, datetime

import discord
from discord import app_commands
from discord.ext import commands


class FeaturesCog(commands.Cog):
    """Admin command that posts a comprehensive bot capabilities guide."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @staticmethod
    def _flatten_commands(commands_list: list[app_commands.Command | app_commands.Group], prefix: str = "") -> list[tuple[str, str]]:
        flattened: list[tuple[str, str]] = []
        for cmd in sorted(commands_list, key=lambda c: c.name):
            full_name = f"{prefix} {cmd.name}".strip()
            description = (cmd.description or "No description provided.").strip()

            if isinstance(cmd, app_commands.Group):
                flattened.append((f"/{full_name}", f"{description} (group)"))
                flattened.extend(FeaturesCog._flatten_commands(list(cmd.commands), full_name))
                continue

            flattened.append((f"/{full_name}", description))

        return flattened

    @staticmethod
    def _chunk_lines(lines: list[str], *, limit: int = 1000) -> list[str]:
        if not lines:
            return ["None"]

        chunks: list[str] = []
        current: list[str] = []
        current_len = 0

        for line in lines:
            line_len = len(line) + 1
            if current and current_len + line_len > limit:
                chunks.append("\n".join(current))
                current = [line]
                current_len = line_len
            else:
                current.append(line)
                current_len += line_len

        if current:
            chunks.append("\n".join(current))

        return chunks

    @app_commands.command(name="features", description="Admin: Post a full FlameBot feature and command guide.")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.guild_only()
    async def features(self, interaction: discord.Interaction) -> None:
        app_cmds = self._flatten_commands(list(self.bot.tree.get_commands()))
        command_lines = [f"• `{name}` — {desc}" for name, desc in app_cmds]

        loaded_extensions = sorted(self.bot.extensions.keys())
        extension_lines = [f"• `{ext}`" for ext in loaded_extensions] or ["• No extensions currently loaded."]

        bg_tasks = sorted(task.get_name() for task in getattr(self.bot, "_bg_tasks", set()) if not task.done())
        task_line = ", ".join(f"`{name}`" for name in bg_tasks) if bg_tasks else "No active background tasks reported."

        embed = discord.Embed(
            title="🔥 FlameBot Features + Full Command Guide",
            color=discord.Color.orange(),
            timestamp=datetime.now(tz=UTC),
            description=(
                "This is a complete operational overview of FlameBot as currently loaded in this server context.\n"
                "It includes architecture behavior, permissions model, loaded modules, and every registered slash command."
            ),
        )

        embed.add_field(
            name="How FlameBot works",
            value=(
                "• Auto-discovers and loads cogs from the `cogs` package at startup.\n"
                "• Registers slash commands via the Discord application command tree.\n"
                "• Uses a restart guard around **12:59 AM EST** and performs a scheduled restart at **1:00 AM EST**.\n"
                "• Runs background health/restart loops and structured startup diagnostics.\n"
                "• Uses persistent Discord views in subsystems that require interactive buttons/select menus."
            ),
            inline=False,
        )

        embed.add_field(
            name="Permissions + safety model",
            value=(
                "• `/features` is **admin-only** (`administrator` permission required).\n"
                "• Moderation, restart, and other sensitive commands are guarded with Discord permission checks.\n"
                "• Command execution during restart window is blocked to avoid partial or unsafe actions."
            ),
            inline=False,
        )

        for idx, chunk in enumerate(self._chunk_lines(extension_lines), start=1):
            field_name = "Loaded modules (cogs/extensions)" if idx == 1 else f"Loaded modules (cont. {idx})"
            embed.add_field(name=field_name, value=chunk, inline=False)

        for idx, chunk in enumerate(self._chunk_lines(command_lines), start=1):
            field_name = "Slash commands" if idx == 1 else f"Slash commands (cont. {idx})"
            embed.add_field(name=field_name, value=chunk, inline=False)

        embed.add_field(name="Active background tasks", value=task_line, inline=False)
        embed.set_footer(text=f"Total slash entries listed: {len(app_cmds)}")

        await interaction.response.send_message(embed=embed, ephemeral=False)

    @features.error
    async def features_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.errors.MissingPermissions):
            message = "You must be an administrator to use `/features`."
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)
            return
        raise error


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(FeaturesCog(bot))
