from __future__ import annotations

import asyncio
import logging
import sys
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands

from services.error_logging import (
    ErrorDumpWriter,
    build_context_from_command,
    build_context_from_interaction,
    merge_logging_context,
)

log = logging.getLogger("error_monitor.cog")


def _is_owner_or_admin(bot: commands.Bot, user: discord.abc.User | discord.Member | None) -> bool:
    if user is None:
        return False
    owner_ids = getattr(bot, "owner_ids", set())
    if getattr(user, "id", None) in owner_ids:
        return True
    return isinstance(user, discord.Member) and user.guild_permissions.administrator


class ErrorMonitor(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.writer = getattr(bot, "error_dump_writer", ErrorDumpWriter())
        setattr(bot, "error_dump_writer", self.writer)
        self._install_ui_hooks()
        self._install_exception_hooks()

    def cog_unload(self) -> None:
        if getattr(self.bot, "error_monitor_cog", None) is self:
            self.bot.error_monitor_cog = None

    def _install_ui_hooks(self) -> None:
        if not getattr(discord.ui.View, "_pulse_error_monitor_installed", False):
            original_view_error = discord.ui.View.on_error

            async def view_on_error(view: discord.ui.View, interaction: discord.Interaction, error: Exception, item: discord.ui.Item[Any]) -> None:
                bot = getattr(interaction.client, "error_monitor_cog", None)
                if bot is not None:
                    extras = {"view_type": type(view).__name__, "item_type": type(item).__name__, "item_custom_id": getattr(item, "custom_id", None)}
                    context = merge_logging_context(build_context_from_interaction(interaction), extras=extras)
                    bot.log_exception(error, source="view", **context)
                await original_view_error(view, interaction, error, item)

            discord.ui.View.on_error = view_on_error
            setattr(discord.ui.View, "_pulse_error_monitor_installed", True)

        if not getattr(discord.ui.Modal, "_pulse_error_monitor_installed", False):
            original_modal_error = discord.ui.Modal.on_error

            async def modal_on_error(modal: discord.ui.Modal, interaction: discord.Interaction, error: Exception) -> None:
                bot = getattr(interaction.client, "error_monitor_cog", None)
                if bot is not None:
                    extras = {"modal_type": type(modal).__name__, "modal_title": getattr(modal, "title", None)}
                    context = merge_logging_context(build_context_from_interaction(interaction), extras=extras)
                    bot.log_exception(error, source="modal", **context)
                await original_modal_error(modal, interaction, error)

            discord.ui.Modal.on_error = modal_on_error
            setattr(discord.ui.Modal, "_pulse_error_monitor_installed", True)

    def _install_exception_hooks(self) -> None:
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()

        def handle_loop_exception(loop_: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
            try:
                exception = context.get("exception")
                task_name = getattr(context.get("task"), "get_name", lambda: None)()
                event_name = context.get("message")
                loop_extras = {k: repr(v) for k, v in context.items() if k != "exception"}
                if isinstance(exception, BaseException):
                    self.log_exception(
                        exception,
                        source="asyncio",
                        **merge_logging_context(
                            event_name=event_name,
                            task_name=task_name,
                            extras=loop_extras,
                        ),
                    )
                else:
                    self.log_exception(
                        RuntimeError(context.get("message", "Asyncio loop exception")),
                        source="asyncio",
                        **merge_logging_context(
                            event_name=event_name,
                            task_name=task_name,
                            extras={k: repr(v) for k, v in context.items()},
                        ),
                    )
            except Exception:
                log.exception("Error monitor failed inside asyncio exception handler")
            if previous_handler is not None:
                previous_handler(loop_, context)
            else:
                loop_.default_exception_handler(context)

        loop.set_exception_handler(handle_loop_exception)

        previous_hook = sys.excepthook

        def handle_sys_exception(exc_type: type[BaseException], exc_value: BaseException, exc_traceback: Any) -> None:
            try:
                self.log_exception(exc_value, source="sys.excepthook", event_name="unhandled_exception")
            except Exception:
                log.exception("Error monitor failed inside sys.excepthook")
            previous_hook(exc_type, exc_value, exc_traceback)

        sys.excepthook = handle_sys_exception

    def log_exception(self, error: BaseException, *, source: str, **context: Any) -> None:
        try:
            self.writer.log_error(error, source=source, **context)
        except Exception:
            log.exception("Error monitor failed to log exception from source=%s", source)

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context[Any], error: commands.CommandError) -> None:
        if getattr(ctx.command, "on_error", None):
            return
        if getattr(ctx.cog, "cog_command_error", None) and ctx.cog is not self:
            return
        self.log_exception(error, source="prefix_command", **build_context_from_command(ctx))

    @commands.Cog.listener()
    async def on_error(self, event_method: str, *args: Any, **kwargs: Any) -> None:
        error = sys.exc_info()[1]
        if error is None:
            error = RuntimeError(f"Unhandled event error in {event_method}")
        extras = {
            "arg_count": len(args),
            "kwarg_keys": list(kwargs.keys()),
        }
        self.log_exception(
            error,
            source="event",
            **merge_logging_context(event_name=event_method, extras=extras),
        )

    @app_commands.command(name="recenterrors", description="Show the most recent logged bot errors.")
    @app_commands.default_permissions(administrator=True)
    async def recent_errors(self, interaction: discord.Interaction, limit: app_commands.Range[int, 1, 10] = 5) -> None:
        if not _is_owner_or_admin(self.bot, interaction.user):
            await interaction.response.send_message("You are not allowed to inspect error logs.", ephemeral=True)
            return

        entries = self.writer.recent_errors(limit)
        if not entries:
            await interaction.response.send_message("No recent errors have been captured yet.", ephemeral=True)
            return

        lines = []
        for entry in reversed(entries):
            lines.append(
                f"• `{entry.timestamp}` | `{entry.error_type}` | source=`{entry.source}` | "
                f"command=`{entry.command_name or '-'}` | event=`{entry.event_name or '-'}` | task=`{entry.task_name or '-'}`\n"
                f"  {entry.error_message[:240]}"
            )

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @app_commands.command(name="lasterrordump", description="Upload the current daily error dump file.")
    @app_commands.default_permissions(administrator=True)
    async def last_error_dump(self, interaction: discord.Interaction) -> None:
        if not _is_owner_or_admin(self.bot, interaction.user):
            await interaction.response.send_message("You are not allowed to inspect error logs.", ephemeral=True)
            return

        log_path = self.writer.latest_log_path()
        if not log_path.exists():
            await interaction.response.send_message("No error dump file exists yet for today.", ephemeral=True)
            return

        await interaction.response.send_message(
            content=f"Latest error dump: `{log_path.name}`",
            ephemeral=True,
            file=discord.File(log_path),
        )


async def setup(bot: commands.Bot) -> None:
    cog = ErrorMonitor(bot)
    setattr(bot, "error_monitor_cog", cog)
    await bot.add_cog(cog)
