from __future__ import annotations

import asyncio
import logging
import sys
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands

from services.error_logging import build_context_from_command, build_context_from_interaction, merge_logging_context

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
        self.diagnostics = getattr(bot, "startup_diagnostics", None)
        self._install_ui_hooks()
        self._install_exception_hooks()

    def _capture(self, error: BaseException, *, subsystem: str, source: str, category: str, context: dict[str, Any] | None = None) -> None:
        if self.diagnostics is None:
            return
        context = context or {}
        self.diagnostics.capture_exception(
            error,
            category=category,
            subsystem=subsystem,
            source=source,
            summary=str(error),
            guild_id=getattr(context.get("guild"), "id", None),
            channel_id=getattr(context.get("channel"), "id", None),
            user_id=getattr(context.get("user"), "id", None),
            command_name=context.get("command_name"),
            interaction_type=context.get("interaction_type"),
            task_name=context.get("task_name"),
            extra_context=context.get("extras", {}),
        )

    def _install_ui_hooks(self) -> None:
        if not getattr(discord.ui.View, "_pulse_diagnostics_installed", False):
            original_view_error = discord.ui.View.on_error

            async def view_on_error(view: discord.ui.View, interaction: discord.Interaction, error: Exception, item: discord.ui.Item[Any]) -> None:
                diag = getattr(interaction.client, "startup_diagnostics", None)
                if diag is not None:
                    extras = {"view_type": type(view).__name__, "item_type": type(item).__name__, "item_custom_id": getattr(item, "custom_id", None)}
                    context = merge_logging_context(build_context_from_interaction(interaction), extras=extras)
                    diag.capture_exception(
                        error,
                        category="view",
                        subsystem="views",
                        source="view.on_error",
                        summary=str(error),
                        guild_id=getattr(context.get("guild"), "id", None),
                        channel_id=getattr(context.get("channel"), "id", None),
                        user_id=getattr(context.get("user"), "id", None),
                        command_name=context.get("command_name"),
                        interaction_type=context.get("interaction_type"),
                        extra_context=context.get("extras", {}),
                    )
                await original_view_error(view, interaction, error, item)

            discord.ui.View.on_error = view_on_error
            setattr(discord.ui.View, "_pulse_diagnostics_installed", True)

        if not getattr(discord.ui.Modal, "_pulse_diagnostics_installed", False):
            original_modal_error = discord.ui.Modal.on_error

            async def modal_on_error(modal: discord.ui.Modal, interaction: discord.Interaction, error: Exception) -> None:
                diag = getattr(interaction.client, "startup_diagnostics", None)
                if diag is not None:
                    extras = {"modal_type": type(modal).__name__, "modal_title": getattr(modal, "title", None)}
                    context = merge_logging_context(build_context_from_interaction(interaction), extras=extras)
                    diag.capture_exception(
                        error,
                        category="modal",
                        subsystem="modals",
                        source="modal.on_error",
                        summary=str(error),
                        guild_id=getattr(context.get("guild"), "id", None),
                        channel_id=getattr(context.get("channel"), "id", None),
                        user_id=getattr(context.get("user"), "id", None),
                        command_name=context.get("command_name"),
                        interaction_type=context.get("interaction_type"),
                        extra_context=context.get("extras", {}),
                    )
                await original_modal_error(modal, interaction, error)

            discord.ui.Modal.on_error = modal_on_error
            setattr(discord.ui.Modal, "_pulse_diagnostics_installed", True)

    def _install_exception_hooks(self) -> None:
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()

        def handle_loop_exception(loop_: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
            try:
                exception = context.get("exception")
                if isinstance(exception, BaseException) and self.diagnostics is not None:
                    self.diagnostics.capture_exception(
                        exception,
                        category="asyncio",
                        subsystem="tasks",
                        source="error_monitor.loop_handler",
                        summary=context.get("message", "Asyncio loop exception"),
                        task_name=getattr(context.get("task"), "get_name", lambda: None)(),
                        extra_context={k: repr(v) for k, v in context.items() if k != "exception"},
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
                if self.diagnostics is not None:
                    self.diagnostics.capture_exception(
                        exc_value,
                        category="unhandled",
                        subsystem="process",
                        source="error_monitor.sys.excepthook",
                        summary="Unhandled exception",
                    )
            except Exception:
                log.exception("Error monitor failed inside sys.excepthook")
            previous_hook(exc_type, exc_value, exc_traceback)

        sys.excepthook = handle_sys_exception

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context[Any], error: commands.CommandError) -> None:
        if getattr(ctx.command, "on_error", None):
            return
        if getattr(ctx.cog, "cog_command_error", None) and ctx.cog is not self:
            return
        self._capture(error, subsystem="commands", source="cog.on_command_error", category="prefix_command", context=build_context_from_command(ctx))

    @commands.Cog.listener()
    async def on_error(self, event_method: str, *args: Any, **kwargs: Any) -> None:
        error = sys.exc_info()[1]
        if error is None:
            error = RuntimeError(f"Unhandled event error in {event_method}")
        self._capture(
            error,
            subsystem="events",
            source=event_method,
            category="event",
            context=merge_logging_context(event_name=event_method, extras={"arg_count": len(args), "kwarg_keys": list(kwargs.keys())}),
        )

    @app_commands.command(name="recenterrors", description="Show the most recent diagnostics errors.")
    @app_commands.default_permissions(administrator=True)
    async def recent_errors(self, interaction: discord.Interaction, limit: app_commands.Range[int, 1, 20] = 10) -> None:
        if not _is_owner_or_admin(self.bot, interaction.user):
            await interaction.response.send_message("You are not allowed to inspect diagnostics.", ephemeral=True)
            return

        if self.diagnostics is None:
            await interaction.response.send_message("Diagnostics service is unavailable.", ephemeral=True)
            return

        entries = [e for e in self.diagnostics.entries if e.status == "FAIL"][-limit:]
        if not entries:
            await interaction.response.send_message("No recent errors have been captured yet.", ephemeral=True)
            return

        lines = [
            f"• `{e.timestamp.isoformat()}` | `{e.exception_type or e.category}` | {e.subsystem}/{e.source} | {e.summary[:160]}"
            for e in reversed(entries)
        ]
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @app_commands.command(name="diagnostics", description="Open the central diagnostics report.")
    @app_commands.default_permissions(administrator=True)
    async def diagnostics_report(self, interaction: discord.Interaction) -> None:
        if not _is_owner_or_admin(self.bot, interaction.user):
            await interaction.response.send_message("You are not allowed to inspect diagnostics.", ephemeral=True)
            return

        if self.diagnostics is None:
            await interaction.response.send_message("Diagnostics service is unavailable.", ephemeral=True)
            return

        from services.startup_diagnostics import DiagnosticsReportView

        await interaction.response.send_message(
            embed=self.diagnostics.render_summary_embed(self.bot),
            view=DiagnosticsReportView(self.diagnostics),
            ephemeral=True,
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ErrorMonitor(bot))
