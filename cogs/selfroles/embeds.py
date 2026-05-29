# What this file is: Embed builders for self-role panels, menus, and setup summaries.
# Last change: 2026-05-29 - Initial role picker embeds.

from __future__ import annotations

from typing import TYPE_CHECKING

import discord

from .config import PANEL_DESCRIPTION, PANEL_FOOTER, PANEL_IMAGE_URL, PANEL_THUMBNAIL_URL, PANEL_TITLE, RoleCategory

if TYPE_CHECKING:
    from .service import SelfRoleSetupSummary


def _chunk_lines(lines: list[str], *, limit: int = 1000) -> list[str]:
    if not lines:
        return ["None"]
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in lines:
        size = len(line) + 1
        if current and current_len + size > limit:
            chunks.append("\n".join(current))
            current = [line]
            current_len = size
        else:
            current.append(line)
            current_len += size
    if current:
        chunks.append("\n".join(current))
    return chunks


def build_panel_embed(*, image_url: str = "", thumbnail_url: str = "") -> discord.Embed:
    embed = discord.Embed(
        title=PANEL_TITLE,
        description=PANEL_DESCRIPTION,
        color=discord.Color.blurple(),
    )
    if thumbnail_url or PANEL_THUMBNAIL_URL:
        embed.set_thumbnail(url=thumbnail_url or PANEL_THUMBNAIL_URL)
    if image_url or PANEL_IMAGE_URL:
        embed.set_image(url=image_url or PANEL_IMAGE_URL)
    embed.set_footer(text=PANEL_FOOTER)
    return embed


def build_category_embed(category: RoleCategory, *, image_url: str = "") -> discord.Embed:
    embed = discord.Embed(
        title=category.title,
        description=category.description,
        color=discord.Color.blurple(),
    )
    if category.embed_thumbnail_url:
        embed.set_thumbnail(url=category.embed_thumbnail_url)
    resolved_image = image_url or category.embed_image_url or category.banner_url
    if resolved_image:
        embed.set_image(url=resolved_image)
    return embed


def build_setup_summary_embed(summary: "SelfRoleSetupSummary") -> discord.Embed:
    embed = discord.Embed(
        title="Setup Result",
        description=summary.panel_action or "Self-role setup completed.",
        color=discord.Color.green() if not summary.warnings else discord.Color.orange(),
    )
    if summary.panel_channel_id:
        channel_value = f"<#{summary.panel_channel_id}>"
    else:
        channel_value = "Unknown"
    message_value = str(summary.panel_message_id) if summary.panel_message_id else "Unknown"
    embed.add_field(name="Panel", value=f"Channel: {channel_value}\nMessage: `{message_value}`", inline=False)

    sections = (
        ("Roles Reused by ID", summary.reused_by_id),
        ("Roles Reused from Storage", summary.reused_saved),
        ("Roles Found by Name", summary.found_by_name),
        ("Roles Created", summary.created),
        ("Warnings", summary.warnings),
    )
    for title, lines in sections:
        for idx, chunk in enumerate(_chunk_lines([f"• {line}" for line in lines]), start=1):
            name = title if idx == 1 else f"{title} (cont. {idx})"
            embed.add_field(name=name, value=chunk, inline=False)

    embed.set_footer(text="Run /setup_roles again any time to refresh this panel safely.")
    return embed
