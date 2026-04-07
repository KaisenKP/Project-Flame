from __future__ import annotations

import asyncio
import logging
import re
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks
from sqlalchemy import text

from services.db import sessions

log = logging.getLogger(__name__)
UTC = timezone.utc

FEED_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "media": "http://search.yahoo.com/mrss/",
    "yt": "http://www.youtube.com/xml/schemas/2015",
}
URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)


@dataclass
class YouTubeConfig:
    guild_id: int
    youtube_channel_id: str | None
    target_channel_id: int | None
    ping_mode: str
    ping_role_id: int | None
    enabled: bool


@dataclass
class FeedEntry:
    video_id: str
    title: str
    url: str
    description: str
    published_at: datetime | None


def _extract_first_url(text_value: str) -> str | None:
    match = URL_RE.search(text_value or "")
    return match.group(0) if match else None


class YouTubeNotificationsCog(commands.Cog):
    CFG_TABLE = "youtube_notification_config"
    POSTED_TABLE = "youtube_posted_videos"

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self._run_lock = asyncio.Lock()
        self.youtube_loop.start()

    def cog_unload(self) -> None:
        self.youtube_loop.cancel()

    async def cog_load(self) -> None:
        await self._ensure_tables()

    async def _ensure_tables(self) -> None:
        sql_cfg = f"""
        CREATE TABLE IF NOT EXISTS {self.CFG_TABLE} (
            guild_id BIGINT NOT NULL,
            youtube_channel_id VARCHAR(64) NULL,
            target_channel_id BIGINT NULL,
            ping_mode VARCHAR(16) NOT NULL DEFAULT 'none',
            ping_role_id BIGINT NULL,
            enabled TINYINT(1) NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id)
        );
        """
        sql_posted = f"""
        CREATE TABLE IF NOT EXISTS {self.POSTED_TABLE} (
            guild_id BIGINT NOT NULL,
            video_id VARCHAR(32) NOT NULL,
            posted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id, video_id)
        );
        """
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(text(sql_cfg))
                await session.execute(text(sql_posted))

    def _feed_url(self, youtube_channel_id: str) -> str:
        q = urllib.parse.urlencode({"channel_id": youtube_channel_id})
        return f"https://www.youtube.com/feeds/videos.xml?{q}"

    async def _fetch_feed(self, youtube_channel_id: str) -> list[FeedEntry]:
        url = self._feed_url(youtube_channel_id)

        def _download() -> bytes:
            req = urllib.request.Request(url, headers={"User-Agent": "FlameBot/1.0"})
            with urllib.request.urlopen(req, timeout=15) as response:
                return response.read()

        try:
            payload = await asyncio.to_thread(_download)
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Could not fetch YouTube feed: {exc}") from exc

        root = ET.fromstring(payload)
        entries: list[FeedEntry] = []
        for entry in root.findall("atom:entry", FEED_NS):
            video_id = (entry.findtext("yt:videoId", default="", namespaces=FEED_NS) or "").strip()
            title = (entry.findtext("atom:title", default="", namespaces=FEED_NS) or "Untitled video").strip()
            link_el = entry.find("atom:link[@rel='alternate']", FEED_NS)
            url_value = str(link_el.attrib.get("href", "")).strip() if link_el is not None else ""
            if not url_value:
                url_value = f"https://www.youtube.com/watch?v={video_id}"
            description = (entry.findtext("media:group/media:description", default="", namespaces=FEED_NS) or "").strip()
            published_raw = (entry.findtext("atom:published", default="", namespaces=FEED_NS) or "").strip()
            published = None
            if published_raw:
                try:
                    published = datetime.fromisoformat(published_raw.replace("Z", "+00:00"))
                except Exception:
                    published = None
            if video_id:
                entries.append(
                    FeedEntry(video_id=video_id, title=title, url=url_value, description=description, published_at=published)
                )
        return entries

    async def fetch_configs(self) -> list[YouTubeConfig]:
        sql = text(
            f"SELECT guild_id, youtube_channel_id, target_channel_id, ping_mode, ping_role_id, enabled FROM {self.CFG_TABLE} WHERE enabled = 1"
        )
        async with self.sessionmaker() as session:
            rows = (await session.execute(sql)).mappings().all()

        out: list[YouTubeConfig] = []
        for row in rows:
            out.append(
                YouTubeConfig(
                    guild_id=int(row["guild_id"]),
                    youtube_channel_id=str(row["youtube_channel_id"]) if row["youtube_channel_id"] else None,
                    target_channel_id=int(row["target_channel_id"]) if row["target_channel_id"] else None,
                    ping_mode=str(row["ping_mode"] or "none"),
                    ping_role_id=int(row["ping_role_id"]) if row["ping_role_id"] else None,
                    enabled=bool(row["enabled"]),
                )
            )
        return out

    async def fetch_config(self, guild_id: int) -> YouTubeConfig:
        sql = text(
            f"SELECT guild_id, youtube_channel_id, target_channel_id, ping_mode, ping_role_id, enabled FROM {self.CFG_TABLE} WHERE guild_id = :g LIMIT 1"
        )
        async with self.sessionmaker() as session:
            row = (await session.execute(sql, {"g": int(guild_id)})).mappings().first()
        if not row:
            return YouTubeConfig(guild_id=guild_id, youtube_channel_id=None, target_channel_id=None, ping_mode="none", ping_role_id=None, enabled=False)
        return YouTubeConfig(
            guild_id=int(row["guild_id"]),
            youtube_channel_id=str(row["youtube_channel_id"]) if row["youtube_channel_id"] else None,
            target_channel_id=int(row["target_channel_id"]) if row["target_channel_id"] else None,
            ping_mode=str(row["ping_mode"] or "none"),
            ping_role_id=int(row["ping_role_id"]) if row["ping_role_id"] else None,
            enabled=bool(row["enabled"]),
        )

    async def upsert_config(self, cfg: YouTubeConfig) -> None:
        sql = text(
            f"""
            INSERT INTO {self.CFG_TABLE} (guild_id, youtube_channel_id, target_channel_id, ping_mode, ping_role_id, enabled)
            VALUES (:guild_id, :youtube_channel_id, :target_channel_id, :ping_mode, :ping_role_id, :enabled)
            ON DUPLICATE KEY UPDATE
                youtube_channel_id = VALUES(youtube_channel_id),
                target_channel_id = VALUES(target_channel_id),
                ping_mode = VALUES(ping_mode),
                ping_role_id = VALUES(ping_role_id),
                enabled = VALUES(enabled)
            """
        )
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    sql,
                    {
                        "guild_id": cfg.guild_id,
                        "youtube_channel_id": cfg.youtube_channel_id,
                        "target_channel_id": cfg.target_channel_id,
                        "ping_mode": cfg.ping_mode,
                        "ping_role_id": cfg.ping_role_id,
                        "enabled": 1 if cfg.enabled else 0,
                    },
                )

    async def was_posted(self, guild_id: int, video_id: str) -> bool:
        sql = text(f"SELECT 1 FROM {self.POSTED_TABLE} WHERE guild_id = :g AND video_id = :v LIMIT 1")
        async with self.sessionmaker() as session:
            row = (await session.execute(sql, {"g": guild_id, "v": video_id})).first()
        return row is not None

    async def mark_posted(self, guild_id: int, video_id: str) -> None:
        sql = text(f"INSERT IGNORE INTO {self.POSTED_TABLE} (guild_id, video_id) VALUES (:g, :v)")
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(sql, {"g": guild_id, "v": video_id})

    def _resolve_ping(self, guild: discord.Guild, cfg: YouTubeConfig) -> str:
        mode = cfg.ping_mode.lower()
        if mode == "everyone":
            return "@everyone"
        if mode == "here":
            return "@here"
        if mode == "role" and cfg.ping_role_id:
            role = guild.get_role(cfg.ping_role_id)
            if role:
                return role.mention
        return ""

    async def _post_entry(self, guild: discord.Guild, cfg: YouTubeConfig, entry: FeedEntry) -> None:
        if not cfg.target_channel_id:
            return
        channel = guild.get_channel(cfg.target_channel_id)
        if not isinstance(channel, discord.TextChannel):
            return

        embed = discord.Embed(
            title=entry.title,
            url=entry.url,
            description=(entry.description[:1000] + "…") if len(entry.description) > 1000 else (entry.description or "New YouTube upload."),
            color=discord.Color.red(),
            timestamp=entry.published_at or datetime.now(tz=UTC),
        )
        embed.add_field(name="Watch", value=f"[Open Video]({entry.url})", inline=False)

        description_link = _extract_first_url(entry.description)
        if description_link:
            embed.add_field(name="Description Link", value=f"[Open Mentioned Link]({description_link})", inline=False)

        ping_text = self._resolve_ping(guild, cfg)
        content = f"{ping_text} New YouTube upload!".strip()
        allowed_mentions = discord.AllowedMentions(everyone=True, roles=True)
        await channel.send(content=content, embed=embed, allowed_mentions=allowed_mentions)

    @tasks.loop(minutes=5)
    async def youtube_loop(self) -> None:
        await self.bot.wait_until_ready()
        async with self._run_lock:
            for cfg in await self.fetch_configs():
                try:
                    if not cfg.youtube_channel_id:
                        continue
                    guild = self.bot.get_guild(cfg.guild_id)
                    if guild is None:
                        continue
                    entries = await self._fetch_feed(cfg.youtube_channel_id)
                    for entry in reversed(entries[:5]):
                        if await self.was_posted(guild.id, entry.video_id):
                            continue
                        await self._post_entry(guild, cfg, entry)
                        await self.mark_posted(guild.id, entry.video_id)
                except Exception as exc:
                    log.warning("YouTube loop failed for guild %s: %s", cfg.guild_id, exc)

    @youtube_loop.before_loop
    async def _before_loop(self) -> None:
        await self.bot.wait_until_ready()

    youtube = app_commands.Group(name="youtube", description="YouTube upload notifications.")

    @youtube.command(name="configure", description="Configure YouTube notifications for this server.")
    @app_commands.choices(
        ping_mode=[
            app_commands.Choice(name="none", value="none"),
            app_commands.Choice(name="role", value="role"),
            app_commands.Choice(name="everyone", value="everyone"),
            app_commands.Choice(name="here", value="here"),
        ]
    )
    @app_commands.default_permissions(manage_guild=True)
    async def configure(
        self,
        interaction: discord.Interaction,
        youtube_channel_id: str,
        target_channel: discord.TextChannel,
        ping_mode: str,
        ping_role: discord.Role | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        raw_channel_id = youtube_channel_id.strip()
        if not raw_channel_id:
            await interaction.response.send_message("YouTube channel ID is required.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            entries = await self._fetch_feed(raw_channel_id)
        except Exception as exc:
            await interaction.followup.send(f"Couldn't validate that YouTube channel feed: {exc}", ephemeral=True)
            return

        if not entries:
            await interaction.followup.send("Feed loaded but no videos were found. Check the channel ID.", ephemeral=True)
            return

        if ping_mode == "role" and ping_role is None:
            await interaction.followup.send("Select a role when ping mode is `role`.", ephemeral=True)
            return

        cfg = YouTubeConfig(
            guild_id=interaction.guild.id,
            youtube_channel_id=raw_channel_id,
            target_channel_id=target_channel.id,
            ping_mode=ping_mode,
            ping_role_id=ping_role.id if ping_role else None,
            enabled=True,
        )
        await self.upsert_config(cfg)

        newest = entries[0]
        for entry in entries[:10]:
            await self.mark_posted(interaction.guild.id, entry.video_id)
        await interaction.followup.send(
            f"✅ YouTube notifications enabled for `{raw_channel_id}` in {target_channel.mention}.\n"
            f"Ping mode: **{ping_mode}**\n"
            f"Latest video saved as baseline: {newest.url}",
            ephemeral=True,
        )

    @youtube.command(name="disable", description="Disable YouTube notifications for this server.")
    @app_commands.default_permissions(manage_guild=True)
    async def disable(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        cfg.enabled = False
        await self.upsert_config(cfg)
        await interaction.response.send_message("✅ YouTube notifications disabled.", ephemeral=True)

    @youtube.command(name="status", description="View current YouTube notification settings.")
    @app_commands.default_permissions(manage_guild=True)
    async def status(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        embed = discord.Embed(title="YouTube Notification Settings", color=discord.Color.red())
        embed.add_field(name="Enabled", value="Yes" if cfg.enabled else "No", inline=True)
        embed.add_field(name="YouTube Channel ID", value=cfg.youtube_channel_id or "Not set", inline=False)
        embed.add_field(name="Target Channel", value=f"<#{cfg.target_channel_id}>" if cfg.target_channel_id else "Not set", inline=True)
        embed.add_field(name="Ping Mode", value=cfg.ping_mode, inline=True)
        embed.add_field(name="Ping Role", value=f"<@&{cfg.ping_role_id}>" if cfg.ping_role_id else "None", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(YouTubeNotificationsCog(bot))
