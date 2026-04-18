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
CHANNEL_ID_RE = re.compile(r'"channelId"\s*:\s*"(?P<id>UC[\w-]{20,})"')
HANDLE_RE = re.compile(r"^@[\w.\-]+$")

DEFAULT_TARGET_CHANNEL_ID = 1479752298195587072
DEFAULT_TEMPLATE = (
    "🚨 **New Blaze Silver Gaming Upload!**\n"
    "**{video_title}**\n"
    "🎬 Watch now: {video_url}"
)
DEFAULT_YOUTUBE_SOURCE = "https://youtube.com/@blazesilvergaming?si=gmJMf0IA6dSZD9UP"


@dataclass
class YouTubeConfig:
    guild_id: int
    youtube_channel_source: str | None
    target_channel_id: int | None
    ping_mode: str
    ping_role_id: int | None
    message_template: str
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
        self._bootstrap_lock = asyncio.Lock()
        self._bootstrap_completed = False
        self.youtube_loop.start()

    def cog_unload(self) -> None:
        self.youtube_loop.cancel()

    async def cog_load(self) -> None:
        await self._ensure_tables()

    async def _ensure_tables(self) -> None:
        sql_cfg = f"""
        CREATE TABLE IF NOT EXISTS {self.CFG_TABLE} (
            guild_id BIGINT NOT NULL,
            youtube_channel_source VARCHAR(255) NULL,
            target_channel_id BIGINT NULL,
            ping_mode VARCHAR(16) NOT NULL DEFAULT 'none',
            ping_role_id BIGINT NULL,
            message_template TEXT NULL,
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
                # Backward compatible migrations.
                await session.execute(
                    text(
                        f"ALTER TABLE {self.CFG_TABLE} ADD COLUMN IF NOT EXISTS youtube_channel_source VARCHAR(255) NULL AFTER guild_id"
                    )
                )
                await session.execute(
                    text(
                        f"ALTER TABLE {self.CFG_TABLE} ADD COLUMN IF NOT EXISTS message_template TEXT NULL AFTER ping_role_id"
                    )
                )
                await session.execute(
                    text(
                        f"UPDATE {self.CFG_TABLE} SET youtube_channel_source = youtube_channel_id WHERE youtube_channel_source IS NULL AND youtube_channel_id IS NOT NULL"
                    )
                )

    @staticmethod
    def _normalize_channel_source(channel_source: str | None) -> str:
        source = (channel_source or "").strip().lower()
        if not source:
            return ""
        source = re.sub(r"^https?://(www\.)?", "", source)
        source = source.rstrip("/")
        source = source.split("?", 1)[0]
        return source

    def _matches_default_bootstrap_intent(self, row: dict[str, object]) -> bool:
        source = self._normalize_channel_source(str(row.get("youtube_channel_source") or ""))
        default_source = self._normalize_channel_source(DEFAULT_YOUTUBE_SOURCE)
        allowed_default_sources = {
            default_source,
            self._normalize_channel_source("https://youtube.com/@blazesilvergaming"),
            self._normalize_channel_source("@blazesilvergaming"),
        }
        ping_mode = str(row.get("ping_mode") or "none").lower()
        message_template = str(row.get("message_template") or DEFAULT_TEMPLATE)
        ping_role_id = row.get("ping_role_id")
        target_channel_id = int(row.get("target_channel_id") or DEFAULT_TARGET_CHANNEL_ID)
        return (
            source in allowed_default_sources
            and target_channel_id == DEFAULT_TARGET_CHANNEL_ID
            and ping_mode == "none"
            and message_template == DEFAULT_TEMPLATE
            and ping_role_id is None
        )

    async def _seed_baseline_posts(self, guild_id: int, youtube_channel_source: str) -> tuple[int, str | None]:
        try:
            _, entries = await self._fetch_feed(youtube_channel_source)
        except Exception as exc:
            log.warning("YouTube bootstrap feed seed failed for guild %s: %s", guild_id, exc)
            return 0, None
        seeded = 0
        latest_url: str | None = entries[0].url if entries else None
        for entry in entries[:10]:
            await self.mark_posted(guild_id, entry.video_id)
            seeded += 1
        return seeded, latest_url

    async def ensure_default_guild_configs(self) -> None:
        async with self._bootstrap_lock:
            if self._bootstrap_completed:
                return
            sql_get = text(
                f"SELECT guild_id, youtube_channel_source, target_channel_id, ping_mode, ping_role_id, message_template, enabled "
                f"FROM {self.CFG_TABLE} WHERE guild_id = :guild_id LIMIT 1"
            )
            sql_insert = text(
                f"""
                INSERT INTO {self.CFG_TABLE}
                (guild_id, youtube_channel_source, target_channel_id, ping_mode, ping_role_id, message_template, enabled)
                VALUES (:guild_id, :youtube_channel_source, :target_channel_id, :ping_mode, :ping_role_id, :message_template, :enabled)
                """
            )
            sql_enable = text(f"UPDATE {self.CFG_TABLE} SET enabled = 1 WHERE guild_id = :guild_id")

            for guild in self.bot.guilds:
                guild_id = int(guild.id)
                target_channel_id = DEFAULT_TARGET_CHANNEL_ID
                action = "skipped"
                seed_count = 0
                latest_url: str | None = None
                async with self.sessionmaker() as session:
                    async with session.begin():
                        row = (await session.execute(sql_get, {"guild_id": guild_id})).mappings().first()
                        if not row:
                            await session.execute(
                                sql_insert,
                                {
                                    "guild_id": guild_id,
                                    "youtube_channel_source": DEFAULT_YOUTUBE_SOURCE,
                                    "target_channel_id": target_channel_id,
                                    "ping_mode": "none",
                                    "ping_role_id": None,
                                    "message_template": DEFAULT_TEMPLATE,
                                    "enabled": 1,
                                },
                            )
                            action = "created"
                        else:
                            target_channel_id = int(row["target_channel_id"]) if row["target_channel_id"] else DEFAULT_TARGET_CHANNEL_ID
                            if not bool(row["enabled"]) and self._matches_default_bootstrap_intent(dict(row)):
                                await session.execute(sql_enable, {"guild_id": guild_id})
                                action = "enabled_default"

                if action in {"created", "enabled_default"}:
                    seed_count, latest_url = await self._seed_baseline_posts(guild_id, DEFAULT_YOUTUBE_SOURCE)

                if latest_url:
                    log.info(
                        "YouTube bootstrap %s for guild_id=%s target_channel_id=%s seeded=%s latest=%s",
                        action,
                        guild_id,
                        target_channel_id,
                        seed_count,
                        latest_url,
                    )
                else:
                    log.info(
                        "YouTube bootstrap %s for guild_id=%s target_channel_id=%s seeded=%s",
                        action,
                        guild_id,
                        target_channel_id,
                        seed_count,
                    )

            self._bootstrap_completed = True

    def _feed_url(self, youtube_channel_id: str) -> str:
        q = urllib.parse.urlencode({"channel_id": youtube_channel_id})
        return f"https://www.youtube.com/feeds/videos.xml?{q}"

    async def _resolve_channel_id(self, channel_source: str) -> str:
        source = (channel_source or "").strip()
        if not source:
            raise RuntimeError("Empty YouTube channel source.")
        if source.startswith("UC") and len(source) >= 22:
            return source

        if HANDLE_RE.match(source):
            url = f"https://www.youtube.com/{source}"
        elif source.startswith("http://") or source.startswith("https://"):
            url = source
        else:
            raise RuntimeError("Use a channel ID, handle (e.g. @name), or full YouTube channel URL.")

        def _download_channel_page() -> str:
            req = urllib.request.Request(url, headers={"User-Agent": "FlameBot/1.0"})
            with urllib.request.urlopen(req, timeout=15) as response:
                return response.read().decode("utf-8", errors="ignore")

        try:
            payload = await asyncio.to_thread(_download_channel_page)
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Could not resolve YouTube channel source: {exc}") from exc

        match = CHANNEL_ID_RE.search(payload)
        if not match:
            raise RuntimeError("Could not parse YouTube channel ID from that source.")
        return match.group("id")

    async def _fetch_feed(self, youtube_channel_source: str) -> tuple[str, list[FeedEntry]]:
        youtube_channel_id = await self._resolve_channel_id(youtube_channel_source)
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
        return youtube_channel_id, entries

    async def fetch_configs(self) -> list[YouTubeConfig]:
        sql = text(
            f"SELECT guild_id, youtube_channel_source, target_channel_id, ping_mode, ping_role_id, message_template, enabled FROM {self.CFG_TABLE} WHERE enabled = 1"
        )
        async with self.sessionmaker() as session:
            rows = (await session.execute(sql)).mappings().all()

        out: list[YouTubeConfig] = []
        for row in rows:
            out.append(
                YouTubeConfig(
                    guild_id=int(row["guild_id"]),
                    youtube_channel_source=str(row["youtube_channel_source"]) if row["youtube_channel_source"] else DEFAULT_YOUTUBE_SOURCE,
                    target_channel_id=int(row["target_channel_id"]) if row["target_channel_id"] else DEFAULT_TARGET_CHANNEL_ID,
                    ping_mode=str(row["ping_mode"] or "none"),
                    ping_role_id=int(row["ping_role_id"]) if row["ping_role_id"] else None,
                    message_template=str(row["message_template"] or DEFAULT_TEMPLATE),
                    enabled=bool(row["enabled"]),
                )
            )
        return out

    async def fetch_config(self, guild_id: int) -> YouTubeConfig:
        sql = text(
            f"SELECT guild_id, youtube_channel_source, target_channel_id, ping_mode, ping_role_id, message_template, enabled FROM {self.CFG_TABLE} WHERE guild_id = :g LIMIT 1"
        )
        async with self.sessionmaker() as session:
            row = (await session.execute(sql, {"g": int(guild_id)})).mappings().first()
        if not row:
            return YouTubeConfig(
                guild_id=guild_id,
                youtube_channel_source=DEFAULT_YOUTUBE_SOURCE,
                target_channel_id=DEFAULT_TARGET_CHANNEL_ID,
                ping_mode="everyone",
                ping_role_id=None,
                message_template=DEFAULT_TEMPLATE,
                enabled=False,
            )
        return YouTubeConfig(
            guild_id=int(row["guild_id"]),
            youtube_channel_source=str(row["youtube_channel_source"]) if row["youtube_channel_source"] else DEFAULT_YOUTUBE_SOURCE,
            target_channel_id=int(row["target_channel_id"]) if row["target_channel_id"] else DEFAULT_TARGET_CHANNEL_ID,
            ping_mode=str(row["ping_mode"] or "none"),
            ping_role_id=int(row["ping_role_id"]) if row["ping_role_id"] else None,
            message_template=str(row["message_template"] or DEFAULT_TEMPLATE),
            enabled=bool(row["enabled"]),
        )

    async def upsert_config(self, cfg: YouTubeConfig) -> None:
        sql = text(
            f"""
            INSERT INTO {self.CFG_TABLE} (guild_id, youtube_channel_source, target_channel_id, ping_mode, ping_role_id, message_template, enabled)
            VALUES (:guild_id, :youtube_channel_source, :target_channel_id, :ping_mode, :ping_role_id, :message_template, :enabled)
            ON DUPLICATE KEY UPDATE
                youtube_channel_source = VALUES(youtube_channel_source),
                target_channel_id = VALUES(target_channel_id),
                ping_mode = VALUES(ping_mode),
                ping_role_id = VALUES(ping_role_id),
                message_template = VALUES(message_template),
                enabled = VALUES(enabled)
            """
        )
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(
                    sql,
                    {
                        "guild_id": cfg.guild_id,
                        "youtube_channel_source": cfg.youtube_channel_source,
                        "target_channel_id": cfg.target_channel_id,
                        "ping_mode": cfg.ping_mode,
                        "ping_role_id": cfg.ping_role_id,
                        "message_template": cfg.message_template,
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

    async def claim_video(self, guild_id: int, video_id: str) -> bool:
        sql = text(f"INSERT IGNORE INTO {self.POSTED_TABLE} (guild_id, video_id) VALUES (:g, :v)")
        async with self.sessionmaker() as session:
            async with session.begin():
                result = await session.execute(sql, {"g": guild_id, "v": video_id})
                return bool(result.rowcount and result.rowcount > 0)

    async def unclaim_video(self, guild_id: int, video_id: str) -> None:
        sql = text(f"DELETE FROM {self.POSTED_TABLE} WHERE guild_id = :g AND video_id = :v")
        async with self.sessionmaker() as session:
            async with session.begin():
                await session.execute(sql, {"g": guild_id, "v": video_id})

    def _render_template(self, template: str, entry: FeedEntry) -> str:
        chosen = (template or DEFAULT_TEMPLATE).strip() or DEFAULT_TEMPLATE
        safe_map = {
            "video_title": entry.title,
            "video_url": entry.url,
            "video_id": entry.video_id,
        }
        try:
            return chosen.format_map(safe_map)
        except KeyError as exc:
            raise ValueError(f"Unknown template variable: {exc.args[0]}") from exc

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
            log.warning("YouTube notifications: target channel %s missing/inaccessible in guild %s", cfg.target_channel_id, guild.id)
            return
        me = guild.me or guild.get_member(self.bot.user.id)  # type: ignore[arg-type]
        if me is None:
            log.warning("YouTube notifications: bot member not cached for guild %s", guild.id)
            return
        perms = channel.permissions_for(me)
        if not perms.send_messages:
            log.warning("YouTube notifications: missing send_messages in channel %s (guild %s)", channel.id, guild.id)
            return
        if not perms.embed_links:
            log.warning("YouTube notifications: missing embed_links in channel %s (guild %s)", channel.id, guild.id)
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
        announcement = self._render_template(cfg.message_template, entry)
        content = f"{ping_text}\n{announcement}".strip()
        allowed_mentions = discord.AllowedMentions(everyone=True, roles=True)
        await channel.send(content=content, embed=embed, allowed_mentions=allowed_mentions)

    @tasks.loop(minutes=5)
    async def youtube_loop(self) -> None:
        await self.bot.wait_until_ready()
        async with self._run_lock:
            for cfg in await self.fetch_configs():
                try:
                    if not cfg.youtube_channel_source:
                        continue
                    guild = self.bot.get_guild(cfg.guild_id)
                    if guild is None:
                        continue
                    _, entries = await self._fetch_feed(cfg.youtube_channel_source)
                    for entry in reversed(entries[:5]):
                        claimed = await self.claim_video(guild.id, entry.video_id)
                        if not claimed:
                            continue
                        try:
                            await self._post_entry(guild, cfg, entry)
                        except Exception:
                            await self.unclaim_video(guild.id, entry.video_id)
                            raise
                except Exception as exc:
                    log.warning("YouTube loop failed for guild %s: %s", cfg.guild_id, exc)

    @youtube_loop.before_loop
    async def _before_loop(self) -> None:
        await self.bot.wait_until_ready()
        await self.ensure_default_guild_configs()

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
        ping_mode: str,
        youtube_channel: str = DEFAULT_YOUTUBE_SOURCE,
        target_channel: discord.TextChannel | None = None,
        ping_role: discord.Role | None = None,
        message_template: str | None = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        raw_channel_source = youtube_channel.strip()
        if not raw_channel_source:
            await interaction.response.send_message("YouTube channel source is required.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            resolved_channel_id, entries = await self._fetch_feed(raw_channel_source)
        except Exception as exc:
            await interaction.followup.send(f"Couldn't validate that YouTube channel feed: {exc}", ephemeral=True)
            return

        if not entries:
            await interaction.followup.send("Feed loaded but no videos were found. Check the channel ID.", ephemeral=True)
            return

        if ping_mode == "role" and ping_role is None:
            await interaction.followup.send("Select a role when ping mode is `role`.", ephemeral=True)
            return

        target = target_channel.id if target_channel else DEFAULT_TARGET_CHANNEL_ID
        chosen_template = (message_template or DEFAULT_TEMPLATE).strip() or DEFAULT_TEMPLATE
        preview_entry = entries[0]
        try:
            self._render_template(chosen_template, preview_entry)
        except ValueError as exc:
            await interaction.followup.send(f"Invalid template: {exc}", ephemeral=True)
            return

        cfg = YouTubeConfig(
            guild_id=interaction.guild.id,
            youtube_channel_source=raw_channel_source,
            target_channel_id=target,
            ping_mode=ping_mode,
            ping_role_id=ping_role.id if ping_role else None,
            message_template=chosen_template,
            enabled=True,
        )
        await self.upsert_config(cfg)

        newest = entries[0]
        for entry in entries[:10]:
            await self.mark_posted(interaction.guild.id, entry.video_id)
        await interaction.followup.send(
            f"✅ YouTube notifications enabled for `{raw_channel_source}` (`{resolved_channel_id}`) in <#{target}>.\n"
            f"Ping mode: **{ping_mode}**\n"
            f"Latest video saved as baseline: {newest.url}",
            ephemeral=True,
        )

    @youtube.command(name="template", description="View, set, or reset the YouTube announcement template.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.choices(
        action=[
            app_commands.Choice(name="view", value="view"),
            app_commands.Choice(name="set", value="set"),
            app_commands.Choice(name="reset", value="reset"),
        ]
    )
    async def template(self, interaction: discord.Interaction, action: str, template: str | None = None) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        cfg = await self.fetch_config(interaction.guild.id)
        if action == "view":
            await interaction.response.send_message(
                "Current template:\n"
                f"```{cfg.message_template}```\n"
                "Variables: `{video_title}`, `{video_url}`, `{video_id}`",
                ephemeral=True,
            )
            return
        if action == "reset":
            cfg.message_template = DEFAULT_TEMPLATE
            await self.upsert_config(cfg)
            await interaction.response.send_message("✅ Template reset to default.", ephemeral=True)
            return
        if not template:
            await interaction.response.send_message("Provide a template when using `set`.", ephemeral=True)
            return
        probe = FeedEntry(video_id="example123", title="Example Video", url="https://youtu.be/example123", description="", published_at=None)
        try:
            rendered = self._render_template(template, probe)
        except ValueError as exc:
            await interaction.response.send_message(f"Invalid template: {exc}", ephemeral=True)
            return
        cfg.message_template = template.strip()
        await self.upsert_config(cfg)
        await interaction.response.send_message(f"✅ Template updated.\nPreview:\n{rendered}", ephemeral=True)

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
        embed.add_field(name="YouTube Source", value=cfg.youtube_channel_source or "Not set", inline=False)
        embed.add_field(name="Target Channel", value=f"<#{cfg.target_channel_id}>" if cfg.target_channel_id else "Not set", inline=True)
        embed.add_field(name="Ping Mode", value=cfg.ping_mode, inline=True)
        embed.add_field(name="Ping Role", value=f"<@&{cfg.ping_role_id}>" if cfg.ping_role_id else "None", inline=True)
        embed.add_field(name="Template", value=f"```{cfg.message_template[:350]}```", inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(YouTubeNotificationsCog(bot))
