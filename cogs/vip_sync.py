# cogs/vip_sync.py
from __future__ import annotations

import asyncio
import logging

import discord
from discord import app_commands
from discord.ext import commands, tasks

from services.config import GUILD_ID, VIP_ROLE_ID

log = logging.getLogger(__name__)

BOOSTER_ROLE_ID = 1460879338546266249


def _has_role(member: discord.Member, role_id: int) -> bool:
    return any(r.id == role_id for r in member.roles)


class VipSyncCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._lock = asyncio.Lock()
        self.vip_sync_loop.start()

    def cog_unload(self) -> None:
        self.vip_sync_loop.cancel()

    async def _sync_member(self, member: discord.Member, *, reason: str) -> bool:
        if int(GUILD_ID) > 0 and int(member.guild.id) != int(GUILD_ID):
            return False
        if int(VIP_ROLE_ID) <= 0:
            return False
        booster = member.guild.get_role(BOOSTER_ROLE_ID)
        vip = member.guild.get_role(int(VIP_ROLE_ID))
        if booster is None or vip is None:
            log.warning("VipSync: role(s) not found in guild %s", member.guild.id)
            return False

        should_have_vip = _has_role(member, BOOSTER_ROLE_ID)
        has_vip = _has_role(member, int(VIP_ROLE_ID))

        if should_have_vip and not has_vip:
            try:
                await member.add_roles(vip, reason=reason)
                return True
            except discord.Forbidden:
                log.warning("VipSync: missing perms to add VIP in guild %s", member.guild.id)
            except discord.HTTPException as e:
                log.warning("VipSync: add_roles failed: %s", e)
            return False

        if (not should_have_vip) and has_vip:
            try:
                await member.remove_roles(vip, reason=reason)
                return True
            except discord.Forbidden:
                log.warning("VipSync: missing perms to remove VIP in guild %s", member.guild.id)
            except discord.HTTPException as e:
                log.warning("VipSync: remove_roles failed: %s", e)
            return False

        return False

    async def _sync_guild(self, guild: discord.Guild, *, reason: str) -> tuple[int, int, int]:
        if int(GUILD_ID) > 0 and int(guild.id) != int(GUILD_ID):
            return (0, 0, 0)
        if int(VIP_ROLE_ID) <= 0:
            return (0, 0, 0)
        if guild.me is None:
            return (0, 0, 0)

        booster = guild.get_role(BOOSTER_ROLE_ID)
        vip = guild.get_role(int(VIP_ROLE_ID))
        if booster is None or vip is None:
            return (0, 0, 0)

        if not guild.me.guild_permissions.manage_roles:
            return (0, 0, 0)

        changed = 0
        scanned = 0
        failed = 0

        async for member in guild.fetch_members(limit=None):
            scanned += 1
            try:
                did = await self._sync_member(member, reason=reason)
                if did:
                    changed += 1
            except Exception:
                failed += 1

        return (scanned, changed, failed)

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if after.guild is None:
            return

        before_boost = _has_role(before, BOOSTER_ROLE_ID)
        after_boost = _has_role(after, BOOSTER_ROLE_ID)
        before_vip = _has_role(before, int(VIP_ROLE_ID))
        after_vip = _has_role(after, int(VIP_ROLE_ID))

        relevant_change = (before_boost != after_boost) or (before_vip != after_vip)
        if not relevant_change:
            return

        async with self._lock:
            await self._sync_member(after, reason="Booster/VIP sync (member update)")

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        async with self._lock:
            await self._sync_member(member, reason="Booster/VIP sync (member join)")

    @tasks.loop(minutes=10)
    async def vip_sync_loop(self):
        await self.bot.wait_until_ready()
        async with self._lock:
            for guild in self.bot.guilds:
                await self._sync_guild(guild, reason="Booster/VIP periodic sync")

    @vip_sync_loop.before_loop
    async def _before_vip_sync_loop(self):
        await self.bot.wait_until_ready()

    @app_commands.command(name="vip_sync_admin", description="Admin: force a full booster->VIP sync for this server.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def vip_sync_admin(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        async with self._lock:
            scanned, changed, failed = await self._sync_guild(
                interaction.guild,
                reason=f"Booster/VIP manual sync by {interaction.user.id}",
            )

        msg = f"✅ Sync complete.\nScanned: **{scanned}**\nChanged: **{changed}**"
        if failed:
            msg += f"\nFailed: **{failed}**"
        await interaction.followup.send(msg, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(VipSyncCog(bot))
