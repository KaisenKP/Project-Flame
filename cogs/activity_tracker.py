# cogs/activity_tracker.py
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import discord
from discord.ext import commands, tasks
from sqlalchemy import select

from db.models import VoiceSessionRow
from services.activity_rules import CHAT_XP, VOICE_XP, vc_xp_per_minute
from services.db import sessions
from services.users import ensure_user_rows
from services.xp_award import award_xp, get_or_create_activity_daily
from services.achievements import (
    CHATROOM_CHANNEL_ID,
    JEVARIUS_BOT_ID,
    SELFIE_CHANNEL_ID,
    check_and_grant_achievements,
    increment_counter,
    queue_achievement_announcements,
)


_ws_re = re.compile(r"\s+")
_nonword_re = re.compile(r"[^\w]+", re.UNICODE)


def _utc_today_date():
    return datetime.now(timezone.utc).date()


def _normalize_message_content(s: str) -> str:
    s = (s or "").strip().lower()
    s = _ws_re.sub(" ", s)
    return s


def _is_emoji_only_or_empty(s: str) -> bool:
    s = (s or "").strip()
    if not s:
        return True
    has_word = bool(re.search(r"\w", s, flags=re.UNICODE))
    if has_word:
        return False
    nonword = _nonword_re.sub("", s)
    return len(nonword) == 0


@dataclass
class _ChatUserState:
    last_award_ts: float = 0.0
    last_norm: str = ""
    last_norm_ts: float = 0.0


ACHIEVEMENT_CHECK_COOLDOWN_SECONDS = 20.0


class ActivityTrackerCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()

        self._chat_state: Dict[Tuple[int, int], _ChatUserState] = {}

        # (guild_id, user_id) -> last_award_unix
        self._vc_last_award: Dict[Tuple[int, int], float] = {}
        # (guild_id, user_id) -> last_achievement_check_unix
        self._achievement_last_check: Dict[Tuple[int, int], float] = {}

        self.vc_tick.start()

    def cog_unload(self):
        self.vc_tick.cancel()

    # --------------------
    # Chat XP
    # --------------------

    async def _award_chat_xp_if_eligible(
        self,
        *,
        guild_id: int,
        user_id: int,
        content: str,
    ) -> bool:
        now = time.time()
        key = (guild_id, user_id)
        state = self._chat_state.get(key)
        if state is None:
            state = _ChatUserState()
            self._chat_state[key] = state

        norm = _normalize_message_content(content)

        if len(norm) < CHAT_XP.min_chars:
            return False

        if _is_emoji_only_or_empty(content):
            return False

        if now - state.last_award_ts < CHAT_XP.cooldown_seconds:
            return False

        if state.last_norm and norm == state.last_norm and (now - state.last_norm_ts) < CHAT_XP.repeat_window_seconds:
            return False

        state.last_award_ts = now
        state.last_norm = norm
        state.last_norm_ts = now
        return True

    def _should_check_achievements(self, *, guild_id: int, user_id: int) -> bool:
        now = time.time()
        key = (guild_id, user_id)
        last = self._achievement_last_check.get(key, 0.0)
        if now - last < ACHIEVEMENT_CHECK_COOLDOWN_SECONDS:
            return False
        self._achievement_last_check[key] = now
        return True

    async def _is_reply_to_jevarius(self, message: discord.Message) -> bool:
        ref = message.reference
        if ref is None or ref.message_id is None:
            return False

        resolved = ref.resolved
        if isinstance(resolved, discord.Message):
            return bool(resolved.author and resolved.author.id == JEVARIUS_BOT_ID)

        # Avoid fetching uncached referenced messages from the API in the hot on_message path.
        # If the message isn't resolved in cache, we skip this optional counter increment.
        return False

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.guild is None:
            return
        if message.author.bot:
            return

        guild_id = message.guild.id
        user_id = message.author.id

        ok = await self._award_chat_xp_if_eligible(
            guild_id=guild_id,
            user_id=user_id,
            content=message.content or "",
        )
        if not ok:
            return

        unlocks = []
        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                day = _utc_today_date()
                daily = await get_or_create_activity_daily(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    day=day,
                )
                daily.message_count += 1
                await increment_counter(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    counter_key="messages_sent",
                    amount=1,
                )
                if message.channel.id == CHATROOM_CHANNEL_ID:
                    await increment_counter(
                        session,
                        guild_id=guild_id,
                        user_id=user_id,
                        counter_key="chatroom_messages",
                        amount=1,
                    )
                if message.attachments:
                    await increment_counter(
                        session,
                        guild_id=guild_id,
                        user_id=user_id,
                        counter_key="images_posted",
                        amount=1,
                    )
                if message.channel.id == SELFIE_CHANNEL_ID and message.attachments:
                    await increment_counter(
                        session,
                        guild_id=guild_id,
                        user_id=user_id,
                        counter_key="selfies_posted",
                        amount=1,
                    )

                mentioned_jevarius = any(m.id == JEVARIUS_BOT_ID for m in message.mentions)
                replied_to_jevarius = await self._is_reply_to_jevarius(message)
                if mentioned_jevarius or replied_to_jevarius:
                    await increment_counter(
                        session,
                        guild_id=guild_id,
                        user_id=user_id,
                        counter_key="jevarius_interactions",
                        amount=1,
                    )

                await award_xp(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    amount=CHAT_XP.xp_per_message,
                )
                if self._should_check_achievements(guild_id=guild_id, user_id=user_id):
                    unlocks = await check_and_grant_achievements(
                        session,
                        guild_id=guild_id,
                        user_id=user_id,
                    )
        if unlocks:
            queue_achievement_announcements(
                bot=self.bot,
                guild_id=guild_id,
                user_id=user_id,
                unlocks=unlocks,
            )

    @commands.Cog.listener()
    async def on_reaction_add(self, reaction: discord.Reaction, user: discord.abc.User):
        if user.bot:
            return
        message = reaction.message
        if message.guild is None:
            return

        guild_id = message.guild.id
        user_id = user.id
        unlocks = []
        async with self.sessionmaker() as session:
            async with session.begin():
                await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)
                await increment_counter(
                    session,
                    guild_id=guild_id,
                    user_id=user_id,
                    counter_key="reactions_added",
                    amount=1,
                )
                if self._should_check_achievements(guild_id=guild_id, user_id=user_id):
                    unlocks = await check_and_grant_achievements(
                        session,
                        guild_id=guild_id,
                        user_id=user_id,
                    )
        if unlocks:
            queue_achievement_announcements(
                bot=self.bot,
                guild_id=guild_id,
                user_id=user_id,
                unlocks=unlocks,
            )

    # --------------------
    # VC helpers
    # --------------------

    def _afk_channel_id(self, guild: discord.Guild) -> Optional[int]:
        return guild.afk_channel.id if guild.afk_channel else None

    def _is_ignored_channel(self, guild: discord.Guild, channel: Optional[discord.abc.GuildChannel]) -> bool:
        if channel is None:
            return True
        if VOICE_XP.ignore_afk_channel:
            afk_id = self._afk_channel_id(guild)
            if afk_id is not None and getattr(channel, "id", None) == afk_id:
                return True
        return False

    def _member_muted_or_deafened(self, member: discord.Member) -> bool:
        vs = member.voice
        if vs is None:
            return True

        # self mute/deaf
        if bool(vs.self_mute) or bool(vs.self_deaf):
            return True

        # server mute/deaf
        if bool(vs.mute) or bool(vs.deaf):
            return True

        return False

    def _vc_member_eligible_now(self, member: discord.Member) -> bool:
        if member.bot:
            return False
        if member.guild is None:
            return False

        vs = member.voice
        if vs is None or vs.channel is None:
            return False

        if self._is_ignored_channel(member.guild, vs.channel):
            return False

        # NEW RULE: no XP if muted or deafened
        if self._member_muted_or_deafened(member):
            return False

        return True

    def _eligible_humans_in_channel(self, channel: discord.VoiceChannel) -> int:
        # counts only members who would be eligible to earn VC XP right now
        count = 0
        for m in channel.members:
            if self._vc_member_eligible_now(m):
                count += 1
        return count

    # --------------------
    # VC Session logging (join/leave) + bookkeeping baseline
    # --------------------

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ):
        if member.bot or member.guild is None:
            return

        guild_id = member.guild.id
        user_id = member.id
        key = (guild_id, user_id)

        before_chan = before.channel
        after_chan = after.channel

        if before_chan is None and after_chan is None:
            return

        # Joined VC: create session + set baseline to now
        if before_chan is None and after_chan is not None:
            if self._is_ignored_channel(member.guild, after_chan):
                return

            self._vc_last_award[key] = time.time()

            async with self.sessionmaker() as session:
                async with session.begin():
                    await ensure_user_rows(session, guild_id=guild_id, user_id=user_id)

                    row = VoiceSessionRow(
                        guild_id=guild_id,
                        user_id=user_id,
                        channel_id=after_chan.id,
                        left_at=None,
                        counted=False,
                    )
                    session.add(row)
            return

        # Left VC: close session + cleanup baseline
        if before_chan is not None and after_chan is None:
            self._vc_last_award.pop(key, None)

            if self._is_ignored_channel(member.guild, before_chan):
                return

            async with self.sessionmaker() as session:
                pending_unlocks: dict[int, list] = {}
                async with session.begin():
                    open_row = await session.scalar(
                        select(VoiceSessionRow)
                        .where(
                            VoiceSessionRow.guild_id == guild_id,
                            VoiceSessionRow.user_id == user_id,
                            VoiceSessionRow.left_at.is_(None),
                        )
                        .order_by(VoiceSessionRow.joined_at.desc())
                    )
                    if open_row is None:
                        return

                    open_row.left_at = datetime.now(timezone.utc)
                    open_row.counted = True
            return

        # Switched channels: close old, open new, reset baseline
        if before_chan is not None and after_chan is not None and before_chan.id != after_chan.id:
            self._vc_last_award[key] = time.time()

            async with self.sessionmaker() as session:
                async with session.begin():
                    open_row = await session.scalar(
                        select(VoiceSessionRow)
                        .where(
                            VoiceSessionRow.guild_id == guild_id,
                            VoiceSessionRow.user_id == user_id,
                            VoiceSessionRow.left_at.is_(None),
                        )
                        .order_by(VoiceSessionRow.joined_at.desc())
                    )
                    if open_row is not None:
                        open_row.left_at = datetime.now(timezone.utc)
                        open_row.counted = True

                    if not self._is_ignored_channel(member.guild, after_chan):
                        row = VoiceSessionRow(
                            guild_id=guild_id,
                            user_id=user_id,
                            channel_id=after_chan.id,
                            left_at=None,
                            counted=False,
                        )
                        session.add(row)
            return

    # --------------------
    # VC Live XP ticker
    # --------------------

    @tasks.loop(seconds=30)
    async def vc_tick(self):
        now_ts = time.time()

        for guild in self.bot.guilds:
            if guild.unavailable:
                continue

            per_min = vc_xp_per_minute()
            day = _utc_today_date()

            # Build awards in memory first
            awards: list[tuple[int, int]] = []  # (user_id, minutes_to_award)
            vc_seconds_add: list[tuple[int, int]] = []  # (user_id, seconds_to_add)

            for vc in guild.voice_channels:
                if self._is_ignored_channel(guild, vc):
                    continue

                # NEW RULE: no XP unless at least 2 eligible humans are present
                eligible_count = self._eligible_humans_in_channel(vc)
                if eligible_count < 2:
                    # Reset baselines so they can't "bank" solo time
                    for m in vc.members:
                        if m.bot:
                            continue
                        self._vc_last_award.pop((guild.id, m.id), None)
                    continue

                for member in vc.members:
                    if not self._vc_member_eligible_now(member):
                        # Also reset baseline so they can't "bank" muted/deaf time
                        self._vc_last_award.pop((guild.id, member.id), None)
                        continue

                    key = (guild.id, member.id)
                    last = self._vc_last_award.get(key)
                    if last is None:
                        self._vc_last_award[key] = now_ts
                        continue

                    elapsed = now_ts - last
                    minutes = int(elapsed // 60)
                    if minutes <= 0:
                        continue

                    # Enforce minimum session duration by delaying first payout naturally:
                    # baseline is set at join (or when eligibility begins), so payout only starts after time passes.
                    min_minutes = int(VOICE_XP.min_session_seconds // 60) if VOICE_XP.min_session_seconds > 0 else 0
                    if min_minutes > 0:
                        # We only want to start paying after they've been eligible long enough.
                        # If they just became eligible recently, baseline was reset and minutes will be small anyway.
                        # This check prevents tiny payouts if you later lower tick interval.
                        pass

                    awards.append((member.id, minutes))
                    vc_seconds_add.append((member.id, minutes * 60))

                    self._vc_last_award[key] = last + (minutes * 60)

            if not awards:
                continue

            pending_unlocks: dict[int, list] = {}
            async with self.sessionmaker() as session:
                async with session.begin():
                    for user_id, seconds in vc_seconds_add:
                        await ensure_user_rows(session, guild_id=guild.id, user_id=user_id)
                        daily = await get_or_create_activity_daily(
                            session,
                            guild_id=guild.id,
                            user_id=user_id,
                            day=day,
                        )
                        daily.vc_seconds += int(seconds)

                    for user_id, minutes in awards:
                        await increment_counter(
                            session,
                            guild_id=guild.id,
                            user_id=user_id,
                            counter_key="vc_minutes",
                            amount=minutes,
                        )

                    for user_id, minutes in awards:
                        await award_xp(
                            session,
                            guild_id=guild.id,
                            user_id=user_id,
                            amount=minutes * per_min,
                        )
                        unlocks = await check_and_grant_achievements(
                            session,
                            guild_id=guild.id,
                            user_id=user_id,
                        )
                        if unlocks:
                            pending_unlocks[user_id] = unlocks
            for user_id, unlocks in pending_unlocks.items():
                queue_achievement_announcements(
                    bot=self.bot,
                    guild_id=guild.id,
                    user_id=user_id,
                    unlocks=unlocks,
                )

    @vc_tick.before_loop
    async def _before_vc_tick(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    await bot.add_cog(ActivityTrackerCog(bot))
