from __future__ import annotations

import asyncio
import contextlib
import io
import random
from dataclasses import dataclass
from datetime import datetime, timezone

import discord
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ActivityDailyRow, BusinessOwnershipRow, UserAssetRow, WalletRow, XpRow
from services.achievement_card import AchievementCardPayload, AchievementCardRenderer
from services.achievement_catalog import ACHIEVEMENT_CATALOG, AchievementDefinition, sorted_achievements

ANNOUNCEMENT_CHANNEL_ID = 1482554988759875758
CHATROOM_CHANNEL_ID = 1460856536795578443
SELFIE_CHANNEL_ID = 1460859587275001866
JEVARIUS_BOT_ID = 974297735559806986
_POST_DELAY_SECONDS = 0.5
_DELETE_AFTER_SECONDS = 10.0

_FUN_LINES = (
    "yo new achievement just dropped 👀",
    "ok wait this one goes hard",
    "achievement unlocked that was kinda cracked",
    "main character energy unlocked fr",
    "chat this is a W",
    "surprise patch notes: achievement unlocked",
)


@dataclass(frozen=True, slots=True)
class AchievementUnlock:
    achievement_key: str
    unlocked_at: datetime


@dataclass(frozen=True, slots=True)
class AchievementContext:
    guild_id: int
    user_id: int
    jobs_completed: int = 0
    businesses_owned: int = 0
    wallet_silver: int = 0
    level: int = 1
    net_worth: int = 0
    messages_sent: int = 0
    chatroom_messages: int = 0
    selfies_posted: int = 0
    images_posted: int = 0
    reactions_added: int = 0
    jevarius_interactions: int = 0
    vc_minutes: int = 0


class AchievementAnnouncementService:
    def __init__(self) -> None:
        self._renderer = AchievementCardRenderer()
        self._queue: asyncio.Queue[tuple[discord.Client, int, int, AchievementDefinition, datetime]] = asyncio.Queue()
        self._task: asyncio.Task | None = None

    def enqueue(self, *, bot: discord.Client, guild_id: int, user_id: int, definition: AchievementDefinition, unlocked_at: datetime) -> None:
        self._queue.put_nowait((bot, guild_id, user_id, definition, unlocked_at))
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._worker(), name="achievements.announcer")

    async def _worker(self) -> None:
        while not self._queue.empty():
            bot, guild_id, user_id, definition, unlocked_at = await self._queue.get()
            try:
                await self._announce(bot=bot, guild_id=guild_id, user_id=user_id, definition=definition, unlocked_at=unlocked_at)
            except Exception:
                pass
            await asyncio.sleep(_POST_DELAY_SECONDS)

    async def _announce(self, *, bot: discord.Client, guild_id: int, user_id: int, definition: AchievementDefinition, unlocked_at: datetime) -> None:
        guild = bot.get_guild(guild_id)
        if guild is None:
            return
        member = guild.get_member(user_id)
        if member is None:
            member = await guild.fetch_member(user_id)
        avatar_bytes = await member.display_avatar.replace(size=256).read()
        payload = AchievementCardPayload(
            username=member.display_name,
            user_id=user_id,
            avatar_bytes=avatar_bytes,
            achievement_name=definition.name,
            achievement_description=definition.description,
            achievement_icon=definition.icon,
            flavor_text=definition.flavor_text,
            tier=definition.tier,
            unlocked_at=unlocked_at,
        )
        png = self._renderer.render(payload)

        channel = bot.get_channel(ANNOUNCEMENT_CHANNEL_ID)
        if not isinstance(channel, discord.abc.Messageable):
            channel = await bot.fetch_channel(ANNOUNCEMENT_CHANNEL_ID)
        if not isinstance(channel, discord.abc.Messageable):
            return

        await channel.send(file=discord.File(io.BytesIO(png), filename=f"achievement-{definition.achievement_key}.png"))
        msg = await channel.send(content=random.choice(_FUN_LINES), allowed_mentions=discord.AllowedMentions.none())

        async def _cleanup() -> None:
            await asyncio.sleep(_DELETE_AFTER_SECONDS)
            with contextlib.suppress(Exception):
                await msg.delete()

        asyncio.create_task(_cleanup(), name="achievements.cleanup")


_ANNOUNCER = AchievementAnnouncementService()


def parse_unlock_condition(condition: str) -> tuple[str, int] | None:
    left, sep, right = condition.partition(">=")
    if sep != ">=":
        return None
    key = left.strip()
    value_text = right.strip().replace("_", "")
    if not key:
        return None
    try:
        return key, int(value_text)
    except ValueError:
        return None


def context_value(ctx: AchievementContext, stat_key: str) -> int:
    return int(getattr(ctx, stat_key, 0))


async def has_achievement(session: AsyncSession, *, guild_id: int, user_id: int, achievement_key: str) -> bool:
    from db.models import UserAchievementRow

    row = await session.scalar(
        select(UserAchievementRow.id).where(
            UserAchievementRow.guild_id == guild_id,
            UserAchievementRow.user_id == user_id,
            UserAchievementRow.achievement_key == achievement_key,
        )
    )
    return row is not None


async def grant_achievement(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    achievement_key: str,
) -> AchievementUnlock | None:
    from db.models import UserAchievementRow

    if achievement_key not in ACHIEVEMENT_CATALOG:
        return None

    unlocked_at = datetime.now(timezone.utc)
    row = UserAchievementRow(
        guild_id=guild_id,
        user_id=user_id,
        achievement_key=achievement_key,
        unlocked_at=unlocked_at,
    )
    try:
        async with session.begin_nested():
            session.add(row)
            await session.flush()
    except IntegrityError:
        return None
    return AchievementUnlock(achievement_key=achievement_key, unlocked_at=unlocked_at)


async def increment_counter(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
    counter_key: str,
    amount: int = 1,
) -> int:
    from db.models import UserAchievementCounterRow

    amt = max(int(amount), 0)
    row = await session.scalar(
        select(UserAchievementCounterRow).where(
            UserAchievementCounterRow.guild_id == guild_id,
            UserAchievementCounterRow.user_id == user_id,
            UserAchievementCounterRow.counter_key == counter_key,
        )
    )
    if row is None:
        row = UserAchievementCounterRow(guild_id=guild_id, user_id=user_id, counter_key=counter_key, counter_value=0)
        session.add(row)
        await session.flush()
    row.counter_value = int(row.counter_value) + amt
    await session.flush()
    return int(row.counter_value)


async def _build_context(session: AsyncSession, *, guild_id: int, user_id: int) -> AchievementContext:
    from db.models import UserAchievementCounterRow

    jobs_counter = await session.scalar(
        select(UserAchievementCounterRow.counter_value).where(
            UserAchievementCounterRow.guild_id == guild_id,
            UserAchievementCounterRow.user_id == user_id,
            UserAchievementCounterRow.counter_key == "jobs_completed",
        )
    )
    jobs_completed = int(jobs_counter or 0)

    chatroom_counter = await session.scalar(
        select(UserAchievementCounterRow.counter_value).where(
            UserAchievementCounterRow.guild_id == guild_id,
            UserAchievementCounterRow.user_id == user_id,
            UserAchievementCounterRow.counter_key == "chatroom_messages",
        )
    )
    chatroom_messages = int(chatroom_counter or 0)

    selfie_counter = await session.scalar(
        select(UserAchievementCounterRow.counter_value).where(
            UserAchievementCounterRow.guild_id == guild_id,
            UserAchievementCounterRow.user_id == user_id,
            UserAchievementCounterRow.counter_key == "selfies_posted",
        )
    )
    selfies_posted = int(selfie_counter or 0)

    images_counter = await session.scalar(
        select(UserAchievementCounterRow.counter_value).where(
            UserAchievementCounterRow.guild_id == guild_id,
            UserAchievementCounterRow.user_id == user_id,
            UserAchievementCounterRow.counter_key == "images_posted",
        )
    )
    images_posted = int(images_counter or 0)

    reactions_counter = await session.scalar(
        select(UserAchievementCounterRow.counter_value).where(
            UserAchievementCounterRow.guild_id == guild_id,
            UserAchievementCounterRow.user_id == user_id,
            UserAchievementCounterRow.counter_key == "reactions_added",
        )
    )
    reactions_added = int(reactions_counter or 0)

    jevarius_counter = await session.scalar(
        select(UserAchievementCounterRow.counter_value).where(
            UserAchievementCounterRow.guild_id == guild_id,
            UserAchievementCounterRow.user_id == user_id,
            UserAchievementCounterRow.counter_key == "jevarius_interactions",
        )
    )
    jevarius_interactions = int(jevarius_counter or 0)

    messages_sent = int(
        await session.scalar(
            select(func.coalesce(func.sum(ActivityDailyRow.message_count), 0)).where(
                ActivityDailyRow.guild_id == guild_id,
                ActivityDailyRow.user_id == user_id,
            )
        )
        or 0
    )

    vc_seconds_total = int(
        await session.scalar(
            select(func.coalesce(func.sum(ActivityDailyRow.vc_seconds), 0)).where(
                ActivityDailyRow.guild_id == guild_id,
                ActivityDailyRow.user_id == user_id,
            )
        )
        or 0
    )
    vc_minutes = vc_seconds_total // 60

    businesses_owned = int(
        await session.scalar(
            select(func.count(BusinessOwnershipRow.id)).where(
                BusinessOwnershipRow.guild_id == guild_id,
                BusinessOwnershipRow.user_id == user_id,
            )
        )
        or 0
    )

    wallet = await session.scalar(
        select(WalletRow).where(WalletRow.guild_id == guild_id, WalletRow.user_id == user_id)
    )
    xp = await session.scalar(select(XpRow).where(XpRow.guild_id == guild_id, XpRow.user_id == user_id))

    assets_value = int(
        await session.scalar(
            select(func.coalesce(func.sum(UserAssetRow.purchase_price), 0)).where(
                UserAssetRow.guild_id == guild_id,
                UserAssetRow.user_id == user_id,
                UserAssetRow.is_seized.is_(False),
            )
        )
        or 0
    )
    businesses_value = int(
        await session.scalar(
            select(func.coalesce(func.sum(BusinessOwnershipRow.total_spent), 0)).where(
                BusinessOwnershipRow.guild_id == guild_id,
                BusinessOwnershipRow.user_id == user_id,
            )
        )
        or 0
    )
    wallet_silver = int(wallet.silver) if wallet is not None else 0
    level = int(xp.level_cached) if xp is not None else 1
    net_worth = wallet_silver + assets_value + businesses_value

    return AchievementContext(
        guild_id=guild_id,
        user_id=user_id,
        jobs_completed=jobs_completed,
        businesses_owned=businesses_owned,
        wallet_silver=wallet_silver,
        level=level,
        net_worth=net_worth,
        messages_sent=messages_sent,
        chatroom_messages=chatroom_messages,
        selfies_posted=selfies_posted,
        images_posted=images_posted,
        reactions_added=reactions_added,
        jevarius_interactions=jevarius_interactions,
        vc_minutes=vc_minutes,
    )


async def build_achievement_context(session: AsyncSession, *, guild_id: int, user_id: int) -> AchievementContext:
    return await _build_context(session, guild_id=guild_id, user_id=user_id)


def check_achievement_conditions(ctx: AchievementContext) -> list[str]:
    unlocked: list[str] = []
    for definition in sorted_achievements():
        parsed = parse_unlock_condition(definition.unlock_condition)
        if parsed is None:
            continue
        stat_key, threshold = parsed
        if context_value(ctx, stat_key) >= threshold:
            unlocked.append(definition.achievement_key)
    return unlocked


async def check_and_grant_achievements(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
) -> list[AchievementUnlock]:
    ctx = await _build_context(session, guild_id=guild_id, user_id=user_id)
    candidates = check_achievement_conditions(ctx)
    granted: list[AchievementUnlock] = []
    for key in candidates:
        unlock = await grant_achievement(session, guild_id=guild_id, user_id=user_id, achievement_key=key)
        if unlock is not None:
            granted.append(unlock)
    return granted


def queue_achievement_announcements(
    *,
    bot: discord.Client,
    guild_id: int,
    user_id: int,
    unlocks: list[AchievementUnlock],
) -> None:
    for unlock in unlocks:
        definition = ACHIEVEMENT_CATALOG.get(unlock.achievement_key)
        if definition is None:
            continue
        _ANNOUNCER.enqueue(
            bot=bot,
            guild_id=guild_id,
            user_id=user_id,
            definition=definition,
            unlocked_at=unlock.unlocked_at,
        )
