from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ProfileBackgroundRow, ProfileSettingsRow


@dataclass(frozen=True)
class BackgroundDef:
    key: str
    name: str
    source: str


DEFAULT_BACKGROUNDS: tuple[BackgroundDef, ...] = (
    BackgroundDef(key="neon_night", name="Neon Night", source="default"),
    BackgroundDef(key="royal_sunrise", name="Royal Sunrise", source="default"),
)

SEASONAL_BACKGROUNDS: tuple[BackgroundDef, ...] = (
    BackgroundDef(key="season_winter", name="Winter Pulse", source="seasonal"),
)

STORE_BACKGROUNDS: tuple[BackgroundDef, ...] = (
    BackgroundDef(key="store_obsidian", name="Obsidian Grid", source="store"),
)

ALL_BACKGROUNDS: dict[str, BackgroundDef] = {
    b.key: b
    for b in (*DEFAULT_BACKGROUNDS, *SEASONAL_BACKGROUNDS, *STORE_BACKGROUNDS)
}


async def ensure_profile_background_rows(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int,
) -> ProfileSettingsRow:
    settings = await session.scalar(
        select(ProfileSettingsRow).where(
            ProfileSettingsRow.guild_id == guild_id,
            ProfileSettingsRow.user_id == user_id,
        )
    )
    if settings is None:
        settings = ProfileSettingsRow(
            guild_id=guild_id,
            user_id=user_id,
            selected_background_key=DEFAULT_BACKGROUNDS[0].key,
        )
        session.add(settings)

    unlocked = await session.scalars(
        select(ProfileBackgroundRow).where(
            ProfileBackgroundRow.guild_id == guild_id,
            ProfileBackgroundRow.user_id == user_id,
        )
    )
    unlocked_keys = {row.background_key for row in unlocked}

    for bg in DEFAULT_BACKGROUNDS:
        if bg.key in unlocked_keys:
            continue
        session.add(
            ProfileBackgroundRow(
                guild_id=guild_id,
                user_id=user_id,
                background_key=bg.key,
                source=bg.source,
            )
        )

    if settings.selected_background_key not in ALL_BACKGROUNDS:
        settings.selected_background_key = DEFAULT_BACKGROUNDS[0].key

    return settings


def resolve_background_key(selected_key: str | None) -> str:
    if not selected_key:
        return DEFAULT_BACKGROUNDS[0].key
    if selected_key in ALL_BACKGROUNDS:
        return selected_key
    return DEFAULT_BACKGROUNDS[0].key
