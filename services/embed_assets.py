from __future__ import annotations

import logging

from db.models import EmbedAssetRow
from services.db import sessions

log = logging.getLogger("services.embed_assets")


class EmbedAssetRepository:
    """Persist Discord image-storage references without putting SQL in the cog."""

    async def record(
        self,
        *,
        asset_id: str,
        guild_id: int,
        storage_channel_id: int,
        storage_message_id: int,
        uploaded_by_user_id: int,
        source_channel_id: int,
        filename: str,
    ) -> None:
        session_factory = sessions()
        async with session_factory() as session:
            session.add(
                EmbedAssetRow(
                    asset_id=asset_id,
                    guild_id=guild_id,
                    storage_channel_id=storage_channel_id,
                    storage_message_id=storage_message_id,
                    uploaded_by_user_id=uploaded_by_user_id,
                    source_channel_id=source_channel_id,
                    filename=filename[:255] or "unknown",
                )
            )
            await session.commit()
        log.info("Recorded embed asset %s for guild %s", asset_id, guild_id)
