# What this file is: Persistent per-guild storage for self-role IDs and panel metadata.
# Last change: 2026-05-29 - Initial MySQL-backed repository layer.

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text

from services.db import sessions

from .config import SCHEMA_VERSION
from .errors import SelfRoleStorageError


def _safe_json_dict(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _coerce_role_ids(raw: dict[str, Any]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for category_key, roles in raw.items():
        if not isinstance(category_key, str) or not isinstance(roles, dict):
            continue
        cleaned: dict[str, int] = {}
        for role_key, role_id in roles.items():
            if not isinstance(role_key, str):
                continue
            try:
                cleaned[role_key] = int(role_id)
            except Exception:
                continue
        out[category_key] = cleaned
    return out


@dataclass(slots=True)
class SelfRolesGuildRecord:
    guild_id: int
    role_ids: dict[str, dict[str, int]] = field(default_factory=dict)
    panel_channel_id: int | None = None
    panel_message_id: int | None = None
    panel_image_url: str = ""
    panel_thumbnail_url: str = ""
    category_image_urls: dict[str, str] = field(default_factory=dict)
    schema_version: int = SCHEMA_VERSION


class SelfRolesStorage:
    TABLE = "self_role_picker_config"

    def __init__(self) -> None:
        self.sessionmaker = sessions()

    async def ensure_tables(self) -> None:
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE} (
            guild_id BIGINT NOT NULL,
            role_ids_json LONGTEXT NULL,
            panel_channel_id BIGINT NULL,
            panel_message_id BIGINT NULL,
            panel_image_url TEXT NULL,
            panel_thumbnail_url TEXT NULL,
            category_image_urls_json LONGTEXT NULL,
            schema_version INT NOT NULL DEFAULT {SCHEMA_VERSION},
            last_setup_at TIMESTAMP NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id)
        );
        """
        migrations = (
            f"ALTER TABLE {self.TABLE} ADD COLUMN IF NOT EXISTS role_ids_json LONGTEXT NULL",
            f"ALTER TABLE {self.TABLE} ADD COLUMN IF NOT EXISTS panel_channel_id BIGINT NULL",
            f"ALTER TABLE {self.TABLE} ADD COLUMN IF NOT EXISTS panel_message_id BIGINT NULL",
            f"ALTER TABLE {self.TABLE} ADD COLUMN IF NOT EXISTS panel_image_url TEXT NULL",
            f"ALTER TABLE {self.TABLE} ADD COLUMN IF NOT EXISTS panel_thumbnail_url TEXT NULL",
            f"ALTER TABLE {self.TABLE} ADD COLUMN IF NOT EXISTS category_image_urls_json LONGTEXT NULL",
            f"ALTER TABLE {self.TABLE} ADD COLUMN IF NOT EXISTS schema_version INT NOT NULL DEFAULT {SCHEMA_VERSION}",
            f"ALTER TABLE {self.TABLE} ADD COLUMN IF NOT EXISTS last_setup_at TIMESTAMP NULL",
        )
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    await session.execute(text(sql))
                    for migration in migrations:
                        await session.execute(text(migration))
        except Exception as exc:
            raise SelfRoleStorageError("Self-role storage is unavailable.") from exc

    async def get(self, guild_id: int) -> SelfRolesGuildRecord:
        await self.ensure_tables()
        sql = text(
            f"""
            SELECT guild_id, role_ids_json, panel_channel_id, panel_message_id,
                   panel_image_url, panel_thumbnail_url, category_image_urls_json, schema_version
            FROM {self.TABLE}
            WHERE guild_id = :guild_id
            LIMIT 1
            """
        )
        try:
            async with self.sessionmaker() as session:
                row = (await session.execute(sql, {"guild_id": int(guild_id)})).mappings().first()
        except Exception as exc:
            raise SelfRoleStorageError("Self-role storage is unavailable.") from exc

        if not row:
            return SelfRolesGuildRecord(guild_id=int(guild_id))

        return SelfRolesGuildRecord(
            guild_id=int(row["guild_id"]),
            role_ids=_coerce_role_ids(_safe_json_dict(row.get("role_ids_json"))),
            panel_channel_id=int(row["panel_channel_id"]) if row["panel_channel_id"] else None,
            panel_message_id=int(row["panel_message_id"]) if row["panel_message_id"] else None,
            panel_image_url=str(row.get("panel_image_url") or ""),
            panel_thumbnail_url=str(row.get("panel_thumbnail_url") or ""),
            category_image_urls={
                str(k): str(v)
                for k, v in _safe_json_dict(row.get("category_image_urls_json")).items()
                if isinstance(k, str) and isinstance(v, str)
            },
            schema_version=int(row.get("schema_version") or SCHEMA_VERSION),
        )

    async def upsert(self, record: SelfRolesGuildRecord, *, touch_setup: bool = False) -> None:
        await self.ensure_tables()
        sql = text(
            f"""
            INSERT INTO {self.TABLE}
                (guild_id, role_ids_json, panel_channel_id, panel_message_id, panel_image_url,
                 panel_thumbnail_url, category_image_urls_json, schema_version, last_setup_at)
            VALUES
                (:guild_id, :role_ids_json, :panel_channel_id, :panel_message_id, :panel_image_url,
                 :panel_thumbnail_url, :category_image_urls_json, :schema_version,
                 {"CURRENT_TIMESTAMP" if touch_setup else "NULL"})
            ON DUPLICATE KEY UPDATE
                role_ids_json = VALUES(role_ids_json),
                panel_channel_id = VALUES(panel_channel_id),
                panel_message_id = VALUES(panel_message_id),
                panel_image_url = VALUES(panel_image_url),
                panel_thumbnail_url = VALUES(panel_thumbnail_url),
                category_image_urls_json = VALUES(category_image_urls_json),
                schema_version = VALUES(schema_version),
                last_setup_at = COALESCE(VALUES(last_setup_at), last_setup_at)
            """
        )
        params = {
            "guild_id": int(record.guild_id),
            "role_ids_json": json.dumps(record.role_ids, sort_keys=True),
            "panel_channel_id": record.panel_channel_id,
            "panel_message_id": record.panel_message_id,
            "panel_image_url": record.panel_image_url,
            "panel_thumbnail_url": record.panel_thumbnail_url,
            "category_image_urls_json": json.dumps(record.category_image_urls, sort_keys=True),
            "schema_version": int(record.schema_version),
        }
        try:
            async with self.sessionmaker() as session:
                async with session.begin():
                    await session.execute(sql, params)
        except Exception as exc:
            raise SelfRoleStorageError("Self-role storage is unavailable.") from exc
