from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    BusinessManagerAssignmentRow,
    BusinessOwnershipRow,
    BusinessRunRow,
    BusinessWorkerAssignmentRow,
    CrownsWalletRow,
    JobProgressRow,
    UserJobHubProgressRow,
    UserJobHubSlotRow,
    UserJobHubToolRow,
    UserJobSlotRow,
    UserJobUpgradeRow,
    UserRow,
    WalletRow,
    XpRow,
)
from services.db import sessions

log = logging.getLogger(__name__)

BACKUP_ROOT = Path("data/economy_backups")
BACKUP_RETENTION_DAYS = 31
AUTO_BACKUP_INTERVAL = dt.timedelta(minutes=30)


def _utc_now() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc)


def _serialize_value(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _serialize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serialize_value(v) for v in value]
    return value


def _deserialize_datetime(value: Any) -> dt.datetime | None:
    if value in (None, ""):
        return None
    parsed = dt.datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def _row_to_dict(row: Any, *, exclude: set[str] | None = None) -> dict[str, Any]:
    excluded = exclude or set()
    payload: dict[str, Any] = {}
    for column in row.__table__.columns:
        key = column.name
        if key in excluded:
            continue
        payload[key] = _serialize_value(getattr(row, key))
    return payload


class EconomyBackupsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessionmaker = sessions()
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None

    async def cog_load(self) -> None:
        BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
        self._task = asyncio.create_task(self._auto_backup_loop(), name="economy_backups.auto")

    async def cog_unload(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    async def _auto_backup_loop(self) -> None:
        await self.bot.wait_until_ready()
        await self.create_all_guild_backups(reason="startup")
        while not self.bot.is_closed():
            await asyncio.sleep(AUTO_BACKUP_INTERVAL.total_seconds())
            await self.create_all_guild_backups(reason="auto")

    async def create_all_guild_backups(self, *, reason: str) -> list[Path]:
        created: list[Path] = []
        async with self._lock:
            for guild in self.bot.guilds:
                try:
                    path = await self._create_backup_for_guild(guild_id=int(guild.id), reason=reason)
                    if path is not None:
                        created.append(path)
                except Exception:
                    log.exception("Failed to create economy backup for guild_id=%s", guild.id)
        return created

    async def run_pre_restart_backup(self, *, reason: str) -> list[Path]:
        return await self.create_all_guild_backups(reason=reason)

    def _guild_dir(self, guild_id: int) -> Path:
        path = BACKUP_ROOT / str(int(guild_id))
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _backup_filename(self, *, guild_id: int, created_at: dt.datetime, reason: str) -> Path:
        stamp = created_at.strftime("%Y-%m-%dT%H-%M-%SZ")
        safe_reason = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in reason.lower()).strip("-") or "manual"
        return self._guild_dir(guild_id) / f"economy-{stamp}-{safe_reason}.json"

    async def _create_backup_for_guild(self, *, guild_id: int, reason: str) -> Path | None:
        snapshot = await self._collect_snapshot(guild_id=guild_id, reason=reason)
        if snapshot is None:
            return None
        created_at = _deserialize_datetime(snapshot["created_at"]) or _utc_now()
        target = self._backup_filename(guild_id=guild_id, created_at=created_at, reason=reason)
        tmp = target.with_suffix(".tmp")
        tmp.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        tmp.replace(target)
        self._cleanup_old_backups(guild_id=guild_id)
        log.info("Created economy backup for guild_id=%s at %s", guild_id, target)
        return target

    async def _collect_snapshot(self, *, guild_id: int, reason: str) -> dict[str, Any] | None:
        async with self.sessionmaker() as session:
            snapshot = {
                "version": 1,
                "guild_id": int(guild_id),
                "reason": str(reason),
                "created_at": _serialize_value(_utc_now()),
                "tables": {
                    "users": await self._fetch_rows(session, UserRow, guild_id=guild_id),
                    "xp": await self._fetch_rows(session, XpRow, guild_id=guild_id),
                    "wallets": await self._fetch_rows(session, WalletRow, guild_id=guild_id),
                    "crowns_wallets": await self._fetch_rows(session, CrownsWalletRow, guild_id=guild_id),
                    "user_job_slots": await self._fetch_rows(session, UserJobSlotRow, guild_id=guild_id),
                    "user_job_hub_slots": await self._fetch_rows(session, UserJobHubSlotRow, guild_id=guild_id),
                    "user_job_hub_progress": await self._fetch_rows(session, UserJobHubProgressRow, guild_id=guild_id),
                    "user_job_hub_tools": await self._fetch_rows(session, UserJobHubToolRow, guild_id=guild_id),
                    "job_progress": await self._fetch_rows(session, JobProgressRow, guild_id=guild_id),
                    "user_job_upgrades": await self._fetch_rows(session, UserJobUpgradeRow, guild_id=guild_id),
                    "business_ownership": await self._fetch_rows(session, BusinessOwnershipRow, guild_id=guild_id),
                    "business_runs": await self._fetch_rows(session, BusinessRunRow, guild_id=guild_id),
                    "business_worker_assignments": await self._fetch_rows(session, BusinessWorkerAssignmentRow, guild_id=guild_id),
                    "business_manager_assignments": await self._fetch_rows(session, BusinessManagerAssignmentRow, guild_id=guild_id),
                },
            }
        return snapshot

    async def _fetch_rows(self, session: AsyncSession, model: type[Any], *, guild_id: int) -> list[dict[str, Any]]:
        rows = (await session.scalars(select(model).where(model.guild_id == int(guild_id)).order_by(model.id.asc()))).all()
        return [_row_to_dict(row) for row in rows]

    def _cleanup_old_backups(self, *, guild_id: int) -> None:
        cutoff = _utc_now() - dt.timedelta(days=BACKUP_RETENTION_DAYS)
        for path in self._guild_dir(guild_id).glob("economy-*.json"):
            try:
                modified = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
            except FileNotFoundError:
                continue
            if modified < cutoff:
                path.unlink(missing_ok=True)

    def _available_backups(self, *, guild_id: int) -> list[Path]:
        self._cleanup_old_backups(guild_id=guild_id)
        return sorted(self._guild_dir(guild_id).glob("economy-*.json"), reverse=True)

    async def _restore_backup(self, *, guild_id: int, backup_path: Path) -> tuple[int, dict[str, int]]:
        payload = json.loads(backup_path.read_text(encoding="utf-8"))
        tables = payload.get("tables", {})
        async with self._lock:
            async with self.sessionmaker() as session:
                async with session.begin():
                    await self._wipe_guild_state(session, guild_id=guild_id)
                    ownership_id_map = await self._restore_ownerships(session, guild_id=guild_id, rows=tables.get("business_ownership", []))
                    counts = {
                        "users": await self._restore_simple(session, UserRow, guild_id=guild_id, rows=tables.get("users", [])),
                        "xp": await self._restore_simple(session, XpRow, guild_id=guild_id, rows=tables.get("xp", []), datetime_fields={"updated_at"}),
                        "wallets": await self._restore_simple(session, WalletRow, guild_id=guild_id, rows=tables.get("wallets", []), datetime_fields={"updated_at"}),
                        "crowns_wallets": await self._restore_simple(session, CrownsWalletRow, guild_id=guild_id, rows=tables.get("crowns_wallets", []), datetime_fields={"updated_at"}),
                        "user_job_slots": await self._restore_simple(session, UserJobSlotRow, guild_id=guild_id, rows=tables.get("user_job_slots", []), datetime_fields={"assigned_at"}),
                        "user_job_hub_slots": await self._restore_simple(session, UserJobHubSlotRow, guild_id=guild_id, rows=tables.get("user_job_hub_slots", []), datetime_fields={"last_switched_at", "updated_at"}),
                        "user_job_hub_progress": await self._restore_simple(session, UserJobHubProgressRow, guild_id=guild_id, rows=tables.get("user_job_hub_progress", []), datetime_fields={"updated_at"}),
                        "user_job_hub_tools": await self._restore_simple(session, UserJobHubToolRow, guild_id=guild_id, rows=tables.get("user_job_hub_tools", []), datetime_fields={"updated_at"}),
                        "job_progress": await self._restore_simple(session, JobProgressRow, guild_id=guild_id, rows=tables.get("job_progress", []), datetime_fields={"updated_at"}),
                        "user_job_upgrades": await self._restore_simple(session, UserJobUpgradeRow, guild_id=guild_id, rows=tables.get("user_job_upgrades", []), datetime_fields={"created_at", "updated_at"}),
                        "business_ownership": len(ownership_id_map),
                        "business_runs": await self._restore_related_business_rows(session, BusinessRunRow, guild_id=guild_id, rows=tables.get("business_runs", []), ownership_id_map=ownership_id_map, datetime_fields={"started_at", "ends_at", "last_payout_at", "completed_at", "created_at", "updated_at"}),
                        "business_worker_assignments": await self._restore_related_business_rows(session, BusinessWorkerAssignmentRow, guild_id=guild_id, rows=tables.get("business_worker_assignments", []), ownership_id_map=ownership_id_map, datetime_fields={"hired_at", "updated_at"}),
                        "business_manager_assignments": await self._restore_related_business_rows(session, BusinessManagerAssignmentRow, guild_id=guild_id, rows=tables.get("business_manager_assignments", []), ownership_id_map=ownership_id_map, datetime_fields={"hired_at", "updated_at"}),
                    }
                await session.commit()
        return sum(counts.values()), counts

    async def _wipe_guild_state(self, session: AsyncSession, *, guild_id: int) -> None:
        models = [
            BusinessRunRow,
            BusinessWorkerAssignmentRow,
            BusinessManagerAssignmentRow,
            BusinessOwnershipRow,
            UserJobHubToolRow,
            UserJobHubProgressRow,
            UserJobHubSlotRow,
            UserJobUpgradeRow,
            JobProgressRow,
            UserJobSlotRow,
            CrownsWalletRow,
            WalletRow,
            XpRow,
            UserRow,
        ]
        for model in models:
            await session.execute(delete(model).where(model.guild_id == int(guild_id)))

    async def _restore_ownerships(self, session: AsyncSession, *, guild_id: int, rows: list[dict[str, Any]]) -> dict[int, int]:
        mapping: dict[int, int] = {}
        for raw in rows:
            original_id = int(raw.get("id", 0) or 0)
            payload = {k: v for k, v in raw.items() if k != "id"}
            payload["guild_id"] = int(guild_id)
            for field in {"created_at", "updated_at"}:
                if field in payload:
                    payload[field] = _deserialize_datetime(payload[field])
            row = BusinessOwnershipRow(**payload)
            session.add(row)
            await session.flush()
            mapping[original_id] = int(row.id)
        return mapping

    async def _restore_related_business_rows(
        self,
        session: AsyncSession,
        model: type[Any],
        *,
        guild_id: int,
        rows: list[dict[str, Any]],
        ownership_id_map: dict[int, int],
        datetime_fields: set[str],
    ) -> int:
        restored = 0
        for raw in rows:
            payload = {k: v for k, v in raw.items() if k != "id"}
            original_owner_id = int(payload.get("ownership_id", 0) or 0)
            if original_owner_id not in ownership_id_map:
                continue
            payload["ownership_id"] = ownership_id_map[original_owner_id]
            payload["guild_id"] = int(guild_id)
            for field in datetime_fields:
                if field in payload:
                    payload[field] = _deserialize_datetime(payload[field])
            session.add(model(**payload))
            restored += 1
        return restored

    async def _restore_simple(
        self,
        session: AsyncSession,
        model: type[Any],
        *,
        guild_id: int,
        rows: list[dict[str, Any]],
        datetime_fields: set[str] | None = None,
    ) -> int:
        restored = 0
        for raw in rows:
            payload = {k: v for k, v in raw.items() if k != "id"}
            payload["guild_id"] = int(guild_id)
            for field in datetime_fields or set():
                if field in payload:
                    payload[field] = _deserialize_datetime(payload[field])
            session.add(model(**payload))
            restored += 1
        return restored

    @app_commands.command(name="economy_backup_create", description="Create a dated backup of this server's economy.")
    @app_commands.default_permissions(administrator=True)
    async def create_backup_cmd(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        path = await self._create_backup_for_guild(guild_id=int(interaction.guild_id), reason="manual")
        if path is None:
            await interaction.followup.send("No backup was created.", ephemeral=True)
            return
        await interaction.followup.send(f"Saved economy backup: `{path.name}`", ephemeral=True)

    @app_commands.command(name="economy_backup_list", description="List available economy backups for this server.")
    @app_commands.default_permissions(administrator=True)
    async def list_backups_cmd(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        backups = self._available_backups(guild_id=int(interaction.guild_id))[:10]
        if not backups:
            await interaction.response.send_message("No backups found for this server yet.", ephemeral=True)
            return
        lines = [f"• `{path.name}`" for path in backups]
        await interaction.response.send_message("Available backups:\n" + "\n".join(lines), ephemeral=True)

    @app_commands.command(name="economy_backup_restore", description="Restore this server's economy from a saved backup file.")
    @app_commands.describe(backup_name="Exact file name from /economy_backup_list")
    @app_commands.default_permissions(administrator=True)
    async def restore_backup_cmd(self, interaction: discord.Interaction, backup_name: str) -> None:
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        backup_path = self._guild_dir(int(interaction.guild_id)) / backup_name
        if not backup_path.exists() or backup_path.suffix != ".json":
            await interaction.response.send_message("Backup file not found. Use /economy_backup_list first.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        total, counts = await self._restore_backup(guild_id=int(interaction.guild_id), backup_path=backup_path)
        summary = ", ".join(f"{key}={value}" for key, value in counts.items() if value)
        await interaction.followup.send(
            f"Restored `{backup_path.name}` with **{total}** rows. {summary}",
            ephemeral=True,
        )

    @restore_backup_cmd.autocomplete("backup_name")
    async def restore_backup_autocomplete(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        if interaction.guild_id is None:
            return []
        current_lower = current.lower()
        choices: list[app_commands.Choice[str]] = []
        for path in self._available_backups(guild_id=int(interaction.guild_id)):
            if current_lower and current_lower not in path.name.lower():
                continue
            choices.append(app_commands.Choice(name=path.name, value=path.name))
            if len(choices) >= 25:
                break
        return choices


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(EconomyBackupsCog(bot))
