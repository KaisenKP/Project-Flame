# /home/container/db/models.py
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    Index,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


TS = DateTime(timezone=True)
NOW = func.now()


class Base(DeclarativeBase):
    pass


# =============================================================================
# Core User + Economy
# =============================================================================

class UserRow(Base):
    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", name="uq_users_guild_user"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    created_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


class XpRow(Base):
    __tablename__ = "xp"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", name="uq_xp_guild_user"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    xp_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    level_cached: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


class WalletRow(Base):
    __tablename__ = "wallets"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", name="uq_wallets_guild_user"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    silver: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    diamonds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    silver_earned: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    silver_spent: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


# =============================================================================
# Activity
# =============================================================================

class ActivityDailyRow(Base):
    __tablename__ = "activity_daily"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", "day", name="uq_activity_daily"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    day: Mapped[date] = mapped_column(Date, nullable=False, index=True)

    message_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    vc_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    activity_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


class VoiceSessionRow(Base):
    __tablename__ = "voice_sessions"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", "joined_at", name="uq_voice_session_start"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    channel_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    joined_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)
    left_at: Mapped[Optional[datetime]] = mapped_column(TS, nullable=True)
    counted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class LevelRewardRow(Base):
    __tablename__ = "level_rewards"
    __table_args__ = (
        UniqueConstraint("guild_id", "level", name="uq_level_rewards_guild_level"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    level: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    silver_reward: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class AdminAuditLogRow(Base):
    __tablename__ = "admin_audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    actor_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    target_user_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)

    action: Mapped[str] = mapped_column(String(32), nullable=False)
    table_name: Mapped[str] = mapped_column(String(64), nullable=False)
    pk_json: Mapped[str] = mapped_column(Text, nullable=False)

    before_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    after_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    reason: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)


# =============================================================================
# Sentinel (Moderation Telemetry)
# =============================================================================

class SentinelEventRow(Base):
    __tablename__ = "sentinel_events"
    __table_args__ = (
        UniqueConstraint("guild_id", "case_id", name="uq_sentinel_events_guild_case"),
        Index("ix_sentinel_events_guild_created", "guild_id", "created_at"),
        Index("ix_sentinel_events_guild_type_created", "guild_id", "event_type", "created_at"),
        Index("ix_sentinel_events_guild_actor_created", "guild_id", "actor_user_id", "created_at"),
        Index("ix_sentinel_events_guild_target_created", "guild_id", "target_user_id", "created_at"),
        Index("ix_sentinel_events_guild_channel_created", "guild_id", "channel_id", "created_at"),
        Index("ix_sentinel_events_guild_message", "guild_id", "channel_id", "message_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    case_id: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(48), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(10), nullable=False, default="INFO")

    actor_user_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)
    target_user_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)

    channel_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)
    message_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)

    summary: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    created_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)


class SentinelBotTrustRow(Base):
    __tablename__ = "sentinel_bot_trust"
    __table_args__ = (
        UniqueConstraint("guild_id", "bot_user_id", name="uq_sentinel_bot_trust_guild_bot"),
        Index("ix_sentinel_bot_trust_guild_score", "guild_id", "trust_score"),
        Index("ix_sentinel_bot_trust_guild_seen", "guild_id", "last_seen_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    bot_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    first_seen_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )

    trust_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_whitelisted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    app_commands_seen: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    interactions_seen: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    note: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)


# =============================================================================
# Jobs + Stamina + Tools
# =============================================================================

class JobRow(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    key: Mapped[str] = mapped_column(String(32), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    base_silver: Mapped[int] = mapped_column(Integer, nullable=False, default=25)
    jackpot_silver: Mapped[int] = mapped_column(Integer, nullable=False, default=250)

    jackpot_chance_bp: Mapped[int] = mapped_column(Integer, nullable=False, default=500)
    fail_chance_bp: Mapped[int] = mapped_column(Integer, nullable=False, default=500)

    work_image_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    created_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


class UserJobSlotRow(Base):
    __tablename__ = "user_job_slots"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", "slot_index", name="uq_user_job_slots"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    slot_index: Mapped[int] = mapped_column(Integer, nullable=False)
    job_id: Mapped[int] = mapped_column(
        ForeignKey("jobs.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    assigned_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)


class JobProgressRow(Base):
    __tablename__ = "job_progress"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", "job_id", name="uq_job_progress"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    job_id: Mapped[int] = mapped_column(
        ForeignKey("jobs.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    job_xp: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    job_level: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    job_title: Mapped[str] = mapped_column(String(64), nullable=False, default="Recruit")

    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


class StaminaRow(Base):
    __tablename__ = "stamina"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", name="uq_stamina_guild_user"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    current_stamina: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_stamina: Mapped[int] = mapped_column(Integer, nullable=False, default=100)

    last_regen_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)
    is_vip: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


class ToolRow(Base):
    __tablename__ = "tools"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(
        ForeignKey("jobs.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    key: Mapped[str] = mapped_column(String(32), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(64), nullable=False)

    max_durability: Mapped[int] = mapped_column(Integer, nullable=False, default=100)

    silver_cost: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    diamond_cost: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    event_only: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    payout_bonus_bp: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


class UserToolRow(Base):
    __tablename__ = "user_tools"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", "tool_id", name="uq_user_tools"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    tool_id: Mapped[int] = mapped_column(
        ForeignKey("tools.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    current_durability: Mapped[int] = mapped_column(Integer, nullable=False, default=100)

    equipped: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    acquired_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


class GamblingStatsRow(Base):
    __tablename__ = "gambling_stats"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", name="uq_gambling_stats"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    lifetime_wagered: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lifetime_won: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lifetime_lost: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    largest_win: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    largest_loss: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


# =============================================================================
# Unified Items System
# =============================================================================

class ItemInventoryRow(Base):
    __tablename__ = "item_inventory"

    guild_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    item_key: Mapped[str] = mapped_column(String(64), primary_key=True)

    qty: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


class ActiveEffectRow(Base):
    __tablename__ = "active_effects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    effect_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    group_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)

    expires_at: Mapped[Optional[datetime]] = mapped_column(TS, nullable=True)
    charges_remaining: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)

    __table_args__ = (
        Index("ix_active_effects_user_group", "guild_id", "user_id", "group_key"),
        Index("ix_active_effects_user_effect", "guild_id", "user_id", "effect_key"),
    )


class ShopPurchaseRow(Base):
    __tablename__ = "shop_purchases"

    guild_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    shop_day_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    item_key: Mapped[str] = mapped_column(String(64), primary_key=True)

    qty: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("ix_shop_purchases_user_day", "guild_id", "user_id", "shop_day_id"),
    )


# =============================================================================
# Lootboxes + Crowns
# =============================================================================

class LootboxInventoryRow(Base):
    __tablename__ = "lootbox_inventory"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", "rarity", name="uq_lootbox_inv"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    rarity: Mapped[str] = mapped_column(String(16), nullable=False)
    amount: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


class LootboxGrantPermRow(Base):
    __tablename__ = "lootbox_grant_perms"
    __table_args__ = (
        UniqueConstraint("guild_id", "role_id", name="uq_lootbox_perm_role"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    role_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    created_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)


class LootboxGrantUserPermRow(Base):
    __tablename__ = "lootbox_grant_user_perms"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", name="uq_lootbox_perm_user"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    created_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)


class CrownsWalletRow(Base):
    __tablename__ = "crowns_wallets"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", name="uq_crowns_wallets_guild_user"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    crowns: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


# =============================================================================
# Business System
# =============================================================================
# This replaces the old failed business schema with a cleaner model built for:
# - multiple businesses per user
# - per-business level / prestige
# - active timed runs
# - future worker / manager assignments
# - guild-scoped economy state
#
# Current intended usage:
# - BusinessOwnershipRow: what businesses a player owns and how progressed they are
# - BusinessRunRow: the active or most recent timed run for a business
# - BusinessWorkerAssignmentRow: future worker slots per owned business
# - BusinessManagerAssignmentRow: future manager slots per owned business
#
# core.py can start with only BusinessOwnershipRow + BusinessRunRow.
# Workers/managers can be layered in after the main loop is alive.

class BusinessOwnershipRow(Base):
    __tablename__ = "business_ownership"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", "business_key", name="uq_business_ownership_user_key"),
        Index("ix_business_ownership_user", "guild_id", "user_id"),
        Index("ix_business_ownership_key", "guild_id", "business_key"),
        Index("ix_business_ownership_user_key", "guild_id", "user_id", "business_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    business_key: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

    level: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    prestige: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    total_earned: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_spent: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


class BusinessRunRow(Base):
    __tablename__ = "business_runs"
    __table_args__ = (
        Index("ix_business_runs_user", "guild_id", "user_id"),
        Index("ix_business_runs_user_key", "guild_id", "user_id", "business_key"),
        Index("ix_business_runs_status", "guild_id", "status"),
        Index("ix_business_runs_ends_at", "guild_id", "ends_at"),
        Index("ix_business_runs_owner_id", "ownership_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    ownership_id: Mapped[int] = mapped_column(
        ForeignKey("business_ownership.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    business_key: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running", index=True)
    # suggested statuses:
    # "running" | "completed" | "cancelled"

    started_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)
    ends_at: Mapped[datetime] = mapped_column(TS, nullable=False, index=True)
    last_payout_at: Mapped[Optional[datetime]] = mapped_column(TS, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(TS, nullable=True)

    runtime_hours_snapshot: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    hourly_profit_snapshot: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    auto_restart_remaining: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    snapshot_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    report_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    silver_paid_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    hours_paid_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


class BusinessWorkerAssignmentRow(Base):
    __tablename__ = "business_worker_assignments"
    __table_args__ = (
        UniqueConstraint(
            "guild_id",
            "user_id",
            "business_key",
            "slot_index",
            name="uq_business_worker_assignments_slot",
        ),
        Index("ix_business_worker_assignments_user", "guild_id", "user_id"),
        Index("ix_business_worker_assignments_user_key", "guild_id", "user_id", "business_key"),
        Index("ix_business_worker_assignments_worker_type", "guild_id", "worker_type"),
        Index("ix_business_worker_assignments_rarity", "guild_id", "rarity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    ownership_id: Mapped[int] = mapped_column(
        ForeignKey("business_ownership.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    business_key: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

    slot_index: Mapped[int] = mapped_column(Integer, nullable=False)

    worker_name: Mapped[str] = mapped_column(String(64), nullable=False)
    worker_type: Mapped[str] = mapped_column(String(16), nullable=False)
    # suggested values:
    # "fast" | "efficient" | "kind"

    rarity: Mapped[str] = mapped_column(String(16), nullable=False)
    # suggested values:
    # "common" | "rare" | "epic" | "legendary" | "mythical"

    flat_profit_bonus: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    percent_profit_bonus_bp: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    special_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    hired_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)

    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )


class BusinessManagerAssignmentRow(Base):
    __tablename__ = "business_manager_assignments"
    __table_args__ = (
        UniqueConstraint(
            "guild_id",
            "user_id",
            "business_key",
            "slot_index",
            name="uq_business_manager_assignments_slot",
        ),
        Index("ix_business_manager_assignments_user", "guild_id", "user_id"),
        Index("ix_business_manager_assignments_user_key", "guild_id", "user_id", "business_key"),
        Index("ix_business_manager_assignments_rarity", "guild_id", "rarity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    ownership_id: Mapped[int] = mapped_column(
        ForeignKey("business_ownership.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    business_key: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

    slot_index: Mapped[int] = mapped_column(Integer, nullable=False)

    manager_name: Mapped[str] = mapped_column(String(64), nullable=False)
    rarity: Mapped[str] = mapped_column(String(16), nullable=False)
    # suggested values:
    # "common" | "rare" | "epic" | "legendary" | "mythical"

    runtime_bonus_hours: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    auto_restart_charges: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    profit_bonus_bp: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    special_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    hired_at: Mapped[datetime] = mapped_column(TS, server_default=NOW, nullable=False)

    updated_at: Mapped[datetime] = mapped_column(
        TS,
        server_default=NOW,
        onupdate=NOW,
        nullable=False,
    )