from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Index, Integer, JSON, String, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class UserRow(Base):
    __tablename__ = "users"
    __table_args__ = (UniqueConstraint("guild_id", "user_id", name="uq_users_guild_user"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ActivityDailyRow(Base):
    __tablename__ = "activity_daily"
    __table_args__ = (
        UniqueConstraint("guild_id", "user_id", "day", name="uq_activity_daily"),
        Index("ix_activity_daily_guild_day", "guild_id", "day"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    day: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    message_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    vc_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class SentinelEventRow(Base):
    __tablename__ = "sentinel_events"
    __table_args__ = (
        UniqueConstraint("guild_id", "case_id", name="uq_sentinel_events_guild_case"),
        Index("ix_sentinel_events_guild_created", "guild_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    case_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    actor_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    target_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    channel_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    summary: Mapped[str] = mapped_column(String(512), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class SentinelBotTrustRow(Base):
    __tablename__ = "sentinel_bot_trust"
    __table_args__ = (UniqueConstraint("guild_id", "bot_user_id", name="uq_sentinel_bot_trust"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    bot_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    trust_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    app_commands_seen: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    interactions_seen: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_whitelisted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
