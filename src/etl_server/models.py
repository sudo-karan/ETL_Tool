"""SQLAlchemy 2.0 ORM models: users, pipelines, runs, run_logs, run_errors,
schedules, secrets.

Types are kept portable (``Uuid``, ``JSON``, tz-aware ``DateTime``) so the same
metadata runs on PostgreSQL (production, via asyncpg) and SQLite (hermetic
tests, via aiosqlite). run_logs / run_errors use an autoincrement integer PK so
it doubles as a monotonic cursor for live SSE streaming.
"""
from __future__ import annotations

import enum
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    TypeDecorator,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from etl_core.errors import utcnow


class UtcDateTime(TypeDecorator):
    """A timezone-aware UTC datetime that behaves identically on PostgreSQL and
    SQLite. Values are normalized to UTC on write and always returned tz-aware,
    so application code never juggles naive vs. aware datetimes (SQLite would
    otherwise hand back naive values)."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value, dialect):  # noqa: ANN001
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def process_result_value(self, value, dialect):  # noqa: ANN001
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value


class Base(DeclarativeBase):
    pass


class RunState(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        return self in (RunState.SUCCEEDED, RunState.FAILED)


class RunTrigger(str, enum.Enum):
    MANUAL = "manual"
    SCHEDULE = "schedule"


def _uuid_pk() -> Mapped[uuid.UUID]:
    return mapped_column(Uuid, primary_key=True, default=uuid.uuid4)


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = _uuid_pk()
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True)
    hashed_password: Mapped[str] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=utcnow)


class Pipeline(Base):
    __tablename__ = "pipelines"

    id: Mapped[uuid.UUID] = _uuid_pk()
    owner_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(200))
    # The pipeline JSON graph -- the exact document the engine executes.
    spec: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime, default=utcnow, onupdate=utcnow
    )


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[uuid.UUID] = _uuid_pk()
    pipeline_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("pipelines.id", ondelete="CASCADE"), index=True
    )
    owner_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    status: Mapped[str] = mapped_column(String(16), default=RunState.QUEUED.value, index=True)
    trigger: Mapped[str] = mapped_column(String(16), default=RunTrigger.MANUAL.value)
    params: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error_count: Mapped[int] = mapped_column(Integer, default=0)
    # A trimmed RunResult (node_results + terminal outputs), set when finished.
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)

    logs: Mapped[list["RunLog"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )
    errors: Mapped[list["RunError"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class RunLog(Base):
    __tablename__ = "run_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("runs.id", ondelete="CASCADE"), index=True
    )
    seq: Mapped[int] = mapped_column(Integer)
    timestamp: Mapped[datetime] = mapped_column(UtcDateTime, default=utcnow)
    level: Mapped[str] = mapped_column(String(16))
    node_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    message: Mapped[str] = mapped_column(String)
    data: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    run: Mapped[Run] = relationship(back_populates="logs")


class RunError(Base):
    __tablename__ = "run_errors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("runs.id", ondelete="CASCADE"), index=True
    )
    node_id: Mapped[str] = mapped_column(String(200))
    node_type: Mapped[str] = mapped_column(String(64))
    category: Mapped[str] = mapped_column(String(32))
    message: Mapped[str] = mapped_column(String)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_summary: Mapped[str | None] = mapped_column(String, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, default=1)
    timestamp: Mapped[datetime] = mapped_column(UtcDateTime, default=utcnow)
    details: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    run: Mapped[Run] = relationship(back_populates="errors")


class Schedule(Base):
    __tablename__ = "schedules"

    id: Mapped[uuid.UUID] = _uuid_pk()
    pipeline_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("pipelines.id", ondelete="CASCADE"), index=True
    )
    owner_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    cron_expr: Mapped[str] = mapped_column(String(120))
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")  # IANA name
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    last_run: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    next_run: Mapped[datetime | None] = mapped_column(
        UtcDateTime, nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=utcnow)


class Secret(Base):
    __tablename__ = "secrets"
    __table_args__ = (UniqueConstraint("owner_id", "ref", name="uq_secret_owner_ref"),)

    id: Mapped[uuid.UUID] = _uuid_pk()
    owner_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    ref: Mapped[str] = mapped_column(String(200))  # the secret_ref used in pipelines
    ciphertext: Mapped[str] = mapped_column(String)  # crypto-layer token (never plaintext)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime, default=utcnow, onupdate=utcnow
    )
