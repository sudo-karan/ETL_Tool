"""Pydantic request/response DTOs for the REST API.

Kept separate from the ORM models so the wire contract is explicit and, in
particular, secret *values* are never in a response model.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# -- auth -------------------------------------------------------------------
class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=256)


class UserRead(ORMModel):
    id: uuid.UUID
    email: EmailStr
    is_active: bool
    created_at: datetime


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


# -- pipelines --------------------------------------------------------------
class PipelineCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    spec: dict[str, Any]


class PipelineUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    spec: dict[str, Any] | None = None


class PipelineRead(ORMModel):
    id: uuid.UUID
    name: str
    spec: dict[str, Any]
    created_at: datetime
    updated_at: datetime


# -- runs -------------------------------------------------------------------
class RunTriggerRequest(BaseModel):
    params: dict[str, Any] | None = None


class RunRead(ORMModel):
    id: uuid.UUID
    pipeline_id: uuid.UUID
    status: str
    trigger: str
    params: dict[str, Any] | None = None
    error_count: int
    result: dict[str, Any] | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


class RunLogRead(ORMModel):
    id: int
    seq: int
    timestamp: datetime
    level: str
    node_id: str | None = None
    message: str
    data: dict[str, Any] | None = None


class RunErrorRead(ORMModel):
    id: int
    node_id: str
    node_type: str
    category: str
    message: str
    http_status: int | None = None
    request_summary: str | None = None
    attempts: int
    timestamp: datetime
    details: dict[str, Any] | None = None


class RunDetail(RunRead):
    logs: list[RunLogRead] = Field(default_factory=list)
    errors: list[RunErrorRead] = Field(default_factory=list)


# -- secrets ----------------------------------------------------------------
class SecretCreate(BaseModel):
    ref: str = Field(min_length=1, max_length=200)
    value: str


class SecretRead(ORMModel):
    id: uuid.UUID
    ref: str
    created_at: datetime
    updated_at: datetime


# -- diagnostics ------------------------------------------------------------
class ConnectionTestRequest(BaseModel):
    """A single source node spec: {"type": "...", "config": {...}}."""

    type: str
    config: dict[str, Any] = Field(default_factory=dict)


# -- schedules --------------------------------------------------------------
class ScheduleCreate(BaseModel):
    pipeline_id: uuid.UUID
    cron_expr: str = Field(min_length=1, max_length=120)
    timezone: str = "UTC"
    enabled: bool = True


class ScheduleUpdate(BaseModel):
    cron_expr: str | None = Field(default=None, min_length=1, max_length=120)
    timezone: str | None = None
    enabled: bool | None = None


class ScheduleRead(ORMModel):
    id: uuid.UUID
    pipeline_id: uuid.UUID
    cron_expr: str
    timezone: str
    enabled: bool
    last_run: datetime | None = None
    next_run: datetime | None = None
    created_at: datetime
