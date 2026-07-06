"""Ownership-scoped lookups shared by the routers.

Every fetch is filtered by ``owner_id`` and 404s otherwise, so one user can
never read or mutate another's pipelines, runs or schedules.
"""
from __future__ import annotations

import uuid

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Pipeline, Run, Schedule


def _not_found(kind: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"{kind} not found")


async def get_owned_pipeline(
    session: AsyncSession, pipeline_id: uuid.UUID, owner_id: uuid.UUID
) -> Pipeline:
    pipeline = await session.get(Pipeline, pipeline_id)
    if pipeline is None or pipeline.owner_id != owner_id:
        raise _not_found("pipeline")
    return pipeline


async def get_owned_run(session: AsyncSession, run_id: uuid.UUID, owner_id: uuid.UUID) -> Run:
    run = await session.get(Run, run_id)
    if run is None or run.owner_id != owner_id:
        raise _not_found("run")
    return run


async def get_owned_schedule(
    session: AsyncSession, schedule_id: uuid.UUID, owner_id: uuid.UUID
) -> Schedule:
    schedule = await session.get(Schedule, schedule_id)
    if schedule is None or schedule.owner_id != owner_id:
        raise _not_found("schedule")
    return schedule
