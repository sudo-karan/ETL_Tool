"""Schedule CRUD. next_run is (re)computed in the schedule's timezone whenever
its cron/timezone changes or it is re-enabled."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..deps import get_current_user, get_session
from ..models import Schedule, User
from ..schemas import ScheduleCreate, ScheduleRead, ScheduleUpdate
from ..scheduler import ScheduleConfigError, refresh_next_run, validate_cron, validate_timezone
from ..service import get_owned_pipeline, get_owned_schedule

router = APIRouter(prefix="/schedules", tags=["schedules"])


def _validate(cron_expr: str, timezone: str) -> None:
    try:
        validate_cron(cron_expr)
        validate_timezone(timezone)
    except ScheduleConfigError as exc:
        raise HTTPException(422, str(exc)) from exc  # version-agnostic 422 constant


@router.post("", response_model=ScheduleRead, status_code=status.HTTP_201_CREATED)
async def create_schedule(
    payload: ScheduleCreate,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Schedule:
    await get_owned_pipeline(session, payload.pipeline_id, user.id)  # 404 if not owned
    _validate(payload.cron_expr, payload.timezone)
    schedule = Schedule(
        pipeline_id=payload.pipeline_id,
        owner_id=user.id,
        cron_expr=payload.cron_expr,
        timezone=payload.timezone,
        enabled=payload.enabled,
    )
    if schedule.enabled:
        await refresh_next_run(session, schedule)
    session.add(schedule)
    await session.commit()
    await session.refresh(schedule)
    return schedule


@router.get("", response_model=list[ScheduleRead])
async def list_schedules(
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[Schedule]:
    result = await session.execute(
        select(Schedule).where(Schedule.owner_id == user.id).order_by(Schedule.created_at.desc())
    )
    return list(result.scalars().all())


@router.get("/{schedule_id}", response_model=ScheduleRead)
async def get_schedule(
    schedule_id: uuid.UUID,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Schedule:
    return await get_owned_schedule(session, schedule_id, user.id)


@router.put("/{schedule_id}", response_model=ScheduleRead)
async def update_schedule(
    schedule_id: uuid.UUID,
    payload: ScheduleUpdate,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Schedule:
    schedule = await get_owned_schedule(session, schedule_id, user.id)
    new_cron = payload.cron_expr if payload.cron_expr is not None else schedule.cron_expr
    new_tz = payload.timezone if payload.timezone is not None else schedule.timezone
    timing_changed = payload.cron_expr is not None or payload.timezone is not None
    if timing_changed:
        _validate(new_cron, new_tz)
        schedule.cron_expr = new_cron
        schedule.timezone = new_tz
    was_enabled = schedule.enabled
    if payload.enabled is not None:
        schedule.enabled = payload.enabled
    # Recompute next_run when the timing changed or the schedule is (re)enabled.
    if schedule.enabled and (timing_changed or not was_enabled or schedule.next_run is None):
        await refresh_next_run(session, schedule)
    await session.commit()
    await session.refresh(schedule)
    return schedule


@router.delete("/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schedule(
    schedule_id: uuid.UUID,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> None:
    schedule = await get_owned_schedule(session, schedule_id, user.id)
    await session.delete(schedule)
    await session.commit()
