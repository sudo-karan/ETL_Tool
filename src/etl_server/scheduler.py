"""Timezone-aware schedule evaluation + the periodic due-run tick.

A schedule's due-ness is evaluated in *its own* IANA timezone: the next fire
time is computed with croniter in local time and stored back as UTC. The tick
(an arq cron job every minute in production; called directly in tests) enqueues
one run per due schedule onto the SAME queue the workers already consume -- a
scheduled run is just a run with ``trigger = schedule``.

Misfire policy: if the tick was delayed (server down), a due schedule fires
once and its next fire time is advanced from *now*, so a long outage does not
produce a backfill storm.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Awaitable, Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_core.errors import utcnow

from .config import Settings
from .db import Database
from .models import Run, RunState, RunTrigger, Schedule


class ScheduleConfigError(ValueError):
    """An invalid cron expression or IANA timezone."""


def validate_timezone(tz_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError, KeyError) as exc:
        raise ScheduleConfigError(f"unknown timezone {tz_name!r}") from exc


def validate_cron(expr: str) -> None:
    if not croniter.is_valid(expr):
        raise ScheduleConfigError(f"invalid cron expression {expr!r}")


def compute_next_run(cron_expr: str, tz_name: str, after: datetime) -> datetime:
    """The next fire time strictly after ``after`` (UTC), evaluated in ``tz_name``."""
    validate_cron(cron_expr)
    tz = validate_timezone(tz_name)
    after_local = after.astimezone(tz)
    nxt = croniter(cron_expr, after_local).get_next(datetime)
    if nxt.tzinfo is None:
        nxt = nxt.replace(tzinfo=tz)
    return nxt.astimezone(timezone.utc)


async def enqueue_due_schedules(
    db: Database,
    settings: Settings,
    enqueue: Callable[[uuid.UUID], Awaitable[None]],
    *,
    now: datetime | None = None,
) -> list[uuid.UUID]:
    """Create + enqueue a run for every schedule due at ``now``. Returns the
    run ids enqueued. Runs are committed before enqueuing so a worker in another
    process sees the row."""
    now = now or utcnow()
    run_ids: list[uuid.UUID] = []
    async with db.sessionmaker() as session:
        due = (
            (
                await session.execute(
                    select(Schedule).where(
                        Schedule.enabled.is_(True),
                        Schedule.next_run.is_not(None),
                        Schedule.next_run <= now,
                    )
                )
            )
            .scalars()
            .all()
        )
        for schedule in due:
            run = Run(
                pipeline_id=schedule.pipeline_id,
                owner_id=schedule.owner_id,
                status=RunState.QUEUED.value,
                trigger=RunTrigger.SCHEDULE.value,
            )
            session.add(run)
            await session.flush()
            run_ids.append(run.id)
            schedule.last_run = now
            try:
                schedule.next_run = compute_next_run(schedule.cron_expr, schedule.timezone, now)
            except ScheduleConfigError:
                # A schedule that became invalid is disabled rather than retried.
                schedule.enabled = False
        await session.commit()

    for run_id in run_ids:
        await enqueue(run_id)
    return run_ids


async def refresh_next_run(session: AsyncSession, schedule: Schedule) -> None:
    """Recompute ``next_run`` from now (used on create/update)."""
    schedule.next_run = compute_next_run(schedule.cron_expr, schedule.timezone, utcnow())
