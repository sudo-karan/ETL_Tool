"""Scheduler: timezone-aware due evaluation + the enqueue tick."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, update

from etl_core.errors import utcnow
from etl_server.models import Run, RunState, Schedule
from etl_server.scheduler import (
    ScheduleConfigError,
    compute_next_run,
    enqueue_due_schedules,
    validate_cron,
    validate_timezone,
)


def test_compute_next_run_utc():
    after = datetime(2026, 7, 6, 12, 2, tzinfo=timezone.utc)
    assert compute_next_run("*/5 * * * *", "UTC", after) == datetime(
        2026, 7, 6, 12, 5, tzinfo=timezone.utc
    )


def test_compute_next_run_respects_timezone():
    # 08:02 in New York (EDT, UTC-4) -> next 09:00 local run = 13:00 UTC.
    after = datetime(2026, 7, 6, 12, 2, tzinfo=timezone.utc)  # 08:02 EDT
    assert compute_next_run("0 9 * * *", "America/New_York", after) == datetime(
        2026, 7, 6, 13, 0, tzinfo=timezone.utc
    )


def test_validators():
    validate_cron("0 9 * * 1")
    validate_timezone("America/New_York")
    with pytest.raises(ScheduleConfigError):
        validate_cron("definitely not cron")
    with pytest.raises(ScheduleConfigError):
        validate_timezone("Mars/Phobos")


async def _fake_enqueue(collector):
    async def _enqueue(run_id):
        collector.append(run_id)

    return _enqueue


async def test_tick_enqueues_due_run(auth_client, make_pipeline, database, settings):
    pid = await make_pipeline()
    await auth_client.post(
        "/schedules", json={"pipeline_id": pid, "cron_expr": "*/5 * * * *", "timezone": "UTC"}
    )
    async with database.sessionmaker() as session:
        await session.execute(update(Schedule).values(next_run=utcnow() - timedelta(minutes=1)))
        await session.commit()

    enqueued: list = []
    ids = await enqueue_due_schedules(database, settings, await _fake_enqueue(enqueued))
    assert len(ids) == 1
    assert enqueued == ids

    async with database.sessionmaker() as session:
        run = await session.get(Run, ids[0])
        assert run.trigger == "schedule"
        assert run.status == RunState.QUEUED.value
        schedule = (await session.execute(select(Schedule))).scalars().one()
        assert schedule.last_run is not None
        assert schedule.next_run > utcnow()  # advanced to the future


async def test_disabled_schedule_not_enqueued(auth_client, make_pipeline, database, settings):
    pid = await make_pipeline()
    await auth_client.post(
        "/schedules",
        json={"pipeline_id": pid, "cron_expr": "*/5 * * * *", "timezone": "UTC", "enabled": False},
    )
    # Force a past next_run anyway; the enabled filter must still exclude it.
    async with database.sessionmaker() as session:
        await session.execute(update(Schedule).values(next_run=utcnow() - timedelta(minutes=1)))
        await session.commit()
    ids = await enqueue_due_schedules(database, settings, await _fake_enqueue([]))
    assert ids == []


async def test_future_schedule_not_yet_due(auth_client, make_pipeline, database, settings):
    pid = await make_pipeline()
    await auth_client.post(
        "/schedules", json={"pipeline_id": pid, "cron_expr": "*/5 * * * *", "timezone": "UTC"}
    )
    # next_run is in the future as created; nothing is due.
    ids = await enqueue_due_schedules(database, settings, await _fake_enqueue([]))
    assert ids == []
