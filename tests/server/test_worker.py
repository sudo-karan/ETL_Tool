"""Worker execution details: idempotency and status transitions."""
from __future__ import annotations

import uuid

from sqlalchemy import func, select

from etl_server.models import Run, RunLog, RunState
from etl_server.worker import execute_run


async def _log_count(database, run_id) -> int:
    async with database.sessionmaker() as session:
        return (
            await session.execute(
                select(func.count()).select_from(RunLog).where(RunLog.run_id == run_id)
            )
        ).scalar_one()


async def test_execute_run_is_idempotent(auth_client, make_pipeline, database, settings):
    pid = await make_pipeline()
    run_id = uuid.UUID(
        (await auth_client.post(f"/pipelines/{pid}/runs", json={})).json()["id"]
    )
    before = await _log_count(database, run_id)
    assert before > 0

    # Re-running an already-terminal run must be a no-op (no duplicate logs).
    await execute_run(database, settings, run_id)
    assert await _log_count(database, run_id) == before

    async with database.sessionmaker() as session:
        run = await session.get(Run, run_id)
        assert run.status == RunState.SUCCEEDED.value
        assert run.started_at is not None and run.finished_at is not None


async def test_execute_run_missing_run_is_noop(database, settings):
    # Unknown run id must not raise.
    await execute_run(database, settings, uuid.uuid4())
