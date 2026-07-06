"""Run trigger, inspection, listing, and live SSE streaming of logs/errors."""
from __future__ import annotations

import asyncio
import json
import uuid

from fastapi import APIRouter, Depends, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import Database
from ..deps import get_current_user, get_database, get_queue, get_session
from ..models import Run, RunError, RunLog, RunState, RunTrigger, User
from ..queue import JobQueue
from ..schemas import RunDetail, RunErrorRead, RunLogRead, RunRead, RunTriggerRequest
from ..service import get_owned_pipeline, get_owned_run

router = APIRouter(tags=["runs"])

_SSE_POLL_S = 0.25
_SSE_MAX_POLLS = 4 * 60 * 15  # ~15 minutes


def _sse(event: str, data: dict) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n".encode()


@router.post(
    "/pipelines/{pipeline_id}/runs",
    response_model=RunRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def trigger_run(
    pipeline_id: uuid.UUID,
    payload: RunTriggerRequest | None = None,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
    queue: JobQueue = Depends(get_queue),
) -> Run:
    pipeline = await get_owned_pipeline(session, pipeline_id, user.id)
    run = Run(
        pipeline_id=pipeline.id,
        owner_id=user.id,
        status=RunState.QUEUED.value,
        trigger=RunTrigger.MANUAL.value,
        params=payload.params if payload else None,
    )
    session.add(run)
    await session.commit()
    run_id = run.id
    # Commit before enqueue so a worker in another process sees the row.
    await queue.enqueue_run(run_id)
    # An inline queue may have already run it; reflect the latest state.
    await session.refresh(run)
    return run


@router.get("/runs", response_model=list[RunRead])
async def list_runs(
    pipeline_id: uuid.UUID | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[Run]:
    query = select(Run).where(Run.owner_id == user.id)
    if pipeline_id is not None:
        query = query.where(Run.pipeline_id == pipeline_id)
    query = query.order_by(Run.created_at.desc()).limit(limit)
    return list((await session.execute(query)).scalars().all())


@router.get("/runs/{run_id}", response_model=RunDetail)
async def get_run(
    run_id: uuid.UUID,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> RunDetail:
    run = await get_owned_run(session, run_id, user.id)
    logs = (
        (await session.execute(select(RunLog).where(RunLog.run_id == run_id).order_by(RunLog.id)))
        .scalars()
        .all()
    )
    errors = (
        (
            await session.execute(
                select(RunError).where(RunError.run_id == run_id).order_by(RunError.id)
            )
        )
        .scalars()
        .all()
    )
    base = RunRead.model_validate(run)  # scalar columns only (no relationship lazy-load)
    return RunDetail(
        **base.model_dump(),
        logs=[RunLogRead.model_validate(log) for log in logs],
        errors=[RunErrorRead.model_validate(error) for error in errors],
    )


@router.get("/runs/{run_id}/events")
async def stream_run_events(
    run_id: uuid.UUID,
    request: Request,
    user: User = Depends(get_current_user),
    db: Database = Depends(get_database),
) -> StreamingResponse:
    # Ownership check up front (short-lived session).
    async with db.sessionmaker() as session:
        await get_owned_run(session, run_id, user.id)

    async def event_stream():
        last_log_id = 0
        last_status: str | None = None
        polls = 0
        while True:
            if await request.is_disconnected():
                return
            async with db.sessionmaker() as session:
                run = await session.get(Run, run_id)
                if run is None:
                    return
                logs = (
                    (
                        await session.execute(
                            select(RunLog)
                            .where(RunLog.run_id == run_id, RunLog.id > last_log_id)
                            .order_by(RunLog.id)
                        )
                    )
                    .scalars()
                    .all()
                )
                current_status = run.status
                terminal = RunState(current_status).terminal
                error_payloads = None
                if terminal:
                    errors = (
                        (
                            await session.execute(
                                select(RunError)
                                .where(RunError.run_id == run_id)
                                .order_by(RunError.id)
                            )
                        )
                        .scalars()
                        .all()
                    )
                    error_payloads = [
                        RunErrorRead.model_validate(e).model_dump(mode="json") for e in errors
                    ]
                log_events = [
                    (log.id, RunLogRead.model_validate(log).model_dump(mode="json"))
                    for log in logs
                ]

            for log_id, payload in log_events:
                last_log_id = log_id
                yield _sse("log", payload)
            if current_status != last_status:
                last_status = current_status
                yield _sse("status", {"status": current_status})
            if terminal:
                yield _sse("done", {"status": current_status, "errors": error_payloads})
                return
            polls += 1
            if polls >= _SSE_MAX_POLLS:
                yield _sse("timeout", {"status": current_status})
                return
            await asyncio.sleep(_SSE_POLL_S)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
