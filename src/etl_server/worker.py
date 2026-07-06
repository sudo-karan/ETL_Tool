"""The run worker: turn a queued Run into engine execution + persisted results.

``execute_run`` is the reusable core (called by the arq task in production and
inline by the in-memory queue in dev/tests). It:

1. claims the run (idempotent: only a ``queued`` run proceeds) and marks it
   ``running``;
2. resolves the owner's encrypted secrets via the engine's ``resolve_secrets``;
3. invokes the pure engine with the deployment's SSRF policy + concurrency cap,
   flushing structured log events to ``run_logs`` incrementally so SSE can
   stream them live;
4. persists structured ``run_errors``, a trimmed result, and the terminal
   status.

Only one DB write-session is ever open at a time (running → log flusher →
finish), so it is safe on SQLite as well as PostgreSQL.
"""
from __future__ import annotations

import asyncio
import uuid
from typing import Any

from etl_core import (
    ExecutionOptions,
    PipelineSpec,
    execute_pipeline,
    resolve_secrets,
)
from etl_core.engine import RunResult, RunStatus
from etl_core.errors import ErrorCategory, NodeError, SecretNotFoundError, utcnow

from .config import Settings
from .db import Database
from .models import Pipeline, Run, RunError, RunLog, RunState
from .secrets_store import DbSecretsProvider

_LOG_FLUSH_INTERVAL_S = 0.2
_OUTPUT_RECORD_CAP = 1000


def _trimmed_result(result: RunResult) -> dict[str, Any]:
    outputs: dict[str, Any] = {}
    truncated = False
    for node_id, records in result.outputs.items():
        if len(records) > _OUTPUT_RECORD_CAP:
            outputs[node_id] = records[:_OUTPUT_RECORD_CAP]
            truncated = True
        else:
            outputs[node_id] = records
    return {
        "status": result.status.value,
        "node_results": {
            node_id: nr.model_dump(mode="json") for node_id, nr in result.node_results.items()
        },
        "outputs": outputs,
        "outputs_truncated": truncated,
    }


async def _persist_errors(session: Any, run_id: uuid.UUID, errors: list[NodeError]) -> None:
    for error in errors:
        session.add(
            RunError(
                run_id=run_id,
                node_id=error.node_id,
                node_type=error.node_type,
                category=error.category.value,
                message=error.message,
                http_status=error.http_status,
                request_summary=error.request_summary,
                attempts=error.attempts,
                timestamp=error.timestamp,
                details=error.details,
            )
        )


async def _fail_run(
    db: Database, run_id: uuid.UUID, error: NodeError, *, started: bool
) -> None:
    async with db.sessionmaker() as session:
        run = await session.get(Run, run_id)
        if run is None:
            return
        run.status = RunState.FAILED.value
        run.finished_at = utcnow()
        if started and run.started_at is None:
            run.started_at = run.finished_at
        run.error_count = 1
        await _persist_errors(session, run_id, [error])
        await session.commit()


async def execute_run(db: Database, settings: Settings, run_id: uuid.UUID) -> None:
    # 1. Claim the run (idempotent).
    async with db.sessionmaker() as session:
        run = await session.get(Run, run_id)
        if run is None or run.status != RunState.QUEUED.value:
            return
        pipeline = await session.get(Pipeline, run.pipeline_id)
        if pipeline is None:
            run.status = RunState.FAILED.value
            run.finished_at = utcnow()
            await session.commit()
            return
        run.status = RunState.RUNNING.value
        run.started_at = utcnow()
        owner_id = run.owner_id
        spec_dict = pipeline.spec
        await session.commit()

    spec = PipelineSpec.model_validate(spec_dict)
    cipher = settings.secrets_cipher()

    # 2. Resolve the owner's secrets.
    try:
        async with db.sessionmaker() as session:
            provider = DbSecretsProvider(session, owner_id, cipher)
            secrets = await resolve_secrets(spec, provider)
    except SecretNotFoundError as exc:
        await _fail_run(
            db,
            run_id,
            NodeError(
                node_id="__pipeline__",
                node_type="pipeline",
                category=ErrorCategory.CONFIG,
                message=f"secret {exc.ref!r} is not configured for this user",
            ),
            started=True,
        )
        return

    # SSRF policy from deployment settings is applied to every server-issued
    # request (HTTP + DB). file_policy is left at the engine default here; a
    # locked-down deployment can set one.
    options = ExecutionOptions(
        max_concurrency=settings.max_run_concurrency,
        ssrf_policy=settings.ssrf_policy(),
    )

    # 3. Run the engine, flushing log events to run_logs live.
    buffer: list[Any] = []
    stop = asyncio.Event()

    def on_event(event: Any) -> None:
        buffer.append(event)

    async def flusher() -> None:
        written = 0
        async with db.sessionmaker() as session:
            while True:
                while written < len(buffer):
                    event = buffer[written]
                    session.add(
                        RunLog(
                            run_id=run_id,
                            seq=written,
                            timestamp=event.timestamp,
                            level=event.level.value,
                            node_id=event.node_id,
                            message=event.message,
                            data=event.data,
                        )
                    )
                    written += 1
                await session.commit()
                if stop.is_set() and written >= len(buffer):
                    return
                try:
                    await asyncio.wait_for(stop.wait(), timeout=_LOG_FLUSH_INTERVAL_S)
                except asyncio.TimeoutError:
                    pass

    flush_task = asyncio.create_task(flusher())
    try:
        result = await execute_pipeline(spec, secrets, options, on_event=on_event)
    finally:
        stop.set()
        await flush_task

    # 4. Persist errors, result, terminal status.
    async with db.sessionmaker() as session:
        run = await session.get(Run, run_id)
        if run is None:
            return
        await _persist_errors(session, run_id, result.errors)
        run.error_count = len(result.errors)
        run.result = _trimmed_result(result)
        run.status = (
            RunState.SUCCEEDED.value
            if result.status == RunStatus.SUCCEEDED
            else RunState.FAILED.value
        )
        run.finished_at = utcnow()
        await session.commit()


# --------------------------------------------------------------------------
# arq worker wiring (production)
# --------------------------------------------------------------------------
async def run_pipeline_task(ctx: dict, run_id: str) -> None:
    await execute_run(ctx["db"], ctx["settings"], uuid.UUID(run_id))


async def scheduler_cron(ctx: dict) -> None:
    from .scheduler import enqueue_due_schedules

    db: Database = ctx["db"]
    settings: Settings = ctx["settings"]

    async def enqueue(run_id: uuid.UUID) -> None:
        await ctx["redis"].enqueue_job("run_pipeline_task", str(run_id))

    await enqueue_due_schedules(db, settings, enqueue)


async def _on_startup(ctx: dict) -> None:
    from .config import get_settings

    settings = get_settings()
    ctx["settings"] = settings
    ctx["db"] = Database(settings.database_url)


async def _on_shutdown(ctx: dict) -> None:
    db: Database | None = ctx.get("db")
    if db is not None:
        await db.dispose()


def build_worker_settings() -> type:
    """Return an arq ``WorkerSettings`` class (imported lazily so the arq/redis
    dependency is only needed when actually running a worker)."""
    from arq import cron
    from arq.connections import RedisSettings

    from .config import get_settings

    settings = get_settings()

    class WorkerSettings:
        functions = [run_pipeline_task]
        cron_jobs = [cron(scheduler_cron, minute=set(range(60)))]  # every minute
        on_startup = _on_startup
        on_shutdown = _on_shutdown
        redis_settings = RedisSettings.from_dsn(settings.redis_url)

    return WorkerSettings
