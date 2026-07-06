"""Job queue abstraction.

The API layer only needs to *enqueue a run*; how it runs is behind this
interface. Production uses arq/Redis; tests and single-process dev use an
in-memory queue that can optionally run the job inline. This is what keeps the
whole request/run path testable without a Redis server.
"""
from __future__ import annotations

import uuid
from typing import Awaitable, Callable, Protocol, runtime_checkable


@runtime_checkable
class JobQueue(Protocol):
    async def enqueue_run(self, run_id: uuid.UUID) -> None: ...


class InMemoryJobQueue:
    """Records enqueued run ids; optionally runs each inline via ``runner``.

    With no runner it just records (tests then drive the worker directly); with
    a runner it executes synchronously on enqueue (single-process dev)."""

    def __init__(self, runner: Callable[[uuid.UUID], Awaitable[None]] | None = None):
        self.enqueued: list[uuid.UUID] = []
        self._runner = runner

    async def enqueue_run(self, run_id: uuid.UUID) -> None:
        self.enqueued.append(run_id)
        if self._runner is not None:
            await self._runner(run_id)


class ArqJobQueue:
    """Enqueues onto Redis for the arq worker pool to consume."""

    def __init__(self, pool: object):
        self._pool = pool  # arq.ArqRedis

    async def enqueue_run(self, run_id: uuid.UUID) -> None:
        await self._pool.enqueue_job("run_pipeline_task", str(run_id))  # type: ignore[attr-defined]
