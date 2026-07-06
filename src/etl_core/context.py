"""Per-run execution context.

Everything a run needs (secrets, options, log, redactor, concurrency
primitives) lives on a :class:`RunContext` created inside
``execute_pipeline`` and passed down explicitly. No module-level mutable
state exists anywhere in the engine, so any number of runs can execute
concurrently in one process or across workers without interference.
"""
from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field

from pydantic import BaseModel, Field

from .events import RunLog
from .http_client import RateLimiter
from .redact import Redactor
from .ssrf import SSRFPolicy


class ExecutionOptions(BaseModel):
    """Caller-supplied knobs for one run."""

    max_concurrency: int = Field(8, ge=1, description="Per-run cap on concurrent HTTP requests and iterator iterations.")
    continue_on_error: bool = Field(False, description="Keep executing branches/iterations that do not depend on a failed node (default is fail-fast).")
    ssrf_policy: SSRFPolicy = Field(default_factory=SSRFPolicy)


@dataclass
class RunContext:
    secrets: Mapping[str, str]
    options: ExecutionOptions
    log: RunLog
    redactor: Redactor
    http_semaphore: asyncio.Semaphore
    _rate_limiters: dict[str, RateLimiter] = field(default_factory=dict)

    def rate_limiter(self, node_id: str, rps: float) -> RateLimiter:
        """One limiter per node per run, shared across concurrent iterations
        of that node so its ``rps`` holds for the whole run."""
        limiter = self._rate_limiters.get(node_id)
        if limiter is None:
            limiter = RateLimiter(rps)
            self._rate_limiters[node_id] = limiter
        return limiter
