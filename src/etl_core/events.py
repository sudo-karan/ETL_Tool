"""Structured, per-run log events.

A :class:`RunLog` instance is created per pipeline run and threaded through
execution via the run context -- there is no module-level logger state, so
concurrent runs never interleave their logs. The optional ``on_event``
callback is the hook the Phase 3 server will use to stream logs live.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Callable

from pydantic import BaseModel, Field

from .errors import utcnow


class LogLevel(str, Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class LogEvent(BaseModel):
    timestamp: datetime = Field(default_factory=utcnow)
    level: LogLevel
    node_id: str | None = None
    message: str
    data: dict[str, Any] | None = None


class RunLog:
    def __init__(self, on_event: Callable[[LogEvent], None] | None = None):
        self.events: list[LogEvent] = []
        self._on_event = on_event

    def log(
        self,
        level: LogLevel,
        message: str,
        *,
        node_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> LogEvent:
        event = LogEvent(level=level, message=message, node_id=node_id, data=data)
        self.events.append(event)
        if self._on_event is not None:
            self._on_event(event)
        return event

    def debug(self, message: str, **kw: Any) -> LogEvent:
        return self.log(LogLevel.DEBUG, message, **kw)

    def info(self, message: str, **kw: Any) -> LogEvent:
        return self.log(LogLevel.INFO, message, **kw)

    def warning(self, message: str, **kw: Any) -> LogEvent:
        return self.log(LogLevel.WARNING, message, **kw)

    def error(self, message: str, **kw: Any) -> LogEvent:
        return self.log(LogLevel.ERROR, message, **kw)
