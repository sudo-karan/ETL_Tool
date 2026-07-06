"""Structured error model shared by the engine and (later) the server and UI.

Errors are never bare strings: every node failure is captured as a
:class:`NodeError` so downstream consumers (CLI today, REST/WebSocket + UI
later) can render category, HTTP status, redacted request summaries and the
retry count without parsing free text.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ErrorCategory(str, Enum):
    DNS = "dns"
    NETWORK = "network"
    TLS = "tls"
    TIMEOUT = "timeout"
    HTTP_STATUS = "http_status"
    AUTH = "auth"
    RATE_LIMIT = "rate_limit"
    VALIDATION = "validation"
    TRANSFORM = "transform"
    DECRYPTION = "decryption"
    CONFIG = "config"
    UNKNOWN = "unknown"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class NodeError(BaseModel):
    """A structured, UI-renderable error produced by a failing node."""

    node_id: str
    node_type: str
    category: ErrorCategory
    message: str
    http_status: int | None = None
    request_summary: str | None = None  # method + REDACTED url, no headers/body
    attempts: int = 1
    timestamp: datetime = Field(default_factory=utcnow)
    details: dict[str, Any] | None = None


class NodeExecutionError(Exception):
    """Raised inside a node's execution; carries the structured NodeError."""

    def __init__(self, node_error: NodeError):
        super().__init__(node_error.message)
        self.node_error = node_error


class PipelineValidationError(Exception):
    def __init__(self, issues: list[str]):
        super().__init__("; ".join(issues))
        self.issues = issues


class SecretNotFoundError(Exception):
    def __init__(self, ref: str):
        super().__init__(f"secret {ref!r} could not be resolved")
        self.ref = ref


class ReferenceResolutionError(Exception):
    """A $upstream / $iter reference could not be resolved at run time."""


class SSRFBlockedError(Exception):
    """A server-issued request was denied by the SSRF policy."""


class PathNotFoundError(Exception):
    """A dotted data path did not exist in the target object."""
