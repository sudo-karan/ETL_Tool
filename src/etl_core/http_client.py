"""Shared HTTP layer: auth, retry with backoff, rate limiting, pagination.

Both the api_source node and the connectivity tester build on this module,
so credentials are applied and errors categorized identically whether the
user is testing a source or running a pipeline.
"""
from __future__ import annotations

import asyncio
import base64
import socket
import ssl
import time
from typing import Any, Callable, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .errors import ErrorCategory, SSRFBlockedError

RETRYABLE_STATUS_MIN = 500
RATE_LIMIT_STATUS = 429


class AuthConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    type: Literal["bearer", "api_key", "basic"]
    secret_ref: str
    # api_key options: header or query parameter name + placement.
    name: str = "X-API-Key"
    location: Literal["header", "query"] = Field("header", alias="in")
    # basic auth: username here, password from the secret. If username is
    # omitted the secret itself must be "username:password".
    username: str | None = None


class RetryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max: int = Field(0, ge=0, le=10, description="Retries after the first attempt.")
    backoff: float = Field(0.5, ge=0, description="Base delay in seconds; doubles per retry.")


class RateLimitConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rps: float = Field(gt=0, description="Max requests per second for this node within one run.")


class PaginationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["cursor", "offset", "page"]
    items_path: str | None = None
    # cursor
    cursor_path: str | None = None
    cursor_param: str | None = None
    # offset
    offset_param: str = "offset"
    limit_param: str | None = None
    limit: int | None = Field(None, gt=0)
    # page
    page_param: str = "page"
    start_page: int = 1
    page_size: int | None = Field(None, gt=0)
    # safety cap for all modes
    max_pages: int = Field(100, ge=1)

    @model_validator(mode="after")
    def _check_mode_fields(self) -> "PaginationConfig":
        if self.type == "cursor" and not (self.cursor_path and self.cursor_param):
            raise ValueError("cursor pagination requires cursor_path and cursor_param")
        return self


def apply_auth(auth: AuthConfig, secret_value: str) -> tuple[dict[str, str], dict[str, str]]:
    """Return (headers, query_params) carrying the credential."""
    if auth.type == "bearer":
        return {"Authorization": f"Bearer {secret_value}"}, {}
    if auth.type == "api_key":
        if auth.location == "header":
            return {auth.name: secret_value}, {}
        return {}, {auth.name: secret_value}
    # basic
    if auth.username is not None:
        userpass = f"{auth.username}:{secret_value}"
    else:
        userpass = secret_value
    encoded = base64.b64encode(userpass.encode()).decode()
    return {"Authorization": f"Basic {encoded}"}, {}


class RateLimiter:
    """Async token-interval limiter: at most ``rps`` acquisitions per second,
    fair across concurrent waiters. Purely instance-state; one per node per run."""

    def __init__(self, rps: float):
        self._interval = 1.0 / rps
        self._next_slot = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self._interval
        delay = slot - now
        if delay > 0:
            await asyncio.sleep(delay)


def categorize_exception(exc: Exception) -> ErrorCategory:
    """Map a transport-level exception to a structured error category."""
    if isinstance(exc, httpx.TimeoutException):
        return ErrorCategory.TIMEOUT
    seen: set[int] = set()
    cause: BaseException | None = exc
    while cause is not None and id(cause) not in seen:
        seen.add(id(cause))
        if isinstance(cause, socket.gaierror):
            return ErrorCategory.DNS
        if isinstance(cause, (ssl.SSLError, ssl.CertificateError)):
            return ErrorCategory.TLS
        cause = cause.__cause__ if cause.__cause__ is not None else cause.__context__
    if isinstance(exc, (httpx.TransportError, httpx.HTTPError, OSError)):
        return ErrorCategory.NETWORK
    return ErrorCategory.UNKNOWN


def categorize_status(status_code: int) -> ErrorCategory:
    if status_code in (401, 403):
        return ErrorCategory.AUTH
    if status_code == RATE_LIMIT_STATUS:
        return ErrorCategory.RATE_LIMIT
    return ErrorCategory.HTTP_STATUS


class RequestFailure(Exception):
    """Internal failure carrier; nodes convert this into a NodeError."""

    def __init__(
        self,
        category: ErrorCategory,
        message: str,
        *,
        http_status: int | None = None,
        attempts: int = 1,
    ):
        super().__init__(message)
        self.category = category
        self.message = message
        self.http_status = http_status
        self.attempts = attempts


def _is_retryable_exception(exc: Exception) -> bool:
    return isinstance(exc, (httpx.TransportError,))


def _is_retryable_status(status_code: int) -> bool:
    return status_code == RATE_LIMIT_STATUS or status_code >= RETRYABLE_STATUS_MIN


def _retry_after_seconds(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None  # HTTP-date form not supported in v1


async def request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_body: Any = None,
    retry: RetryConfig | None = None,
    limiter: RateLimiter | None = None,
    semaphore: asyncio.Semaphore | None = None,
    raise_for_status: bool = True,
    on_retry: Callable[[int, str], None] | None = None,
) -> tuple[httpx.Response, int]:
    """Issue one logical request with retries. Returns (response, attempts).

    Retries transport errors, 429 and 5xx with exponential backoff
    (``backoff * 2**(attempt-1)`` seconds, honoring numeric Retry-After).
    Raises :class:`RequestFailure` when attempts are exhausted, or -- with
    ``raise_for_status`` -- on any non-retryable >=400 status.
    """
    retry = retry or RetryConfig()
    attempts = 0
    while True:
        attempts += 1
        if limiter is not None:
            await limiter.acquire()
        try:
            if semaphore is not None:
                async with semaphore:
                    response = await client.request(
                        method, url, headers=headers, params=params, json=json_body
                    )
            else:
                response = await client.request(
                    method, url, headers=headers, params=params, json=json_body
                )
        except Exception as exc:  # noqa: BLE001 - categorized below
            if isinstance(exc, asyncio.CancelledError):
                raise
            if isinstance(exc, SSRFBlockedError):
                # A redirect (or the initial URL) pointed at a blocked host;
                # never retry, surface as a config error.
                raise RequestFailure(
                    ErrorCategory.CONFIG, str(exc), attempts=attempts
                ) from exc
            category = categorize_exception(exc)
            if attempts <= retry.max and _is_retryable_exception(exc):
                if on_retry is not None:
                    on_retry(attempts, f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(retry.backoff * (2 ** (attempts - 1)))
                continue
            raise RequestFailure(
                category,
                f"request failed: {type(exc).__name__}: {exc}",
                attempts=attempts,
            ) from exc

        status = response.status_code
        if status >= 400 and _is_retryable_status(status) and attempts <= retry.max:
            delay = retry.backoff * (2 ** (attempts - 1))
            retry_after = _retry_after_seconds(response)
            if retry_after is not None:
                delay = max(delay, retry_after)
            if on_retry is not None:
                on_retry(attempts, f"HTTP {status}")
            await asyncio.sleep(delay)
            continue
        if raise_for_status and status >= 400:
            raise RequestFailure(
                categorize_status(status),
                f"HTTP {status} {response.reason_phrase or ''}".strip(),
                http_status=status,
                attempts=attempts,
            )
        return response, attempts
