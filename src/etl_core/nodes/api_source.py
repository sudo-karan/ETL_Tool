"""api_source: fetch records from an HTTP API.

Supports bearer / api-key / basic auth (credentials via secret_ref only),
cursor / offset / page pagination, retries with exponential backoff, an
optional per-run rate limit, and {placeholder} path parameters. The URL host
is checked against the run's SSRF policy before any request is made.
"""
from __future__ import annotations

import socket
import string
import urllib.parse
from typing import Any, ClassVar, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field

from ..errors import ErrorCategory, SSRFBlockedError
from ..http_client import (
    AuthConfig,
    PaginationConfig,
    RateLimitConfig,
    RequestFailure,
    RetryConfig,
    apply_auth,
    request_with_retry,
)
from ..paths import get_path
from ..ssrf import ensure_url_allowed, guarded_event_hooks
from .base import Node, NodeContext, NodeInputs, NodeOutputs, Records
from .registry import register_node

_MISSING = object()


class ApiSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"] = "GET"
    url: str
    path_params: dict[str, Any] = Field(default_factory=dict)
    query_params: dict[str, Any] = Field(default_factory=dict)
    headers: dict[str, Any] = Field(default_factory=dict)
    body: Any = None
    auth: AuthConfig | None = None
    pagination: PaginationConfig | None = None
    retry: RetryConfig = Field(default_factory=RetryConfig)
    rate_limit: RateLimitConfig | None = None
    timeout_s: float = Field(30.0, gt=0)
    # Where the record list lives in each response body (dotted path).
    # For paginated sources, pagination.items_path takes precedence.
    items_path: str | None = None
    verify_tls: bool = True


def _format_url(url: str, path_params: dict[str, Any], ctx: NodeContext) -> str:
    """Fill {name} placeholders, URL-quoting values to prevent path injection."""
    formatter = string.Formatter()
    out: list[str] = []
    for literal, field_name, format_spec, conversion in formatter.parse(url):
        out.append(literal)
        if field_name is None:
            continue
        if field_name == "" or format_spec or conversion:
            raise ctx.error(
                ErrorCategory.CONFIG,
                f"unsupported URL placeholder in {url!r}; use named {{param}} placeholders",
            )
        if field_name not in path_params:
            raise ctx.error(
                ErrorCategory.CONFIG,
                f"URL placeholder {{{field_name}}} has no matching path_params entry",
            )
        out.append(urllib.parse.quote(str(path_params[field_name]), safe=""))
    return "".join(out)


def _to_records(data: Any, items_path: str | None, ctx: NodeContext, summary: str) -> Records:
    if data is None:  # empty body (HEAD, 204, zero-length response)
        return []
    if items_path:
        data = get_path(data, items_path, default=_MISSING)
        if data is _MISSING:
            raise ctx.error(
                ErrorCategory.VALIDATION,
                f"items_path {items_path!r} not found in response",
                request_summary=summary,
            )
    items = data if isinstance(data, list) else [data]
    return [item if isinstance(item, dict) else {"value": item} for item in items]


@register_node
class ApiSourceNode(Node):
    type_name: ClassVar[str] = "api_source"
    config_model: ClassVar[type[BaseModel]] = ApiSourceConfig
    # Optional context input: connecting an upstream node (or iterator) makes
    # its records available to $upstream/$iter references in this config.
    input_ports: ClassVar[tuple[str, ...]] = ("in",)
    output_ports: ClassVar[tuple[str, ...]] = ("out",)

    async def run(self, inputs: NodeInputs, ctx: NodeContext) -> NodeOutputs:
        cfg: ApiSourceConfig = self.config  # type: ignore[assignment]
        url = _format_url(cfg.url, cfg.path_params, ctx)

        summary_base = ctx.run.redactor.request_summary(cfg.method, url)
        try:
            await ensure_url_allowed(url, ctx.run.options.ssrf_policy)
        except SSRFBlockedError as exc:
            raise ctx.error(ErrorCategory.CONFIG, str(exc), request_summary=summary_base) from exc
        except socket.gaierror as exc:
            host = urllib.parse.urlsplit(url).hostname
            raise ctx.error(
                ErrorCategory.DNS,
                f"DNS resolution failed for host {host!r}: {exc}",
                request_summary=summary_base,
            ) from exc

        headers = {str(k): str(v) for k, v in cfg.headers.items()}
        params: dict[str, Any] = dict(cfg.query_params)
        if cfg.auth is not None:
            secret_value = ctx.get_secret(cfg.auth.secret_ref)
            auth_headers, auth_params = apply_auth(cfg.auth, secret_value)
            headers.update(auth_headers)
            params.update(auth_params)

        limiter = (
            ctx.run.rate_limiter(self.node_id, cfg.rate_limit.rps)
            if cfg.rate_limit is not None
            else None
        )

        async with httpx.AsyncClient(
            timeout=cfg.timeout_s,
            verify=cfg.verify_tls,
            follow_redirects=True,
            event_hooks=guarded_event_hooks(ctx.run.options.ssrf_policy),
        ) as client:
            records = await self._fetch_all(client, cfg, url, headers, params, limiter, ctx)

        ctx.info(f"fetched {len(records)} record(s)")
        return {"out": records}

    async def _fetch_all(
        self,
        client: httpx.AsyncClient,
        cfg: ApiSourceConfig,
        url: str,
        headers: dict[str, str],
        params: dict[str, Any],
        limiter: Any,
        ctx: NodeContext,
    ) -> Records:
        pagination = cfg.pagination
        items_path = (pagination.items_path if pagination else None) or cfg.items_path
        records: Records = []
        page_count = 0

        # Pagination cursors/offsets mutate a copy, never the config.
        params = dict(params)
        if pagination is not None and pagination.type == "offset":
            offset = 0
            params[pagination.offset_param] = offset
            if pagination.limit_param and pagination.limit:
                params[pagination.limit_param] = pagination.limit
        if pagination is not None and pagination.type == "page":
            params[pagination.page_param] = pagination.start_page

        max_pages = pagination.max_pages if pagination is not None else 1

        while True:
            page_count += 1
            data, summary = await self._request_json(client, cfg, url, headers, params, limiter, ctx)
            items = _to_records(data, items_path, ctx, summary)
            records.extend(items)

            if pagination is None:
                break
            if pagination.type == "cursor":
                cursor = get_path(data, pagination.cursor_path, default=None)
                if not cursor:
                    break
                params[pagination.cursor_param] = cursor
            elif pagination.type == "offset":
                if not items or (pagination.limit and len(items) < pagination.limit):
                    break
                params[pagination.offset_param] = int(params[pagination.offset_param]) + len(items)
            else:  # page
                if not items:
                    break
                if pagination.page_size and len(items) < pagination.page_size:
                    break
                params[pagination.page_param] = int(params[pagination.page_param]) + 1

            if page_count >= max_pages:
                ctx.warning(
                    f"pagination stopped at max_pages={max_pages}; results may be truncated"
                )
                break

        if page_count > 1:
            ctx.debug(f"fetched {page_count} page(s)")
        return records

    async def _request_json(
        self,
        client: httpx.AsyncClient,
        cfg: ApiSourceConfig,
        url: str,
        headers: dict[str, str],
        params: dict[str, Any],
        limiter: Any,
        ctx: NodeContext,
    ) -> tuple[Any, str]:
        full_url = str(httpx.URL(url).copy_merge_params({k: str(v) for k, v in params.items()}))
        summary = ctx.run.redactor.request_summary(cfg.method, full_url)
        try:
            response, attempts = await request_with_retry(
                client,
                cfg.method,
                url,
                headers=headers,
                params=params,
                json_body=cfg.body,
                retry=cfg.retry,
                limiter=limiter,
                semaphore=ctx.run.http_semaphore,
                on_retry=lambda attempt, reason: ctx.warning(
                    f"attempt {attempt} failed ({reason}); retrying"
                ),
            )
        except RequestFailure as failure:
            raise ctx.error(
                failure.category,
                failure.message,
                http_status=failure.http_status,
                request_summary=summary,
                attempts=failure.attempts,
            ) from failure

        ctx.debug(f"{summary} -> HTTP {response.status_code} (attempt {attempts})")
        if cfg.method == "HEAD" or response.status_code == 204 or not response.content:
            return None, summary
        try:
            return response.json(), summary
        except ValueError as exc:
            raise ctx.error(
                ErrorCategory.VALIDATION,
                f"response body is not valid JSON: {exc}",
                http_status=response.status_code,
                request_summary=summary,
            ) from exc
