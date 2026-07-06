"""Connectivity / preflight diagnostics: test_connection.

Answers "can the SERVER (not the user's browser) reach this source?" by
climbing a ladder and reporting pass/fail + latency per rung:

    dns -> ssrf_policy -> tcp -> tls (https only) -> http -> auth

It reuses the api_source HTTP + auth layer, so what the tester exercises is
exactly what a run would do -- but it returns a diagnostic report, never
pipeline data. A truncated, secret-redacted sample of the response body is
included so users can see the data shape.
"""
from __future__ import annotations

import asyncio
import json
import ssl
import time
import urllib.parse
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field

from .errors import utcnow
from .http_client import apply_auth, categorize_exception
from .nodes.api_source import ApiSourceConfig, _format_url
from .redact import Redactor
from .references import has_references
from .schema import NodeSpec
from .ssrf import SSRFPolicy, find_blocked, guarded_event_hooks, resolve_host

CheckStatus = Literal["passed", "failed", "skipped"]

_DEFAULT_PORTS = {"http": 80, "https": 443}


class DiagnosticCheck(BaseModel):
    name: str  # dns | ssrf_policy | tcp | tls | http | auth | config
    status: CheckStatus
    latency_ms: float | None = None
    detail: str | None = None
    error: str | None = None


class DiagnosticReport(BaseModel):
    source_type: str
    target: str
    ok: bool
    checks: list[DiagnosticCheck] = Field(default_factory=list)
    sample_body: str | None = None
    timestamp: datetime = Field(default_factory=utcnow)


class _Ladder:
    def __init__(self) -> None:
        self.checks: list[DiagnosticCheck] = []

    def add(self, name: str, status: CheckStatus, **kw: Any) -> DiagnosticCheck:
        check = DiagnosticCheck(name=name, status=status, **kw)
        self.checks.append(check)
        return check

    def skip_rest(self, *names: str, detail: str = "skipped: earlier check failed") -> None:
        for name in names:
            self.add(name, "skipped", detail=detail)

    @property
    def ok(self) -> bool:
        return all(check.status != "failed" for check in self.checks)


class _StubContext:
    """Minimal NodeContext stand-in for _format_url outside a run."""

    def __init__(self, redactor: Redactor):
        self._redactor = redactor

    def error(self, category: Any, message: str, **kw: Any) -> Exception:
        return ValueError(message)


async def test_connection(
    source: NodeSpec | Mapping[str, Any],
    secrets: Mapping[str, str] | None = None,
    *,
    ssrf_policy: SSRFPolicy | None = None,
    sample_bytes: int = 2000,
) -> DiagnosticReport:
    """Run the connectivity diagnostic ladder against a source definition.

    ``source`` is a node spec ({"type": "api_source" | "db_source",
    "config": {...}}). api_source climbs dns→ssrf→tcp→tls→http→auth;
    db_source climbs dns→ssrf→tcp→connect→query (connect covers TLS + auth).
    """
    if isinstance(source, NodeSpec):
        source_type, raw_config = source.type, source.config
    else:
        source_type = str(source.get("type", ""))
        raw_config = dict(source.get("config") or {})
    secrets = dict(secrets or {})
    policy = ssrf_policy if ssrf_policy is not None else SSRFPolicy()

    if source_type == "api_source":
        return await _test_api_source(raw_config, secrets, policy, sample_bytes)
    if source_type == "db_source":
        return await _test_db_source(raw_config, secrets, policy, sample_bytes)

    ladder = _Ladder()
    ladder.add(
        "config",
        "failed",
        error=f"unsupported source type {source_type!r}; supported: 'api_source', 'db_source'",
    )
    return DiagnosticReport(
        source_type=source_type or "unknown", target="", ok=False, checks=ladder.checks
    )


async def _test_api_source(
    raw_config: Mapping[str, Any],
    secrets: Mapping[str, str],
    policy: SSRFPolicy,
    sample_bytes: int,
) -> DiagnosticReport:
    source_type = "api_source"
    redactor = Redactor(secrets.values())
    ladder = _Ladder()

    def report(target: str, sample: str | None = None) -> DiagnosticReport:
        return DiagnosticReport(
            source_type=source_type,
            target=redactor.redact_url(target),
            ok=ladder.ok,
            checks=ladder.checks,
            sample_body=sample,
        )

    if has_references(raw_config):
        ladder.add(
            "config",
            "failed",
            error="config contains $upstream/$iter references, which cannot be "
            "resolved outside a pipeline run; use literal values to test",
        )
        return report(target="")
    try:
        cfg = ApiSourceConfig.model_validate(raw_config)
        url = _format_url(cfg.url, cfg.path_params, _StubContext(redactor))  # type: ignore[arg-type]
        parts = urllib.parse.urlsplit(url)
        host = parts.hostname
        scheme = parts.scheme.lower()
        if not host or scheme not in _DEFAULT_PORTS:
            raise ValueError(f"URL must be http(s) with a host, got {url!r}")
        port = parts.port or _DEFAULT_PORTS[scheme]
    except Exception as exc:  # noqa: BLE001 - reported, not raised
        ladder.add("config", "failed", error=redactor.redact(str(exc)))
        return report(target=raw_config.get("url", "") if isinstance(raw_config, Mapping) else "")

    # ---- 1. DNS -----------------------------------------------------------
    started = time.perf_counter()
    try:
        ips = await resolve_host(host, port)
    except OSError as exc:
        ladder.add(
            "dns",
            "failed",
            latency_ms=(time.perf_counter() - started) * 1000,
            error=f"DNS resolution failed: {exc}",
        )
        ladder.skip_rest("tcp", "tls", "http", "auth")
        return report(url)
    ladder.add(
        "dns",
        "passed",
        latency_ms=(time.perf_counter() - started) * 1000,
        detail=", ".join(ips[:5]) + ("…" if len(ips) > 5 else ""),
    )

    # ---- 2. SSRF policy ----------------------------------------------------
    if policy.enabled:
        reason = find_blocked(host, ips, policy)
        if reason is not None:
            ladder.add("ssrf_policy", "failed", error=reason)
            ladder.skip_rest("tcp", "tls", "http", "auth")
            return report(url)
        ladder.add("ssrf_policy", "passed", detail="host allowed by SSRF policy")
    else:
        ladder.add("ssrf_policy", "skipped", detail="SSRF guard disabled")

    # ---- 3. TCP ------------------------------------------------------------
    started = time.perf_counter()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ips[0], port), timeout=cfg.timeout_s
        )
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass
    except (OSError, asyncio.TimeoutError) as exc:
        ladder.add(
            "tcp",
            "failed",
            latency_ms=(time.perf_counter() - started) * 1000,
            error=f"cannot connect to {ips[0]}:{port}: {exc!r}",
        )
        ladder.skip_rest("tls", "http", "auth")
        return report(url)
    ladder.add(
        "tcp",
        "passed",
        latency_ms=(time.perf_counter() - started) * 1000,
        detail=f"{ips[0]}:{port} reachable",
    )

    # ---- 4. TLS (https only) ------------------------------------------------
    if scheme == "https":
        ssl_context = ssl.create_default_context()
        if not cfg.verify_tls:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        started = time.perf_counter()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=ssl_context),
                timeout=cfg.timeout_s,
            )
            ssl_object = writer.get_extra_info("ssl_object")
            detail = ssl_object.version() if ssl_object is not None else "TLS established"
            writer.close()
            try:
                await writer.wait_closed()
            except (OSError, ssl.SSLError):
                pass
        except (OSError, ssl.SSLError, asyncio.TimeoutError) as exc:
            ladder.add(
                "tls",
                "failed",
                latency_ms=(time.perf_counter() - started) * 1000,
                error=f"TLS handshake failed: {exc}",
            )
            ladder.skip_rest("http", "auth")
            return report(url)
        ladder.add(
            "tls",
            "passed",
            latency_ms=(time.perf_counter() - started) * 1000,
            detail=detail,
        )
    else:
        ladder.add("tls", "skipped", detail="not https")

    # ---- 5. HTTP (no credentials) + 6. auth ---------------------------------
    base_headers = {str(k): str(v) for k, v in cfg.headers.items()}
    base_params = {k: str(v) for k, v in cfg.query_params.items()}
    sample: str | None = None
    async with httpx.AsyncClient(
        timeout=cfg.timeout_s,
        verify=cfg.verify_tls,
        follow_redirects=True,
        event_hooks=guarded_event_hooks(policy),
    ) as client:
        started = time.perf_counter()
        try:
            response = await client.request(
                "GET", url, headers=base_headers, params=base_params
            )
        except Exception as exc:  # noqa: BLE001 - reported, not raised
            category = categorize_exception(exc)
            ladder.add(
                "http",
                "failed",
                latency_ms=(time.perf_counter() - started) * 1000,
                error=redactor.redact(f"{category.value}: {type(exc).__name__}: {exc}"),
            )
            ladder.skip_rest("auth")
            return report(url)
        http_latency = (time.perf_counter() - started) * 1000
        unauth_status = response.status_code
        ladder.add(
            "http",
            "passed",
            latency_ms=http_latency,
            detail=f"HTTP {unauth_status} without credentials",
        )
        sample = redactor.redact(response.text[:sample_bytes]) or None

        if cfg.auth is None:
            detail = "no credentials configured"
            if unauth_status in (401, 403):
                detail += f" (endpoint answered HTTP {unauth_status}; it likely requires auth)"
            ladder.add("auth", "skipped", detail=detail)
            return report(url, sample)

        secret_value = secrets.get(cfg.auth.secret_ref)
        if secret_value is None:
            ladder.add(
                "auth",
                "failed",
                error=f"secret {cfg.auth.secret_ref!r} was not provided",
            )
            return report(url, sample)
        auth_headers, auth_params = apply_auth(cfg.auth, secret_value)
        started = time.perf_counter()
        try:
            response = await client.request(
                "GET",
                url,
                headers={**base_headers, **auth_headers},
                params={**base_params, **auth_params},
            )
        except Exception as exc:  # noqa: BLE001 - reported, not raised
            ladder.add(
                "auth",
                "failed",
                latency_ms=(time.perf_counter() - started) * 1000,
                error=redactor.redact(f"{type(exc).__name__}: {exc}"),
            )
            return report(url, sample)
        auth_latency = (time.perf_counter() - started) * 1000
        if response.status_code in (401, 403):
            ladder.add(
                "auth",
                "failed",
                latency_ms=auth_latency,
                error=f"HTTP {response.status_code}: credential was rejected",
            )
        else:
            ladder.add(
                "auth",
                "passed",
                latency_ms=auth_latency,
                detail=f"HTTP {response.status_code} with credentials",
            )
            sample = redactor.redact(response.text[:sample_bytes]) or sample
    return report(url, sample)


async def _test_db_source(
    raw_config: Mapping[str, Any],
    secrets: Mapping[str, str],
    policy: SSRFPolicy,
    sample_bytes: int,
) -> DiagnosticReport:
    """Ladder for db_source: config → dns → ssrf_policy → tcp → connect → query.

    ``connect`` establishes the connection (covering TLS negotiation and
    authentication); ``query`` runs a trivial ``SELECT 1``. DNS / SSRF / TCP
    apply only to network drivers (skipped for SQLite, which is a local file).
    Accepts either a full db_source config or a bare connection object.
    """
    from sqlalchemy import text

    from .db import DbConnectionConfig, categorize_db_exception, open_engine
    from .nodes.db_source import DbSourceConfig, endpoint_summary

    source_type = "db_source"
    redactor = Redactor(secrets.values())
    ladder = _Ladder()

    def report(target: str, sample: str | None = None) -> DiagnosticReport:
        return DiagnosticReport(
            source_type=source_type,
            target=redactor.redact(target),
            ok=ladder.ok,
            checks=ladder.checks,
            sample_body=sample,
        )

    if has_references(raw_config):
        ladder.add(
            "config",
            "failed",
            error="config contains $upstream/$iter references, which cannot be "
            "resolved outside a pipeline run; use literal values to test",
        )
        return report(target="")
    try:
        if "connection" in raw_config:
            conn = DbSourceConfig.model_validate(raw_config).connection
        else:
            conn = DbConnectionConfig.model_validate(raw_config)
    except Exception as exc:  # noqa: BLE001 - reported, not raised
        ladder.add("config", "failed", error=redactor.redact(str(exc)))
        return report(target="")

    target = endpoint_summary(conn)
    host, port = conn.endpoint()
    password = secrets.get(conn.secret_ref) if conn.secret_ref else None
    if conn.secret_ref and password is None:
        ladder.add("config", "failed", error=f"secret {conn.secret_ref!r} was not provided")
        return report(target)

    if host is not None:
        # ---- DNS ----------------------------------------------------------
        started = time.perf_counter()
        try:
            ips = await resolve_host(host, port)
        except OSError as exc:
            ladder.add(
                "dns",
                "failed",
                latency_ms=(time.perf_counter() - started) * 1000,
                error=f"DNS resolution failed: {exc}",
            )
            ladder.skip_rest("ssrf_policy", "tcp", "connect", "query")
            return report(target)
        ladder.add(
            "dns",
            "passed",
            latency_ms=(time.perf_counter() - started) * 1000,
            detail=", ".join(ips[:5]) + ("…" if len(ips) > 5 else ""),
        )
        # ---- SSRF policy --------------------------------------------------
        if policy.enabled:
            reason = find_blocked(host, ips, policy)
            if reason is not None:
                ladder.add("ssrf_policy", "failed", error=reason)
                ladder.skip_rest("tcp", "connect", "query")
                return report(target)
            ladder.add("ssrf_policy", "passed", detail="host allowed by SSRF policy")
        else:
            ladder.add("ssrf_policy", "skipped", detail="SSRF guard disabled")
        # ---- TCP ----------------------------------------------------------
        started = time.perf_counter()
        try:
            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ips[0], port), timeout=conn.connect_timeout_s
            )
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
        except (OSError, asyncio.TimeoutError) as exc:
            ladder.add(
                "tcp",
                "failed",
                latency_ms=(time.perf_counter() - started) * 1000,
                error=f"cannot connect to {ips[0]}:{port}: {exc!r}",
            )
            ladder.skip_rest("connect", "query")
            return report(target)
        ladder.add(
            "tcp",
            "passed",
            latency_ms=(time.perf_counter() - started) * 1000,
            detail=f"{ips[0]}:{port} reachable",
        )
    else:
        for name in ("dns", "ssrf_policy", "tcp"):
            ladder.add(name, "skipped", detail=f"{conn.driver} is not a network driver")

    # ---- connect (TLS + auth) + query -------------------------------------
    engine = open_engine(conn, password)
    sample: str | None = None
    connected = False
    try:
        started = time.perf_counter()
        try:
            async with engine.connect() as connection:
                connected = True
                ladder.add(
                    "connect",
                    "passed",
                    latency_ms=(time.perf_counter() - started) * 1000,
                    detail="connected" + (f" as {conn.user}" if conn.user else ""),
                )
                q_started = time.perf_counter()
                try:
                    rows = (await connection.execute(text("SELECT 1 AS ok"))).mappings().all()
                except Exception as exc:  # noqa: BLE001 - reported, not raised
                    category, message = categorize_db_exception(exc)
                    ladder.add(
                        "query",
                        "failed",
                        latency_ms=(time.perf_counter() - q_started) * 1000,
                        error=redactor.redact(f"{category.value}: {message}"),
                    )
                else:
                    ladder.add(
                        "query",
                        "passed",
                        latency_ms=(time.perf_counter() - q_started) * 1000,
                        detail="trivial query (SELECT 1) succeeded",
                    )
                    sample = (
                        redactor.redact(
                            json.dumps([dict(r) for r in rows], default=str)[:sample_bytes]
                        )
                        or None
                    )
        except Exception as exc:  # noqa: BLE001 - reported, not raised
            if not connected:
                category, message = categorize_db_exception(exc)
                ladder.add(
                    "connect",
                    "failed",
                    latency_ms=(time.perf_counter() - started) * 1000,
                    error=redactor.redact(f"{category.value}: {message}"),
                )
                ladder.skip_rest("query")
            else:
                raise
    finally:
        await engine.dispose()
    return report(target, sample)
