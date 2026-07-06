"""test_connection: the DNS -> policy -> TCP -> TLS -> HTTP -> auth ladder."""
from __future__ import annotations

from conftest import BEARER_TOKEN, local_policy
from etl_core import SSRFPolicy
from etl_core import test_connection as run_diagnostics  # alias: pytest must not collect it


def by_name(report, name):
    return next(check for check in report.checks if check.name == name)


def source(url, **config):
    return {"type": "api_source", "config": {"url": url, "timeout_s": 5, **config}}


async def test_full_ladder_with_valid_bearer_token(http_server):
    report = await run_diagnostics(
        source(f"{http_server}/private", auth={"type": "bearer", "secret_ref": "TOK"}),
        secrets={"TOK": BEARER_TOKEN},
        ssrf_policy=local_policy(),
    )
    assert report.ok, report.model_dump()
    assert by_name(report, "dns").status == "passed"
    assert "127.0.0.1" in by_name(report, "dns").detail
    assert by_name(report, "ssrf_policy").status == "passed"
    assert by_name(report, "tcp").status == "passed"
    assert by_name(report, "tls").status == "skipped"  # plain http
    http_check = by_name(report, "http")
    assert http_check.status == "passed"
    assert "401" in http_check.detail  # unauthenticated probe was rejected
    auth_check = by_name(report, "auth")
    assert auth_check.status == "passed"
    assert "200" in auth_check.detail
    # latencies reported for the network rungs
    assert all(
        by_name(report, rung).latency_ms is not None for rung in ("dns", "tcp", "http", "auth")
    )
    # sample body present and secret-free
    assert "alice" in report.sample_body
    assert BEARER_TOKEN not in (report.sample_body or "")


async def test_rejected_credential_fails_auth_rung(http_server):
    report = await run_diagnostics(
        source(f"{http_server}/private", auth={"type": "bearer", "secret_ref": "TOK"}),
        secrets={"TOK": "wrong-token-000000"},
        ssrf_policy=local_policy(),
    )
    assert not report.ok
    assert by_name(report, "auth").status == "failed"
    assert "401" in by_name(report, "auth").error


async def test_no_auth_configured_skips_auth_rung_with_hint(http_server):
    report = await run_diagnostics(
        source(f"{http_server}/private"), ssrf_policy=local_policy()
    )
    assert report.ok  # connectivity itself is fine
    auth_check = by_name(report, "auth")
    assert auth_check.status == "skipped"
    assert "401" in auth_check.detail  # hint that the endpoint wants credentials


async def test_default_policy_blocks_loopback(http_server):
    report = await run_diagnostics(source(f"{http_server}/users"))  # default policy
    assert not report.ok
    assert by_name(report, "ssrf_policy").status == "failed"
    assert by_name(report, "tcp").status == "skipped"
    assert by_name(report, "http").status == "skipped"


async def test_dns_failure_reported_and_rest_skipped():
    report = await run_diagnostics(source("http://definitely-not-real.invalid/x"))
    assert not report.ok
    assert by_name(report, "dns").status == "failed"
    assert by_name(report, "tcp").status == "skipped"
    assert by_name(report, "auth").status == "skipped"


async def test_tcp_failure_on_closed_port():
    report = await run_diagnostics(
        source("http://127.0.0.1:1/"), ssrf_policy=local_policy()
    )
    assert not report.ok
    assert by_name(report, "tcp").status == "failed"
    assert by_name(report, "tls").status == "skipped"


async def test_tls_rung_passes_with_verification_disabled(tls_server):
    report = await run_diagnostics(
        source(f"{tls_server}/users", verify_tls=False), ssrf_policy=local_policy()
    )
    assert report.ok, report.model_dump()
    tls_check = by_name(report, "tls")
    assert tls_check.status == "passed"
    assert "TLS" in (tls_check.detail or "")


async def test_tls_rung_fails_on_self_signed_cert_with_verification(tls_server):
    report = await run_diagnostics(
        source(f"{tls_server}/users"), ssrf_policy=local_policy()
    )
    assert not report.ok
    assert by_name(report, "tls").status == "failed"
    assert by_name(report, "http").status == "skipped"


async def test_unsupported_source_type_reports_config_failure():
    report = await run_diagnostics({"type": "db_source", "config": {}})
    assert not report.ok
    assert by_name(report, "config").status == "failed"
    assert "db_source" in by_name(report, "config").error


async def test_config_with_references_is_rejected():
    report = await run_diagnostics(source("http://x.test/${upstream.a.id}"))
    assert not report.ok
    assert "references" in by_name(report, "config").error


async def test_missing_secret_fails_auth_rung(http_server):
    report = await run_diagnostics(
        source(f"{http_server}/private", auth={"type": "bearer", "secret_ref": "TOK"}),
        secrets={},
        ssrf_policy=local_policy(),
    )
    assert not report.ok
    assert by_name(report, "auth").status == "failed"
    assert "TOK" in by_name(report, "auth").error


async def test_ssrf_guard_disabled_is_reported_as_skipped(http_server):
    report = await run_diagnostics(
        source(f"{http_server}/users"), ssrf_policy=SSRFPolicy(enabled=False)
    )
    assert report.ok
    assert by_name(report, "ssrf_policy").status == "skipped"
