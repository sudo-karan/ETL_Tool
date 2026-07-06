"""api_source node: auth, pagination, retry, rate limit, errors (respx-mocked)."""
from __future__ import annotations

import base64
import json
import time

import httpx
import pytest
import respx

from conftest import run_pipeline
from etl_core.engine import RunStatus
from etl_core.http_client import RateLimiter

BASE = "https://api.test"


def api_pipeline(config, secrets=None):
    return {
        "pipeline_id": "p",
        "nodes": [{"id": "api", "type": "api_source", "config": config}],
        "edges": [],
    }


async def run_api(config, secrets=None, **options):
    return await run_pipeline(api_pipeline(config), secrets, **options)


@respx.mock
async def test_plain_get_returns_records():
    respx.get(f"{BASE}/users").mock(
        return_value=httpx.Response(200, json=[{"id": 1}, {"id": 2}])
    )
    result = await run_api({"url": f"{BASE}/users"})
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    assert result.outputs["api"] == [{"id": 1}, {"id": 2}]


@respx.mock
async def test_items_path_and_scalar_items():
    respx.get(f"{BASE}/wrapped").mock(
        return_value=httpx.Response(200, json={"data": {"items": [1, 2]}})
    )
    result = await run_api({"url": f"{BASE}/wrapped", "items_path": "data.items"})
    assert result.outputs["api"] == [{"value": 1}, {"value": 2}]


@respx.mock
async def test_single_object_response_becomes_one_record():
    respx.get(f"{BASE}/me").mock(return_value=httpx.Response(200, json={"id": 42}))
    result = await run_api({"url": f"{BASE}/me"})
    assert result.outputs["api"] == [{"id": 42}]


@respx.mock
async def test_path_params_are_url_quoted():
    route = respx.get(f"{BASE}/users/a%20b/posts").mock(
        return_value=httpx.Response(200, json=[])
    )
    result = await run_api(
        {"url": BASE + "/users/{uid}/posts", "path_params": {"uid": "a b"}}
    )
    assert result.status == RunStatus.SUCCEEDED
    assert route.called


@respx.mock
async def test_bearer_auth_header():
    route = respx.get(f"{BASE}/private").mock(return_value=httpx.Response(200, json=[]))
    result = await run_api(
        {"url": f"{BASE}/private", "auth": {"type": "bearer", "secret_ref": "TOKEN"}},
        secrets={"TOKEN": "tok-abc-123"},
    )
    assert result.status == RunStatus.SUCCEEDED
    assert route.calls[0].request.headers["Authorization"] == "Bearer tok-abc-123"


@respx.mock
async def test_api_key_in_header_and_query():
    header_route = respx.get(f"{BASE}/h").mock(return_value=httpx.Response(200, json=[]))
    query_route = respx.get(f"{BASE}/q").mock(return_value=httpx.Response(200, json=[]))
    await run_api(
        {"url": f"{BASE}/h", "auth": {"type": "api_key", "secret_ref": "K", "name": "X-Key"}},
        secrets={"K": "key-value-1"},
    )
    assert header_route.calls[0].request.headers["X-Key"] == "key-value-1"
    await run_api(
        {
            "url": f"{BASE}/q",
            "auth": {"type": "api_key", "secret_ref": "K", "name": "api_key", "in": "query"},
        },
        secrets={"K": "key-value-1"},
    )
    assert query_route.calls[0].request.url.params["api_key"] == "key-value-1"


@respx.mock
async def test_basic_auth():
    route = respx.get(f"{BASE}/b").mock(return_value=httpx.Response(200, json=[]))
    await run_api(
        {
            "url": f"{BASE}/b",
            "auth": {"type": "basic", "secret_ref": "PW", "username": "alice"},
        },
        secrets={"PW": "s3cret-pw"},
    )
    expected = "Basic " + base64.b64encode(b"alice:s3cret-pw").decode()
    assert route.calls[0].request.headers["Authorization"] == expected


@respx.mock
async def test_missing_secret_is_config_error():
    respx.get(f"{BASE}/x").mock(return_value=httpx.Response(200, json=[]))
    result = await run_api(
        {"url": f"{BASE}/x", "auth": {"type": "bearer", "secret_ref": "NOPE"}}, secrets={}
    )
    assert result.status == RunStatus.FAILED
    assert result.errors[0].category.value == "config"
    assert "NOPE" in result.errors[0].message


@respx.mock
async def test_cursor_pagination():
    def responder(request):
        cursor = request.url.params.get("cursor")
        if cursor is None:
            return httpx.Response(200, json={"items": [{"id": 1}], "next": "c2"})
        assert cursor == "c2"
        return httpx.Response(200, json={"items": [{"id": 2}], "next": None})

    route = respx.get(f"{BASE}/pages").mock(side_effect=responder)
    result = await run_api(
        {
            "url": f"{BASE}/pages",
            "pagination": {
                "type": "cursor",
                "items_path": "items",
                "cursor_path": "next",
                "cursor_param": "cursor",
            },
        }
    )
    assert result.outputs["api"] == [{"id": 1}, {"id": 2}]
    assert route.call_count == 2


@respx.mock
async def test_offset_pagination_stops_on_short_page():
    def responder(request):
        offset = int(request.url.params["offset"])
        assert int(request.url.params["limit"]) == 2
        data = {0: [{"id": 1}, {"id": 2}], 2: [{"id": 3}]}.get(offset, [])
        return httpx.Response(200, json=data)

    route = respx.get(f"{BASE}/offset").mock(side_effect=responder)
    result = await run_api(
        {
            "url": f"{BASE}/offset",
            "pagination": {"type": "offset", "limit_param": "limit", "limit": 2},
        }
    )
    assert result.outputs["api"] == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert route.call_count == 2  # short page ends pagination


@respx.mock
async def test_page_pagination_stops_on_empty_page():
    def responder(request):
        page = int(request.url.params["page"])
        data = {1: [{"id": 1}], 2: [{"id": 2}]}.get(page, [])
        return httpx.Response(200, json=data)

    route = respx.get(f"{BASE}/paged").mock(side_effect=responder)
    result = await run_api({"url": f"{BASE}/paged", "pagination": {"type": "page"}})
    assert result.outputs["api"] == [{"id": 1}, {"id": 2}]
    assert route.call_count == 3


@respx.mock
async def test_pagination_max_pages_cap():
    respx.get(f"{BASE}/infinite").mock(
        return_value=httpx.Response(200, json={"items": [{"id": 1}], "next": "again"})
    )
    result = await run_api(
        {
            "url": f"{BASE}/infinite",
            "pagination": {
                "type": "cursor",
                "items_path": "items",
                "cursor_path": "next",
                "cursor_param": "cursor",
                "max_pages": 3,
            },
        }
    )
    assert result.status == RunStatus.SUCCEEDED
    assert len(result.outputs["api"]) == 3
    assert any("max_pages" in event.message for event in result.logs)


@respx.mock
async def test_retry_then_success_counts_attempts():
    route = respx.get(f"{BASE}/flaky")
    route.side_effect = [httpx.Response(500), httpx.Response(200, json=[{"ok": True}])]
    result = await run_api(
        {"url": f"{BASE}/flaky", "retry": {"max": 2, "backoff": 0}}
    )
    assert result.status == RunStatus.SUCCEEDED
    assert result.outputs["api"] == [{"ok": True}]
    assert route.call_count == 2


@respx.mock
async def test_retry_exhaustion_reports_attempts_and_category():
    respx.get(f"{BASE}/down").mock(return_value=httpx.Response(503))
    result = await run_api({"url": f"{BASE}/down", "retry": {"max": 2, "backoff": 0}})
    assert result.status == RunStatus.FAILED
    error = result.errors[0]
    assert error.category.value == "http_status"
    assert error.http_status == 503
    assert error.attempts == 3  # 1 try + 2 retries
    assert error.request_summary.startswith("GET ")


@respx.mock
async def test_status_401_is_auth_error_and_not_retried():
    route = respx.get(f"{BASE}/secure").mock(return_value=httpx.Response(401))
    result = await run_api({"url": f"{BASE}/secure", "retry": {"max": 3, "backoff": 0}})
    error = result.errors[0]
    assert error.category.value == "auth"
    assert route.call_count == 1  # 4xx (non-429) is not retryable


@respx.mock
async def test_status_429_is_rate_limit_error():
    respx.get(f"{BASE}/limited").mock(return_value=httpx.Response(429))
    result = await run_api({"url": f"{BASE}/limited"})
    assert result.errors[0].category.value == "rate_limit"


@respx.mock
async def test_timeout_category():
    respx.get(f"{BASE}/slow").mock(side_effect=httpx.ConnectTimeout("boom"))
    result = await run_api({"url": f"{BASE}/slow"})
    assert result.errors[0].category.value == "timeout"


@respx.mock
async def test_network_errors_are_retried():
    route = respx.get(f"{BASE}/wobbly")
    route.side_effect = [httpx.ConnectError("nope"), httpx.Response(200, json=[{"ok": 1}])]
    result = await run_api({"url": f"{BASE}/wobbly", "retry": {"max": 1, "backoff": 0}})
    assert result.status == RunStatus.SUCCEEDED
    assert route.call_count == 2


@respx.mock
async def test_non_json_response_is_validation_error():
    respx.get(f"{BASE}/html").mock(return_value=httpx.Response(200, text="<html>hi</html>"))
    result = await run_api({"url": f"{BASE}/html"})
    assert result.errors[0].category.value == "validation"


@respx.mock
async def test_secret_never_appears_in_errors_or_logs():
    respx.get(f"{BASE}/fail").mock(return_value=httpx.Response(500))
    secret = "super-secret-value-42"
    result = await run_api(
        {
            "url": f"{BASE}/fail",
            "auth": {"type": "api_key", "secret_ref": "K", "name": "api_key", "in": "query"},
        },
        secrets={"K": secret},
    )
    assert result.status == RunStatus.FAILED
    dumped = result.model_dump_json()
    assert secret not in dumped
    assert "api_key=***" in result.errors[0].request_summary


async def test_rate_limiter_spaces_acquisitions():
    limiter = RateLimiter(rps=50)  # 20ms interval
    started = time.monotonic()
    for _ in range(5):
        await limiter.acquire()
    elapsed = time.monotonic() - started
    assert elapsed >= 4 * 0.02 * 0.7  # 4 gaps, generous tolerance


@respx.mock
async def test_rate_limited_node_still_fetches_everything():
    respx.get(f"{BASE}/rl").mock(return_value=httpx.Response(200, json=[{"ok": 1}]))
    result = await run_api({"url": f"{BASE}/rl", "rate_limit": {"rps": 1000}})
    assert result.status == RunStatus.SUCCEEDED
