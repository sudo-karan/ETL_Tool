"""API-to-API chaining via $upstream references (the Phase 2 headline flow)."""
from __future__ import annotations

import json

import httpx
import respx

from conftest import run_pipeline
from etl_core.engine import RunStatus

BASE = "https://api.test"


@respx.mock
async def test_path_param_from_upstream():
    respx.get(f"{BASE}/lookup").mock(
        return_value=httpx.Response(200, json=[{"id": 7, "login": "octocat"}])
    )
    detail = respx.get(f"{BASE}/users/7").mock(
        return_value=httpx.Response(200, json={"id": 7, "name": "The Octocat"})
    )
    spec = {
        "pipeline_id": "chain",
        "nodes": [
            {"id": "lookup", "type": "api_source", "config": {"url": f"{BASE}/lookup"}},
            {"id": "detail", "type": "api_source",
             "config": {"url": BASE + "/users/{uid}",
                        "path_params": {"uid": "$upstream.lookup.id"}}},
        ],
        "edges": [{"from": "lookup", "to": "detail"}],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    assert detail.called
    assert result.outputs["detail"] == [{"id": 7, "name": "The Octocat"}]


@respx.mock
async def test_embedded_interpolation_from_upstream():
    respx.get(f"{BASE}/lookup").mock(
        return_value=httpx.Response(200, json=[{"login": "octocat"}])
    )
    repos = respx.get(f"{BASE}/users/octocat/repos").mock(
        return_value=httpx.Response(200, json=[{"repo": "hello"}])
    )
    spec = {
        "pipeline_id": "chain",
        "nodes": [
            {"id": "lookup", "type": "api_source", "config": {"url": f"{BASE}/lookup"}},
            {"id": "repos", "type": "api_source",
             "config": {"url": BASE + "/users/${upstream.lookup.login}/repos"}},
        ],
        "edges": [{"from": "lookup", "to": "repos"}],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    assert repos.called
    assert result.outputs["repos"] == [{"repo": "hello"}]


@respx.mock
async def test_post_body_from_upstream_preserves_type():
    respx.get(f"{BASE}/lookup").mock(
        return_value=httpx.Response(200, json=[{"id": 7}])
    )
    echo = respx.post(f"{BASE}/echo").mock(return_value=httpx.Response(200, json={"ok": True}))
    spec = {
        "pipeline_id": "chain",
        "nodes": [
            {"id": "lookup", "type": "api_source", "config": {"url": f"{BASE}/lookup"}},
            {"id": "push", "type": "api_source",
             "config": {"method": "POST", "url": f"{BASE}/echo",
                        "body": {"user_id": "$upstream.lookup.id", "tag": "x"}}},
        ],
        "edges": [{"from": "lookup", "to": "push"}],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    sent = json.loads(echo.calls[0].request.content)
    assert sent == {"user_id": 7, "tag": "x"}  # int preserved, not "7"


@respx.mock
async def test_fan_out_over_upstream_ids_then_chain():
    respx.get(f"{BASE}/users").mock(
        return_value=httpx.Response(200, json=[{"id": 1}, {"id": 2}])
    )

    def posts(request):
        uid = request.url.params["userId"]
        return httpx.Response(200, json=[{"userId": int(uid), "title": f"post-{uid}"}])

    respx.get(f"{BASE}/posts").mock(side_effect=posts)
    spec = {
        "pipeline_id": "fanout-chain",
        "nodes": [
            {"id": "users", "type": "api_source", "config": {"url": f"{BASE}/users"}},
            {"id": "each", "type": "iterator",
             "config": {"mode": "from_upstream", "field": "id"}},
            {"id": "posts", "type": "api_source",
             "config": {"url": f"{BASE}/posts", "query_params": {"userId": "$iter.value"}}},
        ],
        "edges": [{"from": "users", "to": "each"}, {"from": "each", "to": "posts"}],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    titles = {r["title"] for r in result.outputs["posts"]}
    assert titles == {"post-1", "post-2"}
