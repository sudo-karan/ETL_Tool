"""Engine behavior: chaining, ordering, failure modes, run isolation."""
from __future__ import annotations

import asyncio

import httpx
import respx

from conftest import no_ssrf_options, run_pipeline
from etl_core import PipelineSpec, execute_pipeline
from etl_core.engine import NodeStatus, RunStatus

BASE = "https://api.test"


@respx.mock
async def test_api_to_api_chaining_via_upstream_reference():
    respx.get(f"{BASE}/token").mock(return_value=httpx.Response(200, json={"token": "abc-token-1"}))
    detail_route = respx.get(f"{BASE}/detail").mock(
        return_value=httpx.Response(200, json=[{"ok": True}])
    )
    spec = {
        "pipeline_id": "chain",
        "nodes": [
            {"id": "login", "type": "api_source", "config": {"url": f"{BASE}/token"}},
            {
                "id": "detail",
                "type": "api_source",
                "config": {"url": f"{BASE}/detail", "query_params": {"t": "$upstream.login.token"}},
            },
        ],
        "edges": [{"from": "login", "to": "detail"}],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    assert detail_route.calls[0].request.url.params["t"] == "abc-token-1"


@respx.mock
async def test_full_pipeline_iterator_join_transform():
    """Mirror of examples/pipeline.json against a mocked API."""
    respx.get(f"{BASE}/users").mock(
        return_value=httpx.Response(
            200, json=[{"id": 1, "name": "Ann", "junk": 0}, {"id": 2, "name": "Bob", "junk": 0}]
        )
    )

    def posts(request):
        user_id = int(request.url.params["userId"])
        return httpx.Response(
            200,
            json=[
                {"userId": user_id, "id": user_id * 10, "title": f"post-{user_id}-a"},
                {"userId": user_id, "id": user_id * 10 + 1, "title": f"post-{user_id}-b"},
            ],
        )

    respx.get(f"{BASE}/posts").mock(side_effect=posts)

    spec = {
        "pipeline_id": "report",
        "nodes": [
            {"id": "users", "type": "api_source", "config": {"url": f"{BASE}/users"}},
            {
                "id": "users_slim",
                "type": "transform",
                "config": {
                    "ops": [
                        {"op": "select", "fields": ["id", "name"]},
                        {"op": "rename", "mapping": {"id": "userId"}},
                    ]
                },
            },
            {"id": "ids", "type": "iterator", "config": {"mode": "from_upstream", "field": "id"}},
            {
                "id": "posts",
                "type": "api_source",
                "config": {"url": f"{BASE}/posts", "query_params": {"userId": "$iter.value"}},
            },
            {
                "id": "joined",
                "type": "merge",
                "config": {"strategy": "join", "keys": ["userId"], "how": "left"},
            },
            {
                "id": "report",
                "type": "transform",
                "config": {"ops": [{"op": "select", "fields": ["userId", "name", "title"]}]},
            },
        ],
        "edges": [
            {"from": "users", "to": "users_slim"},
            {"from": "users", "to": "ids"},
            {"from": "ids", "to": "posts"},
            {"from": "posts", "to": "joined"},
            {"from": "users_slim", "to": "joined"},
            {"from": "joined", "to": "report"},
        ],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    report = result.outputs["report"]
    assert report == [
        {"userId": 1, "name": "Ann", "title": "post-1-a"},
        {"userId": 1, "name": "Ann", "title": "post-1-b"},
        {"userId": 2, "name": "Bob", "title": "post-2-a"},
        {"userId": 2, "name": "Bob", "title": "post-2-b"},
    ]
    # every node reported a structured result
    assert {r.status for r in result.node_results.values()} == {NodeStatus.SUCCEEDED}


async def test_fail_fast_skips_downstream_and_reports_error():
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": [{"a": 0}]}},
            {
                "id": "bad",
                "type": "transform",
                "config": {"ops": [{"op": "computed", "target": "x", "expression": "1 / a"}]},
            },
            {
                "id": "after",
                "type": "transform",
                "config": {"ops": [{"op": "select", "fields": ["x"]}]},
            },
        ],
        "edges": [{"from": "src", "to": "bad"}, {"from": "bad", "to": "after"}],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.FAILED
    assert result.node_results["src"].status == NodeStatus.SUCCEEDED
    assert result.node_results["bad"].status == NodeStatus.FAILED
    assert result.node_results["after"].status == NodeStatus.SKIPPED
    assert result.errors[0].node_id == "bad"
    assert result.errors[0].category.value == "transform"
    assert "after" not in result.outputs


async def test_continue_on_error_runs_independent_branches():
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": [{"a": 0}]}},
            {
                "id": "bad",
                "type": "transform",
                "config": {"ops": [{"op": "computed", "target": "x", "expression": "1 / a"}]},
            },
            {
                "id": "bad_child",
                "type": "transform",
                "config": {"ops": [{"op": "select", "fields": ["x"]}]},
            },
            {
                "id": "good",
                "type": "transform",
                "config": {"ops": [{"op": "computed", "target": "y", "expression": "a + 1"}]},
            },
        ],
        "edges": [
            {"from": "src", "to": "bad"},
            {"from": "bad", "to": "bad_child"},
            {"from": "src", "to": "good"},
        ],
    }
    result = await run_pipeline(spec, continue_on_error=True)
    assert result.status == RunStatus.FAILED
    assert result.node_results["bad"].status == NodeStatus.FAILED
    assert result.node_results["bad_child"].status == NodeStatus.SKIPPED
    assert result.node_results["good"].status == NodeStatus.SUCCEEDED
    assert result.outputs["good"] == [{"a": 0, "y": 1}]


async def test_invalid_pipeline_returns_structured_validation_errors():
    spec = PipelineSpec.model_validate(
        {
            "pipeline_id": "broken",
            "nodes": [{"id": "t", "type": "transform", "config": {"ops": []}}],
            "edges": [],
        }
    )
    result = await execute_pipeline(spec, options=no_ssrf_options())
    assert result.status == RunStatus.FAILED
    assert result.errors
    assert all(e.category.value == "validation" for e in result.errors)


async def test_terminal_outputs_only():
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": [{"a": 1}]}},
            {
                "id": "t",
                "type": "transform",
                "config": {"ops": [{"op": "select", "fields": ["a"]}]},
            },
        ],
        "edges": [{"from": "src", "to": "t"}],
    }
    result = await run_pipeline(spec)
    assert set(result.outputs) == {"t"}  # src is not terminal


async def test_concurrent_runs_are_isolated():
    def spec_for(tag: str, count: int):
        return {
            "pipeline_id": f"pipe-{tag}",
            "nodes": [
                {
                    "id": "src",
                    "type": "static_source",
                    "config": {"records": [{"tag": tag, "n": i} for i in range(count)]},
                },
                {
                    "id": "t",
                    "type": "transform",
                    "config": {"ops": [{"op": "computed", "target": "who", "expression": f"tag + '-{tag}'"}]},
                },
            ],
            "edges": [{"from": "src", "to": "t"}],
        }

    results = await asyncio.gather(
        *(run_pipeline(spec_for(tag, count)) for tag, count in [("a", 3), ("b", 5), ("c", 2)] * 4)
    )
    for result in results:
        tag = result.pipeline_id.split("-")[1]
        expected = {"a": 3, "b": 5, "c": 2}[tag]
        assert result.status == RunStatus.SUCCEEDED
        assert len(result.outputs["t"]) == expected
        assert all(record["who"] == f"{tag}-{tag}" for record in result.outputs["t"])
        # logs never reference nodes from another run's pipeline
        assert {event.node_id for event in result.logs if event.node_id} <= {"src", "t"}


async def test_run_log_streams_via_on_event():
    spec = PipelineSpec.model_validate(
        {
            "pipeline_id": "p",
            "nodes": [{"id": "src", "type": "static_source", "config": {"records": [{}]}}],
            "edges": [],
        }
    )
    seen = []
    result = await execute_pipeline(spec, options=no_ssrf_options(), on_event=seen.append)
    assert [e.message for e in seen] == [e.message for e in result.logs]
    assert len(seen) >= 2  # start + finish at minimum
