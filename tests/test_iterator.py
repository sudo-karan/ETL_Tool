"""iterator node: fan-out over values, fan-in of results, error handling."""
from __future__ import annotations

from conftest import run_pipeline
from etl_core.engine import NodeStatus, RunStatus


def iter_pipeline(iterator_config, expression="value * 10"):
    """iterator -> transform(computed over the per-iteration record)."""
    return {
        "pipeline_id": "p",
        "nodes": [
            {"id": "it", "type": "iterator", "config": iterator_config},
            {
                "id": "t",
                "type": "transform",
                "config": {"ops": [{"op": "computed", "target": "result", "expression": expression}]},
            },
        ],
        "edges": [{"from": "it", "to": "t"}],
    }


async def test_array_mode_concat_fan_in_is_ordered():
    result = await run_pipeline(iter_pipeline({"mode": "array", "array": [1, 2, 3]}))
    assert result.status == RunStatus.SUCCEEDED
    assert [r["result"] for r in result.outputs["t"]] == [10, 20, 30]
    assert result.node_results["it"].iterations == 3


async def test_range_mode():
    result = await run_pipeline(iter_pipeline({"mode": "range", "range": {"start": 0, "end": 10, "step": 5}}))
    assert [r["result"] for r in result.outputs["t"]] == [0, 50]


async def test_range_mode_negative_step():
    result = await run_pipeline(iter_pipeline({"mode": "range", "range": {"start": 3, "end": 0, "step": -1}}))
    assert [r["value"] for r in result.outputs["t"]] == [3, 2, 1]


async def test_from_upstream_extracts_field_values():
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {
                "id": "src",
                "type": "static_source",
                "config": {"records": [{"user": {"id": 7}}, {"user": {"id": 9}}]},
            },
            {"id": "it", "type": "iterator", "config": {"mode": "from_upstream", "field": "user.id"}},
            {
                "id": "t",
                "type": "transform",
                "config": {"ops": [{"op": "computed", "target": "result", "expression": "value + 1"}]},
            },
        ],
        "edges": [{"from": "src", "to": "it"}, {"from": "it", "to": "t"}],
    }
    result = await run_pipeline(spec)
    assert [r["result"] for r in result.outputs["t"]] == [8, 10]


async def test_from_upstream_whole_records_when_no_field():
    spec = iter_pipeline({"mode": "from_upstream"}, expression="value['n'] * 2")
    spec["nodes"].insert(
        0, {"id": "src", "type": "static_source", "config": {"records": [{"n": 1}, {"n": 2}]}}
    )
    spec["edges"].append({"from": "src", "to": "it"})
    result = await run_pipeline(spec)
    assert [r["result"] for r in result.outputs["t"]] == [2, 4]


async def test_keyed_fan_in_groups_records_per_iteration():
    config = {"mode": "array", "array": ["a", "b"], "fan_in": "keyed"}
    result = await run_pipeline(iter_pipeline(config, expression="value + '!'"))
    out = result.outputs["t"]
    assert [group["key"] for group in out] == ["a", "b"]
    assert out[0]["records"][0]["result"] == "a!"
    assert out[1]["records"][0]["result"] == "b!"


async def test_iter_references_in_scope_config():
    """$iter.value / $iter.index are visible to any node inside the scope."""
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "it", "type": "iterator", "config": {"mode": "array", "array": ["x", "y"]}},
            {
                "id": "src",
                "type": "static_source",
                "config": {"records": [{"tag": "$iter.value", "pos": "$iter.index"}]},
            },
        ],
        "edges": [{"from": "it", "to": "src"}],
    }
    result = await run_pipeline(spec)
    assert result.outputs["src"] == [{"tag": "x", "pos": 0}, {"tag": "y", "pos": 1}]


async def test_scope_node_with_constant_input_from_outside():
    """A merge inside the scope can join per-iteration data with a constant."""
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {
                "id": "lookup",
                "type": "static_source",
                "config": {"records": [{"value": 1, "name": "one"}, {"value": 2, "name": "two"}]},
            },
            {"id": "it", "type": "iterator", "config": {"mode": "array", "array": [1, 2]}},
            {"id": "m", "type": "merge", "config": {"strategy": "join", "keys": ["value"], "how": "left"}},
        ],
        "edges": [{"from": "it", "to": "m"}, {"from": "lookup", "to": "m"}],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    names = [record.get("name") for record in result.outputs["m"]]
    assert names == ["one", "two"]


async def test_empty_value_set_yields_empty_outputs():
    result = await run_pipeline(iter_pipeline({"mode": "array", "array": []}))
    assert result.status == RunStatus.SUCCEEDED
    assert result.outputs["t"] == []
    assert result.node_results["it"].iterations == 0


async def test_iteration_failure_fails_fast_with_iteration_details():
    config = {"mode": "array", "array": [1, 2, 3]}
    result = await run_pipeline(iter_pipeline(config, expression="1 / (value - 2)"))
    assert result.status == RunStatus.FAILED
    assert len(result.errors) >= 1
    error = result.errors[0]
    assert error.category.value == "transform"
    assert error.details["iteration_index"] == 1
    assert result.node_results["t"].status == NodeStatus.FAILED


async def test_iteration_failure_continue_on_error_keeps_other_iterations():
    config = {"mode": "array", "array": [1, 2, 3]}
    result = await run_pipeline(
        iter_pipeline(config, expression="10 // (value - 2)"), continue_on_error=True
    )
    assert result.status == RunStatus.FAILED  # errors occurred...
    assert [r["result"] for r in result.outputs["t"]] == [-10, 10]  # ...but 2 of 3 survived
    assert len(result.errors) == 1
    assert result.node_results["t"].status == NodeStatus.FAILED
    assert result.node_results["t"].records_out == 2


async def test_iterations_run_concurrently_but_respect_cap(probe_node_class):
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {
                "id": "it",
                "type": "iterator",
                "config": {"mode": "range", "range": {"start": 0, "end": 12}, "max_concurrency": 3},
            },
            {"id": "probe", "type": "probe", "config": {}},
        ],
        "edges": [{"from": "it", "to": "probe"}],
    }
    result = await run_pipeline(spec, max_concurrency=8)
    assert result.status == RunStatus.SUCCEEDED
    assert result.node_results["probe"].records_out == 12
    assert 1 < probe_node_class.max_active <= 3  # parallel, but capped


async def test_iterator_value_error_marks_scope_skipped():
    result = await run_pipeline(iter_pipeline({"mode": "array", "array": "not-a-list"}))
    assert result.status == RunStatus.FAILED
    assert result.errors[0].category.value == "config"
    assert result.node_results["t"].status == NodeStatus.SKIPPED
