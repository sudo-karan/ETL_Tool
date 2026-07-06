"""merge node: concat / union / join."""
from __future__ import annotations

from conftest import run_pipeline
from etl_core.engine import RunStatus


def merge_pipeline(left, right, config, third=None):
    nodes = [
        {"id": "left", "type": "static_source", "config": {"records": left}},
        {"id": "right", "type": "static_source", "config": {"records": right}},
        {"id": "m", "type": "merge", "config": config},
    ]
    edges = [{"from": "left", "to": "m"}, {"from": "right", "to": "m"}]
    if third is not None:
        nodes.insert(2, {"id": "third", "type": "static_source", "config": {"records": third}})
        edges.append({"from": "third", "to": "m"})
    return {"pipeline_id": "p", "nodes": nodes, "edges": edges}


async def merged(left, right, config, third=None):
    result = await run_pipeline(merge_pipeline(left, right, config, third))
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    return result.outputs["m"]


async def test_concat_preserves_edge_order():
    out = await merged([{"a": 1}], [{"a": 2}], {"strategy": "concat"}, third=[{"a": 3}])
    assert out == [{"a": 1}, {"a": 2}, {"a": 3}]


async def test_union_drops_exact_duplicates():
    out = await merged(
        [{"a": 1}, {"a": 2}],
        [{"a": 2}, {"a": 3}],
        {"strategy": "union"},
    )
    assert out == [{"a": 1}, {"a": 2}, {"a": 3}]


async def test_join_inner():
    left = [{"id": 1, "x": "l1"}, {"id": 2, "x": "l2"}, {"id": 3, "x": "l3"}]
    right = [{"id": 1, "y": "r1"}, {"id": 2, "y": "r2"}]
    out = await merged(left, right, {"strategy": "join", "keys": ["id"], "how": "inner"})
    assert out == [{"id": 1, "x": "l1", "y": "r1"}, {"id": 2, "x": "l2", "y": "r2"}]


async def test_join_left_keeps_unmatched_left():
    left = [{"id": 1, "x": "l1"}, {"id": 9, "x": "l9"}]
    right = [{"id": 1, "y": "r1"}]
    out = await merged(left, right, {"strategy": "join", "keys": ["id"], "how": "left"})
    assert out == [{"id": 1, "x": "l1", "y": "r1"}, {"id": 9, "x": "l9"}]


async def test_join_outer_keeps_both_sides():
    left = [{"id": 1, "x": "l1"}]
    right = [{"id": 2, "y": "r2"}]
    out = await merged(left, right, {"strategy": "join", "keys": ["id"], "how": "outer"})
    assert out == [{"id": 1, "x": "l1"}, {"id": 2, "y": "r2"}]


async def test_join_multi_match_produces_cartesian_rows():
    left = [{"id": 1, "x": "l"}]
    right = [{"id": 1, "y": "r1"}, {"id": 1, "y": "r2"}]
    out = await merged(left, right, {"strategy": "join", "keys": ["id"]})
    assert out == [{"id": 1, "x": "l", "y": "r1"}, {"id": 1, "x": "l", "y": "r2"}]


async def test_join_collision_gets_suffix():
    left = [{"id": 1, "name": "left-name"}]
    right = [{"id": 1, "name": "right-name"}]
    out = await merged(left, right, {"strategy": "join", "keys": ["id"]})
    assert out == [{"id": 1, "name": "left-name", "name_right": "right-name"}]


async def test_join_missing_key_never_matches():
    left = [{"id": 1, "x": "l1"}, {"x": "no-key"}]
    right = [{"id": 1, "y": "r1"}, {"y": "keyless-right"}]
    inner = await merged(left, right, {"strategy": "join", "keys": ["id"], "how": "inner"})
    assert inner == [{"id": 1, "x": "l1", "y": "r1"}]
    outer = await merged(left, right, {"strategy": "join", "keys": ["id"], "how": "outer"})
    assert {"x": "no-key"} in outer and {"y": "keyless-right"} in outer


async def test_join_on_dotted_key_paths():
    left = [{"user": {"id": 1}, "x": "l"}]
    right = [{"user": {"id": 1}, "y": "r"}]
    out = await merged(left, right, {"strategy": "join", "keys": ["user.id"]})
    assert len(out) == 1 and out[0]["x"] == "l" and out[0]["y"] == "r"
