"""Pipeline schema parsing and static graph validation."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from etl_core import PipelineSpec, validate_pipeline


def make(nodes, edges=()):
    return PipelineSpec.model_validate(
        {"pipeline_id": "p", "nodes": nodes, "edges": list(edges)}
    )


def messages(spec):
    return [issue.message for issue in validate_pipeline(spec)]


def static(node_id, records=()):
    return {"id": node_id, "type": "static_source", "config": {"records": list(records)}}


def transform(node_id, ops=None):
    ops = ops or [{"op": "select", "fields": ["id"]}]
    return {"id": node_id, "type": "transform", "config": {"ops": ops}}


def test_schema_round_trip_uses_from_to_aliases():
    spec = make(
        [static("a"), transform("b")],
        [{"from": "a", "to": "b", "from_port": "out", "to_port": "in"}],
    )
    dumped = spec.model_dump(by_alias=True)
    assert dumped["edges"][0]["from"] == "a"
    assert dumped["edges"][0]["to"] == "b"
    reparsed = PipelineSpec.model_validate_json(spec.to_json())
    assert reparsed.edges[0].from_node == "a"


def test_node_id_pattern_rejects_dots():
    with pytest.raises(ValidationError):
        make([{"id": "a.b", "type": "static_source", "config": {}}])


def test_example_pipeline_file_is_valid():
    spec = PipelineSpec.from_file("examples/pipeline.json")
    assert validate_pipeline(spec) == []


def test_valid_minimal_pipeline():
    spec = make([static("a"), transform("b")], [{"from": "a", "to": "b"}])
    assert validate_pipeline(spec) == []


def test_duplicate_node_ids():
    spec = make([static("a"), static("a")])
    assert any("duplicate node id" in m for m in messages(spec))


def test_unknown_node_type():
    spec = make([{"id": "a", "type": "nope", "config": {}}])
    assert any("unknown node type" in m for m in messages(spec))


def test_invalid_node_config_reported():
    spec = make([{"id": "a", "type": "api_source", "config": {"method": "GET"}}])
    assert any("invalid config" in m and "url" in m for m in messages(spec))


def test_config_with_references_defers_static_validation():
    # query param value is a reference; static validation must not choke on it
    spec = make(
        [
            static("a", [{"id": 1}]),
            {
                "id": "b",
                "type": "api_source",
                "config": {"url": "https://x.test", "query_params": {"v": "$upstream.a.id"}},
            },
        ],
        [{"from": "a", "to": "b"}],
    )
    assert validate_pipeline(spec) == []


def test_edge_to_unknown_node():
    spec = make([static("a")], [{"from": "a", "to": "ghost"}])
    assert any("unknown node 'ghost'" in m for m in messages(spec))


def test_edge_to_unknown_port():
    spec = make(
        [static("a"), transform("b")],
        [{"from": "a", "to": "b", "to_port": "bogus"}],
    )
    assert any("no input port 'bogus'" in m for m in messages(spec))


def test_multiple_edges_into_single_input_port():
    spec = make(
        [static("a"), static("b"), transform("c")],
        [{"from": "a", "to": "c"}, {"from": "b", "to": "c"}],
    )
    assert any("accepts a single edge" in m for m in messages(spec))


def test_transform_requires_input():
    spec = make([transform("t")])
    assert any("required input port" in m for m in messages(spec))


def test_merge_requires_two_inputs():
    spec = make(
        [static("a"), {"id": "m", "type": "merge", "config": {}}],
        [{"from": "a", "to": "m"}],
    )
    assert any("at least 2 inputs" in m for m in messages(spec))


def test_join_requires_exactly_two_inputs():
    spec = make(
        [
            static("a"),
            static("b"),
            static("c"),
            {"id": "m", "type": "merge", "config": {"strategy": "join", "keys": ["id"]}},
        ],
        [{"from": "a", "to": "m"}, {"from": "b", "to": "m"}, {"from": "c", "to": "m"}],
    )
    assert any("exactly 2 inputs" in m for m in messages(spec))


def test_from_upstream_iterator_requires_edge():
    spec = make([{"id": "it", "type": "iterator", "config": {"mode": "from_upstream"}}])
    assert any("required input port" in m for m in messages(spec))


def test_cycle_detected():
    spec = make(
        [transform("a"), transform("b")],
        [{"from": "a", "to": "b"}, {"from": "b", "to": "a"}],
    )
    assert any("cycle" in m for m in messages(spec))


def test_upstream_reference_must_be_ancestor():
    spec = make(
        [
            static("a", [{"id": 1}]),
            static("b", [{"id": 2}]),
            {
                "id": "c",
                "type": "api_source",
                "config": {"url": "https://x.test/${upstream.b.id}"},
            },
        ],
        [{"from": "a", "to": "c"}],
    )
    assert any("not an ancestor" in m for m in messages(spec))


def test_upstream_reference_to_unknown_node():
    spec = make(
        [{"id": "c", "type": "api_source", "config": {"url": "https://x.test/${upstream.nope.id}"}}]
    )
    assert any("$upstream reference to unknown node" in m for m in messages(spec))


def test_iter_reference_outside_iterator_scope():
    spec = make(
        [{"id": "c", "type": "api_source", "config": {"url": "https://x.test/${iter.value}"}}]
    )
    assert any("outside of any iterator" in m for m in messages(spec))


def test_nested_iterators_rejected():
    spec = make(
        [
            {"id": "i1", "type": "iterator", "config": {"mode": "array", "array": [1]}},
            {"id": "i2", "type": "iterator", "config": {"mode": "array", "array": [2]}},
        ],
        [{"from": "i1", "to": "i2"}],
    )
    assert any("nested iterators" in m for m in messages(spec))


def test_overlapping_iterator_scopes_rejected():
    spec = make(
        [
            {"id": "i1", "type": "iterator", "config": {"mode": "array", "array": [1]}},
            {"id": "i2", "type": "iterator", "config": {"mode": "array", "array": [2]}},
            {"id": "m", "type": "merge", "config": {}},
        ],
        [{"from": "i1", "to": "m"}, {"from": "i2", "to": "m"}],
    )
    assert any("downstream of two iterators" in m for m in messages(spec))
