"""transform node: select / rename / filter / computed."""
from __future__ import annotations

import pytest

from conftest import run_pipeline
from etl_core.engine import NodeStatus, RunStatus
from etl_core.nodes.transform import ExpressionError, compile_expression


def pipeline_with_ops(records, ops):
    return {
        "pipeline_id": "p",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": records}},
            {"id": "t", "type": "transform", "config": {"ops": ops}},
        ],
        "edges": [{"from": "src", "to": "t"}],
    }


async def apply_ops(records, ops):
    result = await run_pipeline(pipeline_with_ops(records, ops))
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    return result.outputs["t"]


async def test_select_keeps_listed_fields_and_supports_dotted_paths():
    records = [{"id": 1, "name": "Ann", "meta": {"role": "admin"}}]
    out = await apply_ops(records, [{"op": "select", "fields": ["id", "meta.role"]}])
    assert out == [{"id": 1, "meta.role": "admin"}]


async def test_select_omits_missing_fields():
    out = await apply_ops([{"id": 1}, {"name": "x"}], [{"op": "select", "fields": ["id"]}])
    assert out == [{"id": 1}, {}]


async def test_rename():
    out = await apply_ops(
        [{"id": 1, "name": "Ann"}], [{"op": "rename", "mapping": {"id": "userId"}}]
    )
    assert out == [{"userId": 1, "name": "Ann"}]


async def test_filter_simple_condition():
    records = [{"n": 1}, {"n": 5}, {"n": 10}]
    out = await apply_ops(records, [{"op": "filter", "predicate": {"field": "n", "op": "gte", "value": 5}}])
    assert out == [{"n": 5}, {"n": 10}]


async def test_filter_boolean_groups_and_nested_fields():
    records = [
        {"user": {"role": "admin"}, "active": True},
        {"user": {"role": "admin"}, "active": False},
        {"user": {"role": "guest"}, "active": True},
    ]
    predicate = {
        "all": [
            {"field": "user.role", "op": "eq", "value": "admin"},
            {"not": {"field": "active", "op": "eq", "value": False}},
        ]
    }
    out = await apply_ops(records, [{"op": "filter", "predicate": predicate}])
    assert out == [{"user": {"role": "admin"}, "active": True}]


async def test_filter_any_in_contains_regex_exists():
    records = [{"tag": "alpha", "n": 1}, {"tag": "beta"}, {"other": True}]
    predicate = {
        "any": [
            {"field": "tag", "op": "in", "value": ["beta", "gamma"]},
            {"field": "tag", "op": "regex", "value": "^al"},
            {"field": "n", "op": "not_exists"},
        ]
    }
    out = await apply_ops(records, [{"op": "filter", "predicate": predicate}])
    assert out == records  # alpha matches regex; beta matches in; third has no n


async def test_filter_missing_field_fails_condition():
    records = [{"n": 1}, {}]
    out = await apply_ops(records, [{"op": "filter", "predicate": {"field": "n", "op": "lt", "value": 5}}])
    assert out == [{"n": 1}]


async def test_filter_incomparable_types_fail_condition_not_run():
    records = [{"n": "a string"}, {"n": 10}]
    out = await apply_ops(records, [{"op": "filter", "predicate": {"field": "n", "op": "gt", "value": 5}}])
    assert out == [{"n": 10}]


async def test_computed_arithmetic_and_functions():
    out = await apply_ops(
        [{"a": 3, "b": 4}],
        [
            {"op": "computed", "target": "sum", "expression": "a + b"},
            {"op": "computed", "target": "label", "expression": "upper(str(a)) + '!'"},
            {"op": "computed", "target": "big", "expression": "'yes' if a + b > 5 else 'no'"},
        ],
    )
    assert out == [{"a": 3, "b": 4, "sum": 7, "label": "3!", "big": "yes"}]


async def test_computed_missing_field_reads_none():
    out = await apply_ops([{"a": 1}], [{"op": "computed", "target": "x", "expression": "missing"}])
    assert out == [{"a": 1, "x": None}]


async def test_computed_runtime_error_is_structured_transform_error():
    result = await run_pipeline(
        pipeline_with_ops([{"a": 0}], [{"op": "computed", "target": "x", "expression": "1 / a"}])
    )
    assert result.status == RunStatus.FAILED
    assert result.errors[0].category.value == "transform"
    assert result.node_results["t"].status == NodeStatus.FAILED


async def test_ops_apply_in_order():
    out = await apply_ops(
        [{"id": 1, "junk": True}],
        [
            {"op": "computed", "target": "double", "expression": "id * 2"},
            {"op": "select", "fields": ["double"]},
        ],
    )
    assert out == [{"double": 2}]


async def test_input_records_are_not_mutated():
    records = [{"id": 1}]
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": records}},
            {
                "id": "t",
                "type": "transform",
                "config": {"ops": [{"op": "computed", "target": "x", "expression": "id + 1"}]},
            },
        ],
        "edges": [{"from": "src", "to": "t"}],
    }
    result = await run_pipeline(spec)
    assert result.outputs["t"] == [{"id": 1, "x": 2}]
    assert records == [{"id": 1}]  # source data untouched


# -- expression safety -------------------------------------------------------
def test_expressions_reject_dangerous_constructs():
    for expression in (
        "__import__('os').system('id')",
        "().__class__.__bases__",
        "open('/etc/passwd')",
        "getattr(a, 'b')",
        "[x for x in y]",
        "lambda: 1",
        "a.b(1)",
    ):
        with pytest.raises(ExpressionError):
            compile_expression(expression)({"a": {"b": 1}, "y": [1]})


def test_expression_attribute_access_is_dict_navigation_only():
    evaluator = compile_expression("user.name")
    assert evaluator({"user": {"name": "Ann"}}) == "Ann"
    with pytest.raises(ExpressionError):
        evaluator({"user": "not-a-dict"})


def test_expression_subscript():
    evaluator = compile_expression("items[0] + items[1]")
    assert evaluator({"items": [1, 2]}) == 3
