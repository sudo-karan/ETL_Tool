"""db_source / db_sink against SQLite (same async path as Postgres)."""
from __future__ import annotations

import datetime
import decimal
import uuid

from conftest import run_pipeline
from etl_core import ExecutionOptions, PipelineSpec, SSRFPolicy, execute_pipeline
from etl_core.db import jsonify_value
from etl_core.engine import RunStatus, validate_pipeline

PEOPLE = [{"id": 1, "name": "Ann"}, {"id": 2, "name": "Bob"}]


def conn(tmp_path):
    return {"driver": "sqlite", "database": str(tmp_path / "t.db")}


def write_then_read(tmp_path, records=PEOPLE, sink_extra=None, query="SELECT * FROM people ORDER BY id"):
    c = conn(tmp_path)
    return {
        "pipeline_id": "db",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": records}},
            {"id": "sink", "type": "db_sink",
             "config": {"connection": c, "table": "people", "create": True, **(sink_extra or {})}},
            {"id": "q", "type": "db_source", "config": {"connection": c, "query": query}},
        ],
        "edges": [{"from": "src", "to": "sink"}, {"from": "sink", "to": "q"}],
    }


async def test_sink_create_and_source_round_trip(tmp_path):
    result = await run_pipeline(write_then_read(tmp_path))
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    assert result.outputs["q"] == PEOPLE


async def test_sink_is_pass_through(tmp_path):
    result = await run_pipeline(write_then_read(tmp_path))
    assert result.node_results["sink"].records_out == 2  # emits what it wrote


async def test_replace_mode_clears_existing_rows(tmp_path):
    await run_pipeline(write_then_read(tmp_path))  # seeds Ann, Bob
    result = await run_pipeline(
        write_then_read(tmp_path, records=[{"id": 9, "name": "Cara"}], sink_extra={"mode": "replace"})
    )
    assert result.outputs["q"] == [{"id": 9, "name": "Cara"}]


async def test_append_mode_accumulates(tmp_path):
    await run_pipeline(write_then_read(tmp_path))
    result = await run_pipeline(
        write_then_read(tmp_path, records=[{"id": 3, "name": "Cara"}], sink_extra={"mode": "append"})
    )
    assert len(result.outputs["q"]) == 3


async def test_query_with_bound_params(tmp_path):
    await run_pipeline(write_then_read(tmp_path))
    c = conn(tmp_path)
    spec = {
        "pipeline_id": "q",
        "nodes": [
            {"id": "q", "type": "db_source",
             "config": {"connection": c, "query": "SELECT name FROM people WHERE id = :wanted",
                        "params": {"wanted": 2}}},
        ],
        "edges": [],
    }
    result = await run_pipeline(spec)
    assert result.outputs["q"] == [{"name": "Bob"}]


async def test_source_limit(tmp_path):
    await run_pipeline(write_then_read(tmp_path))
    c = conn(tmp_path)
    spec = {
        "pipeline_id": "q",
        "nodes": [{"id": "q", "type": "db_source",
                   "config": {"connection": c, "query": "SELECT * FROM people", "limit": 1}}],
        "edges": [],
    }
    result = await run_pipeline(spec)
    assert len(result.outputs["q"]) == 1


async def test_nested_value_stored_as_json_string(tmp_path):
    c = conn(tmp_path)
    records = [{"id": 1, "payload": {"nested": [1, 2]}}]
    spec = {
        "pipeline_id": "db",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": records}},
            {"id": "sink", "type": "db_sink",
             "config": {"connection": c, "table": "t", "create": True}},
            {"id": "q", "type": "db_source", "config": {"connection": c, "query": "SELECT * FROM t"}},
        ],
        "edges": [{"from": "src", "to": "sink"}, {"from": "sink", "to": "q"}],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    assert result.outputs["q"][0]["payload"] == '{"nested": [1, 2]}'  # JSON-encoded cell


async def test_missing_table_without_create_is_config_error(tmp_path):
    c = conn(tmp_path)
    spec = {
        "pipeline_id": "db",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": PEOPLE}},
            {"id": "sink", "type": "db_sink",
             "config": {"connection": c, "table": "ghost", "create": False}},
        ],
        "edges": [{"from": "src", "to": "sink"}],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.FAILED
    assert result.errors[0].category.value == "config"
    assert "does not exist" in result.errors[0].message


async def test_query_unknown_table_is_validation_error(tmp_path):
    c = conn(tmp_path)
    # create the file first so the failure is "no such table", not "can't open"
    await run_pipeline(write_then_read(tmp_path))
    spec = {
        "pipeline_id": "q",
        "nodes": [{"id": "q", "type": "db_source",
                   "config": {"connection": c, "query": "SELECT * FROM nonexistent"}}],
        "edges": [],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.FAILED
    assert result.errors[0].category.value == "validation"


async def test_postgres_host_blocked_by_ssrf_guard(tmp_path):
    spec = PipelineSpec.model_validate({
        "pipeline_id": "q",
        "nodes": [{"id": "q", "type": "db_source",
                   "config": {"connection": {"driver": "postgresql", "host": "10.0.0.1",
                                             "database": "app"},
                              "query": "SELECT 1"}}],
        "edges": [],
    })
    # SSRF guard ON (default policy): a private host is refused before connecting.
    result = await execute_pipeline(spec, None, ExecutionOptions(ssrf_policy=SSRFPolicy()))
    assert result.status == RunStatus.FAILED
    assert result.errors[0].category.value == "config"
    assert "blocked range" in result.errors[0].message


def test_unsafe_table_identifier_rejected_at_validation():
    spec = PipelineSpec.model_validate({
        "pipeline_id": "db",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": PEOPLE}},
            {"id": "sink", "type": "db_sink",
             "config": {"connection": {"driver": "sqlite", "database": "x.db"},
                        "table": "people; DROP TABLE users"}},
        ],
        "edges": [{"from": "src", "to": "sink"}],
    })
    issues = validate_pipeline(spec)
    assert any("unsafe SQL identifier" in issue.message for issue in issues)


# --------------------------------------------------------------------------
# Value coercion to JSON-serializable records
# --------------------------------------------------------------------------
def test_jsonify_value_coerces_common_db_types():
    assert jsonify_value(decimal.Decimal("1.50")) == 1.5
    assert jsonify_value(datetime.date(2026, 7, 6)) == "2026-07-06"
    dt = datetime.datetime(2026, 7, 6, 12, 30, tzinfo=datetime.timezone.utc)
    assert jsonify_value(dt).startswith("2026-07-06T12:30")
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    assert jsonify_value(u) == "12345678-1234-5678-1234-567812345678"
    assert jsonify_value(b"\x00\xff") == "AP8="  # base64
    assert jsonify_value({"a": decimal.Decimal("2")}) == {"a": 2.0}
    assert jsonify_value([datetime.date(2026, 1, 1)]) == ["2026-01-01"]
