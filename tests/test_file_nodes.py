"""file_source / file_sink: format round-trips, options, access policy, errors."""
from __future__ import annotations

import json

import pytest

from conftest import run_pipeline
from etl_core.engine import RunStatus
from etl_core.fileio import FileAccessPolicy

RECORDS = [
    {"id": 1, "name": "Ann", "tags": ["x", "y"], "meta": {"k": 1}},
    {"id": 2, "name": "Bob", "tags": [], "meta": {"k": 2}},
]


def sink_then_source(path, fmt, records=RECORDS, sink_extra=None, source_extra=None):
    return {
        "pipeline_id": "file",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": records}},
            {
                "id": "w",
                "type": "file_sink",
                "config": {"path": str(path), "format": fmt, **(sink_extra or {})},
            },
            {
                "id": "r",
                "type": "file_source",
                "config": {"path": str(path), "format": fmt, **(source_extra or {})},
            },
        ],
        "edges": [{"from": "src", "to": "w"}, {"from": "w", "to": "r"}],
    }


@pytest.mark.parametrize("fmt,ext", [("json", "json"), ("jsonl", "jsonl"), ("csv", "csv"), ("parquet", "parquet")])
async def test_round_trip(tmp_path, fmt, ext):
    path = tmp_path / f"data.{ext}"
    result = await run_pipeline(sink_then_source(path, fmt))
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    back = result.outputs["r"]
    assert len(back) == 2
    assert {r["name"] for r in back} == {"Ann", "Bob"}


async def test_json_preserves_nested_structure(tmp_path):
    path = tmp_path / "data.json"
    result = await run_pipeline(sink_then_source(path, "json"))
    assert result.outputs["r"][0]["tags"] == ["x", "y"]
    assert result.outputs["r"][0]["meta"] == {"k": 1}


async def test_csv_json_encodes_nested_values(tmp_path):
    path = tmp_path / "data.csv"
    result = await run_pipeline(sink_then_source(path, "csv"))
    assert result.outputs["r"][0]["tags"] == '["x", "y"]'  # nested -> JSON string cell
    assert result.outputs["r"][0]["id"] == 1  # scalar type preserved


async def test_format_inferred_from_extension(tmp_path):
    path = tmp_path / "data.jsonl"
    spec = sink_then_source(path, "auto")
    result = await run_pipeline(spec)
    assert result.status == RunStatus.SUCCEEDED
    assert path.read_text().count("\n") == 2  # one JSON object per line


async def test_unknown_extension_needs_explicit_format(tmp_path):
    path = tmp_path / "data.weird"
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": RECORDS}},
            {"id": "w", "type": "file_sink", "config": {"path": str(path)}},
        ],
        "edges": [{"from": "src", "to": "w"}],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.FAILED
    assert result.errors[0].category.value == "config"
    assert "infer format" in result.errors[0].message


async def test_limit_caps_rows(tmp_path):
    path = tmp_path / "data.json"
    path.write_text(json.dumps([{"i": i} for i in range(10)]))
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "r", "type": "file_source", "config": {"path": str(path), "limit": 3}},
        ],
        "edges": [],
    }
    result = await run_pipeline(spec)
    assert len(result.outputs["r"]) == 3


async def test_json_records_path(tmp_path):
    path = tmp_path / "wrapped.json"
    path.write_text(json.dumps({"data": {"items": [{"i": 1}, {"i": 2}]}}))
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "r", "type": "file_source",
             "config": {"path": str(path), "records_path": "data.items"}},
        ],
        "edges": [],
    }
    result = await run_pipeline(spec)
    assert result.outputs["r"] == [{"i": 1}, {"i": 2}]


async def test_csv_custom_delimiter_and_no_infer(tmp_path):
    path = tmp_path / "data.csv"
    result = await run_pipeline(
        sink_then_source(
            path, "csv",
            records=[{"id": 1, "name": "Ann"}],
            sink_extra={"delimiter": ";"},
            source_extra={"delimiter": ";", "infer_schema": False},
        )
    )
    assert result.status == RunStatus.SUCCEEDED
    assert ";" in path.read_text()
    assert result.outputs["r"][0]["id"] == "1"  # infer_schema False -> text


async def test_append_mode_accumulates(tmp_path):
    path = tmp_path / "data.jsonl"
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": [{"i": 1}]}},
            {"id": "w", "type": "file_sink",
             "config": {"path": str(path), "mode": "append"}},
        ],
        "edges": [{"from": "src", "to": "w"}],
    }
    await run_pipeline(spec)
    await run_pipeline(spec)
    assert path.read_text().strip().count("\n") == 1  # two lines total


async def test_error_mode_refuses_overwrite(tmp_path):
    path = tmp_path / "data.json"
    path.write_text("[]")
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": RECORDS}},
            {"id": "w", "type": "file_sink",
             "config": {"path": str(path), "mode": "error"}},
        ],
        "edges": [{"from": "src", "to": "w"}],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.FAILED
    assert "refusing to overwrite" in result.errors[0].message


async def test_missing_file_is_config_error(tmp_path):
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "r", "type": "file_source",
             "config": {"path": str(tmp_path / "nope.json")}},
        ],
        "edges": [],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.FAILED
    assert result.errors[0].category.value == "config"
    assert "not found" in result.errors[0].message


async def test_bad_json_is_validation_error(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{not valid json")
    spec = {
        "pipeline_id": "p",
        "nodes": [{"id": "r", "type": "file_source", "config": {"path": str(path)}}],
        "edges": [],
    }
    result = await run_pipeline(spec)
    assert result.errors[0].category.value == "validation"


async def test_make_parents_creates_directories(tmp_path):
    path = tmp_path / "nested" / "deep" / "out.json"
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": [{"i": 1}]}},
            {"id": "w", "type": "file_sink", "config": {"path": str(path)}},
        ],
        "edges": [{"from": "src", "to": "w"}],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.SUCCEEDED
    assert path.exists()


async def test_empty_records_writes_empty_file_with_warning(tmp_path):
    path = tmp_path / "empty.json"
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": []}},
            {"id": "w", "type": "file_sink", "config": {"path": str(path)}},
        ],
        "edges": [{"from": "src", "to": "w"}],
    }
    result = await run_pipeline(spec)
    assert result.status == RunStatus.SUCCEEDED
    assert json.loads(path.read_text()) == []
    assert any("empty" in e.message for e in result.logs)


# --------------------------------------------------------------------------
# FileAccessPolicy
# --------------------------------------------------------------------------
async def test_policy_blocks_path_outside_allowed_dirs(tmp_path):
    outside = tmp_path.parent / "outside.json"
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "r", "type": "file_source",
             "config": {"path": str(outside), "format": "json"}},
        ],
        "edges": [],
    }
    result = await run_pipeline(spec, file_policy=FileAccessPolicy(allowed_dirs=[str(tmp_path)]))
    assert result.status == RunStatus.FAILED
    assert result.errors[0].category.value == "config"
    assert "outside the allowed directories" in result.errors[0].message


async def test_policy_allows_path_inside_allowed_dir(tmp_path):
    path = tmp_path / "sub" / "ok.json"
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "src", "type": "static_source", "config": {"records": [{"i": 1}]}},
            {"id": "w", "type": "file_sink", "config": {"path": str(path)}},
        ],
        "edges": [{"from": "src", "to": "w"}],
    }
    result = await run_pipeline(spec, file_policy=FileAccessPolicy(allowed_dirs=[str(tmp_path)]))
    assert result.status == RunStatus.SUCCEEDED


async def test_policy_blocks_traversal_escape(tmp_path):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    escape = str(allowed / ".." / "secret.json")  # resolves outside `allowed`
    spec = {
        "pipeline_id": "p",
        "nodes": [
            {"id": "r", "type": "file_source",
             "config": {"path": escape, "format": "json"}},
        ],
        "edges": [],
    }
    result = await run_pipeline(spec, file_policy=FileAccessPolicy(allowed_dirs=[str(allowed)]))
    assert result.status == RunStatus.FAILED
    assert result.errors[0].category.value == "config"
