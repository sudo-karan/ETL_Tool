"""CLI: run / test / validate commands (invoked in-process)."""
from __future__ import annotations

import json

import httpx
import pytest
import respx

from conftest import BEARER_TOKEN
from etl_core.cli import main


def write_json(path, payload):
    path.write_text(json.dumps(payload))
    return str(path)


def simple_pipeline(records=None):
    return {
        "pipeline_id": "cli-pipe",
        "nodes": [
            {
                "id": "src",
                "type": "static_source",
                "config": {"records": records or [{"n": 1}, {"n": 2}]},
            },
            {
                "id": "t",
                "type": "transform",
                "config": {"ops": [{"op": "computed", "target": "d", "expression": "n * 2"}]},
            },
        ],
        "edges": [{"from": "src", "to": "t"}],
    }


def test_validate_ok(tmp_path, capsys):
    path = write_json(tmp_path / "p.json", simple_pipeline())
    assert main(["validate", path]) == 0
    assert "is valid" in capsys.readouterr().out


def test_validate_reports_issues(tmp_path, capsys):
    broken = simple_pipeline()
    broken["edges"] = []  # transform loses its required input
    path = write_json(tmp_path / "p.json", broken)
    assert main(["validate", path]) == 1
    assert "required input port" in capsys.readouterr().out


def test_validate_rejects_malformed_json(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{not json")
    with pytest.raises(SystemExit):
        main(["validate", str(path)])


def test_run_prints_summary_and_exits_zero(tmp_path, capsys):
    path = write_json(tmp_path / "p.json", simple_pipeline())
    assert main(["run", path]) == 0
    out = capsys.readouterr().out
    assert "SUCCEEDED" in out
    assert "✔ t (transform)" in out


def test_run_json_output_is_parseable(tmp_path, capsys):
    path = write_json(tmp_path / "p.json", simple_pipeline())
    assert main(["run", path, "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "succeeded"
    assert payload["outputs"]["t"] == [{"n": 1, "d": 2}, {"n": 2, "d": 4}]


def test_run_writes_output_file(tmp_path):
    path = write_json(tmp_path / "p.json", simple_pipeline())
    out_path = tmp_path / "result.json"
    assert main(["run", path, "--output", str(out_path)]) == 0
    assert json.loads(out_path.read_text())["status"] == "succeeded"


def test_run_failure_exits_one_and_shows_error(tmp_path, capsys):
    pipeline = simple_pipeline(records=[{"n": 0}])
    pipeline["nodes"][1]["config"]["ops"] = [
        {"op": "computed", "target": "d", "expression": "1 / n"}
    ]
    path = write_json(tmp_path / "p.json", pipeline)
    assert main(["run", path]) == 1
    out = capsys.readouterr().out
    assert "FAILED" in out
    assert "[transform]" in out


def test_run_missing_secret_aborts_before_running(tmp_path):
    pipeline = {
        "pipeline_id": "p",
        "nodes": [
            {
                "id": "api",
                "type": "api_source",
                "config": {"url": "https://x.test", "auth": {"type": "bearer", "secret_ref": "NOPE"}},
            }
        ],
        "edges": [],
    }
    path = write_json(tmp_path / "p.json", pipeline)
    with pytest.raises(SystemExit, match="missing secret"):
        main(["run", path])


def test_run_reads_secrets_from_env(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("ETL_SECRET_TOK", "env-token-123")
    pipeline = {
        "pipeline_id": "p",
        "nodes": [
            {
                "id": "api",
                "type": "api_source",
                "config": {"url": "https://api.test/x", "auth": {"type": "bearer", "secret_ref": "TOK"}},
            }
        ],
        "edges": [],
    }
    path = write_json(tmp_path / "p.json", pipeline)
    with respx.mock:
        route = respx.get("https://api.test/x").mock(
            return_value=httpx.Response(200, json=[{"ok": 1}])
        )
        assert main(["run", path, "--no-ssrf-guard"]) == 0
        assert route.calls[0].request.headers["Authorization"] == "Bearer env-token-123"


def test_run_end_to_end_against_local_server_redacts_secret(tmp_path, http_server, capsys):
    pipeline = {
        "pipeline_id": "p",
        "nodes": [
            {
                "id": "api",
                "type": "api_source",
                "config": {"url": f"{http_server}/private", "auth": {"type": "bearer", "secret_ref": "TOK"}},
            }
        ],
        "edges": [],
    }
    path = write_json(tmp_path / "p.json", pipeline)
    secrets_path = write_json(tmp_path / "secrets.json", {"TOK": BEARER_TOKEN})
    code = main(
        ["run", path, "--secrets-file", secrets_path, "--allow-host", "127.0.0.1", "--json"]
    )
    out = capsys.readouterr().out
    assert code == 0
    assert BEARER_TOKEN not in out  # redaction end to end
    assert json.loads(out)["outputs"]["api"] == [{"id": 1, "owner": "alice"}]


def test_test_command_ladder_output(tmp_path, http_server, capsys):
    source_path = write_json(
        tmp_path / "s.json",
        {"id": "s", "type": "api_source", "config": {"url": f"{http_server}/users", "timeout_s": 5}},
    )
    code = main(["test", source_path, "--allow-host", "127.0.0.1"])
    out = capsys.readouterr().out
    assert code == 0
    assert "result: OK" in out
    for rung in ("dns", "ssrf_policy", "tcp", "tls", "http", "auth"):
        assert rung in out


def test_test_command_json_and_failure_exit_code(tmp_path, http_server, capsys):
    source_path = write_json(
        tmp_path / "s.json",
        {
            "id": "s",
            "type": "api_source",
            "config": {
                "url": f"{http_server}/private",
                "timeout_s": 5,
                "auth": {"type": "bearer", "secret_ref": "TOK"},
            },
        },
    )
    secrets_path = write_json(tmp_path / "secrets.json", {"TOK": "wrong-token-11111"})
    code = main(
        ["test", source_path, "--secrets-file", secrets_path, "--allow-host", "127.0.0.1", "--json"]
    )
    assert code == 1
    report = json.loads(capsys.readouterr().out)
    assert report["ok"] is False
    auth_check = next(c for c in report["checks"] if c["name"] == "auth")
    assert auth_check["status"] == "failed"


def test_test_command_blocked_by_default_policy(tmp_path, http_server, capsys):
    source_path = write_json(
        tmp_path / "s.json",
        {"id": "s", "type": "api_source", "config": {"url": f"{http_server}/users", "timeout_s": 5}},
    )
    code = main(["test", source_path])
    out = capsys.readouterr().out
    assert code == 1
    assert "blocked" in out
