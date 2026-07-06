"""CLI coverage for the Phase 2 nodes: db test_connection + a file→db run."""
from __future__ import annotations

import json

from etl_core.cli import main


def write_json(path, payload):
    path.write_text(json.dumps(payload))
    return str(path)


def test_test_command_on_sqlite_db_source(tmp_path, capsys):
    dbfile = tmp_path / "t.db"
    source_path = write_json(
        tmp_path / "s.json",
        {"id": "s", "type": "db_source",
         "config": {"connection": {"driver": "sqlite", "database": str(dbfile)},
                    "query": "SELECT 1"}},
    )
    code = main(["test", source_path])  # sqlite has no host -> no allowlist needed
    out = capsys.readouterr().out
    assert code == 0
    assert "result: OK" in out
    for rung in ("connect", "query"):
        assert rung in out


def test_run_file_to_db_pipeline(tmp_path, capsys):
    seed = write_json(tmp_path / "seed.json", [{"id": 1, "name": "Ann"}])
    dbfile = tmp_path / "out.db"
    pipeline = {
        "pipeline_id": "file2db",
        "nodes": [
            {"id": "read", "type": "file_source", "config": {"path": seed}},
            {"id": "write", "type": "db_sink",
             "config": {"connection": {"driver": "sqlite", "database": str(dbfile)},
                        "table": "people", "create": True}},
        ],
        "edges": [{"from": "read", "to": "write"}],
    }
    path = write_json(tmp_path / "p.json", pipeline)
    assert main(["run", path]) == 0
    out = capsys.readouterr().out
    assert "SUCCEEDED" in out
    assert dbfile.exists()
