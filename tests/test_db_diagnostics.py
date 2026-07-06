"""test_connection for db_source: the connect/query ladder + failure paths."""
from __future__ import annotations

from etl_core import SSRFPolicy
from etl_core import test_connection as check_connection  # alias: pytest must not collect it


def db_source(config):
    return {"id": "s", "type": "db_source", "config": config}


async def test_sqlite_connection_passes(tmp_path):
    dbfile = tmp_path / "t.db"
    report = await check_connection(
        db_source({"connection": {"driver": "sqlite", "database": str(dbfile)},
                   "query": "SELECT 1"}),
        ssrf_policy=SSRFPolicy(enabled=False),
    )
    assert report.ok, report.model_dump()
    by_name = {c.name: c.status for c in report.checks}
    assert by_name["connect"] == "passed"
    assert by_name["query"] == "passed"
    # network rungs are not applicable to a local file
    assert by_name["dns"] == "skipped"
    assert by_name["tcp"] == "skipped"
    assert report.sample_body and "ok" in report.sample_body


async def test_accepts_bare_connection_object(tmp_path):
    dbfile = tmp_path / "t.db"
    report = await check_connection(
        db_source({"driver": "sqlite", "database": str(dbfile)}),
        ssrf_policy=SSRFPolicy(enabled=False),
    )
    assert report.ok


async def test_unsupported_source_type():
    report = await check_connection({"type": "mystery", "config": {}})
    assert report.ok is False
    assert report.checks[0].name == "config"
    assert "unsupported source type" in report.checks[0].error


async def test_references_cannot_be_tested():
    report = await check_connection(
        db_source({"connection": {"driver": "sqlite", "database": "$upstream.x.path"},
                   "query": "SELECT 1"})
    )
    assert report.ok is False
    assert "references" in report.checks[0].error


async def test_missing_secret_is_reported():
    report = await check_connection(
        db_source({"connection": {"driver": "postgresql", "host": "db.example",
                                  "database": "app", "secret_ref": "PW"},
                   "query": "SELECT 1"}),
        secrets={},
        ssrf_policy=SSRFPolicy(enabled=False),
    )
    assert report.ok is False
    assert any("secret 'PW' was not provided" in (c.error or "") for c in report.checks)


async def test_postgres_private_host_blocked_by_ssrf():
    report = await check_connection(
        db_source({"connection": {"driver": "postgresql", "host": "10.0.0.1",
                                  "database": "app"}, "query": "SELECT 1"}),
        ssrf_policy=SSRFPolicy(),  # guard enabled
    )
    assert report.ok is False
    by_name = {c.name: c.status for c in report.checks}
    assert by_name["dns"] == "passed"  # IP literal resolves to itself
    assert by_name["ssrf_policy"] == "failed"
    assert by_name["connect"] == "skipped"


async def test_sqlite_connect_failure_is_reported(tmp_path):
    # Pointing the database at a directory makes SQLite fail to open it.
    report = await check_connection(
        db_source({"connection": {"driver": "sqlite", "database": str(tmp_path)},
                   "query": "SELECT 1"}),
        ssrf_policy=SSRFPolicy(enabled=False),
    )
    assert report.ok is False
    assert any(c.status == "failed" for c in report.checks if c.name in ("connect", "query"))
