"""test-connection endpoint: dispatch + the caller's own secrets."""
from __future__ import annotations


async def test_unsupported_source_type(auth_client):
    r = await auth_client.post("/test-connection", json={"type": "mystery", "config": {}})
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert body["checks"][0]["name"] == "config"


async def test_sqlite_db_source_passes(auth_client, tmp_path):
    payload = {
        "type": "db_source",
        "config": {
            "connection": {"driver": "sqlite", "database": str(tmp_path / "diag.db")},
            "query": "SELECT 1",
        },
    }
    r = await auth_client.post("/test-connection", json=payload)
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    statuses = {c["name"]: c["status"] for c in body["checks"]}
    assert statuses["connect"] == "passed"
    assert statuses["query"] == "passed"


async def test_requires_auth(client):
    assert (await client.post("/test-connection", json={"type": "mystery", "config": {}})).status_code == 401
