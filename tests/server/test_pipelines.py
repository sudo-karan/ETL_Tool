"""Pipeline CRUD + per-user ownership."""
from __future__ import annotations

SPEC = {
    "pipeline_id": "p",
    "nodes": [{"id": "gen", "type": "iterator", "config": {"mode": "array", "array": [1]}}],
    "edges": [],
}


async def test_create_get_list(auth_client):
    r = await auth_client.post("/pipelines", json={"name": "first", "spec": SPEC})
    assert r.status_code == 201, r.text
    pid = r.json()["id"]

    got = await auth_client.get(f"/pipelines/{pid}")
    assert got.status_code == 200
    assert got.json()["name"] == "first"
    assert got.json()["spec"] == SPEC

    listing = await auth_client.get("/pipelines")
    assert [p["id"] for p in listing.json()] == [pid]


async def test_invalid_spec_rejected(auth_client):
    r = await auth_client.post("/pipelines", json={"name": "bad", "spec": {"nonsense": True}})
    assert r.status_code == 422


async def test_update(auth_client):
    pid = (await auth_client.post("/pipelines", json={"name": "n", "spec": SPEC})).json()["id"]
    r = await auth_client.put(f"/pipelines/{pid}", json={"name": "renamed"})
    assert r.status_code == 200
    assert r.json()["name"] == "renamed"
    assert r.json()["spec"] == SPEC  # unchanged


async def test_update_with_invalid_spec_rejected(auth_client):
    pid = (await auth_client.post("/pipelines", json={"name": "n", "spec": SPEC})).json()["id"]
    r = await auth_client.put(f"/pipelines/{pid}", json={"spec": {"broken": 1}})
    assert r.status_code == 422


async def test_delete(auth_client):
    pid = (await auth_client.post("/pipelines", json={"name": "n", "spec": SPEC})).json()["id"]
    assert (await auth_client.delete(f"/pipelines/{pid}")).status_code == 204
    assert (await auth_client.get(f"/pipelines/{pid}")).status_code == 404


async def test_ownership_isolation(auth_client, other_headers):
    pid = (await auth_client.post("/pipelines", json={"name": "mine", "spec": SPEC})).json()["id"]
    # other user cannot see, update or delete it
    assert (await auth_client.get(f"/pipelines/{pid}", headers=other_headers)).status_code == 404
    assert (
        await auth_client.put(f"/pipelines/{pid}", headers=other_headers, json={"name": "x"})
    ).status_code == 404
    assert (await auth_client.delete(f"/pipelines/{pid}", headers=other_headers)).status_code == 404
    # their own listing is empty
    assert (await auth_client.get("/pipelines", headers=other_headers)).json() == []
