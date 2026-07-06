"""Secrets: stored encrypted, never returned, per-user, upsert + delete."""
from __future__ import annotations


async def test_create_and_list_hides_value(auth_client):
    r = await auth_client.post("/secrets", json={"ref": "TOKEN", "value": "s3cret"})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["ref"] == "TOKEN"
    assert "value" not in body and "ciphertext" not in body

    listing = (await auth_client.get("/secrets")).json()
    assert [s["ref"] for s in listing] == ["TOKEN"]
    assert all("value" not in s and "ciphertext" not in s for s in listing)


async def test_upsert_replaces_value(auth_client):
    await auth_client.post("/secrets", json={"ref": "K", "value": "v1"})
    await auth_client.post("/secrets", json={"ref": "K", "value": "v2"})
    listing = (await auth_client.get("/secrets")).json()
    assert len([s for s in listing if s["ref"] == "K"]) == 1  # still one row


async def test_delete(auth_client):
    await auth_client.post("/secrets", json={"ref": "GONE", "value": "x"})
    assert (await auth_client.delete("/secrets/GONE")).status_code == 204
    assert (await auth_client.delete("/secrets/GONE")).status_code == 404


async def test_secret_isolation(auth_client, other_headers):
    await auth_client.post("/secrets", json={"ref": "MINE", "value": "x"})
    assert (await auth_client.get("/secrets", headers=other_headers)).json() == []
    # same ref name for another user is independent
    await auth_client.post("/secrets", headers=other_headers, json={"ref": "MINE", "value": "y"})
    assert (await auth_client.delete("/secrets/MINE", headers=other_headers)).status_code == 204
    # the first user's secret is untouched
    assert [s["ref"] for s in (await auth_client.get("/secrets")).json()] == ["MINE"]


async def test_requires_auth(client):
    assert (await client.get("/secrets")).status_code == 401
