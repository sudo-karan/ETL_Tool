"""Auth: registration, login, token-protected access."""
from __future__ import annotations


async def test_register_and_me(client):
    r = await client.post("/auth/register", json={"email": "a@b.com", "password": "password123"})
    assert r.status_code == 201, r.text
    assert r.json()["email"] == "a@b.com"
    assert "password" not in r.json() and "hashed_password" not in r.json()

    token = (
        await client.post("/auth/token", data={"username": "a@b.com", "password": "password123"})
    ).json()["access_token"]
    me = await client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me.status_code == 200
    assert me.json()["email"] == "a@b.com"


async def test_duplicate_email_conflicts(client):
    await client.post("/auth/register", json={"email": "dup@b.com", "password": "password123"})
    r = await client.post("/auth/register", json={"email": "dup@b.com", "password": "password123"})
    assert r.status_code == 409


async def test_short_password_rejected(client):
    r = await client.post("/auth/register", json={"email": "x@b.com", "password": "short"})
    assert r.status_code == 422


async def test_login_wrong_password(client):
    await client.post("/auth/register", json={"email": "c@b.com", "password": "password123"})
    r = await client.post("/auth/token", data={"username": "c@b.com", "password": "wrongpass!"})
    assert r.status_code == 401


async def test_login_unknown_user(client):
    r = await client.post("/auth/token", data={"username": "nobody@b.com", "password": "password123"})
    assert r.status_code == 401


async def test_protected_route_requires_token(client):
    assert (await client.get("/pipelines")).status_code == 401
    assert (await client.get("/auth/me")).status_code == 401


async def test_invalid_token_rejected(client):
    r = await client.get("/pipelines", headers={"Authorization": "Bearer not-a-real-token"})
    assert r.status_code == 401
