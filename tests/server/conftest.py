"""Fixtures for the server tests: a SQLite-backed app + an inline queue.

The whole request/run path is exercised without PostgreSQL, Redis or arq: the
app runs on SQLite and the in-memory queue executes each run inline, so a
triggered run is finished by the time the trigger response returns. Skips
cleanly if the server extra isn't installed.
"""
from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

import httpx  # noqa: E402
import pytest_asyncio  # noqa: E402

from etl_server.app import create_app, inline_queue  # noqa: E402
from etl_server.config import Settings  # noqa: E402
from etl_server.db import Database  # noqa: E402

# A runnable, fully hermetic pipeline: fan out an array, double each value.
RUNNABLE_SPEC = {
    "pipeline_id": "demo",
    "nodes": [
        {"id": "gen", "type": "iterator", "config": {"mode": "array", "array": [1, 2, 3]}},
        {
            "id": "double",
            "type": "transform",
            "config": {"ops": [{"op": "computed", "target": "doubled", "expression": "value * 2"}]},
        },
    ],
    "edges": [{"from": "gen", "to": "double"}],
}


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/server.db",
        jwt_secret="test-jwt-secret-that-is-at-least-32-bytes",
        ssrf_enabled=False,
        secrets_algo="fernet",
    )


@pytest_asyncio.fixture
async def database(settings):
    db = Database(settings.database_url)
    await db.create_all()
    yield db
    await db.dispose()


@pytest.fixture
def app(settings, database):
    return create_app(settings, database, inline_queue(database, settings))


@pytest_asyncio.fixture
async def client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def register_and_login(client: httpx.AsyncClient, email: str, password: str) -> str:
    await client.post("/auth/register", json={"email": email, "password": password})
    resp = await client.post("/auth/token", data={"username": email, "password": password})
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


@pytest_asyncio.fixture
async def auth_client(client):
    token = await register_and_login(client, "user@test.com", "password123")
    client.headers["Authorization"] = f"Bearer {token}"
    return client


@pytest_asyncio.fixture
async def other_headers(client):
    token = await register_and_login(client, "other@test.com", "password456")
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def login_as(client):
    """Callable: register+login a new user, returning bearer headers."""

    async def _login(email: str, password: str = "password123") -> dict:
        token = await register_and_login(client, email, password)
        return {"Authorization": f"Bearer {token}"}

    return _login


@pytest.fixture
def make_pipeline(auth_client):
    """Callable: create a pipeline for the default user, returning its id."""

    async def _make(spec: dict = RUNNABLE_SPEC, name: str = "demo") -> str:
        resp = await auth_client.post("/pipelines", json={"name": name, "spec": spec})
        assert resp.status_code == 201, resp.text
        return resp.json()["id"]

    return _make
