"""FastAPI application factory.

``create_app`` is pure: it takes already-built dependencies (settings, database,
queue) and wires state immediately -- ideal for tests, which use SQLite and an
in-memory queue. ``production_app`` uses a lifespan to build a PostgreSQL engine
and an arq/Redis pool asynchronously at startup. Both share ``_register_routers``.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from etl_core.crypto import Cipher

from . import __version__
from .config import Settings, get_settings
from .db import Database
from .queue import ArqJobQueue, InMemoryJobQueue, JobQueue
from .routers import auth, diagnostics, pipelines, runs, schedules, secrets


def _register_routers(app: FastAPI) -> None:
    app.include_router(auth.router)
    app.include_router(pipelines.router)
    app.include_router(runs.router)
    app.include_router(secrets.router)
    app.include_router(diagnostics.router)
    app.include_router(schedules.router)

    @app.get("/health", tags=["meta"])
    async def health() -> dict:
        return {"status": "ok", "version": __version__}


def _add_cors(app: FastAPI) -> None:
    # Permissive by default so the Phase 4 UI can call the API in dev; restrict
    # allow_origins in production.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def create_app(
    settings: Settings,
    db: Database,
    queue: JobQueue,
    *,
    cipher: Cipher | None = None,
) -> FastAPI:
    app = FastAPI(title="ETL Tool Server", version=__version__)
    app.state.settings = settings
    app.state.db = db
    app.state.queue = queue
    app.state.cipher = cipher or settings.secrets_cipher()
    _add_cors(app)
    _register_routers(app)
    return app


def inline_queue(db: Database, settings: Settings) -> InMemoryJobQueue:
    """A queue that runs each job inline (single-process dev / tests)."""
    from .worker import execute_run

    async def runner(run_id):  # noqa: ANN001
        await execute_run(db, settings, run_id)

    return InMemoryJobQueue(runner=runner)


def production_app() -> FastAPI:
    """ASGI app for uvicorn: PostgreSQL + arq/Redis, built in a lifespan."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        from arq import create_pool
        from arq.connections import RedisSettings

        settings = get_settings()
        db = Database(settings.database_url)
        pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
        app.state.settings = settings
        app.state.db = db
        app.state.cipher = settings.secrets_cipher()
        app.state.queue = ArqJobQueue(pool)
        if settings.create_tables_on_startup:
            await db.create_all()
        try:
            yield
        finally:
            await db.dispose()
            await pool.aclose()

    app = FastAPI(title="ETL Tool Server", version=__version__, lifespan=lifespan)
    _add_cors(app)
    _register_routers(app)
    return app


def dev_app() -> FastAPI:
    """Single-process dev server: no Redis/worker. Tables are created on
    startup and triggered runs execute inline via the in-memory queue.

        uvicorn etl_server.app:dev_app --factory --reload
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = get_settings()
        db = Database(settings.database_url)
        app.state.settings = settings
        app.state.db = db
        app.state.cipher = settings.secrets_cipher()
        app.state.queue = inline_queue(db, settings)
        await db.create_all()
        try:
            yield
        finally:
            await db.dispose()

    app = FastAPI(title="ETL Tool Server (dev)", version=__version__, lifespan=lifespan)
    _add_cors(app)
    _register_routers(app)
    return app
