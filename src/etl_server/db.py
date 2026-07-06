"""Async database wiring.

A :class:`Database` owns the engine + session factory for one app instance and
lives on ``app.state`` -- no module-level engine singleton -- so tests spin up
their own SQLite database and production points at PostgreSQL, both through the
same code. ``create_all`` is for tests/dev; production uses Alembic migrations.
"""
from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .models import Base


class Database:
    def __init__(self, url: str):
        self.engine: AsyncEngine = create_async_engine(url, future=True)
        # SQLite ignores foreign keys unless asked; enable so ON DELETE CASCADE
        # behaves like PostgreSQL in tests.
        if self.engine.dialect.name == "sqlite":

            @event.listens_for(self.engine.sync_engine, "connect")
            def _fk_pragma(dbapi_conn, _record):  # noqa: ANN001
                cursor = dbapi_conn.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

        self.sessionmaker: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self.engine, expire_on_commit=False
        )

    async def create_all(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def dispose(self) -> None:
        await self.engine.dispose()

    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self.sessionmaker() as session:
            yield session
