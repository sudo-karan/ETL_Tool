"""Shared database layer for db_source / db_sink and their diagnostics.

One SQLAlchemy 2.0 async path serves both PostgreSQL (via ``asyncpg``, the
``etl-tool[postgres]`` extra) and SQLite (via ``aiosqlite``); the latter lets
the whole node stack be tested hermetically without a running server. The
db_source/db_sink nodes and ``test_connection`` all build their engine here so
credentials, the SSRF host guard and value coercion behave identically.

SQLAlchemy is imported lazily inside functions (like polars in fileio) so
``import etl_core`` stays cheap for API-only pipelines.

SSRF. db_source/db_sink let the server open a TCP connection to a user-named
host -- the same SSRF surface as api_source -- so the connection host is run
through the run's SSRF policy before connecting. SQLite targets a local file
and has no host, so the network guard does not apply (a future file-path
policy could cover it).
"""
from __future__ import annotations

import asyncio
import base64
import datetime
import decimal
import json
import uuid
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .errors import ErrorCategory
from .ssrf import SSRFPolicy, find_blocked, resolve_host

Records = list[dict[str, Any]]

_DEFAULT_PORTS = {"postgresql": 5432}


class DbConnectionConfig(BaseModel):
    """Connection details. The password is never in the JSON -- only a
    ``secret_ref`` resolved from the run's secrets."""

    model_config = ConfigDict(extra="forbid")

    driver: Literal["postgresql", "sqlite"] = "postgresql"
    host: str | None = None
    port: int | None = None
    # Postgres: database name. SQLite: file path (or ":memory:").
    database: str
    user: str | None = None
    secret_ref: str | None = None  # password
    sslmode: str | None = None  # postgres: "disable" | anything else -> TLS on
    connect_timeout_s: float = Field(10.0, gt=0)

    @model_validator(mode="after")
    def _check_driver_fields(self) -> "DbConnectionConfig":
        if self.driver == "postgresql" and not self.host:
            raise ValueError("postgresql connection requires 'host'")
        return self

    def endpoint(self) -> tuple[str | None, int | None]:
        """(host, port) for a network driver; (None, None) for sqlite."""
        if self.driver != "postgresql":
            return None, None
        return self.host, self.port or _DEFAULT_PORTS.get(self.driver)


def build_url(config: DbConnectionConfig, password: str | None) -> Any:
    """Build a SQLAlchemy async URL (never logged -- it carries the password)."""
    from sqlalchemy.engine import URL

    if config.driver == "sqlite":
        database = config.database
        # sqlite+aiosqlite:///relative/or/abs.db ; "" or ":memory:" -> in-memory
        if database in ("", ":memory:"):
            return URL.create("sqlite+aiosqlite", database=None)
        return URL.create("sqlite+aiosqlite", database=database)
    return URL.create(
        "postgresql+asyncpg",
        username=config.user,
        password=password,
        host=config.host,
        port=config.port or _DEFAULT_PORTS["postgresql"],
        database=config.database,
    )


def _connect_args(config: DbConnectionConfig) -> dict[str, Any]:
    if config.driver != "postgresql":
        return {}
    args: dict[str, Any] = {
        "timeout": config.connect_timeout_s,
        "command_timeout": config.connect_timeout_s,
    }
    if config.sslmode is not None:
        args["ssl"] = False if config.sslmode.lower() == "disable" else True
    return args


def open_engine(config: DbConnectionConfig, password: str | None) -> Any:
    """Create a per-use AsyncEngine (NullPool -- no connection outlives a run)."""
    from sqlalchemy import NullPool
    from sqlalchemy.ext.asyncio import create_async_engine

    return create_async_engine(
        build_url(config, password),
        poolclass=NullPool,
        connect_args=_connect_args(config),
    )


async def ensure_db_host_allowed(config: DbConnectionConfig, policy: SSRFPolicy) -> None:
    """Raise :class:`SSRFBlockedError` if the DB host is denied by policy.

    DNS failures propagate as ``socket.gaierror`` so callers can categorize
    them as DNS errors. No-op for hostless (sqlite) connections.
    """
    from .errors import SSRFBlockedError

    host, port = config.endpoint()
    if host is None or not policy.enabled:
        return
    if host.lower() in policy._allowed_names():
        return
    ips = await resolve_host(host, port)
    reason = find_blocked(host, ips, policy)
    if reason is not None:
        raise SSRFBlockedError(reason)


# --------------------------------------------------------------------------
# Value coercion: DB row -> JSON-serializable record
# --------------------------------------------------------------------------
def jsonify_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, (list, tuple)):
        return [jsonify_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): jsonify_value(item) for key, item in value.items()}
    return str(value)


def _row_to_record(mapping: Any) -> dict[str, Any]:
    return {str(key): jsonify_value(value) for key, value in mapping.items()}


# --------------------------------------------------------------------------
# Queries
# --------------------------------------------------------------------------
async def run_query(
    config: DbConnectionConfig,
    password: str | None,
    query: str,
    params: dict[str, Any] | None = None,
    *,
    limit: int | None = None,
) -> Records:
    from sqlalchemy import text

    engine = open_engine(config, password)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text(query), params or {})
            if not result.returns_rows:
                return []
            mappings = result.mappings()
            rows = mappings.fetchmany(limit) if limit is not None else mappings.all()
            return [_row_to_record(row) for row in rows]
    finally:
        await engine.dispose()


# --------------------------------------------------------------------------
# Writes
# --------------------------------------------------------------------------
def _prepare_row(record: dict[str, Any], columns: set[str]) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for key, value in record.items():
        if key not in columns:
            continue
        row[key] = json.dumps(value, default=str) if isinstance(value, (dict, list)) else value
    return row


def _infer_column_type(records: Records, column: str, sa: Any) -> Any:
    for record in records:
        value = record.get(column)
        if value is None:
            continue
        if isinstance(value, bool):
            return sa.Boolean()
        if isinstance(value, int):
            return sa.BigInteger()
        if isinstance(value, float):
            return sa.Float()
        return sa.Text()
    return sa.Text()


async def write_table(
    config: DbConnectionConfig,
    password: str | None,
    table_name: str,
    records: Records,
    *,
    schema: str | None = None,
    mode: Literal["append", "replace"] = "append",
    create: bool = False,
) -> int:
    """Insert records into a table. ``replace`` deletes existing rows first
    (same transaction); ``create`` builds a missing table by inferring column
    types from the records. Returns the number of rows inserted."""
    import sqlalchemy as sa
    from sqlalchemy import MetaData, Table, insert

    ordered_columns: list[str] = []
    seen: set[str] = set()
    for record in records:
        for key in record:
            if key not in seen:
                seen.add(key)
                ordered_columns.append(key)

    engine = open_engine(config, password)
    try:
        async with engine.begin() as conn:
            metadata = MetaData()

            def _reflect(sync_conn: Any) -> Table | None:
                if engine.dialect.has_table(sync_conn, table_name, schema=schema):
                    return Table(table_name, metadata, schema=schema, autoload_with=sync_conn)
                return None

            table = await conn.run_sync(_reflect)
            if table is None:
                if not create:
                    raise DbTableMissing(table_name)
                if not ordered_columns:
                    raise DbTableMissing(
                        table_name,
                        "no rows to infer a schema from; create the table first",
                    )
                table = Table(
                    table_name,
                    metadata,
                    *[
                        sa.Column(name, _infer_column_type(records, name, sa))
                        for name in ordered_columns
                    ],
                    schema=schema,
                )
                await conn.run_sync(metadata.create_all)
            elif mode == "replace":
                await conn.execute(table.delete())

            if not records:
                return 0
            columns = {col.name for col in table.columns}
            rows = [_prepare_row(record, columns) for record in records]
            await conn.execute(insert(table), rows)
            return len(rows)
    finally:
        await engine.dispose()


# --------------------------------------------------------------------------
# Error categorization: DBAPI/driver exception -> structured category
# --------------------------------------------------------------------------
def categorize_db_exception(exc: BaseException) -> tuple[ErrorCategory, str]:
    """Map a SQLAlchemy/driver exception to a (category, message) pair.

    Driver-agnostic: uses SQLAlchemy exception classes and the ANSI SQLSTATE
    when present, falling back to message heuristics for SQLite.
    """
    from sqlalchemy import exc as sa_exc

    orig = getattr(exc, "orig", None) or exc
    sqlstate = getattr(orig, "sqlstate", None) or getattr(exc, "sqlstate", None)
    message = str(orig).strip() or type(orig).__name__
    low = message.lower()

    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)) or "timeout" in low:
        return ErrorCategory.TIMEOUT, message
    if sqlstate and str(sqlstate).startswith("28"):  # invalid authorization
        return ErrorCategory.AUTH, message
    if "password" in low or "authentication failed" in low or "role" in low and "does not exist" in low:
        return ErrorCategory.AUTH, message
    if (sqlstate and str(sqlstate)[:2] in ("42", "3D", "3F")) or isinstance(
        exc, (sa_exc.ProgrammingError, sa_exc.IntegrityError)
    ):
        return ErrorCategory.VALIDATION, message
    if "no such table" in low or "syntax error" in low or "no such column" in low:
        return ErrorCategory.VALIDATION, message
    if "unable to open database" in low or "no such file" in low:
        return ErrorCategory.CONFIG, message
    if isinstance(exc, (sa_exc.OperationalError, sa_exc.InterfaceError)) or isinstance(
        orig, (OSError, ConnectionError)
    ):
        return ErrorCategory.NETWORK, message
    return ErrorCategory.UNKNOWN, message


class DbTableMissing(Exception):
    def __init__(self, table_name: str, detail: str | None = None):
        message = f"table {table_name!r} does not exist"
        if detail:
            message += f" ({detail})"
        else:
            message += "; set create=true to create it from the records"
        super().__init__(message)
        self.table_name = table_name
