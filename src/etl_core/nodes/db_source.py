"""db_source: run a SQL query and emit the result rows as records.

Targets PostgreSQL (``etl-tool[postgres]``) or SQLite through the shared async
DB layer. The password comes from a ``secret_ref``; the connection host is run
through the SSRF policy before connecting (same guard as api_source). Result
values are coerced to JSON-serializable forms so they honor the record
contract downstream.
"""
from __future__ import annotations

import socket
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field

from ..db import (
    DbConnectionConfig,
    categorize_db_exception,
    ensure_db_host_allowed,
    run_query,
)
from ..errors import ErrorCategory, SSRFBlockedError
from .base import Node, NodeContext, NodeInputs, NodeOutputs
from .registry import register_node


def endpoint_summary(config: DbConnectionConfig) -> str:
    """A credential-free connection summary for error reporting."""
    if config.driver == "sqlite":
        return f"sqlite:///{config.database}"
    host, port = config.endpoint()
    return f"{config.driver}://{host}:{port}/{config.database}"


class DbSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connection: DbConnectionConfig
    query: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)  # bound as :name in query
    limit: int | None = Field(None, ge=0)


@register_node
class DbSourceNode(Node):
    type_name: ClassVar[str] = "db_source"
    config_model: ClassVar[type[BaseModel]] = DbSourceConfig
    input_ports: ClassVar[tuple[str, ...]] = ("in",)
    output_ports: ClassVar[tuple[str, ...]] = ("out",)

    async def run(self, inputs: NodeInputs, ctx: NodeContext) -> NodeOutputs:
        cfg: DbSourceConfig = self.config  # type: ignore[assignment]
        conn = cfg.connection
        summary = endpoint_summary(conn)
        password = ctx.get_secret(conn.secret_ref) if conn.secret_ref else None

        try:
            await ensure_db_host_allowed(conn, ctx.run.options.ssrf_policy)
        except SSRFBlockedError as exc:
            raise ctx.error(ErrorCategory.CONFIG, str(exc), request_summary=summary) from exc
        except socket.gaierror as exc:
            raise ctx.error(
                ErrorCategory.DNS,
                f"DNS resolution failed for DB host {conn.host!r}: {exc}",
                request_summary=summary,
            ) from exc

        try:
            records = await run_query(conn, password, cfg.query, cfg.params, limit=cfg.limit)
        except Exception as exc:  # noqa: BLE001 - categorized into a structured error
            if isinstance(exc, SSRFBlockedError):
                raise ctx.error(ErrorCategory.CONFIG, str(exc), request_summary=summary) from exc
            category, message = categorize_db_exception(exc)
            raise ctx.error(category, f"query failed: {message}", request_summary=summary) from exc

        ctx.info(f"fetched {len(records)} row(s) from {conn.driver}")
        return {"out": records}
