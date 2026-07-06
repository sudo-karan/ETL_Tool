"""db_sink: write the incoming records into a database table.

Modes: ``append`` adds rows; ``replace`` clears the table (same transaction)
then inserts. ``create`` builds a missing table by inferring column types from
the records. Nested values are JSON-encoded into the column. Like file_sink,
this node is pass-through -- it emits the written records on ``out`` so it can
sit mid-pipeline or be terminal. Password via ``secret_ref``; the host passes
the SSRF guard before connecting.
"""
from __future__ import annotations

import re
import socket
from typing import ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..db import (
    DbConnectionConfig,
    DbTableMissing,
    categorize_db_exception,
    ensure_db_host_allowed,
    write_table,
)
from ..errors import ErrorCategory, SSRFBlockedError
from .base import Node, NodeContext, NodeInputs, NodeOutputs
from .db_source import endpoint_summary
from .registry import register_node

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


class DbSinkConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    connection: DbConnectionConfig
    table: str
    schema_: str | None = Field(None, alias="schema")
    mode: Literal["append", "replace"] = "append"
    create: bool = False  # create the table from the records if missing

    @field_validator("table", "schema_")
    @classmethod
    def _safe_identifier(cls, value: str | None) -> str | None:
        if value is not None and not _IDENTIFIER_RE.match(value):
            raise ValueError(
                f"unsafe SQL identifier {value!r}; use letters, digits and underscores"
            )
        return value


@register_node
class DbSinkNode(Node):
    type_name: ClassVar[str] = "db_sink"
    config_model: ClassVar[type[BaseModel]] = DbSinkConfig
    input_ports: ClassVar[tuple[str, ...]] = ("in",)
    output_ports: ClassVar[tuple[str, ...]] = ("out",)

    @classmethod
    def required_input_ports(cls, config: BaseModel) -> tuple[str, ...]:
        return ("in",)

    async def run(self, inputs: NodeInputs, ctx: NodeContext) -> NodeOutputs:
        cfg: DbSinkConfig = self.config  # type: ignore[assignment]
        conn = cfg.connection
        records = inputs["in"][0]
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
            written = await write_table(
                conn,
                password,
                cfg.table,
                records,
                schema=cfg.schema_,
                mode=cfg.mode,
                create=cfg.create,
            )
        except DbTableMissing as exc:
            raise ctx.error(ErrorCategory.CONFIG, str(exc), request_summary=summary) from exc
        except Exception as exc:  # noqa: BLE001 - categorized into a structured error
            if isinstance(exc, SSRFBlockedError):
                raise ctx.error(ErrorCategory.CONFIG, str(exc), request_summary=summary) from exc
            category, message = categorize_db_exception(exc)
            raise ctx.error(
                category, f"write to {cfg.table!r} failed: {message}", request_summary=summary
            ) from exc

        ctx.info(f"wrote {written} row(s) to {cfg.table} ({conn.driver}, mode={cfg.mode})")
        return {"out": records}
