"""file_source: read records from a local CSV / JSON / JSONL / Parquet file.

The format is inferred from the path suffix by default, or set explicitly.
The path is checked against the run's file-access policy before any read, so
a locked-down server deployment can confine reads to allowed directories.
Blocking file reads run in a thread so they never stall the event loop.
"""
from __future__ import annotations

import asyncio
from typing import ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

from ..errors import ErrorCategory
from ..fileio import (
    FileAccessError,
    FileFormatError,
    infer_format,
    read_records,
)
from .base import Node, NodeContext, NodeInputs, NodeOutputs
from .registry import register_node


class FileSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    format: Literal["auto", "csv", "json", "jsonl", "parquet"] = "auto"
    limit: int | None = Field(None, ge=0)  # cap rows read (preview / testing)
    # csv options
    has_header: bool = True
    delimiter: str = ","
    infer_schema: bool = True  # False -> every csv column read as text
    # json only: dotted path to the record list, e.g. "data.items"
    records_path: str | None = None


@register_node
class FileSourceNode(Node):
    type_name: ClassVar[str] = "file_source"
    config_model: ClassVar[type[BaseModel]] = FileSourceConfig
    # Optional context input: an upstream/iterator edge makes its records
    # available to $upstream/$iter references in this config (e.g. path).
    input_ports: ClassVar[tuple[str, ...]] = ("in",)
    output_ports: ClassVar[tuple[str, ...]] = ("out",)

    async def run(self, inputs: NodeInputs, ctx: NodeContext) -> NodeOutputs:
        cfg: FileSourceConfig = self.config  # type: ignore[assignment]
        try:
            fmt = infer_format(cfg.path) if cfg.format == "auto" else cfg.format
            resolved = ctx.run.options.file_policy.resolve(cfg.path)
        except (FileFormatError, FileAccessError) as exc:
            raise ctx.error(ErrorCategory.CONFIG, str(exc)) from exc

        try:
            records = await asyncio.to_thread(
                read_records,
                resolved,
                fmt,
                limit=cfg.limit,
                has_header=cfg.has_header,
                delimiter=cfg.delimiter,
                infer_schema=cfg.infer_schema,
                records_path=cfg.records_path,
            )
        except FileAccessError as exc:
            raise ctx.error(ErrorCategory.CONFIG, str(exc)) from exc
        except FileFormatError as exc:
            raise ctx.error(ErrorCategory.VALIDATION, str(exc)) from exc

        ctx.info(f"read {len(records)} record(s) from {cfg.path} ({fmt})")
        return {"out": records}
