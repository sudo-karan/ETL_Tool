"""file_sink: write the incoming records to a local file.

Supports CSV / JSON / JSONL / Parquet with overwrite / append / error-if-exists
modes. The node is pass-through: it writes the records and also emits them on
its ``out`` port, so a sink can sit mid-pipeline (persist, then keep going) or
be a terminal node. The path is checked against the run's file-access policy;
the blocking write runs in a thread.
"""
from __future__ import annotations

import asyncio
from typing import ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

from ..errors import ErrorCategory
from ..fileio import FileAccessError, FileFormatError, infer_format, write_records
from .base import Node, NodeContext, NodeInputs, NodeOutputs
from .registry import register_node


class FileSinkConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    format: Literal["auto", "csv", "json", "jsonl", "parquet"] = "auto"
    mode: Literal["overwrite", "append", "error"] = "overwrite"
    make_parents: bool = True  # create missing parent directories
    # csv options
    has_header: bool = True
    delimiter: str = ","
    # json only
    json_indent: int | None = Field(2, ge=0)


@register_node
class FileSinkNode(Node):
    type_name: ClassVar[str] = "file_sink"
    config_model: ClassVar[type[BaseModel]] = FileSinkConfig
    input_ports: ClassVar[tuple[str, ...]] = ("in",)
    output_ports: ClassVar[tuple[str, ...]] = ("out",)

    @classmethod
    def required_input_ports(cls, config: BaseModel) -> tuple[str, ...]:
        return ("in",)

    async def run(self, inputs: NodeInputs, ctx: NodeContext) -> NodeOutputs:
        cfg: FileSinkConfig = self.config  # type: ignore[assignment]
        records = inputs["in"][0]
        try:
            fmt = infer_format(cfg.path) if cfg.format == "auto" else cfg.format
            resolved = ctx.run.options.file_policy.resolve(cfg.path, for_write=True)
        except (FileFormatError, FileAccessError) as exc:
            raise ctx.error(ErrorCategory.CONFIG, str(exc)) from exc

        try:
            written = await asyncio.to_thread(
                write_records,
                records,
                resolved,
                fmt,
                mode=cfg.mode,
                make_parents=cfg.make_parents,
                has_header=cfg.has_header,
                delimiter=cfg.delimiter,
                json_indent=cfg.json_indent,
            )
        except FileAccessError as exc:
            raise ctx.error(ErrorCategory.CONFIG, str(exc)) from exc
        except (FileFormatError, ValueError, OSError) as exc:
            raise ctx.error(
                ErrorCategory.CONFIG,
                f"failed to write {cfg.path} ({fmt}): {type(exc).__name__}: {exc}",
            ) from exc

        if not records:
            ctx.warning(f"no input records; wrote an empty {fmt} file to {cfg.path}")
        else:
            ctx.info(f"wrote {written} record(s) to {cfg.path} ({fmt}, mode={cfg.mode})")
        return {"out": records}
