"""The pipeline JSON schema -- the contract between engine, server and UI.

A pipeline is a serializable directed graph: nodes (id + type + config) and
edges (from/to node + port). The React Flow editor (Phase 4) emits exactly
this document; the server stores it; the engine executes it.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# Node ids appear in $upstream references, which are dot-separated, so ids
# must not contain dots or whitespace.
NODE_ID_PATTERN = r"^[A-Za-z_][A-Za-z0-9_\-]*$"


class NodeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=NODE_ID_PATTERN)
    type: str = Field(min_length=1)
    config: dict[str, Any] = Field(default_factory=dict)


class EdgeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    from_node: str = Field(alias="from")
    from_port: str = "out"
    to_node: str = Field(alias="to")
    to_port: str = "in"


class PipelineSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_id: str = Field(min_length=1)
    nodes: list[NodeSpec]
    edges: list[EdgeSpec] = Field(default_factory=list)

    @classmethod
    def from_file(cls, path: str | Path) -> "PipelineSpec":
        return cls.model_validate_json(Path(path).read_text())

    def to_json(self, **kw: Any) -> str:
        return self.model_dump_json(by_alias=True, **kw)


class ValidationIssue(BaseModel):
    node_id: str | None = None
    message: str

    def __str__(self) -> str:
        prefix = f"[{self.node_id}] " if self.node_id else ""
        return f"{prefix}{self.message}"
