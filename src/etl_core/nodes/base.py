"""Node plugin interface.

New node types subclass :class:`Node` and register with ``@register_node``;
the engine discovers everything through the registry, so adding a node type
requires no engine changes. The one carve-out is control-flow: a node class
with ``fan_out = True`` (the iterator) asks the engine to execute the node's
downstream subgraph once per value -- data nodes never need this.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from pydantic import BaseModel

from ..context import RunContext
from ..errors import ErrorCategory, NodeError, NodeExecutionError
from ..events import LogEvent, LogLevel
from ..references import IterContext

Records = list[dict[str, Any]]
# Every input port receives a list of record-streams, one per inbound edge
# (in edge declaration order). Single-input nodes read inputs["in"][0].
NodeInputs = dict[str, list[Records]]
NodeOutputs = dict[str, Records]


class NodeContext:
    """Per-node-execution view of the run: logging, secrets, errors."""

    def __init__(
        self,
        run: RunContext,
        node_id: str,
        node_type: str,
        iter_ctx: IterContext | None = None,
    ):
        self.run = run
        self.node_id = node_id
        self.node_type = node_type
        self.iter_ctx = iter_ctx

    # -- logging ---------------------------------------------------------
    def log(self, level: LogLevel, message: str, data: dict[str, Any] | None = None) -> LogEvent:
        return self.run.log.log(
            level,
            self.run.redactor.redact(message),
            node_id=self.node_id,
            data=self.run.redactor.redact_obj(data) if data else None,
        )

    def debug(self, message: str, data: dict[str, Any] | None = None) -> LogEvent:
        return self.log(LogLevel.DEBUG, message, data)

    def info(self, message: str, data: dict[str, Any] | None = None) -> LogEvent:
        return self.log(LogLevel.INFO, message, data)

    def warning(self, message: str, data: dict[str, Any] | None = None) -> LogEvent:
        return self.log(LogLevel.WARNING, message, data)

    # -- secrets ---------------------------------------------------------
    def get_secret(self, ref: str) -> str:
        try:
            return self.run.secrets[ref]
        except KeyError:
            raise self.error(
                ErrorCategory.CONFIG,
                f"secret {ref!r} was not provided to this run",
            ) from None

    # -- errors ----------------------------------------------------------
    def error(
        self,
        category: ErrorCategory,
        message: str,
        *,
        http_status: int | None = None,
        request_summary: str | None = None,
        attempts: int = 1,
        details: dict[str, Any] | None = None,
    ) -> NodeExecutionError:
        """Build a structured, secret-redacted error ready to raise."""
        redactor = self.run.redactor
        return NodeExecutionError(
            NodeError(
                node_id=self.node_id,
                node_type=self.node_type,
                category=category,
                message=redactor.redact(message),
                http_status=http_status,
                request_summary=redactor.redact(request_summary) if request_summary else None,
                attempts=attempts,
                details=redactor.redact_obj(details) if details else None,
            )
        )


class Node(ABC):
    """Base class for all node types."""

    type_name: ClassVar[str]
    config_model: ClassVar[type[BaseModel]]
    input_ports: ClassVar[tuple[str, ...]] = ()
    output_ports: ClassVar[tuple[str, ...]] = ("out",)
    # True for nodes (merge) that accept several edges into one port.
    allow_multi_input: ClassVar[bool] = False
    # True for control-flow nodes whose downstream subgraph the engine
    # executes once per value (iterator).
    fan_out: ClassVar[bool] = False

    def __init__(self, node_id: str, config: BaseModel):
        self.node_id = node_id
        self.config = config

    @classmethod
    def required_input_ports(cls, config: BaseModel) -> tuple[str, ...]:
        """Ports that must have at least one inbound edge for this config."""
        return ()

    @classmethod
    def check_spec(cls, config: BaseModel, in_edge_counts: dict[str, int]) -> list[str]:
        """Extra node-specific graph validation; returns issue messages."""
        return []

    @abstractmethod
    async def run(self, inputs: NodeInputs, ctx: NodeContext) -> NodeOutputs:
        ...
