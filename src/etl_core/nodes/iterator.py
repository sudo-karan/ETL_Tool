"""iterator: ForEach / fan-out over a value set.

The engine executes the iterator's downstream subgraph once per value; the
current value is available to every node in that subgraph as ``$iter.value``
(and ``$iter.index``). Results fan back in per the ``fan_in`` mode:

* ``concat`` -- record lists from all iterations are concatenated in
  iteration order.
* ``keyed``  -- one record per iteration: ``{"key": <value>, "records": [...]}``.

Value sources: a literal ``array`` (which may itself be a whole-string
$upstream reference resolving to a list), a numeric ``range`` (end
exclusive), or ``from_upstream`` -- one value per upstream record, optionally
extracted via a dotted ``field`` path.
"""
from __future__ import annotations

from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..errors import ErrorCategory
from ..paths import get_path
from .base import Node, NodeContext, NodeInputs, NodeOutputs
from .registry import register_node

_MISSING = object()


class RangeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: int
    end: int
    step: int = 1

    @field_validator("step")
    @classmethod
    def _step_not_zero(cls, v: int) -> int:
        if v == 0:
            raise ValueError("step must not be 0")
        return v


class IteratorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["array", "range", "from_upstream"]
    array: Any = None
    range: RangeSpec | None = None
    # from_upstream: dotted path extracted from each upstream record;
    # omit to iterate over the records themselves.
    field: str | None = None
    fan_in: Literal["concat", "keyed"] = "concat"
    # Cap on concurrent iterations; defaults to the run's max_concurrency.
    max_concurrency: int | None = Field(None, ge=1)

    @model_validator(mode="after")
    def _check_mode_fields(self) -> "IteratorConfig":
        if self.mode == "array" and self.array is None:
            raise ValueError("array mode requires an 'array' value")
        if self.mode == "range" and self.range is None:
            raise ValueError("range mode requires a 'range' value")
        return self


@register_node
class IteratorNode(Node):
    type_name: ClassVar[str] = "iterator"
    config_model: ClassVar[type[BaseModel]] = IteratorConfig
    input_ports: ClassVar[tuple[str, ...]] = ("in",)
    output_ports: ClassVar[tuple[str, ...]] = ("out",)
    fan_out: ClassVar[bool] = True

    @classmethod
    def required_input_ports(cls, config: BaseModel) -> tuple[str, ...]:
        if isinstance(config, IteratorConfig) and config.mode == "from_upstream":
            return ("in",)
        return ()

    def get_values(self, inputs: NodeInputs, ctx: NodeContext) -> list[Any]:
        cfg: IteratorConfig = self.config  # type: ignore[assignment]
        if cfg.mode == "array":
            if not isinstance(cfg.array, list):
                raise ctx.error(
                    ErrorCategory.CONFIG,
                    "iterator 'array' must be (or resolve to) a list, got "
                    f"{type(cfg.array).__name__}",
                )
            return list(cfg.array)
        if cfg.mode == "range":
            assert cfg.range is not None  # enforced by the config validator
            return list(range(cfg.range.start, cfg.range.end, cfg.range.step))
        # from_upstream
        streams = inputs.get("in") or []
        if not streams:
            raise ctx.error(
                ErrorCategory.VALIDATION,
                "from_upstream iterator has no connected input",
            )
        records = streams[0]
        if cfg.field is None:
            return list(records)
        values: list[Any] = []
        for i, record in enumerate(records):
            value = get_path(record, cfg.field, default=_MISSING)
            if value is _MISSING:
                raise ctx.error(
                    ErrorCategory.VALIDATION,
                    f"field {cfg.field!r} missing from upstream record {i}",
                )
            values.append(value)
        return values

    async def run(self, inputs: NodeInputs, ctx: NodeContext) -> NodeOutputs:
        # The engine drives fan-out; running the iterator standalone just
        # materializes its value set.
        values = self.get_values(inputs, ctx)
        return {"out": [{"value": v, "index": i} for i, v in enumerate(values)]}
