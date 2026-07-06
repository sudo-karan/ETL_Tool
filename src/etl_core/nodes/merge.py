"""merge: combine 2+ input streams.

* ``concat`` -- append streams in edge-declaration order.
* ``union``  -- concat, then drop exact-duplicate records.
* ``join``   -- hash join of exactly two inputs (first edge = left, second =
  right) on ``keys`` (dotted paths allowed), ``how`` = inner | left | outer.
  Non-key right-side fields that collide with a left field are suffixed
  (default ``_right``). Records missing a join key never match; left/outer
  keep them on their respective sides.

Records are treated as nested JSON throughout (no early flattening); Arrow /
polars materialization arrives with the tabular file/db nodes in Phase 2.
"""
from __future__ import annotations

import json
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..errors import ErrorCategory
from ..paths import get_path
from .base import Node, NodeContext, NodeInputs, NodeOutputs, Records
from .registry import register_node

_MISSING = object()


class MergeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy: Literal["concat", "union", "join"] = "concat"
    keys: list[str] | None = None
    how: Literal["inner", "left", "outer"] = "inner"
    suffix: str = Field("_right", min_length=1)

    @model_validator(mode="after")
    def _check_join_fields(self) -> "MergeConfig":
        if self.strategy == "join" and not self.keys:
            raise ValueError("join requires non-empty 'keys'")
        return self


def _canonical(record: dict[str, Any]) -> str:
    return json.dumps(record, sort_keys=True, default=repr)


def _key_of(record: dict[str, Any], keys: list[str]) -> tuple[Any, ...] | None:
    parts: list[Any] = []
    for key in keys:
        value = get_path(record, key, default=_MISSING)
        if value is _MISSING:
            return None
        if isinstance(value, (dict, list)):
            value = _canonical(value) if isinstance(value, dict) else json.dumps(value, default=repr)
        parts.append(value)
    try:
        hash(tuple(parts))
    except TypeError:
        return None
    return tuple(parts)


@register_node
class MergeNode(Node):
    type_name: ClassVar[str] = "merge"
    config_model: ClassVar[type[BaseModel]] = MergeConfig
    input_ports: ClassVar[tuple[str, ...]] = ("in",)
    output_ports: ClassVar[tuple[str, ...]] = ("out",)
    allow_multi_input: ClassVar[bool] = True

    @classmethod
    def required_input_ports(cls, config: BaseModel) -> tuple[str, ...]:
        return ("in",)

    @classmethod
    def check_spec(cls, config: BaseModel, in_edge_counts: dict[str, int]) -> list[str]:
        count = in_edge_counts.get("in", 0)
        issues: list[str] = []
        if isinstance(config, MergeConfig) and config.strategy == "join":
            if count != 2:
                issues.append(f"join merge requires exactly 2 inputs, found {count}")
        elif count < 2:
            issues.append(f"merge requires at least 2 inputs, found {count}")
        return issues

    async def run(self, inputs: NodeInputs, ctx: NodeContext) -> NodeOutputs:
        cfg: MergeConfig = self.config  # type: ignore[assignment]
        streams = inputs.get("in") or []
        if cfg.strategy == "concat":
            merged = [record for stream in streams for record in stream]
        elif cfg.strategy == "union":
            merged = []
            seen: set[str] = set()
            for stream in streams:
                for record in stream:
                    marker = _canonical(record)
                    if marker not in seen:
                        seen.add(marker)
                        merged.append(record)
        else:
            merged = self._join(streams, cfg, ctx)
        ctx.info(f"merged {len(streams)} input(s) into {len(merged)} record(s)")
        return {"out": merged}

    def _join(self, streams: list[Records], cfg: MergeConfig, ctx: NodeContext) -> Records:
        if len(streams) != 2:
            raise ctx.error(
                ErrorCategory.VALIDATION,
                f"join requires exactly 2 inputs, got {len(streams)}",
            )
        left, right = streams
        keys = cfg.keys or []

        right_index: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        keyless_right: list[dict[str, Any]] = []
        for record in right:
            key = _key_of(record, keys)
            if key is None:
                keyless_right.append(record)
            else:
                right_index.setdefault(key, []).append(record)

        matched: set[tuple[Any, ...]] = set()
        out: Records = []
        for record in left:
            key = _key_of(record, keys)
            matches = right_index.get(key) if key is not None else None
            if matches:
                matched.add(key)  # type: ignore[arg-type]
                for right_record in matches:
                    combined = dict(record)
                    for field, value in right_record.items():
                        if field in keys:
                            continue
                        if field in combined:
                            combined[f"{field}{cfg.suffix}"] = value
                        else:
                            combined[field] = value
                    out.append(combined)
            elif cfg.how in ("left", "outer"):
                out.append(dict(record))
        if cfg.how == "outer":
            for key, records in right_index.items():
                if key not in matched:
                    out.extend(dict(r) for r in records)
            out.extend(dict(r) for r in keyless_right)
        return out
