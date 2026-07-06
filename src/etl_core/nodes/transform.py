"""transform: per-record operations, applied in order.

* ``select``   -- keep only the listed fields (dotted paths allowed; the
  output key is the path string). Missing fields are omitted.
* ``rename``   -- rename top-level fields ({old: new}).
* ``filter``   -- keep records matching a structured predicate: a condition
  ``{field, op, value}`` or a boolean group ``{"all": [...]}, {"any": [...]},
  {"not": ...}``. A missing field satisfies only ``not_exists``.
* ``computed`` -- add/overwrite a field from a safe arithmetic/boolean
  expression over the record's fields (evaluated by a whitelisted-AST
  interpreter -- no Python eval). Missing fields read as None.

Input records are never mutated; every op builds new records.
"""
from __future__ import annotations

import ast
import re
from typing import Annotated, Any, Callable, ClassVar, Literal, Union

from pydantic import BaseModel, ConfigDict, Field

from ..errors import ErrorCategory
from ..paths import get_path
from .base import Node, NodeContext, NodeInputs, NodeOutputs, Records
from .registry import register_node

_MISSING = object()


# --------------------------------------------------------------------------
# Predicates
# --------------------------------------------------------------------------
class Condition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str
    op: Literal[
        "eq", "ne", "gt", "gte", "lt", "lte",
        "in", "not_in", "contains", "regex",
        "exists", "not_exists",
    ]
    value: Any = None


class AllOf(BaseModel):
    model_config = ConfigDict(extra="forbid")

    all: "list[Predicate]"


class AnyOf(BaseModel):
    model_config = ConfigDict(extra="forbid")

    any: "list[Predicate]"


class NotOf(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    not_: "Predicate" = Field(alias="not")


Predicate = Union[Condition, AllOf, AnyOf, NotOf]
AllOf.model_rebuild()
AnyOf.model_rebuild()
NotOf.model_rebuild()


def _compare(op: str, left: Any, right: Any) -> bool:
    try:
        if op == "eq":
            return bool(left == right)
        if op == "ne":
            return bool(left != right)
        if op == "gt":
            return bool(left > right)
        if op == "gte":
            return bool(left >= right)
        if op == "lt":
            return bool(left < right)
        if op == "lte":
            return bool(left <= right)
        if op == "in":
            return left in right
        if op == "not_in":
            return left not in right
        if op == "contains":
            return right in left
        if op == "regex":
            return re.search(str(right), str(left)) is not None
    except TypeError:
        # Incomparable types (e.g. "a" > 1) fail the condition rather than
        # the run; heterogeneous API data is the norm, not the exception.
        return False
    raise ValueError(f"unknown condition op {op!r}")


def eval_predicate(predicate: Predicate, record: dict[str, Any]) -> bool:
    if isinstance(predicate, AllOf):
        return all(eval_predicate(p, record) for p in predicate.all)
    if isinstance(predicate, AnyOf):
        return any(eval_predicate(p, record) for p in predicate.any)
    if isinstance(predicate, NotOf):
        return not eval_predicate(predicate.not_, record)
    value = get_path(record, predicate.field, default=_MISSING)
    if predicate.op == "exists":
        return value is not _MISSING
    if predicate.op == "not_exists":
        return value is _MISSING
    if value is _MISSING:
        return False
    return _compare(predicate.op, value, predicate.value)


# --------------------------------------------------------------------------
# Safe computed-column expressions
# --------------------------------------------------------------------------
_ALLOWED_FUNCTIONS: dict[str, Callable[..., Any]] = {
    "len": len,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "round": round,
    "abs": abs,
    "min": min,
    "max": max,
    "lower": lambda value: str(value).lower(),
    "upper": lambda value: str(value).upper(),
    "strip": lambda value: str(value).strip(),
}

_ALLOWED_BINOPS: dict[type[ast.operator], Callable[[Any, Any], Any]] = {
    ast.Add: lambda a, b: a + b,
    ast.Sub: lambda a, b: a - b,
    ast.Mult: lambda a, b: a * b,
    ast.Div: lambda a, b: a / b,
    ast.FloorDiv: lambda a, b: a // b,
    ast.Mod: lambda a, b: a % b,
    ast.Pow: lambda a, b: a**b,
}

_ALLOWED_CMPOPS: dict[type[ast.cmpop], Callable[[Any, Any], bool]] = {
    ast.Eq: lambda a, b: a == b,
    ast.NotEq: lambda a, b: a != b,
    ast.Lt: lambda a, b: a < b,
    ast.LtE: lambda a, b: a <= b,
    ast.Gt: lambda a, b: a > b,
    ast.GtE: lambda a, b: a >= b,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
}


class ExpressionError(Exception):
    pass


def compile_expression(expression: str) -> Callable[[dict[str, Any]], Any]:
    """Parse and validate an expression, returning an evaluator over a record.

    Only literals, record-field names, arithmetic/boolean/comparison
    operators, conditional expressions, subscripts, dict-key attribute
    access and a small function whitelist are permitted. There is no access
    to builtins, imports, attributes of Python objects, or comprehensions.
    """
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as exc:
        raise ExpressionError(f"invalid expression {expression!r}: {exc.msg}") from exc

    def evaluate(node: ast.AST, record: dict[str, Any]) -> Any:
        if isinstance(node, ast.Expression):
            return evaluate(node.body, record)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (str, int, float, bool, type(None))):
                return node.value
            raise ExpressionError(f"literal {node.value!r} is not allowed")
        if isinstance(node, ast.Name):
            return record.get(node.id)
        if isinstance(node, ast.Attribute):
            base = evaluate(node.value, record)
            if isinstance(base, dict):
                return base.get(node.attr)
            raise ExpressionError(
                f"attribute access '.{node.attr}' is only allowed on objects (dicts)"
            )
        if isinstance(node, ast.Subscript):
            base = evaluate(node.value, record)
            index = evaluate(node.slice, record)
            try:
                return base[index]
            except (KeyError, IndexError, TypeError) as exc:
                raise ExpressionError(f"subscript failed: {exc}") from exc
        if isinstance(node, ast.BinOp):
            handler = _ALLOWED_BINOPS.get(type(node.op))
            if handler is None:
                raise ExpressionError(f"operator {type(node.op).__name__} is not allowed")
            return handler(evaluate(node.left, record), evaluate(node.right, record))
        if isinstance(node, ast.UnaryOp):
            operand = evaluate(node.operand, record)
            if isinstance(node.op, ast.USub):
                return -operand
            if isinstance(node.op, ast.UAdd):
                return +operand
            if isinstance(node.op, ast.Not):
                return not operand
            raise ExpressionError(f"operator {type(node.op).__name__} is not allowed")
        if isinstance(node, ast.BoolOp):
            if isinstance(node.op, ast.And):
                result: Any = True
                for value_node in node.values:
                    result = evaluate(value_node, record)
                    if not result:
                        return result
                return result
            result = False
            for value_node in node.values:
                result = evaluate(value_node, record)
                if result:
                    return result
            return result
        if isinstance(node, ast.Compare):
            left = evaluate(node.left, record)
            for op, comparator in zip(node.ops, node.comparators):
                handler = _ALLOWED_CMPOPS.get(type(op))
                if handler is None:
                    raise ExpressionError(f"comparison {type(op).__name__} is not allowed")
                right = evaluate(comparator, record)
                if not handler(left, right):
                    return False
                left = right
            return True
        if isinstance(node, ast.IfExp):
            condition = evaluate(node.test, record)
            return evaluate(node.body if condition else node.orelse, record)
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or node.func.id not in _ALLOWED_FUNCTIONS:
                raise ExpressionError(
                    "only these functions are allowed: "
                    + ", ".join(sorted(_ALLOWED_FUNCTIONS))
                )
            if node.keywords:
                raise ExpressionError("keyword arguments are not allowed")
            args = [evaluate(arg, record) for arg in node.args]
            return _ALLOWED_FUNCTIONS[node.func.id](*args)
        if isinstance(node, (ast.List, ast.Tuple)):
            return [evaluate(item, record) for item in node.elts]
        raise ExpressionError(f"expression element {type(node).__name__} is not allowed")

    return lambda record: evaluate(tree, record)


# --------------------------------------------------------------------------
# Ops
# --------------------------------------------------------------------------
class SelectOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["select"]
    fields: list[str] = Field(min_length=1)


class RenameOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["rename"]
    mapping: dict[str, str] = Field(min_length=1)


class FilterOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["filter"]
    predicate: Predicate


class ComputedOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["computed"]
    target: str = Field(min_length=1)
    expression: str = Field(min_length=1)


TransformOp = Annotated[
    Union[SelectOp, RenameOp, FilterOp, ComputedOp], Field(discriminator="op")
]


class TransformConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ops: list[TransformOp] = Field(min_length=1)


@register_node
class TransformNode(Node):
    type_name: ClassVar[str] = "transform"
    config_model: ClassVar[type[BaseModel]] = TransformConfig
    input_ports: ClassVar[tuple[str, ...]] = ("in",)
    output_ports: ClassVar[tuple[str, ...]] = ("out",)

    @classmethod
    def required_input_ports(cls, config: BaseModel) -> tuple[str, ...]:
        return ("in",)

    async def run(self, inputs: NodeInputs, ctx: NodeContext) -> NodeOutputs:
        cfg: TransformConfig = self.config  # type: ignore[assignment]
        records = inputs["in"][0]
        for op in cfg.ops:
            records = self._apply(op, records, ctx)
        ctx.info(f"transformed to {len(records)} record(s)")
        return {"out": records}

    def _apply(self, op: Any, records: Records, ctx: NodeContext) -> Records:
        if isinstance(op, SelectOp):
            out: Records = []
            for record in records:
                projected: dict[str, Any] = {}
                for field in op.fields:
                    value = get_path(record, field, default=_MISSING)
                    if value is not _MISSING:
                        projected[field] = value
                out.append(projected)
            return out
        if isinstance(op, RenameOp):
            return [
                {op.mapping.get(key, key): value for key, value in record.items()}
                for record in records
            ]
        if isinstance(op, FilterOp):
            return [record for record in records if eval_predicate(op.predicate, record)]
        # computed
        try:
            evaluator = compile_expression(op.expression)
        except ExpressionError as exc:
            raise ctx.error(ErrorCategory.CONFIG, str(exc)) from exc
        out = []
        for i, record in enumerate(records):
            new_record = dict(record)
            try:
                new_record[op.target] = evaluator(record)
            except ExpressionError as exc:
                raise ctx.error(
                    ErrorCategory.TRANSFORM,
                    f"computed field {op.target!r} failed: {exc}",
                    details={"expression": op.expression, "record_index": i},
                ) from exc
            except Exception as exc:  # arithmetic on None, bad casts, ...
                raise ctx.error(
                    ErrorCategory.TRANSFORM,
                    f"computed field {op.target!r} failed on record {i}: "
                    f"{type(exc).__name__}: {exc}",
                    details={"expression": op.expression, "record_index": i},
                ) from exc
            out.append(new_record)
        return out
