"""Runtime resolution of $upstream / $iter references in node config.

Two forms are supported anywhere in a node's config JSON:

* Whole-string references keep the referenced value's type::

      "user_id": "$upstream.n1.id"        # -> 7 (int), from n1's first record
      "values":  "$upstream.n1"           # -> n1's full record list

* Embedded ``${...}`` templates interpolate into strings::

      "url": "https://api.example.com/users/${iter.value}/posts"

``$upstream.<node_id>.<path>`` resolves against the named node's primary
output records: an integer first path segment picks a record by index,
otherwise the first record is used. ``$iter.value`` / ``$iter.index`` are the
current iterator value/position and only exist inside an iterator's scope.

A leading ``$$`` escapes a literal dollar sign ("$$upstream" -> "$upstream").
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .errors import PathNotFoundError, ReferenceResolutionError
from .paths import get_path, split_path

WHOLE_RE = re.compile(r"^\$(upstream|iter)\.([^\s{}]+)$")
EMBED_RE = re.compile(r"\$\{(upstream|iter)\.([^}]+)\}")


@dataclass(frozen=True)
class IterContext:
    value: Any
    index: int


@dataclass(frozen=True)
class ReferenceContext:
    upstream: Mapping[str, list[dict[str, Any]]]  # node_id -> primary output records
    iter: IterContext | None = None


def _resolve_reference(kind: str, path: str, ctx: ReferenceContext) -> Any:
    segments = split_path(path)
    if not segments:
        raise ReferenceResolutionError(f"empty ${kind} reference")

    if kind == "iter":
        if ctx.iter is None:
            raise ReferenceResolutionError(
                "$iter reference used outside of an iterator scope"
            )
        head, *rest = segments
        if head == "value":
            base: Any = ctx.iter.value
        elif head == "index":
            base = ctx.iter.index
        else:
            raise ReferenceResolutionError(
                f"unknown $iter field {head!r} (expected 'value' or 'index')"
            )
        if not rest:
            return base
        try:
            return get_path(base, rest)
        except PathNotFoundError as exc:
            raise ReferenceResolutionError(f"$iter.{path}: {exc}") from exc

    node_id, *rest = segments
    if node_id not in ctx.upstream:
        raise ReferenceResolutionError(
            f"$upstream reference to {node_id!r}, which has no available output "
            "(not an upstream node of this one, or it has not run)"
        )
    records = ctx.upstream[node_id]
    if not rest:
        return records
    if rest[0].lstrip("-").isdigit():
        index = int(rest[0])
        rest = rest[1:]
        if not (-len(records) <= index < len(records)):
            raise ReferenceResolutionError(
                f"$upstream.{path}: record index {index} out of range "
                f"({len(records)} records)"
            )
        record: Any = records[index]
    else:
        if not records:
            raise ReferenceResolutionError(
                f"$upstream.{path}: node {node_id!r} produced no records"
            )
        record = records[0]
    if not rest:
        return record
    try:
        return get_path(record, rest)
    except PathNotFoundError as exc:
        raise ReferenceResolutionError(f"$upstream.{path}: {exc}") from exc


def resolve_config(value: Any, ctx: ReferenceContext) -> Any:
    """Recursively resolve references in a raw config structure."""
    if isinstance(value, str):
        if value.startswith("$$"):
            return value[1:]
        whole = WHOLE_RE.match(value)
        if whole:
            return _resolve_reference(whole.group(1), whole.group(2), ctx)
        return EMBED_RE.sub(
            lambda m: str(_resolve_reference(m.group(1), m.group(2), ctx)), value
        )
    if isinstance(value, dict):
        return {key: resolve_config(item, ctx) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_config(item, ctx) for item in value]
    return value


def find_references(value: Any) -> list[tuple[str, str]]:
    """Collect (kind, path) for every reference in a raw config structure.

    Used by graph validation to check that $upstream targets are ancestors
    and $iter is only used under an iterator.
    """
    found: list[tuple[str, str]] = []
    if isinstance(value, str):
        if value.startswith("$$"):
            return found
        whole = WHOLE_RE.match(value)
        if whole:
            found.append((whole.group(1), whole.group(2)))
        else:
            for match in EMBED_RE.finditer(value):
                found.append((match.group(1), match.group(2)))
    elif isinstance(value, dict):
        for item in value.values():
            found.extend(find_references(item))
    elif isinstance(value, list):
        for item in value:
            found.extend(find_references(item))
    return found


def has_references(value: Any) -> bool:
    return bool(find_references(value))
