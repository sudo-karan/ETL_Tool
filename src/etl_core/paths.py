"""Dotted-path navigation over JSON-like data.

Used by reference resolution ($upstream / $iter), pagination (items_path,
cursor_path), the iterator's from_upstream field extraction, transform
predicates and merge join keys, so all of them share one path dialect:
dot-separated segments, where an integer segment indexes into a list.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .errors import PathNotFoundError

_MISSING = object()


def split_path(path: str) -> list[str]:
    return [seg for seg in path.split(".") if seg != ""]


def _is_index(segment: str) -> bool:
    return segment.lstrip("-").isdigit()


def get_path(obj: Any, path: str | Sequence[str], default: Any = _MISSING) -> Any:
    """Navigate ``obj`` by dotted path. ``"a.b.0.c"`` -> obj["a"]["b"][0]["c"].

    Raises :class:`PathNotFoundError` when the path is absent, unless a
    ``default`` is supplied.
    """
    segments = split_path(path) if isinstance(path, str) else list(path)
    current = obj
    for i, segment in enumerate(segments):
        if isinstance(current, Mapping):
            if segment in current:
                current = current[segment]
                continue
        elif isinstance(current, Sequence) and not isinstance(current, (str, bytes)):
            if _is_index(segment):
                index = int(segment)
                if -len(current) <= index < len(current):
                    current = current[index]
                    continue
        if default is not _MISSING:
            return default
        traversed = ".".join(segments[: i + 1])
        raise PathNotFoundError(
            f"path {'.'.join(segments)!r} not found (failed at {traversed!r})"
        )
    return current


def has_path(obj: Any, path: str | Sequence[str]) -> bool:
    return get_path(obj, path, default=_MISSING) is not _MISSING
