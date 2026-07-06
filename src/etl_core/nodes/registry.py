"""Node type registry.

Write-once at import time (each node module registers its class when first
imported); never mutated during execution, so it does not compromise the
engine's statelessness.
"""
from __future__ import annotations

from typing import TypeVar

from .base import Node

NODE_REGISTRY: dict[str, type[Node]] = {}

N = TypeVar("N", bound=type[Node])


def register_node(cls: N) -> N:
    type_name = getattr(cls, "type_name", None)
    if not type_name:
        raise ValueError(f"{cls.__name__} must define a type_name")
    existing = NODE_REGISTRY.get(type_name)
    if existing is not None and existing is not cls:
        raise ValueError(f"node type {type_name!r} is already registered by {existing.__name__}")
    NODE_REGISTRY[type_name] = cls
    return cls


def get_node_class(type_name: str) -> type[Node] | None:
    return NODE_REGISTRY.get(type_name)
