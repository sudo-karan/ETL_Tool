"""Secrets resolution.

Pipeline JSON stores ``secret_ref`` names only -- never secret values. A
:class:`SecretsProvider` turns refs into values right before a run; the
engine itself receives an already-resolved mapping so it stays a pure
function of (spec, secrets, inputs).

Phase 1 ships the env-backed provider (dev/local). Phase 3 adds a provider
backed by AES-GCM/Fernet-encrypted rows in PostgreSQL using this same
interface.
"""
from __future__ import annotations

import os
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any

from .errors import SecretNotFoundError
from .schema import PipelineSpec


class SecretsProvider(ABC):
    @abstractmethod
    async def get(self, ref: str) -> str:
        """Return the secret value for ``ref`` or raise SecretNotFoundError."""


class EnvSecretsProvider(SecretsProvider):
    """Resolves ``secret_ref`` -> environment variable ``<prefix><ref>``.

    Only prefixed variables are readable so a pipeline cannot exfiltrate
    arbitrary process environment (PATH, AWS_*, ...).
    """

    def __init__(self, prefix: str = "ETL_SECRET_", env: Mapping[str, str] | None = None):
        self._prefix = prefix
        self._env = env if env is not None else os.environ

    async def get(self, ref: str) -> str:
        value = self._env.get(f"{self._prefix}{ref}")
        if value is None:
            raise SecretNotFoundError(ref)
        return value


class StaticSecretsProvider(SecretsProvider):
    """In-memory provider for tests and the CLI ``--secrets-file`` option."""

    def __init__(self, values: Mapping[str, str]):
        self._values = dict(values)

    async def get(self, ref: str) -> str:
        if ref not in self._values:
            raise SecretNotFoundError(ref)
        return self._values[ref]


def collect_refs_from_config(value: Any) -> set[str]:
    """All ``secret_ref`` values mentioned anywhere in a raw config structure."""
    refs: set[str] = set()

    def walk(item: Any) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                if key == "secret_ref" and isinstance(child, str):
                    refs.add(child)
                else:
                    walk(child)
        elif isinstance(item, list):
            for child in item:
                walk(child)

    walk(value)
    return refs


def collect_secret_refs(spec: PipelineSpec) -> set[str]:
    """All ``secret_ref`` values mentioned anywhere in the pipeline config."""
    refs: set[str] = set()
    for node in spec.nodes:
        refs |= collect_refs_from_config(node.config)
    return refs


async def resolve_secrets(
    spec: PipelineSpec, provider: SecretsProvider
) -> dict[str, str]:
    return {ref: await provider.get(ref) for ref in sorted(collect_secret_refs(spec))}
