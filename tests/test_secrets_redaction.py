"""Secrets providers, ref collection, and redaction."""
from __future__ import annotations

import base64

import pytest

from etl_core import PipelineSpec, collect_secret_refs, resolve_secrets
from etl_core.errors import SecretNotFoundError
from etl_core.redact import Redactor
from etl_core.secrets import EnvSecretsProvider, StaticSecretsProvider, collect_refs_from_config


async def test_env_provider_reads_prefixed_vars_only():
    provider = EnvSecretsProvider(env={"ETL_SECRET_TOK": "value-1", "PATH": "/bin"})
    assert await provider.get("TOK") == "value-1"
    with pytest.raises(SecretNotFoundError):
        await provider.get("PATH")  # unprefixed env is not reachable


async def test_static_provider_and_missing_ref():
    provider = StaticSecretsProvider({"A": "1"})
    assert await provider.get("A") == "1"
    with pytest.raises(SecretNotFoundError):
        await provider.get("B")


def test_collect_secret_refs_walks_nested_config():
    spec = PipelineSpec.model_validate(
        {
            "pipeline_id": "p",
            "nodes": [
                {
                    "id": "a",
                    "type": "api_source",
                    "config": {
                        "url": "https://x.test",
                        "auth": {"type": "bearer", "secret_ref": "TOKEN_A"},
                        "extra": [{"deep": {"secret_ref": "TOKEN_B"}}],
                    },
                }
            ],
            "edges": [],
        }
    )
    assert collect_secret_refs(spec) == {"TOKEN_A", "TOKEN_B"}


def test_collect_refs_from_config_ignores_non_strings():
    assert collect_refs_from_config({"secret_ref": 42}) == set()


async def test_resolve_secrets():
    spec = PipelineSpec.model_validate(
        {
            "pipeline_id": "p",
            "nodes": [
                {
                    "id": "a",
                    "type": "api_source",
                    "config": {"url": "https://x.test", "auth": {"type": "bearer", "secret_ref": "T"}},
                }
            ],
            "edges": [],
        }
    )
    resolved = await resolve_secrets(spec, StaticSecretsProvider({"T": "v"}))
    assert resolved == {"T": "v"}


# -- Redactor ---------------------------------------------------------------
def test_redacts_plain_quoted_and_base64_variants():
    secret = "se cret+value"
    redactor = Redactor([secret])
    assert secret not in redactor.redact(f"error with {secret} inside")
    assert "se%20cret%2Bvalue" not in redactor.redact("url?x=se%20cret%2Bvalue")
    encoded = base64.b64encode(secret.encode()).decode()
    assert encoded not in redactor.redact(f"Basic {encoded}")


def test_short_secrets_are_not_substring_replaced():
    redactor = Redactor(["ab"])
    assert redactor.redact("about") == "about"


def test_redact_url_masks_sensitive_param_names_even_without_known_secret():
    redactor = Redactor([])
    masked = redactor.redact_url("https://x.test/p?api_key=abc123&page=2")
    assert "abc123" not in masked
    assert "page=2" in masked


def test_redact_url_masks_userinfo():
    redactor = Redactor([])
    masked = redactor.redact_url("https://user:pw12345@x.test/p")
    assert "pw12345" not in masked
    assert "x.test" in masked


def test_request_summary():
    redactor = Redactor(["tok-secret-1"])
    summary = redactor.request_summary("get", "https://x.test/a?token=tok-secret-1")
    assert summary.startswith("GET ")
    assert "tok-secret-1" not in summary


def test_redact_obj_recurses():
    redactor = Redactor(["deep-secret-1"])
    scrubbed = redactor.redact_obj({"a": ["deep-secret-1", {"b": "x deep-secret-1 y"}]})
    assert "deep-secret-1" not in str(scrubbed)
