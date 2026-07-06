"""SSRF guard: default-deny private ranges, allowlist, enforcement in api_source."""
from __future__ import annotations

import pytest

from conftest import run_pipeline
from etl_core import SSRFPolicy
from etl_core.engine import RunStatus
from etl_core.errors import SSRFBlockedError
from etl_core.ssrf import ensure_url_allowed, find_blocked


@pytest.mark.parametrize(
    "ip",
    [
        "127.0.0.1",  # loopback
        "10.1.2.3",  # RFC1918
        "172.16.0.9",  # RFC1918
        "192.168.1.1",  # RFC1918
        "169.254.169.254",  # cloud metadata / link-local
        "100.64.0.1",  # CGNAT
        "0.0.0.0",  # unspecified
        "::1",  # IPv6 loopback
        "fe80::1",  # IPv6 link-local
        "fd00::1",  # IPv6 unique-local
        "::ffff:10.0.0.1",  # IPv4-mapped private
    ],
)
def test_private_and_metadata_ranges_blocked_by_default(ip):
    assert find_blocked("some-host", [ip], SSRFPolicy()) is not None


@pytest.mark.parametrize("ip", ["93.184.216.34", "1.1.1.1", "2606:4700::1111"])
def test_public_addresses_allowed(ip):
    assert find_blocked("example.com", [ip], SSRFPolicy()) is None


def test_hostname_allowlist_bypasses_ip_check():
    policy = SSRFPolicy(allow_hosts=["internal.corp"])
    assert find_blocked("internal.corp", ["10.0.0.5"], policy) is None
    assert find_blocked("INTERNAL.CORP", ["10.0.0.5"], policy) is None  # case-insensitive
    assert find_blocked("other.corp", ["10.0.0.5"], policy) is not None


def test_cidr_allowlist():
    policy = SSRFPolicy(allow_hosts=["10.2.0.0/16"])
    assert find_blocked("db.internal", ["10.2.3.4"], policy) is None
    assert find_blocked("db.internal", ["10.3.0.1"], policy) is not None


def test_single_ip_allowlist():
    policy = SSRFPolicy(allow_hosts=["192.168.1.50"])
    assert find_blocked("nas.local", ["192.168.1.50"], policy) is None
    assert find_blocked("nas.local", ["192.168.1.51"], policy) is not None


def test_disabled_policy_allows_everything():
    assert find_blocked("x", ["127.0.0.1"], SSRFPolicy(enabled=False)) is None


async def test_ensure_url_allowed_blocks_ip_literal_without_dns():
    with pytest.raises(SSRFBlockedError):
        await ensure_url_allowed("http://127.0.0.1:8080/x", SSRFPolicy())
    with pytest.raises(SSRFBlockedError):
        await ensure_url_allowed("http://169.254.169.254/latest/meta-data/", SSRFPolicy())


async def test_ensure_url_allowed_passes_allowlisted_hostname_without_dns():
    # would raise gaierror if it tried to resolve this fake host
    await ensure_url_allowed("http://api.test/x", SSRFPolicy(allow_hosts=["api.test"]))


async def test_api_source_run_blocked_by_default_policy(http_server):
    spec = {
        "pipeline_id": "p",
        "nodes": [{"id": "api", "type": "api_source", "config": {"url": f"{http_server}/users"}}],
        "edges": [],
    }
    from etl_core import ExecutionOptions, PipelineSpec, execute_pipeline

    result = await execute_pipeline(
        PipelineSpec.model_validate(spec), options=ExecutionOptions()  # default policy ON
    )
    assert result.status == RunStatus.FAILED
    error = result.errors[0]
    assert error.category.value == "config"
    assert "blocked" in error.message


async def test_api_source_run_allowed_with_allowlist(http_server):
    from etl_core import ExecutionOptions, PipelineSpec, execute_pipeline

    spec = PipelineSpec.model_validate(
        {
            "pipeline_id": "p",
            "nodes": [{"id": "api", "type": "api_source", "config": {"url": f"{http_server}/users"}}],
            "edges": [],
        }
    )
    options = ExecutionOptions(ssrf_policy=SSRFPolicy(allow_hosts=["127.0.0.1"]))
    result = await execute_pipeline(spec, options=options)
    assert result.status == RunStatus.SUCCEEDED, [e.message for e in result.errors]
    assert result.outputs["api"] == [{"id": 1, "name": "Ann"}, {"id": 2, "name": "Bob"}]
