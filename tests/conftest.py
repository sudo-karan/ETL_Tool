"""Shared fixtures: local HTTP/TLS servers and test-only plugin nodes.

The test-only nodes (static_source, probe) double as a test of the plugin
interface itself: they register through the public decorator and the engine
runs them without any engine changes.
"""
from __future__ import annotations

import asyncio
import datetime
import ipaddress
import json
import ssl
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict

from etl_core import ExecutionOptions, PipelineSpec, SSRFPolicy, execute_pipeline
from etl_core.nodes.base import Node, NodeContext, NodeInputs, NodeOutputs
from etl_core.nodes.registry import NODE_REGISTRY, register_node

BEARER_TOKEN = "sekret-token-123456"


# --------------------------------------------------------------------------
# Test-only plugin nodes
# --------------------------------------------------------------------------
class StaticSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    records: list[dict[str, Any]] = []


class ProbeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    delay_s: float = 0.02


if "static_source" not in NODE_REGISTRY:

    @register_node
    class StaticSourceNode(Node):
        type_name = "static_source"
        config_model = StaticSourceConfig
        input_ports = ("in",)

        async def run(self, inputs: NodeInputs, ctx: NodeContext) -> NodeOutputs:
            return {"out": [dict(record) for record in self.config.records]}


if "probe" not in NODE_REGISTRY:

    @register_node
    class ProbeNode(Node):
        """Tracks how many instances run concurrently (class-level, test-only)."""

        type_name = "probe"
        config_model = ProbeConfig
        input_ports = ("in",)

        active = 0
        max_active = 0

        async def run(self, inputs: NodeInputs, ctx: NodeContext) -> NodeOutputs:
            cls = type(self)
            cls.active += 1
            cls.max_active = max(cls.max_active, cls.active)
            try:
                await asyncio.sleep(self.config.delay_s)
            finally:
                cls.active -= 1
            return {"out": [{"ok": True}]}


@pytest.fixture
def probe_node_class():
    cls = NODE_REGISTRY["probe"]
    cls.active = 0
    cls.max_active = 0
    return cls


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def no_ssrf_options(**kw: Any) -> ExecutionOptions:
    """Options for tests using mocked/hermetic hosts (no real resolution)."""
    return ExecutionOptions(ssrf_policy=SSRFPolicy(enabled=False), **kw)


def local_policy() -> SSRFPolicy:
    """Guard enabled but 127.0.0.1 allowlisted (also exercises the allowlist)."""
    return SSRFPolicy(allow_hosts=["127.0.0.1"])


async def run_pipeline(spec_dict: dict[str, Any], secrets: dict[str, str] | None = None, **options: Any):
    spec = PipelineSpec.model_validate(spec_dict)
    return await execute_pipeline(spec, secrets, no_ssrf_options(**options))


# --------------------------------------------------------------------------
# Local HTTP server (plain + TLS)
# --------------------------------------------------------------------------
class _Handler(BaseHTTPRequestHandler):
    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _respond(self) -> None:
        path = self.path.split("?")[0]
        if path.startswith("/private"):
            if self.headers.get("Authorization") != f"Bearer {BEARER_TOKEN}":
                self._send_json({"error": "unauthorized"}, status=401)
            else:
                self._send_json([{"id": 1, "owner": "alice"}])
        else:
            self._send_json([{"id": 1, "name": "Ann"}, {"id": 2, "name": "Bob"}])

    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        self._respond()

    def do_HEAD(self) -> None:  # noqa: N802
        self._respond()

    def log_message(self, *args: Any) -> None:  # silence test output
        pass


def _start_server(server: ThreadingHTTPServer) -> threading.Thread:
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread


@pytest.fixture(scope="session")
def http_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    _start_server(server)
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def _make_self_signed_cert(cert_path, key_path) -> None:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                    x509.DNSName("localhost"),
                ]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )


@pytest.fixture(scope="session")
def tls_server(tmp_path_factory):
    cert_dir = tmp_path_factory.mktemp("certs")
    cert_path, key_path = cert_dir / "cert.pem", cert_dir / "key.pem"
    _make_self_signed_cert(cert_path, key_path)

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_path, key_path)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    _start_server(server)
    yield f"https://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
