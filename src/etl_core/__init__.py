"""etl_core: the headless, stateless pipeline execution engine (Phase 1).

Layering contract: this package is a pure function of
(pipeline spec, resolved secrets, options) -> (outputs, structured logs and
errors). The server (Phase 3) invokes it once per run inside a worker; the
UI (Phase 4) emits the same JSON schema it consumes. Nothing in here touches
module-level mutable state, databases, or global config.
"""
from .context import ExecutionOptions
from .crypto import Cipher, CryptoError, make_cipher
from .diagnostics import DiagnosticCheck, DiagnosticReport, test_connection
from .engine import (
    NodeResult,
    NodeStatus,
    RunResult,
    RunStatus,
    execute_pipeline,
    validate_pipeline,
)
from .errors import ErrorCategory, NodeError
from .fileio import FileAccessPolicy
from .schema import EdgeSpec, NodeSpec, PipelineSpec, ValidationIssue
from .secrets import (
    EnvSecretsProvider,
    SecretsProvider,
    StaticSecretsProvider,
    collect_secret_refs,
    resolve_secrets,
)
from .ssrf import SSRFPolicy

from . import nodes  # noqa: F401  -- importing registers the built-in node types

__version__ = "0.2.0"

__all__ = [
    "Cipher",
    "CryptoError",
    "DiagnosticCheck",
    "DiagnosticReport",
    "EdgeSpec",
    "EnvSecretsProvider",
    "ErrorCategory",
    "ExecutionOptions",
    "FileAccessPolicy",
    "NodeError",
    "NodeResult",
    "NodeSpec",
    "NodeStatus",
    "PipelineSpec",
    "RunResult",
    "RunStatus",
    "SSRFPolicy",
    "SecretsProvider",
    "StaticSecretsProvider",
    "ValidationIssue",
    "collect_secret_refs",
    "execute_pipeline",
    "make_cipher",
    "resolve_secrets",
    "test_connection",
    "validate_pipeline",
    "__version__",
]
