"""The pure, stateless execution engine.

``execute_pipeline`` is a function of (pipeline spec, resolved secrets,
options) -> RunResult. All state lives in locals created per call -- no
module-level mutable state -- so any number of runs can execute concurrently
(in one event loop or across arq workers) without interfering.

Execution model:

* The DAG is validated (unique ids, known types, valid config, connected
  ports, acyclic, reference targets are ancestors, iterator scopes are
  disjoint and un-nested) BEFORE anything runs.
* Nodes execute in topological order; each node's output is buffered for
  its downstream consumers.
* An iterator node fans out: the engine executes the iterator's downstream
  subgraph once per value (concurrently, capped by max_concurrency) and
  fans the per-iteration outputs back in (concat or keyed).
* Failures produce structured :class:`NodeError` objects. Default is
  fail-fast; with ``continue_on_error`` unaffected branches and iterations
  keep running and downstream nodes of the failure are marked skipped.
"""
from __future__ import annotations

import asyncio
import time
from collections import ChainMap, defaultdict
from collections.abc import Mapping
from datetime import datetime
from enum import Enum
from typing import Any, Callable

from pydantic import BaseModel, Field, ValidationError

from .context import ExecutionOptions, RunContext
from .errors import ErrorCategory, NodeError, NodeExecutionError, ReferenceResolutionError, utcnow
from .events import LogEvent, RunLog
from .nodes.base import Node, NodeContext, NodeOutputs, Records
from .nodes.registry import NODE_REGISTRY
from .redact import Redactor
from .references import IterContext, ReferenceContext, find_references, has_references, resolve_config
from .schema import EdgeSpec, NodeSpec, PipelineSpec, ValidationIssue

PIPELINE_NODE_ID = "__pipeline__"


class NodeStatus(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


class RunStatus(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class NodeResult(BaseModel):
    node_id: str
    node_type: str
    status: NodeStatus
    records_out: int | None = None
    duration_ms: float | None = None
    iterations: int | None = None
    error: NodeError | None = None


class RunResult(BaseModel):
    pipeline_id: str
    status: RunStatus
    started_at: datetime
    finished_at: datetime
    node_results: dict[str, NodeResult] = Field(default_factory=dict)
    errors: list[NodeError] = Field(default_factory=list)
    logs: list[LogEvent] = Field(default_factory=list)
    # Primary-port records of every terminal node (no outgoing edges).
    outputs: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)


# --------------------------------------------------------------------------
# Graph helpers
# --------------------------------------------------------------------------
class _Graph:
    def __init__(self, spec: PipelineSpec):
        self.spec = spec
        self.nodes: dict[str, NodeSpec] = {node.id: node for node in spec.nodes}
        self.in_edges: dict[str, list[EdgeSpec]] = defaultdict(list)
        self.out_edges: dict[str, list[EdgeSpec]] = defaultdict(list)
        for edge in spec.edges:
            self.in_edges[edge.to_node].append(edge)
            self.out_edges[edge.from_node].append(edge)

    def topo_order(self) -> list[str] | None:
        """Kahn's algorithm; None when the graph has a cycle. Deterministic:
        ties broken by node declaration order."""
        order_index = {node.id: i for i, node in enumerate(self.spec.nodes)}
        in_degree = {node_id: 0 for node_id in self.nodes}
        for edge in self.spec.edges:
            if edge.to_node in in_degree and edge.from_node in self.nodes:
                in_degree[edge.to_node] += 1
        ready = sorted(
            (node_id for node_id, degree in in_degree.items() if degree == 0),
            key=order_index.__getitem__,
        )
        order: list[str] = []
        while ready:
            node_id = ready.pop(0)
            order.append(node_id)
            newly_ready = []
            for edge in self.out_edges.get(node_id, []):
                if edge.to_node not in in_degree:
                    continue
                in_degree[edge.to_node] -= 1
                if in_degree[edge.to_node] == 0:
                    newly_ready.append(edge.to_node)
            ready.extend(sorted(newly_ready, key=order_index.__getitem__))
            ready.sort(key=order_index.__getitem__)
        if len(order) != len(self.nodes):
            return None
        return order

    def descendants(self, node_id: str) -> set[str]:
        found: set[str] = set()
        stack = [node_id]
        while stack:
            current = stack.pop()
            for edge in self.out_edges.get(current, []):
                if edge.to_node not in found:
                    found.add(edge.to_node)
                    stack.append(edge.to_node)
        return found

    def ancestors(self, node_id: str) -> set[str]:
        found: set[str] = set()
        stack = [node_id]
        while stack:
            current = stack.pop()
            for edge in self.in_edges.get(current, []):
                if edge.from_node not in found:
                    found.add(edge.from_node)
                    stack.append(edge.from_node)
        return found


def _primary(outputs: NodeOutputs) -> Records:
    if "out" in outputs:
        return outputs["out"]
    if outputs:
        return next(iter(outputs.values()))
    return []


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------
def _format_validation_error(exc: ValidationError) -> str:
    parts = []
    for error in exc.errors()[:5]:
        location = ".".join(str(loc) for loc in error["loc"]) or "config"
        parts.append(f"{location}: {error['msg']}")
    return "invalid config: " + "; ".join(parts)


def validate_pipeline(spec: PipelineSpec) -> list[ValidationIssue]:
    """Static validation of the graph. Empty list means runnable."""
    issues: list[ValidationIssue] = []

    # Unique node ids.
    seen_ids: set[str] = set()
    for node in spec.nodes:
        if node.id in seen_ids:
            issues.append(ValidationIssue(node_id=node.id, message="duplicate node id"))
        seen_ids.add(node.id)
    if not spec.nodes:
        issues.append(ValidationIssue(message="pipeline has no nodes"))

    graph = _Graph(spec)

    # Node types + config validation. Configs containing $references are
    # validated after resolution at run time instead (their final types are
    # not knowable statically).
    configs: dict[str, Any] = {}
    for node in spec.nodes:
        node_cls = NODE_REGISTRY.get(node.type)
        if node_cls is None:
            known = ", ".join(sorted(NODE_REGISTRY))
            issues.append(
                ValidationIssue(
                    node_id=node.id,
                    message=f"unknown node type {node.type!r} (known: {known})",
                )
            )
            continue
        if has_references(node.config):
            continue
        try:
            configs[node.id] = node_cls.config_model.model_validate(node.config)
        except ValidationError as exc:
            issues.append(
                ValidationIssue(node_id=node.id, message=_format_validation_error(exc))
            )

    # Edges: endpoints and ports exist; single-input ports get one edge.
    port_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for edge in spec.edges:
        for endpoint, node_id in (("from", edge.from_node), ("to", edge.to_node)):
            if node_id not in graph.nodes:
                issues.append(
                    ValidationIssue(message=f"edge {endpoint!r} references unknown node {node_id!r}")
                )
        source = graph.nodes.get(edge.from_node)
        target = graph.nodes.get(edge.to_node)
        source_cls = NODE_REGISTRY.get(source.type) if source else None
        target_cls = NODE_REGISTRY.get(target.type) if target else None
        if source_cls is not None and edge.from_port not in source_cls.output_ports:
            issues.append(
                ValidationIssue(
                    node_id=edge.from_node,
                    message=f"node has no output port {edge.from_port!r}",
                )
            )
        if target_cls is not None:
            if edge.to_port not in target_cls.input_ports:
                issues.append(
                    ValidationIssue(
                        node_id=edge.to_node,
                        message=f"node has no input port {edge.to_port!r}",
                    )
                )
            else:
                port_counts[edge.to_node][edge.to_port] += 1
                if (
                    not target_cls.allow_multi_input
                    and port_counts[edge.to_node][edge.to_port] > 1
                ):
                    issues.append(
                        ValidationIssue(
                            node_id=edge.to_node,
                            message=f"port {edge.to_port!r} accepts a single edge",
                        )
                    )

    # Required ports + node-specific checks (needs a parsed config).
    for node in spec.nodes:
        node_cls = NODE_REGISTRY.get(node.type)
        config = configs.get(node.id)
        if node_cls is None or config is None:
            continue
        counts = dict(port_counts.get(node.id, {}))
        for port in node_cls.required_input_ports(config):
            if counts.get(port, 0) == 0:
                issues.append(
                    ValidationIssue(
                        node_id=node.id,
                        message=f"required input port {port!r} has no inbound edge",
                    )
                )
        for message in node_cls.check_spec(config, counts):
            issues.append(ValidationIssue(node_id=node.id, message=message))

    # Acyclicity.
    topo = graph.topo_order()
    if topo is None:
        issues.append(ValidationIssue(message="pipeline graph contains a cycle"))
        return issues  # downstream checks need a DAG

    # Reference targets and iterator scoping.
    iterator_ids = [
        node.id
        for node in spec.nodes
        if (cls := NODE_REGISTRY.get(node.type)) is not None and cls.fan_out
    ]
    scopes = {iterator_id: graph.descendants(iterator_id) for iterator_id in iterator_ids}

    for iterator_id, scope in scopes.items():
        for other_id in iterator_ids:
            if other_id != iterator_id and other_id in scope:
                issues.append(
                    ValidationIssue(
                        node_id=other_id,
                        message=f"nested iterators are not supported (inside scope of {iterator_id!r})",
                    )
                )
    scope_owner: dict[str, str] = {}
    for iterator_id, scope in scopes.items():
        for member in scope:
            if member in scope_owner and scope_owner[member] != iterator_id:
                issues.append(
                    ValidationIssue(
                        node_id=member,
                        message=(
                            f"node is downstream of two iterators ({scope_owner[member]!r} "
                            f"and {iterator_id!r}); iterator scopes must not overlap"
                        ),
                    )
                )
            scope_owner[member] = iterator_id

    for node in spec.nodes:
        if node.id not in graph.nodes:
            continue
        ancestors = graph.ancestors(node.id)
        for kind, path in find_references(node.config):
            if kind == "iter":
                in_scope = any(node.id in scope for scope in scopes.values())
                if not in_scope:
                    issues.append(
                        ValidationIssue(
                            node_id=node.id,
                            message="$iter reference used outside of any iterator's downstream subgraph",
                        )
                    )
            else:
                target = path.split(".", 1)[0]
                if target not in graph.nodes:
                    issues.append(
                        ValidationIssue(
                            node_id=node.id,
                            message=f"$upstream reference to unknown node {target!r}",
                        )
                    )
                elif target not in ancestors:
                    issues.append(
                        ValidationIssue(
                            node_id=node.id,
                            message=(
                                f"$upstream reference to {target!r}, which is not an "
                                "ancestor of this node (add an edge to order them)"
                            ),
                        )
                    )
    return issues


# --------------------------------------------------------------------------
# Execution
# --------------------------------------------------------------------------
class _IterationFailure(Exception):
    def __init__(self, index: int, node_id: str, node_error: NodeError):
        super().__init__(node_error.message)
        self.index = index
        self.node_id = node_id
        self.node_error = node_error


def _prepare_node(
    graph: _Graph,
    node_id: str,
    ctx: RunContext,
    outputs_view: Mapping[str, NodeOutputs],
    iter_ctx: IterContext | None,
) -> tuple[Node, dict[str, list[Records]], NodeContext]:
    spec_node = graph.nodes[node_id]
    node_cls = NODE_REGISTRY[spec_node.type]
    node_ctx = NodeContext(ctx, spec_node.id, spec_node.type, iter_ctx)

    reference_ctx = ReferenceContext(
        upstream={upstream_id: _primary(out) for upstream_id, out in outputs_view.items()},
        iter=iter_ctx,
    )
    try:
        resolved = resolve_config(spec_node.config, reference_ctx)
    except ReferenceResolutionError as exc:
        raise node_ctx.error(ErrorCategory.VALIDATION, str(exc)) from exc
    try:
        config = node_cls.config_model.model_validate(resolved)
    except ValidationError as exc:
        raise node_ctx.error(ErrorCategory.CONFIG, _format_validation_error(exc)) from exc

    inputs: dict[str, list[Records]] = {}
    for edge in graph.in_edges.get(node_id, []):
        upstream_outputs = outputs_view.get(edge.from_node)
        if upstream_outputs is None or edge.from_port not in upstream_outputs:
            raise node_ctx.error(
                ErrorCategory.VALIDATION,
                f"input from {edge.from_node!r}.{edge.from_port} is unavailable",
            )
        inputs.setdefault(edge.to_port, []).append(upstream_outputs[edge.from_port])

    node = node_cls(spec_node.id, config)
    return node, inputs, node_ctx


async def _execute_node(
    graph: _Graph,
    node_id: str,
    ctx: RunContext,
    outputs_view: Mapping[str, NodeOutputs],
    iter_ctx: IterContext | None,
) -> NodeOutputs:
    node, inputs, node_ctx = _prepare_node(graph, node_id, ctx, outputs_view, iter_ctx)
    try:
        return await node.run(inputs, node_ctx)
    except NodeExecutionError:
        raise
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - converted to a structured error
        raise node_ctx.error(
            ErrorCategory.UNKNOWN, f"{type(exc).__name__}: {exc}"
        ) from exc


class _State:
    """Mutable state of one run; local to execute_pipeline."""

    def __init__(self) -> None:
        self.outputs: dict[str, NodeOutputs] = {}
        self.results: dict[str, NodeResult] = {}
        self.errors: list[NodeError] = []


def _skip(state: _State, graph: _Graph, node_ids: set[str] | list[str]) -> None:
    for node_id in node_ids:
        if node_id not in state.results:
            state.results[node_id] = NodeResult(
                node_id=node_id,
                node_type=graph.nodes[node_id].type,
                status=NodeStatus.SKIPPED,
            )


async def _run_iterator(
    graph: _Graph,
    iterator_id: str,
    scope_topo: list[str],
    ctx: RunContext,
    state: _State,
) -> bool:
    """Fan out the iterator's scope. Returns True if the run should abort."""
    spec_node = graph.nodes[iterator_id]
    started = time.perf_counter()
    try:
        node, inputs, node_ctx = _prepare_node(graph, iterator_id, ctx, state.outputs, None)
        values = node.get_values(inputs, node_ctx)  # type: ignore[attr-defined]
    except NodeExecutionError as exc:
        state.errors.append(exc.node_error)
        state.results[iterator_id] = NodeResult(
            node_id=iterator_id,
            node_type=spec_node.type,
            status=NodeStatus.FAILED,
            duration_ms=(time.perf_counter() - started) * 1000,
            error=exc.node_error,
        )
        _skip(state, graph, scope_topo)
        return not ctx.options.continue_on_error

    fan_in = node.config.fan_in  # type: ignore[attr-defined]
    iteration_cap = node.config.max_concurrency or ctx.options.max_concurrency  # type: ignore[attr-defined]
    ctx.log.info(
        f"fanning out {len(values)} iteration(s) over {len(scope_topo)} node(s)",
        node_id=iterator_id,
    )

    iteration_outputs: list[dict[str, NodeOutputs] | None] = [None] * len(values)
    durations: dict[str, float] = defaultdict(float)
    iteration_errors: list[_IterationFailure] = []
    semaphore = asyncio.Semaphore(iteration_cap)

    async def run_iteration(index: int, value: Any) -> None:
        async with semaphore:
            iter_ctx = IterContext(value=value, index=index)
            local: dict[str, NodeOutputs] = {
                iterator_id: {"out": [{"value": value, "index": index}]}
            }
            view = ChainMap(local, state.outputs)
            for scope_node_id in scope_topo:
                node_started = time.perf_counter()
                try:
                    local[scope_node_id] = await _execute_node(
                        graph, scope_node_id, ctx, view, iter_ctx
                    )
                except NodeExecutionError as exc:
                    error = exc.node_error.model_copy(
                        update={
                            "details": {
                                **(exc.node_error.details or {}),
                                "iteration_index": index,
                                "iteration_value": ctx.redactor.redact(repr(value)[:200]),
                            }
                        }
                    )
                    raise _IterationFailure(index, scope_node_id, error) from exc
                finally:
                    durations[scope_node_id] += (time.perf_counter() - node_started) * 1000
            iteration_outputs[index] = local

    if ctx.options.continue_on_error:
        gathered = await asyncio.gather(
            *(run_iteration(i, v) for i, v in enumerate(values)),
            return_exceptions=True,
        )
        for result in gathered:
            if isinstance(result, _IterationFailure):
                iteration_errors.append(result)
            elif isinstance(result, BaseException):
                raise result
    else:
        try:
            async with asyncio.TaskGroup() as task_group:
                for i, v in enumerate(values):
                    task_group.create_task(run_iteration(i, v))
        except* _IterationFailure as group:
            iteration_errors.extend(
                sorted(group.exceptions, key=lambda e: e.index)  # type: ignore[arg-type]
            )

    for failure in iteration_errors:
        state.errors.append(failure.node_error)
        ctx.log.error(failure.node_error.message, node_id=failure.node_id)

    if iteration_errors and not ctx.options.continue_on_error:
        first = iteration_errors[0]
        state.results[first.node_id] = NodeResult(
            node_id=first.node_id,
            node_type=graph.nodes[first.node_id].type,
            status=NodeStatus.FAILED,
            duration_ms=durations.get(first.node_id),
            error=first.node_error,
        )
        _skip(state, graph, [iterator_id] + scope_topo)
        return True

    # Fan results back in (deterministic: iteration order, not completion order).
    completed = [
        (index, outputs) for index, outputs in enumerate(iteration_outputs) if outputs is not None
    ]
    failed_node_ids = {failure.node_id for failure in iteration_errors}
    first_error_for: dict[str, NodeError] = {}
    for failure in iteration_errors:
        first_error_for.setdefault(failure.node_id, failure.node_error)

    for member_id in [iterator_id] + scope_topo:
        member_cls = NODE_REGISTRY[graph.nodes[member_id].type]
        combined: NodeOutputs = {}
        ports: set[str] = set(member_cls.output_ports)
        for _, outputs in completed:
            if member_id in outputs:
                ports.update(outputs[member_id])
        for port in ports:
            if fan_in == "concat":
                combined[port] = [
                    record
                    for _, outputs in completed
                    if member_id in outputs
                    for record in outputs[member_id].get(port, [])
                ]
            else:  # keyed
                combined[port] = [
                    {"key": values[index], "records": outputs[member_id].get(port, [])}
                    for index, outputs in completed
                    if member_id in outputs
                ]
        state.outputs[member_id] = combined
        participations = sum(1 for _, outputs in completed if member_id in outputs)
        failed_here = member_id in failed_node_ids
        state.results[member_id] = NodeResult(
            node_id=member_id,
            node_type=graph.nodes[member_id].type,
            status=NodeStatus.FAILED if failed_here else NodeStatus.SUCCEEDED,
            records_out=len(_primary(combined)),
            duration_ms=(
                (time.perf_counter() - started) * 1000
                if member_id == iterator_id
                else durations.get(member_id)
            ),
            iterations=len(values) if member_id == iterator_id else participations,
            error=first_error_for.get(member_id),
        )
    return False


async def execute_pipeline(
    spec: PipelineSpec,
    secrets: Mapping[str, str] | None = None,
    options: ExecutionOptions | None = None,
    *,
    on_event: Callable[[LogEvent], None] | None = None,
) -> RunResult:
    """Run a pipeline. Never raises for pipeline problems -- validation and
    node failures come back as structured errors on the RunResult."""
    options = options or ExecutionOptions()
    secrets = dict(secrets or {})
    started_at = utcnow()
    run_log = RunLog(on_event)

    issues = validate_pipeline(spec)
    if issues:
        node_types = {node.id: node.type for node in spec.nodes}
        errors = [
            NodeError(
                node_id=issue.node_id or PIPELINE_NODE_ID,
                node_type=node_types.get(issue.node_id or "", "pipeline"),
                category=ErrorCategory.VALIDATION,
                message=issue.message,
            )
            for issue in issues
        ]
        for error in errors:
            run_log.error(error.message, node_id=error.node_id)
        return RunResult(
            pipeline_id=spec.pipeline_id,
            status=RunStatus.FAILED,
            started_at=started_at,
            finished_at=utcnow(),
            errors=errors,
            logs=run_log.events,
        )

    graph = _Graph(spec)
    topo = graph.topo_order()
    assert topo is not None  # validated above

    ctx = RunContext(
        secrets=secrets,
        options=options,
        log=run_log,
        redactor=Redactor(secrets.values()),
        http_semaphore=asyncio.Semaphore(options.max_concurrency),
    )
    state = _State()

    iterator_scopes: dict[str, set[str]] = {
        node.id: graph.descendants(node.id)
        for node in spec.nodes
        if NODE_REGISTRY[node.type].fan_out
    }

    run_log.info(f"starting pipeline {spec.pipeline_id!r} ({len(topo)} node(s))")
    abort = False
    for node_id in topo:
        if node_id in state.results:
            continue
        if abort:
            _skip(state, graph, [node_id])
            continue

        spec_node = graph.nodes[node_id]
        if NODE_REGISTRY[spec_node.type].fan_out:
            scope_topo = [n for n in topo if n in iterator_scopes[node_id]]
            abort = await _run_iterator(graph, node_id, scope_topo, ctx, state)
            continue

        run_log.debug("node started", node_id=node_id)
        node_started = time.perf_counter()
        try:
            outputs = await _execute_node(graph, node_id, ctx, state.outputs, None)
        except NodeExecutionError as exc:
            duration_ms = (time.perf_counter() - node_started) * 1000
            state.errors.append(exc.node_error)
            state.results[node_id] = NodeResult(
                node_id=node_id,
                node_type=spec_node.type,
                status=NodeStatus.FAILED,
                duration_ms=duration_ms,
                error=exc.node_error,
            )
            run_log.error(exc.node_error.message, node_id=node_id)
            if options.continue_on_error:
                _skip(state, graph, graph.descendants(node_id))
            else:
                abort = True
            continue
        duration_ms = (time.perf_counter() - node_started) * 1000
        state.outputs[node_id] = outputs
        state.results[node_id] = NodeResult(
            node_id=node_id,
            node_type=spec_node.type,
            status=NodeStatus.SUCCEEDED,
            records_out=len(_primary(outputs)),
            duration_ms=duration_ms,
        )
        run_log.debug(
            f"node succeeded in {duration_ms:.1f} ms",
            node_id=node_id,
        )

    status = RunStatus.FAILED if state.errors else RunStatus.SUCCEEDED
    terminal_outputs = {
        node_id: _primary(outputs)
        for node_id, outputs in state.outputs.items()
        if not graph.out_edges.get(node_id)
    }
    run_log.info(f"pipeline finished: {status.value}")
    return RunResult(
        pipeline_id=spec.pipeline_id,
        status=status,
        started_at=started_at,
        finished_at=utcnow(),
        node_results=state.results,
        errors=state.errors,
        logs=run_log.events,
        outputs=terminal_outputs,
    )
