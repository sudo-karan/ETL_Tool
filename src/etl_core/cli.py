"""Command-line interface.

    etl run <pipeline.json>     execute a pipeline and print a run report
    etl test <source.json>      run connectivity diagnostics for a source
    etl validate <pipeline.json>  static-check a pipeline without running it

Secrets are supplied as ETL_SECRET_<REF> environment variables or via
--secrets-file (a JSON object of {ref: value}; dev convenience only). The
SSRF guard is ON by default; use repeated --allow-host entries for internal
hosts or --no-ssrf-guard to disable entirely (not recommended).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from .context import ExecutionOptions
from .diagnostics import DiagnosticReport, test_connection
from .engine import NodeStatus, RunResult, RunStatus, execute_pipeline, validate_pipeline
from .errors import SecretNotFoundError
from .redact import Redactor
from .schema import NodeSpec, PipelineSpec
from .secrets import (
    EnvSecretsProvider,
    StaticSecretsProvider,
    collect_refs_from_config,
    collect_secret_refs,
)
from .ssrf import SSRFPolicy

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2

_STATUS_ICONS = {"passed": "✔", "failed": "✖", "skipped": "○"}
_NODE_ICONS = {
    NodeStatus.SUCCEEDED: "✔",
    NodeStatus.FAILED: "✖",
    NodeStatus.SKIPPED: "○",
}


def _add_common_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--secrets-file",
        type=Path,
        help="JSON file of {secret_ref: value}; takes precedence over env vars",
    )
    parser.add_argument(
        "--secrets-prefix",
        default="ETL_SECRET_",
        help="env var prefix for secret refs (default: ETL_SECRET_)",
    )
    parser.add_argument(
        "--allow-host",
        action="append",
        default=[],
        metavar="HOST_OR_CIDR",
        help="allowlist a private host/IP/CIDR for the SSRF guard (repeatable)",
    )
    parser.add_argument(
        "--no-ssrf-guard",
        action="store_true",
        help="disable the SSRF guard entirely (allows requests to private ranges)",
    )
    parser.add_argument("--json", action="store_true", help="print the full result as JSON")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="etl", description="Visual node-based ETL platform - headless engine CLI"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="execute a pipeline JSON file")
    run.add_argument("pipeline", type=Path)
    _add_common_options(run)
    run.add_argument("--continue-on-error", action="store_true")
    run.add_argument("--max-concurrency", type=int, default=8)
    run.add_argument("--output", type=Path, help="write the full RunResult JSON here")
    run.add_argument(
        "--show-records",
        type=int,
        default=3,
        metavar="N",
        help="preview up to N records per terminal output (default 3)",
    )

    test = sub.add_parser("test", help="run connectivity diagnostics for a source JSON file")
    test.add_argument("source", type=Path)
    _add_common_options(test)

    validate = sub.add_parser("validate", help="static-check a pipeline JSON file")
    validate.add_argument("pipeline", type=Path)

    return parser


def _ssrf_policy(args: argparse.Namespace) -> SSRFPolicy:
    return SSRFPolicy(enabled=not args.no_ssrf_guard, allow_hosts=list(args.allow_host))


async def _gather_secrets(args: argparse.Namespace, refs: set[str]) -> dict[str, str]:
    providers = []
    if args.secrets_file:
        try:
            file_values = json.loads(args.secrets_file.read_text())
        except (OSError, ValueError) as exc:
            raise SystemExit(f"error: cannot read secrets file: {exc}")
        if not isinstance(file_values, dict):
            raise SystemExit("error: secrets file must be a JSON object of {ref: value}")
        providers.append(StaticSecretsProvider({k: str(v) for k, v in file_values.items()}))
    providers.append(EnvSecretsProvider(prefix=args.secrets_prefix))

    resolved: dict[str, str] = {}
    missing: list[str] = []
    for ref in sorted(refs):
        for provider in providers:
            try:
                resolved[ref] = await provider.get(ref)
                break
            except SecretNotFoundError:
                continue
        else:
            missing.append(ref)
    if missing:
        raise SystemExit(
            "error: missing secret(s): "
            + ", ".join(missing)
            + f" (set {args.secrets_prefix}<REF> or use --secrets-file)"
        )
    return resolved


def _load_pipeline(path: Path) -> PipelineSpec:
    try:
        return PipelineSpec.from_file(path)
    except OSError as exc:
        raise SystemExit(f"error: cannot read {path}: {exc}")
    except ValueError as exc:
        raise SystemExit(f"error: {path} is not a valid pipeline document:\n{exc}")


def _print_run_summary(result: RunResult, show_records: int) -> None:
    duration = (result.finished_at - result.started_at).total_seconds()
    print(f"pipeline {result.pipeline_id!r}: {result.status.value.upper()} in {duration:.2f}s")
    for node in result.node_results.values():
        icon = _NODE_ICONS[node.status]
        line = f"  {icon} {node.node_id} ({node.node_type}) - {node.status.value}"
        if node.records_out is not None:
            line += f", {node.records_out} record(s)"
        if node.iterations is not None:
            line += f", {node.iterations} iteration(s)"
        if node.duration_ms is not None:
            line += f", {node.duration_ms:.0f} ms"
        print(line)
    if result.errors:
        print("errors:")
        for error in result.errors:
            location = f"{error.node_id} [{error.category.value}]"
            print(f"  ✖ {location}: {error.message}")
            if error.request_summary:
                print(f"      request: {error.request_summary}")
            if error.http_status:
                print(f"      http_status: {error.http_status}, attempts: {error.attempts}")
    if result.outputs and show_records > 0:
        print("outputs:")
        for node_id, records in result.outputs.items():
            print(f"  {node_id}: {len(records)} record(s)")
            for record in records[:show_records]:
                print(f"    {json.dumps(record, default=str)[:400]}")


def _print_test_report(report: DiagnosticReport) -> None:
    print(f"target: {report.target or '(invalid config)'}")
    for check in report.checks:
        icon = _STATUS_ICONS[check.status]
        latency = f"{check.latency_ms:7.1f} ms" if check.latency_ms is not None else " " * 10
        note = check.detail or check.error or ""
        print(f"  {icon} {check.name:<12} {latency}  {note}")
    print(f"result: {'OK' if report.ok else 'FAILED'}")
    if report.sample_body:
        print("sample body (truncated, redacted):")
        print("  " + report.sample_body[:500].replace("\n", "\n  "))


async def _cmd_run(args: argparse.Namespace) -> int:
    spec = _load_pipeline(args.pipeline)
    issues = validate_pipeline(spec)
    if issues:
        print(f"pipeline {spec.pipeline_id!r} is invalid:", file=sys.stderr)
        for issue in issues:
            print(f"  - {issue}", file=sys.stderr)
        return EXIT_USAGE
    secrets = await _gather_secrets(args, collect_secret_refs(spec))
    options = ExecutionOptions(
        max_concurrency=args.max_concurrency,
        continue_on_error=args.continue_on_error,
        ssrf_policy=_ssrf_policy(args),
    )
    result = await execute_pipeline(spec, secrets, options)

    redactor = Redactor(secrets.values())
    result_json = redactor.redact(result.model_dump_json(indent=2))
    if args.output:
        args.output.write_text(result_json)
        print(f"wrote full run result to {args.output}", file=sys.stderr)
    if args.json:
        print(result_json)
    else:
        _print_run_summary(result, args.show_records)
    return EXIT_OK if result.status == RunStatus.SUCCEEDED else EXIT_FAILED


async def _cmd_test(args: argparse.Namespace) -> int:
    try:
        raw = json.loads(args.source.read_text())
        source = NodeSpec.model_validate(raw)
    except OSError as exc:
        raise SystemExit(f"error: cannot read {args.source}: {exc}")
    except ValueError as exc:
        raise SystemExit(
            f"error: {args.source} must be a node spec "
            f'{{"type": "api_source", "config": {{...}}}}:\n{exc}'
        )
    secrets = await _gather_secrets(args, collect_refs_from_config(source.config))
    report = await test_connection(source, secrets, ssrf_policy=_ssrf_policy(args))
    if args.json:
        print(report.model_dump_json(indent=2))
    else:
        _print_test_report(report)
    return EXIT_OK if report.ok else EXIT_FAILED


def _cmd_validate(args: argparse.Namespace) -> int:
    spec = _load_pipeline(args.pipeline)
    issues = validate_pipeline(spec)
    if issues:
        print(f"pipeline {spec.pipeline_id!r} is invalid:")
        for issue in issues:
            print(f"  - {issue}")
        return EXIT_FAILED
    print(f"pipeline {spec.pipeline_id!r} is valid ({len(spec.nodes)} nodes, {len(spec.edges)} edges)")
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "run":
        return asyncio.run(_cmd_run(args))
    if args.command == "test":
        return asyncio.run(_cmd_test(args))
    return _cmd_validate(args)


def entrypoint() -> None:
    sys.exit(main())


if __name__ == "__main__":
    entrypoint()
