// Wire types mirroring etl_server/schemas.py and the engine's pipeline schema.
// The UI holds no business logic: it only renders these and emits the same
// pipeline JSON the engine defines.

export interface EdgeSpec {
  from: string;
  from_port: string;
  to: string;
  to_port: string;
}

export interface NodeSpec {
  id: string;
  type: string;
  config: Record<string, unknown>;
}

export interface PipelineSpec {
  pipeline_id: string;
  nodes: NodeSpec[];
  edges: EdgeSpec[];
}

export interface UserRead {
  id: string;
  email: string;
  is_active: boolean;
  created_at: string;
}

export interface Token {
  access_token: string;
  token_type: string;
}

export interface PipelineRead {
  id: string;
  name: string;
  spec: PipelineSpec;
  created_at: string;
  updated_at: string;
}

export type RunStatus = "queued" | "running" | "succeeded" | "failed";

export interface RunRead {
  id: string;
  pipeline_id: string;
  status: RunStatus;
  trigger: string;
  params: Record<string, unknown> | null;
  error_count: number;
  result: RunResultSummary | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
}

export type NodeRunStatus = "succeeded" | "failed" | "skipped";

export interface NodeResult {
  node_id: string;
  node_type: string;
  status: NodeRunStatus;
  records_out: number | null;
  duration_ms: number | null;
  iterations: number | null;
}

export interface RunResultSummary {
  status: string;
  node_results: Record<string, NodeResult>;
  outputs: Record<string, Record<string, unknown>[]>;
  outputs_truncated: boolean;
}

export interface RunLogRead {
  id: number;
  seq: number;
  timestamp: string;
  level: "debug" | "info" | "warning" | "error";
  node_id: string | null;
  message: string;
  data: Record<string, unknown> | null;
}

export type ErrorCategory =
  | "dns"
  | "network"
  | "tls"
  | "timeout"
  | "http_status"
  | "auth"
  | "rate_limit"
  | "validation"
  | "transform"
  | "decryption"
  | "config"
  | "unknown";

export interface RunErrorRead {
  id: number;
  node_id: string;
  node_type: string;
  category: ErrorCategory;
  message: string;
  http_status: number | null;
  request_summary: string | null;
  attempts: number;
  timestamp: string;
  details: Record<string, unknown> | null;
}

export interface RunDetail extends RunRead {
  logs: RunLogRead[];
  errors: RunErrorRead[];
}

export interface SecretRead {
  id: string;
  ref: string;
  created_at: string;
  updated_at: string;
}

export interface ScheduleRead {
  id: string;
  pipeline_id: string;
  cron_expr: string;
  timezone: string;
  enabled: boolean;
  last_run: string | null;
  next_run: string | null;
  created_at: string;
}

export type CheckStatus = "passed" | "failed" | "skipped";

export interface DiagnosticCheck {
  name: string;
  status: CheckStatus;
  latency_ms: number | null;
  detail: string | null;
  error: string | null;
}

export interface DiagnosticReport {
  source_type: string;
  target: string;
  ok: boolean;
  checks: DiagnosticCheck[];
  sample_body: string | null;
  timestamp: string;
}
