// Typed client for the etl_server API. Every screen is a thin consumer of
// these calls -- no business logic lives here beyond request plumbing + auth.
import type {
  DiagnosticReport,
  PipelineRead,
  PipelineSpec,
  RunDetail,
  RunRead,
  ScheduleRead,
  SecretRead,
  Token,
  UserRead,
} from "./types";

const RAW_BASE = import.meta.env.VITE_API_BASE as string | undefined;
export const API_BASE = (RAW_BASE && RAW_BASE.replace(/\/$/, "")) || "http://localhost:8000";

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

let authToken: string | null = null;
export function setAuthToken(token: string | null): void {
  authToken = token;
}
export function getAuthToken(): string | null {
  return authToken;
}

export function authHeaders(): Record<string, string> {
  return authToken ? { Authorization: `Bearer ${authToken}` } : {};
}

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const headers: Record<string, string> = { ...authHeaders() };
  let payload: BodyInit | undefined;
  if (body instanceof URLSearchParams) {
    headers["Content-Type"] = "application/x-www-form-urlencoded";
    payload = body;
  } else if (body !== undefined) {
    headers["Content-Type"] = "application/json";
    payload = JSON.stringify(body);
  }
  const resp = await fetch(`${API_BASE}${path}`, { method, headers, body: payload });
  if (!resp.ok) {
    throw new ApiError(resp.status, await extractError(resp));
  }
  if (resp.status === 204) return undefined as T;
  const text = await resp.text();
  return (text ? JSON.parse(text) : undefined) as T;
}

async function extractError(resp: Response): Promise<string> {
  try {
    const data = await resp.json();
    if (typeof data?.detail === "string") return data.detail;
    if (Array.isArray(data?.detail)) {
      return data.detail.map((d: { msg?: string }) => d.msg ?? "invalid").join("; ");
    }
    return JSON.stringify(data);
  } catch {
    return resp.statusText || `HTTP ${resp.status}`;
  }
}

export const api = {
  // auth
  register: (email: string, password: string) =>
    request<UserRead>("POST", "/auth/register", { email, password }),
  login: (email: string, password: string) =>
    request<Token>("POST", "/auth/token", new URLSearchParams({ username: email, password })),
  me: () => request<UserRead>("GET", "/auth/me"),

  // pipelines
  listPipelines: () => request<PipelineRead[]>("GET", "/pipelines"),
  getPipeline: (id: string) => request<PipelineRead>("GET", `/pipelines/${id}`),
  createPipeline: (name: string, spec: PipelineSpec) =>
    request<PipelineRead>("POST", "/pipelines", { name, spec }),
  updatePipeline: (id: string, body: { name?: string; spec?: PipelineSpec }) =>
    request<PipelineRead>("PUT", `/pipelines/${id}`, body),
  deletePipeline: (id: string) => request<void>("DELETE", `/pipelines/${id}`),

  // runs
  listRuns: (pipelineId?: string) =>
    request<RunRead[]>(
      "GET",
      pipelineId ? `/runs?pipeline_id=${encodeURIComponent(pipelineId)}` : "/runs",
    ),
  getRun: (id: string) => request<RunDetail>("GET", `/runs/${id}`),
  triggerRun: (pipelineId: string, params?: Record<string, unknown>) =>
    request<RunRead>("POST", `/pipelines/${pipelineId}/runs`, { params: params ?? null }),

  // secrets
  listSecrets: () => request<SecretRead[]>("GET", "/secrets"),
  setSecret: (ref: string, value: string) =>
    request<SecretRead>("POST", "/secrets", { ref, value }),
  deleteSecret: (ref: string) => request<void>("DELETE", `/secrets/${encodeURIComponent(ref)}`),

  // diagnostics
  testConnection: (type: string, config: Record<string, unknown>) =>
    request<DiagnosticReport>("POST", "/test-connection", { type, config }),

  // schedules
  listSchedules: () => request<ScheduleRead[]>("GET", "/schedules"),
  createSchedule: (body: {
    pipeline_id: string;
    cron_expr: string;
    timezone: string;
    enabled?: boolean;
  }) => request<ScheduleRead>("POST", "/schedules", body),
  updateSchedule: (
    id: string,
    body: { cron_expr?: string; timezone?: string; enabled?: boolean },
  ) => request<ScheduleRead>("PUT", `/schedules/${id}`, body),
  deleteSchedule: (id: string) => request<void>("DELETE", `/schedules/${id}`),
};
