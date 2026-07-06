import { useState } from "react";
import { api } from "../api/client";
import type { DiagnosticReport } from "../api/types";
import { ErrorText, StatusBadge } from "../components/ui";

const TEMPLATES: Record<string, string> = {
  api_source: JSON.stringify(
    { method: "GET", url: "https://api.example.com/items", auth: null },
    null,
    2,
  ),
  db_source: JSON.stringify(
    {
      connection: { driver: "postgresql", host: "db.internal", database: "app", secret_ref: "PGPASSWORD" },
      query: "SELECT 1",
    },
    null,
    2,
  ),
};

export function DiagnosticsPage() {
  const [type, setType] = useState<"api_source" | "db_source">("api_source");
  const [configText, setConfigText] = useState(TEMPLATES.api_source);
  const [report, setReport] = useState<DiagnosticReport | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  function switchType(next: "api_source" | "db_source") {
    setType(next);
    setConfigText(TEMPLATES[next]);
    setReport(null);
  }

  async function run() {
    setBusy(true);
    setError(null);
    setReport(null);
    let config: Record<string, unknown>;
    try {
      config = JSON.parse(configText);
    } catch (err) {
      setError(`Config is not valid JSON: ${(err as Error).message}`);
      setBusy(false);
      return;
    }
    try {
      setReport(await api.testConnection(type, config));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="page">
      <div className="page-head">
        <h1>Connectivity diagnostics</h1>
      </div>
      <p className="muted">
        The <strong>server</strong> probes the source (not your browser), using your stored secrets
        and the deployment SSRF policy.
      </p>

      <div className="diag-layout">
        <div className="panel">
          <label>
            Source type
            <select value={type} onChange={(e) => switchType(e.target.value as typeof type)}>
              <option value="api_source">api_source</option>
              <option value="db_source">db_source</option>
            </select>
          </label>
          <label>
            Config (JSON)
            <textarea
              className="code"
              rows={12}
              value={configText}
              onChange={(e) => setConfigText(e.target.value)}
            />
          </label>
          <button className="btn btn-primary" onClick={run} disabled={busy}>
            {busy ? "Testing…" : "Run test"}
          </button>
          <ErrorText error={error} />
        </div>

        <div className="panel">
          {report ? <LadderView report={report} /> : <div className="muted">Run a test to see the ladder.</div>}
        </div>
      </div>
    </div>
  );
}

export function LadderView({ report }: { report: DiagnosticReport }) {
  return (
    <div>
      <div className="ladder-head">
        <span className="mono">{report.target || "(invalid config)"}</span>
        <StatusBadge status={report.ok ? "succeeded" : "failed"} />
      </div>
      <table className="ladder">
        <tbody>
          {report.checks.map((c) => (
            <tr key={c.name}>
              <td className="ladder-icon">
                {c.status === "passed" ? "✔" : c.status === "failed" ? "✖" : "○"}
              </td>
              <td className="ladder-name">{c.name}</td>
              <td className="ladder-latency muted">
                {c.latency_ms != null ? `${c.latency_ms.toFixed(1)} ms` : ""}
              </td>
              <td className="ladder-note">{c.detail ?? c.error ?? ""}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {report.sample_body && (
        <details className="sample">
          <summary>Sample response (truncated, redacted)</summary>
          <pre className="code">{report.sample_body}</pre>
        </details>
      )}
    </div>
  );
}
