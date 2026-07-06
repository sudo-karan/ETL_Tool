import { useEffect, useRef, useState } from "react";
import type { RunErrorRead, RunRead } from "../../api/types";
import { ErrorModal } from "../../components/ErrorModal";
import { StatusBadge } from "../../components/ui";
import type { RunView } from "./useRunController";

interface Props {
  view: RunView;
  onTrigger: () => void;
  onSelectRun: (runId: string) => void;
  runs: RunRead[] | null;
  activeError: RunErrorRead | null;
  onOpenError: (error: RunErrorRead | null) => void;
}

type Tab = "logs" | "errors" | "history";

export function RunPanel({ view, onTrigger, onSelectRun, runs, activeError, onOpenError }: Props) {
  const [tab, setTab] = useState<Tab>("logs");

  useEffect(() => {
    if (view.errors.length > 0) setTab("errors");
  }, [view.errors.length]);

  return (
    <div className="run-panel">
      <div className="run-panel-head">
        <button className="btn btn-primary sm" onClick={onTrigger} disabled={view.running}>
          {view.running ? "Running…" : "▶ Run"}
        </button>
        {view.status !== "idle" && <StatusBadge status={view.status} />}
        <div className="tabs">
          {(["logs", "errors", "history"] as Tab[]).map((t) => (
            <button
              key={t}
              className={tab === t ? "tab active" : "tab"}
              onClick={() => setTab(t)}
            >
              {t}
              {t === "logs" && view.logs.length ? ` (${view.logs.length})` : ""}
              {t === "errors" && view.errors.length ? ` (${view.errors.length})` : ""}
            </button>
          ))}
        </div>
      </div>

      <div className="run-panel-body">
        {tab === "logs" && <LogList logs={view.logs} />}
        {tab === "errors" && <ErrorList errors={view.errors} onOpen={onOpenError} />}
        {tab === "history" && (
          <HistoryList runs={runs} activeId={view.runId} onSelect={onSelectRun} />
        )}
      </div>

      {activeError && <ErrorModal error={activeError} onClose={() => onOpenError(null)} />}
    </div>
  );
}

function LogList({ logs }: { logs: RunView["logs"] }) {
  const bottom = useRef<HTMLDivElement>(null);
  useEffect(() => {
    bottom.current?.scrollIntoView({ behavior: "smooth" });
  }, [logs.length]);
  if (logs.length === 0) return <div className="muted pad">No logs yet. Run the pipeline.</div>;
  return (
    <div className="log-list">
      {logs.map((log) => (
        <div key={log.id} className={`log-line lvl-${log.level}`}>
          <span className="log-lvl">{log.level}</span>
          {log.node_id && <span className="log-node">{log.node_id}</span>}
          <span className="log-msg">{log.message}</span>
        </div>
      ))}
      <div ref={bottom} />
    </div>
  );
}

function ErrorList({
  errors,
  onOpen,
}: {
  errors: RunErrorRead[];
  onOpen: (error: RunErrorRead) => void;
}) {
  if (errors.length === 0) return <div className="muted pad">No errors.</div>;
  return (
    <div className="error-list">
      {errors.map((err) => (
        <button key={err.id} className="error-card" onClick={() => onOpen(err)}>
          <span className={`cat cat-${err.category}`}>{err.category}</span>
          <span className="error-card-node">{err.node_id}</span>
          <span className="error-card-msg">{err.message}</span>
        </button>
      ))}
    </div>
  );
}

function HistoryList({
  runs,
  activeId,
  onSelect,
}: {
  runs: RunRead[] | null;
  activeId: string | null;
  onSelect: (runId: string) => void;
}) {
  if (!runs) return <div className="muted pad">Loading…</div>;
  if (runs.length === 0) return <div className="muted pad">No runs yet.</div>;
  return (
    <table className="table compact">
      <tbody>
        {runs.map((run) => (
          <tr
            key={run.id}
            className={run.id === activeId ? "active-row" : "clickable"}
            onClick={() => onSelect(run.id)}
          >
            <td>
              <StatusBadge status={run.status} />
            </td>
            <td className="muted">{run.trigger}</td>
            <td className="muted">{new Date(run.created_at).toLocaleString()}</td>
            <td className="muted">{run.error_count > 0 ? `${run.error_count} error(s)` : ""}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
