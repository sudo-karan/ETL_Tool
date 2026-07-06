import type { ReactNode } from "react";
import type { RunStatus, NodeRunStatus, CheckStatus } from "../api/types";

export function Spinner({ label = "Loading…" }: { label?: string }) {
  return <div className="center muted">{label}</div>;
}

export function ErrorText({ error }: { error: string | null }) {
  if (!error) return null;
  return <div className="error-text">⚠ {error}</div>;
}

export function EmptyState({ children }: { children: ReactNode }) {
  return <div className="empty">{children}</div>;
}

const STATUS_CLASS: Record<string, string> = {
  queued: "badge-queued",
  running: "badge-running",
  succeeded: "badge-succeeded",
  failed: "badge-failed",
  skipped: "badge-skipped",
  passed: "badge-succeeded",
};

export function StatusBadge({ status }: { status: RunStatus | NodeRunStatus | CheckStatus | string }) {
  return <span className={`badge ${STATUS_CLASS[status] ?? "badge-idle"}`}>{status}</span>;
}

export function Modal({
  title,
  onClose,
  children,
  wide = false,
}: {
  title: string;
  onClose: () => void;
  children: ReactNode;
  wide?: boolean;
}) {
  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className={`modal ${wide ? "modal-wide" : ""}`} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <h3>{title}</h3>
          <button className="icon-btn" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </div>
        <div className="modal-body">{children}</div>
      </div>
    </div>
  );
}
