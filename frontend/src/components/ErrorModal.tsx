import type { RunErrorRead } from "../api/types";
import { Modal } from "./ui";

export function ErrorModal({ error, onClose }: { error: RunErrorRead; onClose: () => void }) {
  return (
    <Modal title={`Error · ${error.node_id}`} onClose={onClose} wide>
      <div className="err-grid">
        <div>
          <span className="k">Category</span>
          <span className={`cat cat-${error.category}`}>{error.category}</span>
        </div>
        <div>
          <span className="k">Node</span>
          {error.node_id} <span className="muted">({error.node_type})</span>
        </div>
        {error.http_status != null && (
          <div>
            <span className="k">HTTP status</span>
            {error.http_status}
          </div>
        )}
        <div>
          <span className="k">Attempts</span>
          {error.attempts}
        </div>
        <div>
          <span className="k">Time</span>
          {new Date(error.timestamp).toLocaleString()}
        </div>
      </div>

      <div className="err-message">{error.message}</div>

      {error.request_summary && (
        <div className="err-request">
          <span className="k">Request (secrets redacted)</span>
          <code>{error.request_summary}</code>
        </div>
      )}

      {error.details && (
        <details>
          <summary>Details</summary>
          <pre className="code">{JSON.stringify(error.details, null, 2)}</pre>
        </details>
      )}
    </Modal>
  );
}
