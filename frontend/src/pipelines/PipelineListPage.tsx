import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api/client";
import type { PipelineRead } from "../api/types";
import { useAsync } from "../lib/useAsync";
import { EmptyState, ErrorText, Modal, Spinner } from "../components/ui";

function slugify(name: string): string {
  return name.trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "") || "pipeline";
}

export function PipelineListPage() {
  const navigate = useNavigate();
  const { data, error, loading, reload } = useAsync(() => api.listPipelines(), []);
  const [creating, setCreating] = useState(false);

  return (
    <div className="page">
      <div className="page-head">
        <h1>Pipelines</h1>
        <button className="btn btn-primary" onClick={() => setCreating(true)}>
          + New pipeline
        </button>
      </div>
      <ErrorText error={error} />
      {loading ? (
        <Spinner />
      ) : !data || data.length === 0 ? (
        <EmptyState>No pipelines yet. Create one to start composing a graph.</EmptyState>
      ) : (
        <div className="card-grid">
          {data.map((p) => (
            <PipelineCard key={p.id} pipeline={p} onDeleted={reload} />
          ))}
        </div>
      )}
      {creating && (
        <CreateModal
          onClose={() => setCreating(false)}
          onCreated={(id) => navigate(`/pipelines/${id}`)}
        />
      )}
    </div>
  );
}

function PipelineCard({ pipeline, onDeleted }: { pipeline: PipelineRead; onDeleted: () => void }) {
  const navigate = useNavigate();
  const [busy, setBusy] = useState(false);
  const nodeCount = pipeline.spec?.nodes?.length ?? 0;

  async function remove() {
    if (!confirm(`Delete pipeline "${pipeline.name}"? This also deletes its runs.`)) return;
    setBusy(true);
    try {
      await api.deletePipeline(pipeline.id);
      onDeleted();
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="card">
      <div className="card-title">{pipeline.name}</div>
      <div className="card-meta muted">
        {nodeCount} node{nodeCount === 1 ? "" : "s"} · updated{" "}
        {new Date(pipeline.updated_at).toLocaleString()}
      </div>
      <div className="card-actions">
        <button className="btn" onClick={() => navigate(`/pipelines/${pipeline.id}`)}>
          Open
        </button>
        <button className="btn btn-danger" onClick={remove} disabled={busy}>
          Delete
        </button>
      </div>
    </div>
  );
}

function CreateModal({
  onClose,
  onCreated,
}: {
  onClose: () => void;
  onCreated: (id: string) => void;
}) {
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function create() {
    setBusy(true);
    setError(null);
    try {
      const created = await api.createPipeline(name, {
        pipeline_id: slugify(name),
        nodes: [],
        edges: [],
      });
      onCreated(created.id);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setBusy(false);
    }
  }

  return (
    <Modal title="New pipeline" onClose={onClose}>
      <label>
        Name
        <input
          autoFocus
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="Daily orders sync"
        />
      </label>
      <ErrorText error={error} />
      <div className="modal-actions">
        <button className="btn" onClick={onClose}>
          Cancel
        </button>
        <button className="btn btn-primary" onClick={create} disabled={busy || !name.trim()}>
          Create
        </button>
      </div>
    </Modal>
  );
}
