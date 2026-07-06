import { useState } from "react";
import { api } from "../api/client";
import { useAsync } from "../lib/useAsync";
import { EmptyState, ErrorText, Spinner } from "../components/ui";

export function SecretsPage() {
  const { data, error, loading, reload } = useAsync(() => api.listSecrets(), []);
  const [ref, setRef] = useState("");
  const [value, setValue] = useState("");
  const [formError, setFormError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function save() {
    setBusy(true);
    setFormError(null);
    try {
      await api.setSecret(ref.trim(), value);
      setRef("");
      setValue("");
      reload();
    } catch (err) {
      setFormError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  async function remove(name: string) {
    if (!confirm(`Delete secret "${name}"?`)) return;
    await api.deleteSecret(name);
    reload();
  }

  return (
    <div className="page">
      <div className="page-head">
        <h1>Secrets</h1>
      </div>
      <p className="muted">
        Values are encrypted at rest and never shown again. Pipelines reference them by name via{" "}
        <code>secret_ref</code>.
      </p>

      <div className="panel">
        <div className="inline-form">
          <input placeholder="REF_NAME" value={ref} onChange={(e) => setRef(e.target.value)} />
          <input
            type="password"
            placeholder="value"
            value={value}
            onChange={(e) => setValue(e.target.value)}
          />
          <button
            className="btn btn-primary"
            disabled={busy || !ref.trim() || !value}
            onClick={save}
          >
            Save secret
          </button>
        </div>
        <ErrorText error={formError} />
      </div>

      <ErrorText error={error} />
      {loading ? (
        <Spinner />
      ) : !data || data.length === 0 ? (
        <EmptyState>No secrets stored.</EmptyState>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>Ref</th>
              <th>Updated</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {data.map((s) => (
              <tr key={s.id}>
                <td>
                  <code>{s.ref}</code>
                </td>
                <td className="muted">{new Date(s.updated_at).toLocaleString()}</td>
                <td className="right">
                  <button className="btn btn-danger sm" onClick={() => remove(s.ref)}>
                    Delete
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
