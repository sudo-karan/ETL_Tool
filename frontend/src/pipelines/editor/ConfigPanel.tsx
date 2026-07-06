import { useEffect, useState } from "react";
import type { EtlNode } from "./mapping";
import { NODE_TYPE_MAP, type FieldSpec } from "./nodeCatalog";

interface Props {
  node: EtlNode | null;
  onChangeConfig: (config: Record<string, unknown>) => void;
  onRename: (newId: string) => boolean;
  onDelete: () => void;
}

export function ConfigPanel({ node, onChangeConfig, onRename, onDelete }: Props) {
  return (
    <aside className="config-panel">
      {node ? (
        <NodeConfig
          key={node.id}
          node={node}
          onChangeConfig={onChangeConfig}
          onRename={onRename}
          onDelete={onDelete}
        />
      ) : (
        <div className="muted pad">Select a node to configure it.</div>
      )}
    </aside>
  );
}

function NodeConfig({ node, onChangeConfig, onRename, onDelete }: Props & { node: EtlNode }) {
  const meta = NODE_TYPE_MAP[node.data.nodeType];
  const config = node.data.config;
  const [idText, setIdText] = useState(node.id);
  const [jsonText, setJsonText] = useState(() => JSON.stringify(config, null, 2));
  const [jsonError, setJsonError] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);

  useEffect(() => {
    if (!dirty) setJsonText(JSON.stringify(config, null, 2));
  }, [config, dirty]);

  function setField(key: string, value: unknown) {
    const next = { ...config };
    if (value === undefined || value === "") delete next[key];
    else next[key] = value;
    onChangeConfig(next);
  }

  function applyJson() {
    try {
      const parsed = JSON.parse(jsonText);
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        throw new Error("config must be a JSON object");
      }
      onChangeConfig(parsed as Record<string, unknown>);
      setDirty(false);
      setJsonError(null);
    } catch (e) {
      setJsonError((e as Error).message);
    }
  }

  return (
    <div className="config-inner">
      <div className="config-head">
        <span className="config-type">{meta?.label ?? node.data.nodeType}</span>
        <button className="btn btn-danger sm" onClick={onDelete}>
          Delete
        </button>
      </div>

      <label>
        Node id
        <div className="id-row">
          <input value={idText} onChange={(e) => setIdText(e.target.value)} />
          <button
            className="btn sm"
            disabled={idText === node.id}
            onClick={() => {
              if (!onRename(idText)) setIdText(node.id);
            }}
          >
            Rename
          </button>
        </div>
        <span className="field-help">Referenced by edges and <code>$upstream.{node.id}</code>.</span>
      </label>

      {meta?.fields.map((field) => (
        <QuickField key={field.key} field={field} value={config[field.key]} onChange={setField} />
      ))}

      <details className="raw-config">
        <summary>Raw config (advanced)</summary>
        <textarea
          className="code"
          rows={10}
          value={jsonText}
          onChange={(e) => {
            setJsonText(e.target.value);
            setDirty(true);
          }}
        />
        {jsonError && <div className="error-text">⚠ {jsonError}</div>}
        <div className="modal-actions">
          <button
            className="btn sm"
            onClick={() => {
              setJsonText(JSON.stringify(config, null, 2));
              setDirty(false);
              setJsonError(null);
            }}
            disabled={!dirty}
          >
            Revert
          </button>
          <button className="btn btn-primary sm" onClick={applyJson} disabled={!dirty}>
            Apply JSON
          </button>
        </div>
      </details>
    </div>
  );
}

function QuickField({
  field,
  value,
  onChange,
}: {
  field: FieldSpec;
  value: unknown;
  onChange: (key: string, value: unknown) => void;
}) {
  if (field.type === "boolean") {
    return (
      <label className="check-row">
        <input
          type="checkbox"
          checked={!!value}
          onChange={(e) => onChange(field.key, e.target.checked)}
        />
        {field.label}
      </label>
    );
  }
  if (field.type === "select") {
    return (
      <label>
        {field.label}
        <select
          value={String(value ?? field.options?.[0] ?? "")}
          onChange={(e) => onChange(field.key, e.target.value)}
        >
          {field.options?.map((o) => (
            <option key={o} value={o}>
              {o}
            </option>
          ))}
        </select>
      </label>
    );
  }
  return (
    <label>
      {field.label}
      <input
        type={field.type === "number" ? "number" : "text"}
        value={value === undefined || value === null ? "" : String(value)}
        placeholder={field.placeholder}
        onChange={(e) =>
          onChange(
            field.key,
            field.type === "number"
              ? e.target.value === ""
                ? undefined
                : Number(e.target.value)
              : e.target.value,
          )
        }
      />
      {field.help && <span className="field-help">{field.help}</span>}
    </label>
  );
}
