// Declarative catalog of the nine engine node types. Drives the palette and
// the per-node config panel. Every built-in node has a single "in" and "out"
// port, so edges always map from_port="out" -> to_port="in".

export type NodeCategory = "source" | "transform" | "sink" | "control";

export interface FieldSpec {
  key: string;
  label: string;
  type: "text" | "number" | "boolean" | "select";
  options?: string[];
  placeholder?: string;
  help?: string;
}

export interface NodeType {
  type: string;
  label: string;
  category: NodeCategory;
  hasInput: boolean; // a source still accepts an optional context edge
  defaultConfig: Record<string, unknown>;
  fields: FieldSpec[]; // quick top-level fields; full config editable as JSON
}

const HTTP_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"];

export const NODE_TYPES: NodeType[] = [
  {
    type: "api_source",
    label: "API Source",
    category: "source",
    hasInput: true,
    defaultConfig: { method: "GET", url: "" },
    fields: [
      { key: "method", label: "Method", type: "select", options: HTTP_METHODS },
      { key: "url", label: "URL", type: "text", placeholder: "https://api.example.com/items" },
      { key: "items_path", label: "Items path", type: "text", placeholder: "data.items" },
    ],
  },
  {
    type: "file_source",
    label: "File Source",
    category: "source",
    hasInput: true,
    defaultConfig: { path: "", format: "auto" },
    fields: [
      { key: "path", label: "Path", type: "text", placeholder: "data/users.json" },
      { key: "format", label: "Format", type: "select", options: ["auto", "csv", "json", "jsonl", "parquet"] },
    ],
  },
  {
    type: "db_source",
    label: "DB Source",
    category: "source",
    hasInput: true,
    defaultConfig: { connection: { driver: "postgresql", host: "", database: "" }, query: "" },
    fields: [{ key: "query", label: "SQL query", type: "text", placeholder: "SELECT * FROM t" }],
  },
  {
    type: "iterator",
    label: "Iterator (fan-out)",
    category: "control",
    hasInput: true,
    defaultConfig: { mode: "array", array: [], fan_in: "concat" },
    fields: [
      { key: "mode", label: "Mode", type: "select", options: ["array", "range", "from_upstream"] },
      { key: "fan_in", label: "Fan-in", type: "select", options: ["concat", "keyed"] },
      { key: "field", label: "Field (from_upstream)", type: "text", placeholder: "user.id" },
    ],
  },
  {
    type: "merge",
    label: "Merge",
    category: "transform",
    hasInput: true,
    defaultConfig: { strategy: "concat" },
    fields: [
      { key: "strategy", label: "Strategy", type: "select", options: ["concat", "union", "join"] },
      { key: "how", label: "Join how", type: "select", options: ["inner", "left", "outer"] },
    ],
  },
  {
    type: "transform",
    label: "Transform",
    category: "transform",
    hasInput: true,
    defaultConfig: { ops: [] },
    fields: [],
  },
  {
    type: "decrypt",
    label: "Decrypt",
    category: "transform",
    hasInput: true,
    defaultConfig: { algo: "fernet", secret_ref: "", fields: [] },
    fields: [
      { key: "algo", label: "Algorithm", type: "select", options: ["fernet", "aes-gcm"] },
      { key: "secret_ref", label: "Key secret ref", type: "text", placeholder: "FIELD_KEY" },
    ],
  },
  {
    type: "file_sink",
    label: "File Sink",
    category: "sink",
    hasInput: true,
    defaultConfig: { path: "", format: "auto", mode: "overwrite" },
    fields: [
      { key: "path", label: "Path", type: "text", placeholder: "out/report.csv" },
      { key: "format", label: "Format", type: "select", options: ["auto", "csv", "json", "jsonl", "parquet"] },
      { key: "mode", label: "Mode", type: "select", options: ["overwrite", "append", "error"] },
    ],
  },
  {
    type: "db_sink",
    label: "DB Sink",
    category: "sink",
    hasInput: true,
    defaultConfig: { connection: { driver: "postgresql", host: "", database: "" }, table: "", create: false },
    fields: [
      { key: "table", label: "Table", type: "text", placeholder: "orders" },
      { key: "mode", label: "Mode", type: "select", options: ["append", "replace"] },
      { key: "create", label: "Create if missing", type: "boolean" },
    ],
  },
];

export const NODE_TYPE_MAP: Record<string, NodeType> = Object.fromEntries(
  NODE_TYPES.map((n) => [n.type, n]),
);

export const CATEGORY_COLORS: Record<NodeCategory, string> = {
  source: "#2563eb",
  transform: "#7c3aed",
  sink: "#059669",
  control: "#d97706",
};
