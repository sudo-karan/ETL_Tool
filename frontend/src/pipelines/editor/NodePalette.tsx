import { CATEGORY_COLORS, NODE_TYPES, type NodeCategory } from "./nodeCatalog";

const CATEGORY_LABELS: Record<NodeCategory, string> = {
  source: "Sources",
  control: "Control",
  transform: "Transforms",
  sink: "Sinks",
};
const ORDER: NodeCategory[] = ["source", "control", "transform", "sink"];

export function NodePalette({ onAdd }: { onAdd: (type: string) => void }) {
  return (
    <aside className="palette">
      <div className="palette-title">Nodes</div>
      <p className="palette-hint">Drag onto the canvas, or click to add.</p>
      {ORDER.map((cat) => (
        <div key={cat} className="palette-group">
          <div className="palette-group-title">{CATEGORY_LABELS[cat]}</div>
          {NODE_TYPES.filter((n) => n.category === cat).map((n) => (
            <div
              key={n.type}
              className="palette-item"
              style={{ borderLeftColor: CATEGORY_COLORS[cat] }}
              draggable
              onDragStart={(e) => {
                e.dataTransfer.setData("application/etl-node", n.type);
                e.dataTransfer.effectAllowed = "move";
              }}
              onClick={() => onAdd(n.type)}
              title={`Add ${n.label}`}
            >
              {n.label}
            </div>
          ))}
        </div>
      ))}
    </aside>
  );
}
