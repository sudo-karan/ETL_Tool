import { Handle, Position, type NodeProps } from "@xyflow/react";
import { CATEGORY_COLORS, NODE_TYPE_MAP } from "./nodeCatalog";
import type { EtlNode } from "./mapping";

export function CanvasNode({ id, data, selected }: NodeProps<EtlNode>) {
  const meta = NODE_TYPE_MAP[data.nodeType];
  const color = meta ? CATEGORY_COLORS[meta.category] : "#64748b";
  const runState = data.runState ?? "idle";
  return (
    <div
      className={`etl-node run-${runState} ${selected ? "sel" : ""}`}
      style={{ borderTopColor: color }}
    >
      {meta?.hasInput && <Handle type="target" position={Position.Left} />}
      <div className="etl-node-type" style={{ color }}>
        {meta?.label ?? data.nodeType}
      </div>
      <div className="etl-node-id">{id}</div>
      {runState !== "idle" && <div className={`etl-node-state state-${runState}`}>{runState}</div>}
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
