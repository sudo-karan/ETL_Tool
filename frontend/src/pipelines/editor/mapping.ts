// Pure conversions between React Flow's graph and the engine's pipeline JSON.
// This is the only place that knows both shapes; it is unit-tested.
import type { Edge as RFEdge, Node as RFNode } from "@xyflow/react";
import type { PipelineSpec } from "../../api/types";

export type NodeRunState = "idle" | "running" | "succeeded" | "failed" | "skipped";

export interface EtlNodeData extends Record<string, unknown> {
  nodeType: string;
  config: Record<string, unknown>;
  runState?: NodeRunState;
}

export type EtlNode = RFNode<EtlNodeData>;

export function specToFlow(
  spec: PipelineSpec,
  positions: Record<string, { x: number; y: number }>,
): { nodes: EtlNode[]; edges: RFEdge[] } {
  const nodes: EtlNode[] = spec.nodes.map((node) => ({
    id: node.id,
    type: "etlNode",
    position: positions[node.id] ?? { x: 0, y: 0 },
    data: { nodeType: node.type, config: node.config ?? {} },
  }));
  const edges: RFEdge[] = spec.edges.map((edge) => ({
    id: `${edge.from}->${edge.to}`,
    source: edge.from,
    target: edge.to,
  }));
  return { nodes, edges };
}

export function flowToSpec(pipelineId: string, nodes: EtlNode[], edges: RFEdge[]): PipelineSpec {
  return {
    pipeline_id: pipelineId,
    nodes: nodes.map((node) => ({
      id: node.id,
      type: node.data.nodeType,
      config: node.data.config ?? {},
    })),
    // Every built-in node uses in/out, so the port names are fixed here.
    edges: edges.map((edge) => ({
      from: edge.source,
      from_port: "out",
      to: edge.target,
      to_port: "in",
    })),
  };
}
