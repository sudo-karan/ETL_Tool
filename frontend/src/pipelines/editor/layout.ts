// Simple layered (left-to-right) auto-layout so a loaded pipeline renders
// sensibly without persisting coordinates in the pipeline JSON. Positions the
// user drags are kept in localStorage per pipeline (see EditorPage).
import type { PipelineSpec } from "../../api/types";

const COL_WIDTH = 260;
const ROW_HEIGHT = 130;
const X0 = 40;
const Y0 = 40;

export function layeredLayout(spec: PipelineSpec): Record<string, { x: number; y: number }> {
  const ids = spec.nodes.map((n) => n.id);
  const preds = new Map<string, string[]>(ids.map((id) => [id, []]));
  const succs = new Map<string, string[]>(ids.map((id) => [id, []]));
  for (const edge of spec.edges) {
    if (preds.has(edge.to) && succs.has(edge.from)) {
      preds.get(edge.to)!.push(edge.from);
      succs.get(edge.from)!.push(edge.to);
    }
  }

  // Longest-path layering over a Kahn topological order; leftovers (cycles)
  // are appended to the last column so nothing overlaps at the origin.
  const layer = new Map<string, number>();
  const indeg = new Map<string, number>(ids.map((id) => [id, preds.get(id)!.length]));
  const queue = ids.filter((id) => indeg.get(id) === 0);
  queue.forEach((id) => layer.set(id, 0));
  const ordered: string[] = [];
  while (queue.length) {
    const id = queue.shift()!;
    ordered.push(id);
    for (const next of succs.get(id)!) {
      layer.set(next, Math.max(layer.get(next) ?? 0, (layer.get(id) ?? 0) + 1));
      indeg.set(next, (indeg.get(next) ?? 0) - 1);
      if (indeg.get(next) === 0) queue.push(next);
    }
  }
  const maxLayer = ordered.reduce((m, id) => Math.max(m, layer.get(id) ?? 0), 0);
  for (const id of ids) {
    if (!layer.has(id)) layer.set(id, maxLayer + 1); // cyclic leftovers
  }

  const rowInLayer = new Map<number, number>();
  const positions: Record<string, { x: number; y: number }> = {};
  for (const id of ids) {
    const l = layer.get(id) ?? 0;
    const row = rowInLayer.get(l) ?? 0;
    rowInLayer.set(l, row + 1);
    positions[id] = { x: X0 + l * COL_WIDTH, y: Y0 + row * ROW_HEIGHT };
  }
  return positions;
}
