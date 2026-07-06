import { describe, expect, it } from "vitest";
import { flowToSpec, specToFlow } from "../pipelines/editor/mapping";
import { layeredLayout } from "../pipelines/editor/layout";
import { parseFrame } from "../api/sse";
import type { PipelineSpec } from "../api/types";

const SPEC: PipelineSpec = {
  pipeline_id: "demo",
  nodes: [
    { id: "gen", type: "iterator", config: { mode: "array", array: [1, 2] } },
    { id: "double", type: "transform", config: { ops: [] } },
  ],
  edges: [{ from: "gen", from_port: "out", to: "double", to_port: "in" }],
};

describe("RF <-> pipeline mapping", () => {
  it("round-trips a spec through React Flow and back", () => {
    const { nodes, edges } = specToFlow(SPEC, { gen: { x: 0, y: 0 }, double: { x: 1, y: 0 } });
    const back = flowToSpec(SPEC.pipeline_id, nodes, edges);
    expect(back.pipeline_id).toBe("demo");
    expect(back.nodes).toEqual(SPEC.nodes);
    expect(back.edges).toEqual(SPEC.edges); // ports normalized to out/in
  });

  it("preserves node config objects", () => {
    const { nodes } = specToFlow(SPEC, {});
    expect(nodes[0].data.config).toEqual({ mode: "array", array: [1, 2] });
    expect(nodes[0].type).toBe("etlNode");
  });
});

describe("layeredLayout", () => {
  it("places downstream nodes to the right of their sources", () => {
    const pos = layeredLayout(SPEC);
    expect(pos.double.x).toBeGreaterThan(pos.gen.x);
  });

  it("does not stack disconnected nodes at the origin", () => {
    const spec: PipelineSpec = {
      pipeline_id: "p",
      nodes: [
        { id: "a", type: "iterator", config: {} },
        { id: "b", type: "iterator", config: {} },
      ],
      edges: [],
    };
    const pos = layeredLayout(spec);
    expect(pos.a.y).not.toBe(pos.b.y);
  });
});

describe("SSE parseFrame", () => {
  it("parses a typed log event", () => {
    const frame = 'event: log\ndata: {"id":1,"seq":0,"level":"info","message":"hi","node_id":"n1"}';
    const parsed = parseFrame(frame);
    expect(parsed?.type).toBe("log");
    if (parsed?.type === "log") expect(parsed.data.message).toBe("hi");
  });

  it("parses a done event and ignores malformed frames", () => {
    expect(parseFrame('event: done\ndata: {"status":"succeeded","errors":[]}')?.type).toBe("done");
    expect(parseFrame("event: log\ndata: {not json")).toBeNull();
    expect(parseFrame(": comment only")).toBeNull();
  });
});
