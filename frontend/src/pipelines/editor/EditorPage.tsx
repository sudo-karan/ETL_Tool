import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  addEdge,
  useEdgesState,
  useNodesState,
  useReactFlow,
  type Connection,
  type Edge,
  type EdgeChange,
  type NodeChange,
} from "@xyflow/react";
import { api } from "../../api/client";
import type { PipelineRead, RunErrorRead } from "../../api/types";
import { useAsync } from "../../lib/useAsync";
import { ErrorText, Spinner } from "../../components/ui";
import { CanvasNode } from "./CanvasNode";
import { ConfigPanel } from "./ConfigPanel";
import { NodePalette } from "./NodePalette";
import { RunPanel } from "./RunPanel";
import { layeredLayout } from "./layout";
import { flowToSpec, specToFlow, type EtlNode } from "./mapping";
import { NODE_TYPE_MAP } from "./nodeCatalog";
import { useRunController } from "./useRunController";

const NODE_ID_RE = /^[A-Za-z_][A-Za-z0-9_-]*$/;

export function EditorPage() {
  const { id = "" } = useParams();
  const { data, error, loading } = useAsync(() => api.getPipeline(id), [id]);
  if (loading) return <Spinner />;
  if (error || !data) return <ErrorText error={error ?? "Pipeline not found"} />;
  return (
    <ReactFlowProvider>
      <Editor pipeline={data} />
    </ReactFlowProvider>
  );
}

function loadPositions(pipelineId: string): Record<string, { x: number; y: number }> {
  try {
    return JSON.parse(localStorage.getItem(`etl_pos_${pipelineId}`) ?? "{}");
  } catch {
    return {};
  }
}
function savePositions(pipelineId: string, nodes: EtlNode[]): void {
  const map = Object.fromEntries(nodes.map((n) => [n.id, n.position]));
  localStorage.setItem(`etl_pos_${pipelineId}`, JSON.stringify(map));
}

function Editor({ pipeline }: { pipeline: PipelineRead }) {
  const nodeTypes = useMemo(() => ({ etlNode: CanvasNode }), []);
  const initial = useMemo(() => {
    const positions = { ...layeredLayout(pipeline.spec), ...loadPositions(pipeline.id) };
    return specToFlow(pipeline.spec, positions);
  }, [pipeline]);

  const [nodes, setNodes, onNodesChange] = useNodesState<EtlNode>(initial.nodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>(initial.edges);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [activeError, setActiveError] = useState<RunErrorRead | null>(null);

  const { screenToFlowPosition } = useReactFlow();
  const run = useRunController(pipeline.id);
  const runs = useAsync(() => api.listRuns(pipeline.id), [pipeline.id]);

  const nodesRef = useRef(nodes);
  nodesRef.current = nodes;

  // Reflect per-node run status on the canvas.
  useEffect(() => {
    setNodes((ns) =>
      ns.map((n) => ({
        ...n,
        data: { ...n.data, runState: run.view.nodeStates[n.id] ?? "idle" },
      })),
    );
  }, [run.view.nodeStates, setNodes]);

  // Refresh run history when a run finishes.
  useEffect(() => {
    if (run.view.status === "succeeded" || run.view.status === "failed") runs.reload();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [run.view.status]);

  const markDirty = (changes: NodeChange[] | EdgeChange[]) => {
    if (changes.some((c) => c.type !== "select" && c.type !== "dimensions")) setDirty(true);
  };
  const handleNodesChange = (changes: NodeChange<EtlNode>[]) => {
    markDirty(changes);
    onNodesChange(changes);
  };
  const handleEdgesChange = (changes: EdgeChange[]) => {
    markDirty(changes);
    onEdgesChange(changes);
  };

  const onConnect = useCallback(
    (c: Connection) => {
      setEdges((eds) => addEdge({ ...c, id: `${c.source}->${c.target}` }, eds));
      setDirty(true);
    },
    [setEdges],
  );

  const addNode = useCallback(
    (type: string, position?: { x: number; y: number }) => {
      const meta = NODE_TYPE_MAP[type];
      if (!meta) return;
      setNodes((ns) => {
        let n = 1;
        while (ns.some((node) => node.id === `${type}_${n}`)) n += 1;
        const newNode: EtlNode = {
          id: `${type}_${n}`,
          type: "etlNode",
          position: position ?? { x: 120 + ns.length * 24, y: 100 + ns.length * 24 },
          data: { nodeType: type, config: structuredClone(meta.defaultConfig) },
        };
        return [...ns, newNode];
      });
      setDirty(true);
    },
    [setNodes],
  );

  const updateConfig = useCallback(
    (nodeId: string, config: Record<string, unknown>) => {
      setNodes((ns) =>
        ns.map((n) => (n.id === nodeId ? { ...n, data: { ...n.data, config } } : n)),
      );
      setDirty(true);
    },
    [setNodes],
  );

  const renameNode = useCallback(
    (oldId: string, newId: string): boolean => {
      if (!NODE_ID_RE.test(newId)) {
        alert("Invalid node id. Use letters, digits, underscore and hyphen; start with a letter.");
        return false;
      }
      if (nodesRef.current.some((n) => n.id === newId)) {
        alert(`A node named "${newId}" already exists.`);
        return false;
      }
      setNodes((ns) => ns.map((n) => (n.id === oldId ? { ...n, id: newId } : n)));
      setEdges((es) =>
        es.map((e) => {
          const source = e.source === oldId ? newId : e.source;
          const target = e.target === oldId ? newId : e.target;
          return { ...e, source, target, id: `${source}->${target}` };
        }),
      );
      setSelectedId(newId);
      setDirty(true);
      return true;
    },
    [setNodes, setEdges],
  );

  const deleteNode = useCallback(
    (nodeId: string) => {
      setNodes((ns) => ns.filter((n) => n.id !== nodeId));
      setEdges((es) => es.filter((e) => e.source !== nodeId && e.target !== nodeId));
      setSelectedId((s) => (s === nodeId ? null : s));
      setDirty(true);
    },
    [setNodes, setEdges],
  );

  const save = useCallback(async () => {
    setSaving(true);
    setSaveError(null);
    try {
      const spec = flowToSpec(pipeline.spec.pipeline_id, nodesRef.current, edges);
      await api.updatePipeline(pipeline.id, { spec });
      savePositions(pipeline.id, nodesRef.current);
      setDirty(false);
    } catch (err) {
      setSaveError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  }, [pipeline.id, pipeline.spec.pipeline_id, edges]);

  const selectedNode = nodes.find((n) => n.id === selectedId) ?? null;

  return (
    <div className="editor">
      <div className="editor-toolbar">
        <Link to="/pipelines" className="link-btn">
          ← Pipelines
        </Link>
        <h2>{pipeline.name}</h2>
        {dirty && <span className="dirty-dot" title="Unsaved changes" />}
        <div className="grow" />
        <ErrorText error={saveError} />
        <button className="btn btn-primary sm" onClick={save} disabled={!dirty || saving}>
          {saving ? "Saving…" : "Save"}
        </button>
      </div>

      <div className="editor-body">
        <NodePalette onAdd={(type) => addNode(type)} />

        <div
          className="canvas"
          onDrop={(e) => {
            e.preventDefault();
            const type = e.dataTransfer.getData("application/etl-node");
            if (type) addNode(type, screenToFlowPosition({ x: e.clientX, y: e.clientY }));
          }}
          onDragOver={(e) => {
            e.preventDefault();
            e.dataTransfer.dropEffect = "move";
          }}
        >
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onNodesChange={handleNodesChange}
            onEdgesChange={handleEdgesChange}
            onConnect={onConnect}
            onNodeDragStop={() => savePositions(pipeline.id, nodesRef.current)}
            onNodeClick={(_, node) => {
              setSelectedId(node.id);
              const err = run.view.errors.find((e) => e.node_id === node.id);
              if (err) setActiveError(err);
            }}
            onPaneClick={() => setSelectedId(null)}
            fitView
            proOptions={{ hideAttribution: true }}
          >
            <Background />
            <Controls />
            <MiniMap pannable zoomable />
          </ReactFlow>
        </div>

        <ConfigPanel
          node={selectedNode}
          onChangeConfig={(config) => selectedNode && updateConfig(selectedNode.id, config)}
          onRename={(newId) => (selectedNode ? renameNode(selectedNode.id, newId) : false)}
          onDelete={() => selectedNode && deleteNode(selectedNode.id)}
        />
      </div>

      <RunPanel
        view={run.view}
        onTrigger={run.trigger}
        onSelectRun={run.showRun}
        runs={runs.data}
        activeError={activeError}
        onOpenError={setActiveError}
      />
    </div>
  );
}
