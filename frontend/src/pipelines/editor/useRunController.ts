import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "../../api/client";
import { streamRunEvents } from "../../api/sse";
import type { RunDetail, RunErrorRead, RunLogRead, RunStatus } from "../../api/types";
import type { NodeRunState } from "./mapping";

export type NodeStates = Record<string, NodeRunState>;

export interface RunView {
  runId: string | null;
  status: RunStatus | "idle";
  running: boolean;
  logs: RunLogRead[];
  errors: RunErrorRead[];
  nodeStates: NodeStates;
}

const EMPTY: RunView = {
  runId: null,
  status: "idle",
  running: false,
  logs: [],
  errors: [],
  nodeStates: {},
};

const TERMINAL = new Set(["succeeded", "failed"]);

function nodeStatesFromDetail(detail: RunDetail): NodeStates {
  const states: NodeStates = {};
  const results = detail.result?.node_results ?? {};
  for (const [id, r] of Object.entries(results)) states[id] = r.status;
  for (const err of detail.errors) states[err.node_id] = "failed";
  return states;
}

function bumpRunning(states: NodeStates, nodeId: string): NodeStates {
  const current = states[nodeId];
  if (current === "succeeded" || current === "failed" || current === "skipped") return states;
  return { ...states, [nodeId]: "running" };
}

export function useRunController(pipelineId: string) {
  const [view, setView] = useState<RunView>(EMPTY);
  const abortRef = useRef<(() => void) | null>(null);

  const stop = useCallback(() => {
    abortRef.current?.();
    abortRef.current = null;
  }, []);

  useEffect(() => () => stop(), [stop]);

  const finalize = useCallback(async (runId: string) => {
    try {
      const detail = await api.getRun(runId);
      setView((v) => ({
        runId,
        status: detail.status,
        running: false,
        logs: detail.logs.length ? detail.logs : v.logs,
        errors: detail.errors,
        nodeStates: nodeStatesFromDetail(detail),
      }));
    } catch {
      setView((v) => ({ ...v, running: false }));
    }
  }, []);

  const attach = useCallback(
    (runId: string) => {
      abortRef.current = streamRunEvents(
        runId,
        (ev) => {
          if (ev.type === "log") {
            setView((v) => ({
              ...v,
              logs: [...v.logs, ev.data],
              nodeStates: ev.data.node_id ? bumpRunning(v.nodeStates, ev.data.node_id) : v.nodeStates,
            }));
          } else if (ev.type === "status") {
            setView((v) => ({ ...v, status: ev.data.status as RunStatus }));
          }
          if (ev.type === "done" || ev.type === "timeout") {
            stop();
            void finalize(runId);
          }
        },
        () => {
          stop();
          void finalize(runId);
        },
      );
    },
    [stop, finalize],
  );

  const trigger = useCallback(async () => {
    stop();
    setView({ ...EMPTY, status: "queued", running: true });
    let run;
    try {
      run = await api.triggerRun(pipelineId);
    } catch (e) {
      setView({
        ...EMPTY,
        status: "failed",
        running: false,
        errors: [
          {
            id: 0,
            node_id: "__pipeline__",
            node_type: "pipeline",
            category: "config",
            message: e instanceof Error ? e.message : String(e),
            http_status: null,
            request_summary: null,
            attempts: 1,
            timestamp: new Date().toISOString(),
            details: null,
          },
        ],
      });
      return;
    }
    setView((v) => ({ ...v, runId: run.id, status: run.status }));
    attach(run.id);
  }, [pipelineId, stop, attach]);

  const showRun = useCallback(
    async (runId: string) => {
      stop();
      const detail = await api.getRun(runId);
      const terminal = TERMINAL.has(detail.status);
      setView({
        runId,
        status: detail.status,
        running: !terminal,
        logs: detail.logs,
        errors: detail.errors,
        nodeStates: nodeStatesFromDetail(detail),
      });
      if (!terminal) attach(runId);
    },
    [stop, attach],
  );

  const clear = useCallback(() => {
    stop();
    setView(EMPTY);
  }, [stop]);

  return { view, trigger, showRun, clear };
}
