// Server-Sent Events over fetch, so we can send the Authorization header
// (EventSource can't). Frames are `event: <name>\ndata: <json>\n\n`.
import type { RunErrorRead, RunLogRead } from "./types";
import { API_BASE, authHeaders } from "./client";

export type RunStreamEvent =
  | { type: "log"; data: RunLogRead }
  | { type: "status"; data: { status: string } }
  | { type: "done"; data: { status: string; errors: RunErrorRead[] | null } }
  | { type: "timeout"; data: { status: string } };

/** Parse one raw SSE frame into a typed event (pure; unit-tested). */
export function parseFrame(frame: string): RunStreamEvent | null {
  let event = "message";
  const dataLines: string[] = [];
  for (const line of frame.split("\n")) {
    if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) dataLines.push(line.slice(5).trimStart());
  }
  if (dataLines.length === 0) return null;
  let data: unknown;
  try {
    data = JSON.parse(dataLines.join("\n"));
  } catch {
    return null;
  }
  switch (event) {
    case "log":
      return { type: "log", data: data as RunLogRead };
    case "status":
      return { type: "status", data: data as { status: string } };
    case "done":
      return { type: "done", data: data as { status: string; errors: RunErrorRead[] | null } };
    case "timeout":
      return { type: "timeout", data: data as { status: string } };
    default:
      return null;
  }
}

/** Open a run event stream. Returns a function that cancels it. */
export function streamRunEvents(
  runId: string,
  onEvent: (event: RunStreamEvent) => void,
  onError?: (err: unknown) => void,
): () => void {
  const controller = new AbortController();
  void (async () => {
    try {
      const resp = await fetch(`${API_BASE}/runs/${runId}/events`, {
        headers: { ...authHeaders(), Accept: "text/event-stream" },
        signal: controller.signal,
      });
      if (!resp.ok || !resp.body) throw new Error(`stream failed: HTTP ${resp.status}`);
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let sep: number;
        while ((sep = buffer.indexOf("\n\n")) >= 0) {
          const frame = buffer.slice(0, sep);
          buffer = buffer.slice(sep + 2);
          const parsed = parseFrame(frame);
          if (parsed) onEvent(parsed);
        }
      }
    } catch (err) {
      if (!controller.signal.aborted) onError?.(err);
    }
  })();
  return () => controller.abort();
}
