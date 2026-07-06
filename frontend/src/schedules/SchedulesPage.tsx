import { useMemo, useState } from "react";
import { api } from "../api/client";
import type { PipelineRead, ScheduleRead } from "../api/types";
import { useAsync } from "../lib/useAsync";
import { EmptyState, ErrorText, Spinner } from "../components/ui";

const BROWSER_TZ = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";

export function SchedulesPage() {
  const pipelines = useAsync(() => api.listPipelines(), []);
  const schedules = useAsync(() => api.listSchedules(), []);
  const nameById = useMemo(() => {
    const map = new Map<string, string>();
    (pipelines.data ?? []).forEach((p) => map.set(p.id, p.name));
    return map;
  }, [pipelines.data]);

  return (
    <div className="page">
      <div className="page-head">
        <h1>Schedules</h1>
      </div>
      <p className="muted">
        A scheduled run is enqueued onto the same queue as a manual run. Due-ness is evaluated in
        the schedule's timezone.
      </p>

      <CreateSchedule pipelines={pipelines.data ?? []} onCreated={schedules.reload} />

      <ErrorText error={schedules.error} />
      {schedules.loading ? (
        <Spinner />
      ) : !schedules.data || schedules.data.length === 0 ? (
        <EmptyState>No schedules yet.</EmptyState>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>Pipeline</th>
              <th>Cron</th>
              <th>Timezone</th>
              <th>Next run</th>
              <th>Last run</th>
              <th>Enabled</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {schedules.data.map((s) => (
              <ScheduleRow
                key={s.id}
                schedule={s}
                pipelineName={nameById.get(s.pipeline_id) ?? s.pipeline_id}
                onChanged={schedules.reload}
              />
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function CreateSchedule({
  pipelines,
  onCreated,
}: {
  pipelines: PipelineRead[];
  onCreated: () => void;
}) {
  const [pipelineId, setPipelineId] = useState("");
  const [cron, setCron] = useState("0 9 * * *");
  const [tz, setTz] = useState(BROWSER_TZ);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function create() {
    setBusy(true);
    setError(null);
    try {
      await api.createSchedule({ pipeline_id: pipelineId, cron_expr: cron, timezone: tz });
      onCreated();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="panel">
      <div className="inline-form">
        <select value={pipelineId} onChange={(e) => setPipelineId(e.target.value)}>
          <option value="">Select pipeline…</option>
          {pipelines.map((p) => (
            <option key={p.id} value={p.id}>
              {p.name}
            </option>
          ))}
        </select>
        <input value={cron} onChange={(e) => setCron(e.target.value)} placeholder="0 9 * * *" />
        <input value={tz} onChange={(e) => setTz(e.target.value)} placeholder="America/New_York" />
        <button className="btn btn-primary" disabled={busy || !pipelineId} onClick={create}>
          Add schedule
        </button>
      </div>
      <ErrorText error={error} />
    </div>
  );
}

function ScheduleRow({
  schedule,
  pipelineName,
  onChanged,
}: {
  schedule: ScheduleRead;
  pipelineName: string;
  onChanged: () => void;
}) {
  const [busy, setBusy] = useState(false);

  async function toggle() {
    setBusy(true);
    try {
      await api.updateSchedule(schedule.id, { enabled: !schedule.enabled });
      onChanged();
    } finally {
      setBusy(false);
    }
  }
  async function remove() {
    if (!confirm("Delete schedule?")) return;
    await api.deleteSchedule(schedule.id);
    onChanged();
  }
  const fmt = (t: string | null) => (t ? new Date(t).toLocaleString() : "—");

  return (
    <tr>
      <td>{pipelineName}</td>
      <td>
        <code>{schedule.cron_expr}</code>
      </td>
      <td className="muted">{schedule.timezone}</td>
      <td className="muted">{fmt(schedule.next_run)}</td>
      <td className="muted">{fmt(schedule.last_run)}</td>
      <td>
        <button className={`toggle ${schedule.enabled ? "on" : ""}`} onClick={toggle} disabled={busy}>
          {schedule.enabled ? "Enabled" : "Disabled"}
        </button>
      </td>
      <td className="right">
        <button className="btn btn-danger sm" onClick={remove}>
          Delete
        </button>
      </td>
    </tr>
  );
}
