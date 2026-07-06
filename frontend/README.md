# ETL Tool — Frontend (Phase 4)

A React + TypeScript + Vite single-page app built on **React Flow**. It is a
pure consumer of the `etl_server` API — it holds no business logic and emits the
**same pipeline JSON** the engine defines. Every screen renders backend state.

## Screens

- **Login** — register / sign in (JWT stored in `localStorage`).
- **Pipelines** — per-user list; create / open / delete.
- **Editor** — drag-and-drop React Flow canvas with a node palette (all nine
  engine node types), per-node config panels (quick fields + a raw-JSON escape
  hatch), and **Save**, which emits the pipeline JSON schema. Layout is
  auto-computed and drag positions persist per pipeline in `localStorage`.
- **Run monitor** — trigger a run, watch **per-node status** light up on the
  canvas (idle / running / succeeded / failed) and **live logs stream** over
  SSE. A run-history tab replays past runs.
- **Error drill-down** — click a failed node (or an entry in the Errors tab) to
  see its structured `NodeError`: category, HTTP status, redacted request
  summary, retries, details.
- **Diagnostics** — run `test_connection` on an `api_source` / `db_source` and
  see the DNS → TCP → TLS → HTTP → auth ladder with pass/fail + latency + a
  truncated, redacted sample body.
- **Schedules** — cron + IANA timezone, enable/disable, next/last run.
- **Secrets** — store encrypted secrets by `ref` (values are never returned).

## Develop

```bash
npm install
# Point at the API (defaults to http://localhost:8000):
echo 'VITE_API_BASE=http://localhost:8000' > .env
npm run dev            # http://localhost:5173
```

Run the API without Redis in a second terminal:

```bash
pip install -e ".[server]"
ETL_JWT_SECRET=dev-secret-please-change-me-32bytes \
  uvicorn etl_server.app:dev_app --factory --reload   # SQLite + inline runs
```

## Scripts

```bash
npm run typecheck   # tsc project references, no emit
npm run build       # tsc -b && vite build  ->  dist/
npm test            # vitest: mapping, layout, SSE-frame parsing
npm run preview     # serve the production build
```

## Notes

- SSE uses `fetch` + a `ReadableStream` reader (not `EventSource`) so the
  `Authorization: Bearer` header can be sent.
- CORS is permissive on the server in dev; restrict `allow_origins` in prod.
- The connection host allowlist, secrets and SSRF policy live on the server —
  the UI only surfaces their effects.
