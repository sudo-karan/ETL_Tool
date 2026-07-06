# ETL Tool — visual node-based ETL platform

A multi-user platform where data pipelines are composed as directed graphs,
tested for connectivity, scheduled, executed concurrently on a server, and
inspected — all from a browser. The primary use case is **API integration**:
chaining one API's output into another API's input, fanning out calls over a
value set, and merging results.

## Architecture: three layers, one JSON contract

A pipeline is a serializable JSON graph (nodes + edges + per-node config).
The layers are built in order, and the JSON schema is the contract between
them:

1. **Execution engine** (this phase) — a pure, stateless function of
   *(pipeline spec, resolved secrets, options) → (outputs, structured
   logs/errors)*. No module-level mutable state, so many runs execute
   concurrently without interfering.
2. **Server** (Phase 3) — FastAPI + PostgreSQL + arq/Redis: auth, pipeline
   storage, a job queue whose workers invoke the engine once per run,
   scheduling, connectivity diagnostics, run tracking, log/error streaming,
   encrypted secrets.
3. **UI** (Phase 4) — React + React Flow drag-and-drop editor that emits the
   same JSON schema. Thick in screens, thin in logic: every screen renders
   backend state.

**Current status: Phase 1 complete** — headless engine, four node types,
connectivity tester, structured errors, `run`/`test`/`validate` CLI, full
test suite.

| Phase | Scope | Status |
| --- | --- | --- |
| 1 | Headless engine: schema, api_source / iterator / merge / transform, test_connection, CLI, NodeError | ✅ done |
| 2 | file/db source+sink, decrypt node, richer chaining *(SecretsProvider interface + env provider already landed in Phase 1 because api_source auth needs it)* | ⏳ next |
| 3a | Server foundation: PostgreSQL, Alembic, JWT auth, pipeline CRUD | — |
| 3b | arq + Redis execution service, run tracking, streaming | — |
| 3c | Scheduler (cron over `schedules` table, timezone-aware) | — |
| 4 | React Flow UI | — |

## Install & test

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest            # 163 tests, all hermetic (mocked APIs / loopback servers)
```

## CLI

### `etl run <pipeline.json>`

```bash
etl run examples/pipeline.json                 # uses the public JSONPlaceholder API
etl run pipeline.json --json                   # full RunResult as JSON
etl run pipeline.json --output result.json     # write RunResult to a file
etl run pipeline.json --continue-on-error      # don't fail-fast
etl run pipeline.json --allow-host 10.0.0.5    # SSRF allowlist (repeatable)
```

Secrets are provided per run, never stored in pipeline JSON:

```bash
export ETL_SECRET_GITHUB_TOKEN=ghp_...         # ref "GITHUB_TOKEN"
etl run pipeline.json
# or (dev only)
etl run pipeline.json --secrets-file secrets.json   # {"GITHUB_TOKEN": "..."}
```

Exit codes: `0` success, `1` run failed, `2` invalid input/usage.

### `etl test <source.json>` — connectivity diagnostics

Answers "can the **server** reach this source?" with a pass/fail + latency
ladder, and shows a truncated, secret-redacted sample of the response body:

```
$ etl test examples/source.json
target: https://jsonplaceholder.typicode.com/users
  ✔ dns             12.1 ms  104.21.48.1, 172.67.146.185
  ✔ ssrf_policy              host allowed by SSRF policy
  ✔ tcp              8.7 ms  104.21.48.1:443 reachable
  ✔ tls             22.9 ms  TLSv1.3
  ✔ http            41.3 ms  HTTP 200 without credentials
  ○ auth                     no credentials configured
result: OK
```

Rungs: **dns** (resolution) → **ssrf_policy** (see below) → **tcp**
(host:port reachable) → **tls** (handshake, https only) → **http** (request
without credentials, any status = reachable) → **auth** (does the supplied
credential avoid 401/403?).

### `etl validate <pipeline.json>`

Static checks without running: unique ids, known node types, config schema,
ports and edges, acyclicity, reference targets, iterator scoping.

## Pipeline JSON

```json
{
  "pipeline_id": "users-posts-report",
  "nodes": [
    { "id": "users", "type": "api_source",
      "config": { "url": "https://jsonplaceholder.typicode.com/users" } },
    { "id": "user_ids", "type": "iterator",
      "config": { "mode": "array", "array": [1, 2, 3] } },
    { "id": "posts", "type": "api_source",
      "config": { "url": "https://jsonplaceholder.typicode.com/posts",
                   "query_params": { "userId": "$iter.value" } } }
  ],
  "edges": [
    { "from": "user_ids", "to": "posts", "from_port": "out", "to_port": "in" }
  ]
}
```

`from_port`/`to_port` default to `"out"`/`"in"`. See
[examples/pipeline.json](examples/pipeline.json) for a full graph that
chains two APIs, fans out over user ids, joins against a lookup, and
filters/derives columns.

### Upstream & iterator references

Any config value can reference upstream node data or the current iterator
value — this is what makes API-to-API chaining work:

| Syntax | Meaning |
| --- | --- |
| `"$upstream.n1.id"` | field `id` of node `n1`'s **first** record (whole-string form preserves the value's type) |
| `"$upstream.n1.2.id"` | field `id` of record index 2 |
| `"$upstream.n1"` | node `n1`'s full record list |
| `"${upstream.n1.id}"` | embedded string interpolation: `"https://api/x/${upstream.n1.id}/y"` |
| `"$iter.value"` / `"$iter.index"` | current iterator value / position (only inside an iterator's downstream subgraph) |
| `"$$upstream…"` | escaped literal `$upstream…` |

`$upstream` targets must be ancestors in the graph (validated). To fan out
over *all* records of an upstream node, use an iterator with
`mode: from_upstream`.

### Data contract

Data on every edge is a stream of JSON-serializable records
(`list[dict]`). API payloads are nested JSON, so nothing is flattened
implicitly — `transform` ops flatten/derive explicitly. Arrow/polars
materialization for genuinely tabular data arrives with the file/db nodes in
Phase 2 (the dependencies are already pinned).

## Node types (Phase 1)

### `api_source`

```jsonc
{
  "method": "GET",                       // GET|POST|PUT|PATCH|DELETE|HEAD
  "url": "https://api.x.com/users/{uid}/posts",
  "path_params": { "uid": "$upstream.login.id" },   // fills {uid}, URL-quoted
  "query_params": { "limit": 10 },
  "headers": { "Accept": "application/json" },
  "body": null,                          // JSON body for POST/PUT/PATCH
  "auth": {                              // credential comes from a secret ref
    "type": "bearer",                    // bearer | api_key | basic
    "secret_ref": "MY_TOKEN",
    "name": "X-API-Key",                 // api_key: header/param name
    "in": "header",                      // api_key: header | query
    "username": "alice"                  // basic (password = secret)
  },
  "pagination": {
    "type": "cursor",                    // cursor | offset | page
    "items_path": "data.items",          // where records live in the body
    "cursor_path": "meta.next",          // cursor: read next cursor here...
    "cursor_param": "cursor",            // ...and send it as this param
    "offset_param": "offset",            // offset mode
    "limit_param": "limit", "limit": 100,
    "page_param": "page", "start_page": 1, "page_size": 50,  // page mode
    "max_pages": 100                     // safety cap, all modes
  },
  "retry": { "max": 3, "backoff": 0.5 }, // exponential; retries transport
                                          // errors, 429 and 5xx; honors
                                          // numeric Retry-After
  "rate_limit": { "rps": 5 },            // per-node, per-run
  "timeout_s": 30,
  "items_path": "data",                  // for non-paginated responses
  "verify_tls": true
}
```

### `iterator` (ForEach / fan-out)

```jsonc
{
  "mode": "array",                       // array | range | from_upstream
  "array": [1, 2, 3],                    // or "$upstream.n1" (must resolve to a list)
  "range": { "start": 0, "end": 10, "step": 2 },   // end exclusive
  "field": "user.id",                    // from_upstream: extract per record
  "fan_in": "concat",                    // concat | keyed
  "max_concurrency": 4                   // cap concurrent iterations
}
```

The engine executes the iterator's entire downstream subgraph once per
value (concurrently, capped), then fans results back in: `concat`
concatenates records in iteration order; `keyed` yields one record per
iteration: `{"key": <value>, "records": [...]}`. Nodes in the subgraph may
also take *constant* inputs from outside it (e.g. a lookup table for a
join). Nested iterators and overlapping iterator scopes are rejected at
validation in v1.

### `merge`

```jsonc
{ "strategy": "join",                    // concat | union | join
  "keys": ["userId"],                    // join keys (dotted paths allowed)
  "how": "inner",                        // inner | left | outer
  "suffix": "_right" }                   // for colliding non-key fields
```

`concat` appends inputs in edge order; `union` also drops exact duplicates.
`join` takes exactly two inputs (first edge = left). Records missing a join
key never match (kept by `left`/`outer` on their side).

### `transform`

```jsonc
{ "ops": [
  { "op": "select", "fields": ["id", "user.name"] },
  { "op": "rename", "mapping": { "id": "userId" } },
  { "op": "filter", "predicate": {
      "all": [ { "field": "score", "op": "gte", "value": 10 },
               { "not": { "field": "hidden", "op": "eq", "value": true } } ] } },
  { "op": "computed", "target": "label", "expression": "upper(name) + '!'" }
] }
```

Ops apply in order; records are never mutated in place. Predicate ops:
`eq ne gt gte lt lte in not_in contains regex exists not_exists`, composable
with `all` / `any` / `not`. Computed expressions run in a whitelisted-AST
interpreter (arithmetic, comparisons, ternary, `len/str/int/float/bool/
round/abs/min/max/lower/upper/strip`) — no Python `eval`, no attribute or
import access; missing fields read as `None`.

## Error model

Every failure is a structured `NodeError`, persisted with the run and (in
Phase 3) streamed live — never a bare string:

```json
{
  "node_id": "posts", "node_type": "api_source",
  "category": "http_status",
  "message": "HTTP 503 Service Unavailable",
  "http_status": 503,
  "request_summary": "GET https://api.test/posts?api_key=***",
  "attempts": 3,
  "timestamp": "2026-07-06T10:00:00Z",
  "details": { "iteration_index": 1, "iteration_value": "2" }
}
```

Categories: `dns network tls timeout http_status auth rate_limit validation
transform decryption config unknown`. Secrets are redacted from every
message, request summary, log line and diagnostic sample (plain,
URL-encoded and Base64 forms). Fail-fast is the default;
`continue_on_error` keeps independent branches and iterations running and
marks only the failure's descendants as skipped.

## Security

**SSRF guard.** `test_connection` and `api_source` make the *server* issue
requests to user-supplied hosts — a Server-Side Request Forgery primitive.
By default the engine refuses to touch private, loopback, link-local, CGNAT
and otherwise non-global ranges (which covers cloud metadata endpoints such
as `169.254.169.254`), for both the tester **and** pipeline runs. Internal
ETL deployments opt in per host with an allowlist (`--allow-host
db.internal`, `--allow-host 10.2.0.0/16`) — entries may be hostnames, IPs
or CIDR blocks. `--no-ssrf-guard` disables the guard entirely (not
recommended).

**Secrets.** Pipeline JSON carries `secret_ref` names only. The
`SecretsProvider` interface resolves refs at run time; Phase 1 ships an
env-backed provider (`ETL_SECRET_<REF>`, prefix-scoped so pipelines cannot
read arbitrary process env). Phase 3 stores secret values AES-GCM/Fernet
encrypted in PostgreSQL and decrypts them only inside the worker.

## Concurrency model

Each run gets its own context (log, redactor, semaphores, rate limiters) —
the engine holds no global state, so concurrent runs are isolated by
construction (there's a test that runs 12 pipelines concurrently). Within a
run, iterator iterations execute concurrently and all HTTP requests share a
per-run semaphore (`max_concurrency`, default 8). Per-node `rate_limit.rps`
holds across concurrent iterations of that node.

## Known limitations (v1, by design — documented for later phases)

- **Rate limiting is per-run only.** Multiple concurrent runs hitting the
  same external API are not globally throttled. Future: global, cross-run,
  per-host rate limiting in the server layer.
- **SSRF DNS pinning.** The guard resolves and checks a host, then the HTTP
  client resolves again; a hostile DNS server could rebind between lookups.
  Pinning the vetted IP into the connection is planned for Phase 3, where
  the guard is baked into the server's HTTP layer.
- Nested / overlapping iterator scopes are rejected rather than executed.
- Run outputs are buffered in memory; fine for API-sized payloads, revisit
  for large file/db loads in Phase 2.

## Repository layout

```
src/etl_core/
  schema.py         pipeline JSON schema (pydantic)
  engine.py         validation + topological execution + iterator fan-out
  references.py     $upstream / $iter resolution
  http_client.py    shared HTTP layer: auth, retry, rate limit, pagination
  diagnostics.py    test_connection ladder
  ssrf.py           SSRF policy (default-deny private ranges + allowlist)
  secrets.py        SecretsProvider interface + env/static providers
  redact.py         secret redaction for logs/errors/URLs
  errors.py         NodeError + error categories
  events.py         structured per-run log events
  context.py        per-run execution context (no global state)
  cli.py            etl run / test / validate
  nodes/            plugin interface + api_source, iterator, merge, transform
tests/              163 tests: unit + engine integration (respx-mocked APIs,
                    loopback HTTP/TLS servers for diagnostics)
examples/           runnable pipeline + source definitions
```

New node types register via the plugin interface — subclass `Node`, declare
a pydantic `config_model`, decorate with `@register_node` — no engine
changes required (the test suite's `static_source`/`probe` nodes do exactly
this).
