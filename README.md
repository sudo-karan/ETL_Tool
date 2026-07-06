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

**Current status: Phase 2 complete** — headless engine with nine node types
(api / file / db sources & sinks, iterator, merge, transform, decrypt),
connectivity diagnostics for API **and** database sources, a shared crypto
layer, structured errors, `run`/`test`/`validate` CLI, full test suite.

| Phase | Scope | Status |
| --- | --- | --- |
| 1 | Headless engine: schema, api_source / iterator / merge / transform, test_connection, CLI, NodeError | ✅ done |
| 2 | file_source/sink (CSV/JSON/JSONL/Parquet), db_source/sink (Postgres/SQLite) + db test_connection, decrypt node, API-to-API chaining, SecretsProvider | ✅ done |
| 3a | Server foundation: PostgreSQL, Alembic, JWT auth, pipeline CRUD | ⏳ next |
| 3b | arq + Redis execution service, run tracking, streaming | — |
| 3c | Scheduler (cron over `schedules` table, timezone-aware) | — |
| 4 | React Flow UI | — |

*(The `SecretsProvider` interface + env provider and the `$upstream`/`$iter`
reference engine landed in Phase 1 — api_source auth and iterator fan-out
needed them — so Phase 2 hardened API-to-API chaining rather than introducing
it. The Phase 2 crypto layer is deliberately shared: the `decrypt` node uses
it now and the Phase 3 server will reuse it to encrypt secrets at rest.)*

## Install & test

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest            # 233 tests, all hermetic (mocked APIs, loopback servers, SQLite)

# To run db_source/db_sink against a real PostgreSQL server, add the driver:
pip install -e ".[dev,postgres]"     # pulls in asyncpg
```

The database nodes go through SQLAlchemy 2.0 async: **PostgreSQL** via
`asyncpg` (the `[postgres]` extra) and **SQLite** via `aiosqlite` — the same
code path, so the whole node stack is tested hermetically against SQLite with
no server to stand up.

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

Rungs for `api_source`: **dns** (resolution) → **ssrf_policy** (see below) →
**tcp** (host:port reachable) → **tls** (handshake, https only) → **http**
(request without credentials, any status = reachable) → **auth** (does the
supplied credential avoid 401/403?).

`test` also works on a **`db_source`** definition (connect + auth + a trivial
`SELECT 1`), reusing the same SSRF host guard:

```
$ etl test examples/db_source.json --allow-host db.internal.example
target: postgresql://db.internal.example:5432/shop
  ✔ dns             3.1 ms  10.2.0.9
  ✔ ssrf_policy             host allowed by SSRF policy
  ✔ tcp             1.4 ms  10.2.0.9:5432 reachable
  ✔ connect        22.0 ms  connected as readonly
  ✔ query           0.9 ms  trivial query (SELECT 1) succeeded
result: OK
```

Rungs for `db_source`: **dns** → **ssrf_policy** → **tcp** → **connect** (the
handshake covers TLS + authentication) → **query** (a trivial `SELECT 1`).
For SQLite the network rungs are skipped (it is a local file).

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
implicitly — `transform` ops flatten/derive explicitly. The tabular file/db
nodes (`file_source/sink`, `db_source/sink`) materialize through polars /
pyarrow, JSON-encoding any still-nested values into columnar cells.

## Node types

Nine built-in types. Each registers through the plugin interface, so the
engine needs no changes to gain a node (see *Repository layout*).

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

### `file_source` / `file_sink` (Phase 2)

Read/write local `csv`, `json`, `jsonl` or `parquet`. Format is inferred from
the path suffix or set with `format`.

```jsonc
// file_source
{ "path": "data/users.json",
  "format": "auto",                    // auto | csv | json | jsonl | parquet
  "limit": 1000,                       // cap rows (preview/testing)
  "records_path": "data.items",        // json only: dotted path to the list
  "has_header": true, "delimiter": ",", "infer_schema": true }   // csv opts

// file_sink (pass-through: also emits the records on `out`)
{ "path": "out/report.csv", "format": "auto",
  "mode": "overwrite",                 // overwrite | append | error
  "make_parents": true,                // create missing directories
  "json_indent": 2 }
```

**Format contract:** `json`/`jsonl` round-trip nested structure exactly.
`csv`/`parquet` are tabular — scalar columns keep their types, but nested
values (objects/arrays) are JSON-encoded into a string cell. Use json/jsonl
when you need to preserve nesting.

### `db_source` / `db_sink` (Phase 2)

Query or load PostgreSQL / SQLite through SQLAlchemy async. The password is a
`secret_ref` (never in the JSON); the connection host passes the SSRF guard
before any connection. Result values are coerced to JSON-serializable forms
(datetime→ISO string, Decimal→float, bytes→Base64, UUID→string).

```jsonc
// db_source
{ "connection": {
    "driver": "postgresql",            // postgresql | sqlite
    "host": "db.internal", "port": 5432,
    "database": "shop",                // sqlite: a file path or ":memory:"
    "user": "readonly",
    "secret_ref": "PGPASSWORD",        // password, resolved at run time
    "sslmode": "require" },            // "disable" turns TLS off
  "query": "SELECT id, total FROM orders WHERE created_at >= :since",
  "params": { "since": "2026-01-01" }, // bound as :name (no string-building)
  "limit": 1000 }

// db_sink (pass-through: also emits the records on `out`)
{ "connection": { "driver": "sqlite", "database": "out.db" },
  "table": "orders", "schema": null,
  "mode": "append",                    // append | replace (replace clears first)
  "create": true }                     // create a missing table from the records
```

`db_sink` reflects an existing table and inserts the intersecting columns
(nested values JSON-encoded); with `create: true` it infers a table from the
record keys. `replace` clears the table inside the same transaction, then
inserts. Table/schema names are validated as plain SQL identifiers.

### `decrypt` (Phase 2)

Field-level decryption. Selected fields (dotted paths) hold ciphertext; the
node replaces them with the plaintext. The key is a `secret_ref`, and the
crypto layer is shared with the Phase 3 secrets-at-rest store.

```jsonc
{ "algo": "fernet",                    // fernet | aes-gcm
  "secret_ref": "FIELD_KEY",           // key material (never in the JSON)
  "fields": ["email_enc", "user.ssn"], // dotted paths to decrypt
  "key_encoding": "base64",            // aes-gcm key: base64 | hex | raw
  "token_encoding": "base64",          // aes-gcm token: base64 | hex
  "aad": null,                         // aes-gcm additional authenticated data
  "output": "text",                    // text | json | bytes_base64
  "on_missing": "error" }              // error | skip (field absent in a record)
```

- **fernet** — token/key are the standard urlsafe-Base64 Fernet forms.
- **aes-gcm** — token layout is `nonce(12) ‖ ciphertext ‖ tag(16)`,
  transported Base64 (default) or hex; key is 16/24/32 bytes; optional AAD.

Input records are deep-copied (never mutated). Decrypted plaintext flows on
the output edge but is never logged; the key is redacted from every log and
error. A wrong key / tampered token surfaces as a `decryption` NodeError.

```bash
# runnable demo (the demo key is public — never do this for real data):
ETL_SECRET_DEMO_KEY='ZXRsLXRvb2wtZGVtby1rZXktMDEyMzQ1Njc4OWFiY2Q=' \
  etl run examples/pipeline_decrypt.json
```

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

**SSRF guard.** `test_connection`, `api_source` **and `db_source`/`db_sink`**
make the *server* open connections to user-supplied hosts — a Server-Side
Request Forgery primitive. By default the engine refuses to touch private,
loopback, link-local, CGNAT and otherwise non-global ranges (which covers
cloud metadata endpoints such as `169.254.169.254`), for the tester **and**
pipeline runs, HTTP **and** database hosts alike. Internal ETL deployments
opt in per host with an allowlist (`--allow-host db.internal`, `--allow-host
10.2.0.0/16`) — entries may be hostnames, IPs or CIDR blocks. `--no-ssrf-guard`
disables the guard entirely (not recommended).

**File access policy.** `file_source`/`file_sink` are a local read/write
primitive. A `FileAccessPolicy` (`ExecutionOptions.file_policy`) can confine
every path to a set of allowed directories — the multi-user server will set
this per deployment/user. The headless/dev default is unrestricted (a local
CLI legitimately reads arbitrary paths); symlink and `..` traversal are
resolved before the containment check.

**Secrets & crypto.** Pipeline JSON carries `secret_ref` names only. The
`SecretsProvider` interface resolves refs at run time; the env-backed provider
(`ETL_SECRET_<REF>`) is prefix-scoped so pipelines cannot read arbitrary
process env. The `decrypt` node's crypto layer (`crypto.py`, AES-GCM / Fernet)
is the same one Phase 3 will use to store secret values encrypted at rest in
PostgreSQL, decrypting them only inside the worker.

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
- **Records are buffered in memory** (no streaming), so file/db loads are
  bounded by RAM — fine for API-sized payloads and moderate tables; chunked
  streaming is future work.
- **DB value coercion is lossy where JSON is:** `Decimal` becomes `float`
  (use a `transform` cast or `SELECT col::text` if you need exact precision),
  and `bytes` become Base64.
- **DB TLS is coarse in the engine:** `sslmode` is `disable` (off) vs. anything
  else (on, default verification). Fine-grained cert pinning / `verify-full`
  lands with the Phase 3 server HTTP+DB layer. SQLite is intended for local
  dev/testing; PostgreSQL is the production target.

## Repository layout

```
src/etl_core/
  schema.py         pipeline JSON schema (pydantic)
  engine.py         validation + topological execution + iterator fan-out
  references.py     $upstream / $iter resolution
  paths.py          dotted-path get/set over JSON-like data
  http_client.py    shared HTTP layer: auth, retry, rate limit, pagination
  db.py             shared DB layer: SQLAlchemy async, SSRF host guard, coercion
  fileio.py         file formats (csv/json/jsonl/parquet) + FileAccessPolicy
  crypto.py         shared AES-GCM / Fernet layer (decrypt node + Phase 3 secrets)
  diagnostics.py    test_connection ladder (api_source + db_source)
  ssrf.py           SSRF policy (default-deny private ranges + allowlist)
  secrets.py        SecretsProvider interface + env/static providers
  redact.py         secret redaction for logs/errors/URLs
  errors.py         NodeError + error categories
  events.py         structured per-run log events
  context.py        per-run execution context (no global state)
  cli.py            etl run / test / validate
  nodes/            plugin interface + api_source, file_source/sink,
                    db_source/sink, iterator, merge, transform, decrypt
tests/              233 tests: unit + engine integration (respx-mocked APIs,
                    loopback HTTP/TLS servers, SQLite-backed db nodes)
examples/           runnable pipelines (api chaining, files, decrypt) + sources
```

New node types register via the plugin interface — subclass `Node`, declare
a pydantic `config_model`, decorate with `@register_node` — no engine
changes required (the test suite's `static_source`/`probe` nodes do exactly
this).
