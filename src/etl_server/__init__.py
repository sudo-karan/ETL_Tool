"""etl_server: the Phase 3 multi-user server around the headless engine.

Layers (all built on the same pipeline JSON contract the engine defines):

* **3a foundation** -- PostgreSQL models, Alembic migrations, JWT auth, and
  per-user pipeline CRUD, with the SSRF guard wired into the HTTP/DB layer.
* **3b execution** -- an arq/Redis worker that resolves encrypted secrets,
  invokes the engine once per run, tracks status and persists structured
  logs/errors; run-trigger, connectivity-test and live SSE streaming endpoints.
* **3c scheduler** -- a per-minute tick that enqueues due schedules (evaluated
  in each schedule's timezone) onto the very same run queue.

The server is a *consumer* of the engine: it never reimplements node logic.
The React Flow UI (Phase 4) will in turn be a consumer of these endpoints.
"""

__all__ = ["__version__"]

__version__ = "0.3.0"
