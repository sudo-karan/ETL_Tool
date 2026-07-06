"""FastAPI routers, one module per resource."""
from . import auth, diagnostics, pipelines, runs, schedules, secrets

__all__ = ["auth", "diagnostics", "pipelines", "runs", "schedules", "secrets"]
