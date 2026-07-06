"""arq worker entrypoint.

Run the worker pool (which also carries the per-minute scheduler tick) with::

    arq etl_server.arq_worker.WorkerSettings

Kept separate from ``worker.py`` so importing the run logic (``execute_run``)
from the API process does not require the arq/Redis dependency at import time.
"""
from __future__ import annotations

from .worker import build_worker_settings

WorkerSettings = build_worker_settings()
