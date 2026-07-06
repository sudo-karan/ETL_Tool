"""``etl-server`` entrypoint: serve the API with uvicorn.

    etl-server                 # serve on 0.0.0.0:8000
    etl-server --reload        # dev autoreload

The arq worker is run separately with ``arq etl_server.arq_worker.WorkerSettings``.
Production configuration comes from ``ETL_``-prefixed env vars (see config.py).
"""
from __future__ import annotations

import argparse


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="etl-server", description="ETL Tool API server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")
    parser.add_argument(
        "--dev",
        action="store_true",
        help="single-process dev server (SQLite + inline runs, no Redis/worker)",
    )
    args = parser.parse_args(argv)

    import uvicorn

    uvicorn.run(
        "etl_server.app:dev_app" if args.dev else "etl_server.app:production_app",
        factory=True,
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
