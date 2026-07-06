"""The ASGI app factories wire the expected routes."""
from __future__ import annotations

from etl_server.app import dev_app, production_app

EXPECTED = {"/health", "/auth/token", "/pipelines", "/runs", "/secrets", "/test-connection", "/schedules"}


def test_dev_app_registers_routes():
    assert EXPECTED <= set(dev_app().openapi()["paths"])


def test_production_app_registers_routes():
    assert EXPECTED <= set(production_app().openapi()["paths"])
