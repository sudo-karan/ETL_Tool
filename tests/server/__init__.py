# Makes tests/server a package so its conftest namespaces as `server.conftest`
# and does not shadow the root tests/conftest.py that the engine tests import
# via `from conftest import ...`.
