"""
tests/conftest.py — openframe-adapters-db-oracle
===================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.

All tests run with zero network calls. oracledb is mocked at the
``openframe.adapters.db.oracle.connection`` import level so no real
Oracle server is needed.
"""
from __future__ import annotations

# Canonical OTel reset fixtures from openframe-core v3.0.
# Provides (autouse): reset_telemetry_state
# Provides (on-demand): span_exporter, metric_reader
from openframe.core.testing.fixtures import *  # noqa: F401, F403

import pytest
from unittest.mock import AsyncMock, MagicMock


def _make_mock_cursor() -> MagicMock:
    """A fully mocked oracledb.AsyncCursor supporting `async with`."""
    cur = MagicMock()
    cur.execute = AsyncMock()
    cur.fetchone = AsyncMock(return_value=None)
    cur.fetchall = AsyncMock(return_value=[])
    cur.description = [("ID",), ("NAME",)]
    cur.rowcount = 0
    cur.__aenter__ = AsyncMock(return_value=cur)
    cur.__aexit__ = AsyncMock(return_value=False)
    return cur


@pytest.fixture
def mock_pool() -> MagicMock:
    """A fully mocked oracledb.AsyncConnectionPool."""
    pool = MagicMock()
    conn = MagicMock()
    conn.cursor = MagicMock(side_effect=_make_mock_cursor)
    conn.commit = AsyncMock()
    conn.close = AsyncMock()
    pool.acquire = AsyncMock(return_value=conn)
    pool.release = AsyncMock()
    pool.close = AsyncMock()
    # Expose the connection/cursor factory for tests to reach into.
    pool._conn = conn
    return pool


@pytest.fixture
def mock_settings() -> object:
    """An OracleSettings instance with a dummy DSN."""
    from openframe.adapters.db.oracle import OracleSettings

    return OracleSettings(oracle_dsn="test/test@localhost:1521/testdb")


@pytest.fixture
def repo(mock_settings: object, mock_pool: MagicMock, monkeypatch: pytest.MonkeyPatch):
    """
    An OracleRepository wired to a mocked pool.

    The mock pool is injected directly into ``_pool_cache`` so
    ``get_oracle_pool()`` never attempts a real connection.
    """
    from openframe.adapters.db.oracle import OracleRepository
    import openframe.adapters.db.oracle.connection as conn_module

    conn_module._pool_cache[conn_module._cache_key(mock_settings)] = mock_pool  # type: ignore[attr-defined]
    r = OracleRepository(mock_settings, table="items", id_column="id")  # type: ignore[arg-type]
    yield r
    conn_module._pool_cache.clear()
