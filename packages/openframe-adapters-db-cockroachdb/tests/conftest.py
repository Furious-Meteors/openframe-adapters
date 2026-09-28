"""
tests/conftest.py — openframe-adapters-db-cockroachdb
========================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.

All tests run with zero network calls. asyncpg is mocked at the
``openframe.adapters.db.cockroachdb.connection`` import level so no real
CockroachDB cluster is needed.
"""
from __future__ import annotations

# Canonical OTel reset fixtures from openframe-core v3.0.
# Provides (autouse): reset_telemetry_state
# Provides (on-demand): span_exporter, metric_reader
from openframe.core.testing.fixtures import *  # noqa: F401, F403

import pytest
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def mock_pool() -> MagicMock:
    """A fully mocked asyncpg Pool."""
    pool = MagicMock()
    pool.fetchrow = AsyncMock()
    pool.fetch = AsyncMock()
    pool.fetchval = AsyncMock()
    pool.execute = AsyncMock()
    pool.close = AsyncMock()
    # acquire() returns an async context manager that yields the pool itself
    # so tests can call conn.fetch / conn.fetchval through the context.
    conn = MagicMock()
    conn.fetch = AsyncMock()
    conn.fetchval = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    pool.acquire = MagicMock(return_value=conn)
    return pool


@pytest.fixture
def mock_settings() -> object:
    """A CockroachdbSettings instance with a dummy DSN."""
    from openframe.adapters.db.cockroachdb import CockroachdbSettings

    return CockroachdbSettings(cockroachdb_url="postgresql://test:test@localhost:26257/test")


@pytest.fixture
def repo(mock_settings: object, mock_pool: MagicMock, monkeypatch: pytest.MonkeyPatch):
    """
    A CockroachdbRepository wired to a mocked pool.

    The mock pool is injected directly into ``_pool_cache`` so
    ``get_cockroachdb_pool()`` never attempts a real connection.
    """
    from openframe.adapters.db.cockroachdb import CockroachdbRepository
    import openframe.adapters.db.cockroachdb.connection as conn_module

    conn_module._pool_cache[conn_module._cache_key(mock_settings)] = mock_pool  # type: ignore[attr-defined]
    r = CockroachdbRepository(mock_settings, table="items", id_column="id")  # type: ignore[arg-type]
    yield r
    conn_module._pool_cache.clear()
