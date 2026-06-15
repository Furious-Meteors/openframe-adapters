"""
tests/conftest.py — openframe-adapters-db-postgres
=====================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.

All tests run with zero network calls. asyncpg is mocked at the
``openframe.adapters.db.postgres.connection`` import level so no real
Postgres server is needed.
"""
from __future__ import annotations

# Canonical OTel reset fixtures from openframe-core v2.0.
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
    """A PostgresSettings instance with a dummy DSN."""
    from openframe.adapters.db.postgres import PostgresSettings

    return PostgresSettings(database_url="postgresql://test:test@localhost/test")


@pytest.fixture
def repo(mock_settings: object, mock_pool: MagicMock, monkeypatch: pytest.MonkeyPatch):
    """
    A PostgresRepository wired to a mocked pool.

    The mock pool is injected directly into ``_pool_cache`` so
    ``get_postgres_pool()`` never attempts a real connection.
    """
    from openframe.adapters.db.postgres import PostgresRepository
    import openframe.adapters.db.postgres.connection as conn_module

    conn_module._pool_cache[mock_settings.database_url] = mock_pool  # type: ignore[attr-defined]
    r = PostgresRepository(mock_settings, table="items", id_column="id")  # type: ignore[arg-type]
    yield r
    conn_module._pool_cache.clear()
