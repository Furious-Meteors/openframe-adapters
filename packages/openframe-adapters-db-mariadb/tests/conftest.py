"""
tests/conftest.py — openframe-adapters-db-mariadb
====================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.

All tests run with zero network calls. aiomysql is mocked at the
``openframe.adapters.db.mariadb.connection`` import level so no real
MariaDB server is needed.
"""
from __future__ import annotations

# Canonical OTel reset fixtures from openframe-core v3.0.
# Provides (autouse): reset_telemetry_state
# Provides (on-demand): span_exporter, metric_reader
from openframe.core.testing.fixtures import *  # noqa: F401, F403

import pytest
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def mock_cursor() -> MagicMock:
    """A fully mocked aiomysql cursor, usable as an async context manager."""
    cur = MagicMock()
    cur.execute = AsyncMock()
    cur.fetchone = AsyncMock()
    cur.fetchall = AsyncMock(return_value=[])
    cur.rowcount = 0
    cur.lastrowid = 0
    cur.__aenter__ = AsyncMock(return_value=cur)
    cur.__aexit__ = AsyncMock(return_value=False)
    return cur


@pytest.fixture
def mock_conn(mock_cursor: MagicMock) -> MagicMock:
    """A fully mocked aiomysql connection, usable as an async context manager."""
    conn = MagicMock()
    conn.commit = AsyncMock()
    # cursor() ignores its args (e.g. aiomysql.DictCursor) and always returns
    # the same mock cursor, exactly like the plain `cursor()` call for health().
    conn.cursor = MagicMock(return_value=mock_cursor)
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    return conn


@pytest.fixture
def mock_pool(mock_conn: MagicMock) -> MagicMock:
    """A fully mocked aiomysql Pool."""
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=mock_conn)
    pool.close = MagicMock()
    pool.wait_closed = AsyncMock()
    return pool


@pytest.fixture
def mock_settings() -> object:
    """A MariadbSettings instance with a dummy DSN."""
    from openframe.adapters.db.mariadb import MariadbSettings

    return MariadbSettings(database_url="mysql://test:test@localhost/test")


@pytest.fixture
def repo(mock_settings: object, mock_pool: MagicMock, monkeypatch: pytest.MonkeyPatch):
    """
    A MariadbRepository wired to a mocked pool.

    The mock pool is injected directly into ``_pool_cache`` so
    ``get_mariadb_pool()`` never attempts a real connection.
    """
    from openframe.adapters.db.mariadb import MariadbRepository
    import openframe.adapters.db.mariadb.connection as conn_module

    conn_module._pool_cache[conn_module._cache_key(mock_settings)] = mock_pool  # type: ignore[attr-defined]
    r = MariadbRepository(mock_settings, table="items", id_column="id")  # type: ignore[arg-type]
    yield r
    conn_module._pool_cache.clear()
