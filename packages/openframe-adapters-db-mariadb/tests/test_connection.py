"""
tests/test_connection.py
==========================
Unit tests for get_mariadb_pool() and _pool_cache.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from openframe.adapters.db.mariadb import MariadbSettings
from openframe.adapters.db.mariadb.connection import _cache_key, _pool_cache, get_mariadb_pool
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)


@pytest.fixture(autouse=True)
def clear_pool_cache():
    """Ensure _pool_cache is clean before and after every test."""
    _pool_cache.clear()
    yield
    _pool_cache.clear()


@pytest.fixture
def settings() -> MariadbSettings:
    return MariadbSettings(database_url="mysql://u:p@localhost/db")


@pytest.fixture
def settings_alt() -> MariadbSettings:
    return MariadbSettings(database_url="mysql://u:p@localhost/db2")


class TestGetMariadbPool:
    async def test_returns_cached_pool_on_second_call(
        self, settings: MariadbSettings
    ) -> None:
        fake_pool = MagicMock()
        with patch(
            "openframe.adapters.db.mariadb.connection.aiomysql.create_pool",
            new=AsyncMock(return_value=fake_pool),
        ):
            pool1 = await get_mariadb_pool(settings)
            pool2 = await get_mariadb_pool(settings)

        assert pool1 is pool2
        assert pool1 is fake_pool

    async def test_different_urls_produce_different_pools(
        self, settings: MariadbSettings, settings_alt: MariadbSettings
    ) -> None:
        pool_a = MagicMock(name="pool_a")
        pool_b = MagicMock(name="pool_b")
        create_pool_mock = AsyncMock(side_effect=[pool_a, pool_b])
        with patch(
            "openframe.adapters.db.mariadb.connection.aiomysql.create_pool",
            new=create_pool_mock,
        ):
            p1 = await get_mariadb_pool(settings)
            p2 = await get_mariadb_pool(settings_alt)

        assert p1 is not p2
        assert p1 is pool_a
        assert p2 is pool_b

    async def test_create_pool_connection_error_raises_adapter_connection_error(
        self, settings: MariadbSettings
    ) -> None:
        import pymysql.err

        with patch(
            "openframe.adapters.db.mariadb.connection.aiomysql.create_pool",
            new=AsyncMock(
                side_effect=pymysql.err.OperationalError(2003, "Can't connect to MariaDB server")
            ),
        ):
            with pytest.raises(AdapterConnectionError) as exc_info:
                await get_mariadb_pool(settings)

        assert exc_info.value.cause is not None

    async def test_create_pool_auth_failure_raises_adapter_connection_error(
        self, settings: MariadbSettings
    ) -> None:
        import pymysql.err

        with patch(
            "openframe.adapters.db.mariadb.connection.aiomysql.create_pool",
            new=AsyncMock(
                side_effect=pymysql.err.OperationalError(1045, "Access denied for user")
            ),
        ):
            with pytest.raises(AdapterConnectionError):
                await get_mariadb_pool(settings)

    async def test_timeout_raises_adapter_timeout_error(
        self, settings: MariadbSettings
    ) -> None:
        with patch(
            "openframe.adapters.db.mariadb.connection.aiomysql.create_pool",
            new=AsyncMock(side_effect=asyncio.TimeoutError()),
        ):
            with pytest.raises(AdapterTimeoutError) as exc_info:
                await get_mariadb_pool(settings)

        assert exc_info.value.operation == "connect"

    async def test_unknown_database_raises_adapter_configuration_error(
        self, settings: MariadbSettings
    ) -> None:
        import pymysql.err

        with patch(
            "openframe.adapters.db.mariadb.connection.aiomysql.create_pool",
            new=AsyncMock(
                side_effect=pymysql.err.OperationalError(1049, "Unknown database 'db'")
            ),
        ):
            with pytest.raises(AdapterConfigurationError) as exc_info:
                await get_mariadb_pool(settings)

        assert exc_info.value.operation == "init"

    async def test_os_error_raises_adapter_connection_error(
        self, settings: MariadbSettings
    ) -> None:
        with patch(
            "openframe.adapters.db.mariadb.connection.aiomysql.create_pool",
            new=AsyncMock(side_effect=OSError("network unreachable")),
        ):
            with pytest.raises(AdapterConnectionError):
                await get_mariadb_pool(settings)

    async def test_pool_stored_in_cache_after_creation(
        self, settings: MariadbSettings
    ) -> None:
        fake_pool = MagicMock()
        with patch(
            "openframe.adapters.db.mariadb.connection.aiomysql.create_pool",
            new=AsyncMock(return_value=fake_pool),
        ):
            await get_mariadb_pool(settings)

        assert _pool_cache[_cache_key(settings)] is fake_pool

    async def test_same_url_different_pool_size_produces_different_pools(
        self, settings: MariadbSettings
    ) -> None:
        """
        Regression test: two Settings for the SAME database_url but
        different pool_size must NOT share a pool — the second caller
        must not silently inherit the first caller's pool configuration.
        """
        settings_bigger_pool = MariadbSettings(
            database_url=settings.database_url, pool_size=50
        )
        pool_small = MagicMock(name="pool_small")
        pool_big = MagicMock(name="pool_big")
        create_pool_mock = AsyncMock(side_effect=[pool_small, pool_big])
        with patch(
            "openframe.adapters.db.mariadb.connection.aiomysql.create_pool",
            new=create_pool_mock,
        ):
            p1 = await get_mariadb_pool(settings)
            p2 = await get_mariadb_pool(settings_bigger_pool)

        assert p1 is not p2
        assert p1 is pool_small
        assert p2 is pool_big
        # A third call with settings matching the second config reuses it.
        with patch(
            "openframe.adapters.db.mariadb.connection.aiomysql.create_pool",
            new=AsyncMock(side_effect=AssertionError("should not create a third pool")),
        ):
            p3 = await get_mariadb_pool(settings_bigger_pool)
        assert p3 is pool_big

    def test_parse_dsn_extracts_connection_kwargs(self) -> None:
        from openframe.adapters.db.mariadb.connection import _parse_dsn

        kwargs = _parse_dsn("mysql://user:pw@dbhost:3307/mydb")
        assert kwargs == {
            "host": "dbhost",
            "port": 3307,
            "user": "user",
            "password": "pw",
            "db": "mydb",
        }

    def test_parse_dsn_defaults_port(self) -> None:
        from openframe.adapters.db.mariadb.connection import _parse_dsn

        kwargs = _parse_dsn("mysql://user:pw@dbhost/mydb")
        assert kwargs["port"] == 3306
