"""
tests/test_connection.py
==========================
Unit tests for get_postgres_pool() and _pool_cache.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from openframe.adapters.db.postgres import PostgresSettings
from openframe.adapters.db.postgres.connection import _pool_cache, get_postgres_pool
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
def settings() -> PostgresSettings:
    return PostgresSettings(database_url="postgresql://u:p@localhost/db")


@pytest.fixture
def settings_alt() -> PostgresSettings:
    return PostgresSettings(database_url="postgresql://u:p@localhost/db2")


class TestGetPostgresPool:
    async def test_returns_cached_pool_on_second_call(
        self, settings: PostgresSettings
    ) -> None:
        fake_pool = MagicMock()
        with patch(
            "openframe.adapters.db.postgres.connection.asyncpg.create_pool",
            new=AsyncMock(return_value=fake_pool),
        ):
            pool1 = await get_postgres_pool(settings)
            pool2 = await get_postgres_pool(settings)

        assert pool1 is pool2
        assert pool1 is fake_pool

    async def test_different_urls_produce_different_pools(
        self, settings: PostgresSettings, settings_alt: PostgresSettings
    ) -> None:
        pool_a = MagicMock(name="pool_a")
        pool_b = MagicMock(name="pool_b")
        create_pool_mock = AsyncMock(side_effect=[pool_a, pool_b])
        with patch(
            "openframe.adapters.db.postgres.connection.asyncpg.create_pool",
            new=create_pool_mock,
        ):
            p1 = await get_postgres_pool(settings)
            p2 = await get_postgres_pool(settings_alt)

        assert p1 is not p2
        assert p1 is pool_a
        assert p2 is pool_b

    async def test_create_pool_error_raises_adapter_connection_error(
        self, settings: PostgresSettings
    ) -> None:
        import asyncpg

        with patch(
            "openframe.adapters.db.postgres.connection.asyncpg.create_pool",
            new=AsyncMock(side_effect=asyncpg.InvalidPasswordError("bad pw")),
        ):
            with pytest.raises(AdapterConnectionError) as exc_info:
                await get_postgres_pool(settings)

        assert exc_info.value.cause is not None

    async def test_timeout_raises_adapter_timeout_error(
        self, settings: PostgresSettings
    ) -> None:
        with patch(
            "openframe.adapters.db.postgres.connection.asyncpg.create_pool",
            new=AsyncMock(side_effect=asyncio.TimeoutError()),
        ):
            with pytest.raises(AdapterTimeoutError) as exc_info:
                await get_postgres_pool(settings)

        assert exc_info.value.operation == "connect"

    async def test_invalid_catalog_raises_adapter_configuration_error(
        self, settings: PostgresSettings
    ) -> None:
        import asyncpg

        with patch(
            "openframe.adapters.db.postgres.connection.asyncpg.create_pool",
            new=AsyncMock(
                side_effect=asyncpg.InvalidCatalogNameError("no such db")
            ),
        ):
            with pytest.raises(AdapterConfigurationError) as exc_info:
                await get_postgres_pool(settings)

        assert exc_info.value.operation == "init"

    async def test_pool_stored_in_cache_after_creation(
        self, settings: PostgresSettings
    ) -> None:
        fake_pool = MagicMock()
        with patch(
            "openframe.adapters.db.postgres.connection.asyncpg.create_pool",
            new=AsyncMock(return_value=fake_pool),
        ):
            await get_postgres_pool(settings)

        assert _pool_cache[settings.database_url] is fake_pool
