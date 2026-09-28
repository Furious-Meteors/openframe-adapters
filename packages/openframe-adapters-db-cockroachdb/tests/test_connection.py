"""
tests/test_connection.py
==========================
Unit tests for get_cockroachdb_pool() and _pool_cache.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from openframe.adapters.db.cockroachdb import CockroachdbSettings
from openframe.adapters.db.cockroachdb.connection import _cache_key, _pool_cache, get_cockroachdb_pool
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
def settings() -> CockroachdbSettings:
    return CockroachdbSettings(cockroachdb_url="postgresql://u:p@localhost:26257/db")


@pytest.fixture
def settings_alt() -> CockroachdbSettings:
    return CockroachdbSettings(cockroachdb_url="postgresql://u:p@localhost:26257/db2")


class TestGetCockroachdbPool:
    async def test_returns_cached_pool_on_second_call(
        self, settings: CockroachdbSettings
    ) -> None:
        fake_pool = MagicMock()
        with patch(
            "openframe.adapters.db.cockroachdb.connection.asyncpg.create_pool",
            new=AsyncMock(return_value=fake_pool),
        ):
            pool1 = await get_cockroachdb_pool(settings)
            pool2 = await get_cockroachdb_pool(settings)

        assert pool1 is pool2
        assert pool1 is fake_pool

    async def test_different_urls_produce_different_pools(
        self, settings: CockroachdbSettings, settings_alt: CockroachdbSettings
    ) -> None:
        pool_a = MagicMock(name="pool_a")
        pool_b = MagicMock(name="pool_b")
        create_pool_mock = AsyncMock(side_effect=[pool_a, pool_b])
        with patch(
            "openframe.adapters.db.cockroachdb.connection.asyncpg.create_pool",
            new=create_pool_mock,
        ):
            p1 = await get_cockroachdb_pool(settings)
            p2 = await get_cockroachdb_pool(settings_alt)

        assert p1 is not p2
        assert p1 is pool_a
        assert p2 is pool_b

    async def test_create_pool_error_raises_adapter_connection_error(
        self, settings: CockroachdbSettings
    ) -> None:
        import asyncpg

        with patch(
            "openframe.adapters.db.cockroachdb.connection.asyncpg.create_pool",
            new=AsyncMock(side_effect=asyncpg.InvalidPasswordError("bad pw")),
        ):
            with pytest.raises(AdapterConnectionError) as exc_info:
                await get_cockroachdb_pool(settings)

        assert exc_info.value.cause is not None

    async def test_timeout_raises_adapter_timeout_error(
        self, settings: CockroachdbSettings
    ) -> None:
        with patch(
            "openframe.adapters.db.cockroachdb.connection.asyncpg.create_pool",
            new=AsyncMock(side_effect=asyncio.TimeoutError()),
        ):
            with pytest.raises(AdapterTimeoutError) as exc_info:
                await get_cockroachdb_pool(settings)

        assert exc_info.value.operation == "connect"

    async def test_invalid_catalog_raises_adapter_configuration_error(
        self, settings: CockroachdbSettings
    ) -> None:
        import asyncpg

        with patch(
            "openframe.adapters.db.cockroachdb.connection.asyncpg.create_pool",
            new=AsyncMock(
                side_effect=asyncpg.InvalidCatalogNameError("no such db")
            ),
        ):
            with pytest.raises(AdapterConfigurationError) as exc_info:
                await get_cockroachdb_pool(settings)

        assert exc_info.value.operation == "init"

    async def test_pool_stored_in_cache_after_creation(
        self, settings: CockroachdbSettings
    ) -> None:
        fake_pool = MagicMock()
        with patch(
            "openframe.adapters.db.cockroachdb.connection.asyncpg.create_pool",
            new=AsyncMock(return_value=fake_pool),
        ):
            await get_cockroachdb_pool(settings)

        assert _pool_cache[_cache_key(settings)] is fake_pool

    async def test_same_url_different_pool_size_produces_different_pools(
        self, settings: CockroachdbSettings
    ) -> None:
        """
        Regression test: two Settings for the SAME cockroachdb_url but
        different pool_size must NOT share a pool — the second caller
        must not silently inherit the first caller's pool configuration.
        """
        settings_bigger_pool = CockroachdbSettings(
            cockroachdb_url=settings.cockroachdb_url, pool_size=50
        )
        pool_small = MagicMock(name="pool_small")
        pool_big = MagicMock(name="pool_big")
        create_pool_mock = AsyncMock(side_effect=[pool_small, pool_big])
        with patch(
            "openframe.adapters.db.cockroachdb.connection.asyncpg.create_pool",
            new=create_pool_mock,
        ):
            p1 = await get_cockroachdb_pool(settings)
            p2 = await get_cockroachdb_pool(settings_bigger_pool)

        assert p1 is not p2
        assert p1 is pool_small
        assert p2 is pool_big
        # A third call with settings matching the second config reuses it.
        with patch(
            "openframe.adapters.db.cockroachdb.connection.asyncpg.create_pool",
            new=AsyncMock(side_effect=AssertionError("should not create a third pool")),
        ):
            p3 = await get_cockroachdb_pool(settings_bigger_pool)
        assert p3 is pool_big
