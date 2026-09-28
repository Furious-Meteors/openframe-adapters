"""
tests/test_connection.py — openframe-adapters-db-redis
========================================================
Unit tests for get_redis_client() and the client cache.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis.exceptions

from openframe.adapters.db.redis import RedisSettings
from openframe.adapters.db.redis.connection import _cache_key, _client_cache, get_redis_client
from openframe.core.exceptions import AdapterConnectionError, AdapterTimeoutError


class TestGetRedisClient:
    async def test_returns_cached_client_on_second_call(
        self, mock_settings: RedisSettings, mock_redis: MagicMock
    ) -> None:
        """Second call with same URL returns the exact same client object."""
        import openframe.adapters.db.redis.connection as conn_module
        conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_redis

        client1 = await get_redis_client(mock_settings)
        client2 = await get_redis_client(mock_settings)
        assert client1 is client2
        conn_module._client_cache.clear()

    async def test_different_urls_produce_different_clients(
        self, mock_redis: MagicMock
    ) -> None:
        """Each unique REDIS_URL gets its own independent client."""
        import openframe.adapters.db.redis.connection as conn_module

        settings_a = RedisSettings(redis_url="redis://host-a:6379/0")
        settings_b = RedisSettings(redis_url="redis://host-b:6379/0")

        client_a = MagicMock()
        client_b = MagicMock()
        conn_module._client_cache[conn_module._cache_key(settings_a)] = client_a
        conn_module._client_cache[conn_module._cache_key(settings_b)] = client_b

        result_a = await get_redis_client(settings_a)
        result_b = await get_redis_client(settings_b)
        assert result_a is not result_b
        conn_module._client_cache.clear()

    async def test_same_url_different_pool_size_produces_different_clients(
        self, mock_redis: MagicMock
    ) -> None:
        """
        Regression test: two Settings for the SAME redis_url but different
        redis_max_connections must NOT share a client — the second caller
        must not silently inherit the first caller's pool configuration.
        """
        import openframe.adapters.db.redis.connection as conn_module

        settings_small = RedisSettings(redis_url="redis://host:6379/0", redis_max_connections=10)
        settings_big = RedisSettings(redis_url="redis://host:6379/0", redis_max_connections=100)

        client_small = MagicMock(name="client_small")
        client_big = MagicMock(name="client_big")
        conn_module._client_cache[conn_module._cache_key(settings_small)] = client_small
        conn_module._client_cache[conn_module._cache_key(settings_big)] = client_big

        result_small = await get_redis_client(settings_small)
        result_big = await get_redis_client(settings_big)
        assert result_small is not result_big
        assert result_small is client_small
        assert result_big is client_big
        conn_module._client_cache.clear()

    async def test_client_stored_in_cache_keyed_by_url(
        self, mock_settings: RedisSettings, mock_redis: MagicMock
    ) -> None:
        """After get_redis_client(), the cache contains the client at the cache key."""
        import openframe.adapters.db.redis.connection as conn_module
        conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_redis

        await get_redis_client(mock_settings)
        assert conn_module._cache_key(mock_settings) in conn_module._client_cache
        conn_module._client_cache.clear()

    async def test_connection_error_raises_adapter_connection_error(
        self, mock_settings: RedisSettings
    ) -> None:
        """redis.exceptions.ConnectionError → AdapterConnectionError."""
        import openframe.adapters.db.redis.connection as conn_module
        conn_module._client_cache.clear()

        with patch(
            "openframe.adapters.db.redis.connection.aioredis.ConnectionPool.from_url",
            side_effect=redis.exceptions.ConnectionError("refused"),
        ):
            with pytest.raises(AdapterConnectionError):
                await get_redis_client(mock_settings)

    async def test_authentication_error_raises_adapter_connection_error(
        self, mock_settings: RedisSettings
    ) -> None:
        """redis.exceptions.AuthenticationError → AdapterConnectionError."""
        import openframe.adapters.db.redis.connection as conn_module
        conn_module._client_cache.clear()

        with patch(
            "openframe.adapters.db.redis.connection.aioredis.ConnectionPool.from_url",
            side_effect=redis.exceptions.AuthenticationError("bad password"),
        ):
            with pytest.raises(AdapterConnectionError):
                await get_redis_client(mock_settings)

    async def test_timeout_raises_adapter_timeout_error(
        self, mock_settings: RedisSettings
    ) -> None:
        """asyncio.TimeoutError → AdapterTimeoutError."""
        import openframe.adapters.db.redis.connection as conn_module
        conn_module._client_cache.clear()

        with patch(
            "openframe.adapters.db.redis.connection.aioredis.ConnectionPool.from_url",
            side_effect=asyncio.TimeoutError(),
        ):
            with pytest.raises(AdapterTimeoutError):
                await get_redis_client(mock_settings)
