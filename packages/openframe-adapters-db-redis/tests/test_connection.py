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
from openframe.adapters.db.redis.connection import _client_cache, get_redis_client
from openframe.core.exceptions import AdapterConnectionError, AdapterTimeoutError


class TestGetRedisClient:
    async def test_returns_cached_client_on_second_call(
        self, mock_settings: RedisSettings, mock_redis: MagicMock
    ) -> None:
        """Second call with same URL returns the exact same client object."""
        import openframe.adapters.db.redis.connection as conn_module
        conn_module._client_cache[mock_settings.redis_url] = mock_redis

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
        conn_module._client_cache[settings_a.redis_url] = client_a
        conn_module._client_cache[settings_b.redis_url] = client_b

        result_a = await get_redis_client(settings_a)
        result_b = await get_redis_client(settings_b)
        assert result_a is not result_b
        conn_module._client_cache.clear()

    async def test_client_stored_in_cache_keyed_by_url(
        self, mock_settings: RedisSettings, mock_redis: MagicMock
    ) -> None:
        """After get_redis_client(), the cache contains the client at the URL key."""
        import openframe.adapters.db.redis.connection as conn_module
        conn_module._client_cache[mock_settings.redis_url] = mock_redis

        await get_redis_client(mock_settings)
        assert mock_settings.redis_url in conn_module._client_cache
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
