"""
openframe/adapters/db/redis/connection.py
==========================================
Async factory that creates and caches a ``redis.asyncio.Redis`` client.

The client is shared across all ``RedisRepository`` instances in the same
process. Cache key is the ``REDIS_URL`` string so different URLs each
maintain an independent client.
"""
from __future__ import annotations

import asyncio

import redis.asyncio as aioredis
import redis.exceptions

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)

from .config import RedisSettings

__all__ = ["get_redis_client", "_client_cache"]

# Module-level cache: redis_url → redis.asyncio.Redis
_client_cache: dict[str, aioredis.Redis] = {}  # type: ignore[type-arg]


async def get_redis_client(settings: RedisSettings) -> aioredis.Redis:  # type: ignore[type-arg]
    """
    Create or return the cached ``redis.asyncio.Redis`` client.

    Creates the client on first call. Subsequent calls with the same
    ``REDIS_URL`` return the cached instance. The client is shared
    across all ``RedisRepository`` instances in the same process.

    Args:
        settings: A ``RedisSettings`` instance.

    Returns:
        ``redis.asyncio.Redis`` — ready to use, with ``decode_responses=True``.

    Raises:
        AdapterConnectionError:    Redis server is unreachable or auth failed.
        AdapterConfigurationError: ``REDIS_URL`` is malformed.
        AdapterTimeoutError:       Connection attempt exceeded the timeout.
    """
    if settings.redis_url in _client_cache:
        return _client_cache[settings.redis_url]

    try:
        async with asyncio.timeout(settings.connection_timeout):
            pool = aioredis.ConnectionPool.from_url(
                settings.redis_url,
                max_connections=settings.redis_max_connections,
                socket_timeout=settings.redis_socket_timeout,
                socket_connect_timeout=settings.redis_socket_connect_timeout,
                decode_responses=True,
            )
            client: aioredis.Redis = aioredis.Redis(connection_pool=pool)  # type: ignore[type-arg]
            await client.ping()
    except asyncio.TimeoutError as exc:
        raise AdapterTimeoutError(
            f"Redis connection to {settings.redis_url!r} timed out "
            f"after {settings.connection_timeout}s",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except redis.exceptions.AuthenticationError as exc:
        raise AdapterConnectionError(
            f"Redis authentication failed for {settings.redis_url!r}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except redis.exceptions.ConnectionError as exc:
        raise AdapterConnectionError(
            f"Redis connection failed for {settings.redis_url!r}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except redis.exceptions.ResponseError as exc:
        raise AdapterConfigurationError(
            f"Redis configuration error for {settings.redis_url!r}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
        ) from exc

    _client_cache[settings.redis_url] = client
    return client
