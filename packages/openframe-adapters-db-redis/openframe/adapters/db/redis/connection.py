"""
openframe/adapters/db/redis/connection.py
==========================================
Async factory that creates and caches a ``redis.asyncio.Redis`` client.

The client is shared across all ``RedisRepository`` instances with matching
settings in the same process. Cache key is ``(REDIS_URL, pool-config-tuple)``
— not the URL alone — so two ``RedisSettings`` for the same URL but
different pool sizing get two distinct clients, rather than the second one
silently inheriting the first one's configuration.
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

__all__ = ["get_redis_client", "_client_cache", "_cache_key"]

_ClientCacheKey = tuple[str, tuple[int, float, float]]

# Module-level cache: (redis_url, pool config) → redis.asyncio.Redis
_client_cache: dict[_ClientCacheKey, aioredis.Redis] = {}  # type: ignore[type-arg]


def _cache_key(settings: RedisSettings) -> _ClientCacheKey:
    """
    Cache key covering the URL plus every pool-shape setting.

    Two ``RedisSettings`` for the same ``redis_url`` but different pool
    sizing must not share a client — a shared key here would mean the
    second caller silently gets the first caller's pool configuration.
    """
    return (
        settings.redis_url,
        (
            settings.redis_max_connections,
            settings.redis_socket_timeout,
            settings.redis_socket_connect_timeout,
        ),
    )


async def get_redis_client(settings: RedisSettings) -> aioredis.Redis:  # type: ignore[type-arg]
    """
    Create or return the cached ``redis.asyncio.Redis`` client.

    Creates the client on first call. Subsequent calls with matching
    ``REDIS_URL`` and pool settings return the cached instance. A different
    pool configuration for the same URL gets its own client rather than
    reusing the first one's.

    Args:
        settings: A ``RedisSettings`` instance.

    Returns:
        ``redis.asyncio.Redis`` — ready to use, with ``decode_responses=True``.

    Raises:
        AdapterConnectionError:    Redis server is unreachable or auth failed.
        AdapterConfigurationError: ``REDIS_URL`` is malformed.
        AdapterTimeoutError:       Connection attempt exceeded the timeout.
    """
    key = _cache_key(settings)
    if key in _client_cache:
        return _client_cache[key]

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

    _client_cache[key] = client
    return client
