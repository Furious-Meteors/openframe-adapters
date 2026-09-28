"""
openframe/adapters/db/cockroachdb/connection.py
==================================================
asyncpg connection pool factory and cache for CockroachDB.

CockroachDB speaks the PostgreSQL wire protocol, so this module is
byte-for-byte the same pooling strategy as
``openframe-adapters-db-postgres``'s ``connection.py`` — same driver
(``asyncpg``), same pool-creation call shape, same exception hierarchy.

``get_cockroachdb_pool()`` is the single entry point for obtaining an
asyncpg pool. It creates the pool on first call and returns the cached
instance on every subsequent call with the same ``cockroachdb_url`` AND the
same pool-relevant settings (``pool_size``, ``pool_max_inactive_conn_lifetime``,
``pool_command_timeout``, ``pool_max_queries``). Multiple
``CockroachdbRepository`` instances constructed with matching settings share
the same pool.

Pool cache: ``_pool_cache`` is a module-level dict keyed by
``(cockroachdb_url, pool-config-tuple)`` — not the URL alone. Two
``CockroachdbSettings`` instances with the same ``cockroachdb_url`` but
different pool sizing get two distinct pools, rather than the second one
silently inheriting the first one's configuration (the bug this key shape
fixes — the same live bug found in the Postgres/Mongo/Redis adapters before
the ecosystem-wide checklist existed).
Do NOT replace this with ``@lru_cache`` — that decorator does not support
async functions and would create a new coroutine on each call.
"""
from __future__ import annotations

import asyncio
from typing import Any

import asyncpg

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)

from .config import CockroachdbSettings

__all__ = ["get_cockroachdb_pool", "_pool_cache", "_cache_key"]

_PoolCacheKey = tuple[str, tuple[int, float, float, int]]

_pool_cache: dict[_PoolCacheKey, asyncpg.Pool] = {}  # type: ignore[type-arg]


def _cache_key(settings: CockroachdbSettings) -> _PoolCacheKey:
    """
    Cache key covering the URL plus every pool-shape setting.

    Two ``CockroachdbSettings`` for the same ``cockroachdb_url`` but
    different pool sizing must not share a pool — a shared key here would
    mean the second caller silently gets the first caller's pool
    configuration.
    """
    return (
        settings.cockroachdb_url,
        (
            settings.pool_size,
            settings.pool_max_inactive_conn_lifetime,
            settings.pool_command_timeout,
            settings.pool_max_queries,
        ),
    )


async def get_cockroachdb_pool(settings: CockroachdbSettings) -> asyncpg.Pool:  # type: ignore[type-arg]
    """
    Create or return the cached asyncpg connection pool.

    Creates the pool on first call for a given ``(cockroachdb_url, pool
    config)`` pair. Subsequent calls with matching URL and pool settings
    return the cached pool without re-connecting. A different pool
    configuration for the same URL gets its own pool rather than reusing
    the first one's.

    Args:
        settings: A fully-validated ``CockroachdbSettings`` instance.

    Returns:
        An ``asyncpg.Pool`` that is ready to use.

    Raises:
        AdapterConnectionError:    Pool creation failed — host unreachable,
                                   bad credentials, TLS error, etc.
        AdapterConfigurationError: ``COCKROACHDB_URL`` is syntactically
                                   invalid (``asyncpg.InvalidCatalogNameError``).
        AdapterTimeoutError:       Pool creation exceeded
                                   ``settings.connection_timeout``.
    """
    url = settings.cockroachdb_url
    key = _cache_key(settings)
    if key in _pool_cache:
        return _pool_cache[key]

    try:
        async with asyncio.timeout(settings.connection_timeout):
            pool: asyncpg.Pool = await asyncpg.create_pool(  # type: ignore[assignment]
                dsn=url,
                min_size=settings.pool_size,
                max_size=settings.pool_size,
                max_inactive_connection_lifetime=settings.pool_max_inactive_conn_lifetime,
                command_timeout=settings.pool_command_timeout,
                max_queries=settings.pool_max_queries,
            )
    except asyncio.TimeoutError as exc:
        raise AdapterTimeoutError(
            f"Pool creation exceeded {settings.connection_timeout}s connection_timeout",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except asyncpg.InvalidCatalogNameError as exc:
        raise AdapterConfigurationError(
            f"COCKROACHDB_URL references an invalid or non-existent catalog: {url!r}",
            adapter=settings.adapter_name,
            operation="init",
            cause=exc,
        ) from exc
    except (
        asyncpg.InvalidPasswordError,
        asyncpg.CannotConnectNowError,
        asyncpg.TooManyConnectionsError,
        OSError,
    ) as exc:
        raise AdapterConnectionError(
            f"Cannot connect to CockroachDB at {url!r}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc

    _pool_cache[key] = pool
    return pool
