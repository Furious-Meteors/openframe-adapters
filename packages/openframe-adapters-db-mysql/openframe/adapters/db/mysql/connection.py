"""
openframe/adapters/db/mysql/connection.py
============================================
aiomysql connection pool factory and cache.

``get_mysql_pool()`` is the single entry point for obtaining an aiomysql
pool. It creates the pool on first call and returns the cached instance on
every subsequent call with the same ``database_url`` AND the same
pool-relevant settings (``pool_size``, ``pool_recycle``,
``pool_connect_timeout``). Multiple ``MySQLRepository`` instances
constructed with matching settings share the same pool.

Pool cache: ``_pool_cache`` is a module-level dict keyed by
``(database_url, pool-config-tuple)`` — not the URL alone. Two
``MySQLSettings`` instances with the same ``database_url`` but different
pool sizing get two distinct pools, rather than the second one silently
inheriting the first one's configuration (the bug this key shape fixes).
Do NOT replace this with ``@lru_cache`` — that decorator does not support
async functions and would create a new coroutine on each call.

Error-code gotcha:
    PyMySQL/aiomysql funnel most connect-time failures through
    ``pymysql.err.OperationalError`` regardless of *why* the connection
    failed — genuine unreachability (error code 2003), a stale/closed
    connection (2006), a dropped socket (2013/2002), bad credentials
    (1045), or a missing/renamed schema (1049, which PyMySQL also raises
    as ``OperationalError``, not a distinct "no such database" type the way
    asyncpg has ``InvalidCatalogNameError``). Pool creation therefore
    inspects ``exc.args[0]`` (the numeric MySQL error code) rather than
    trusting the exception's Python type alone.
"""
from __future__ import annotations

import asyncio
from urllib.parse import urlparse

import aiomysql
import pymysql.err

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)

from .config import MySQLSettings

__all__ = ["get_mysql_pool", "_pool_cache", "_cache_key"]

_PoolCacheKey = tuple[str, tuple[int, float, float]]

_pool_cache: dict[_PoolCacheKey, aiomysql.Pool] = {}

# Genuine connection-class MySQL error codes — unreachable host, dropped
# socket, server gone away, or authentication failure. All of these are
# retryable in the sense that a fresh connection attempt may succeed.
_CONNECTION_ERROR_CODES = frozenset({
    2002,  # Can't connect through socket
    2003,  # Can't connect to MySQL server
    2006,  # MySQL server has gone away
    2013,  # Lost connection to MySQL server during query
    1045,  # Access denied for user (auth failure)
})

# Configuration-class error codes — the DSN/credentials are syntactically
# fine but reference something that doesn't exist. Not retryable without a
# config change.
_CONFIGURATION_ERROR_CODES = frozenset({
    1049,  # Unknown database
})


def _cache_key(settings: MySQLSettings) -> _PoolCacheKey:
    """
    Cache key covering the URL plus every pool-shape setting.

    Two ``MySQLSettings`` for the same ``database_url`` but different pool
    sizing must not share a pool — a shared key here would mean the second
    caller silently gets the first caller's pool configuration.
    """
    return (
        settings.database_url,
        (
            settings.pool_size,
            settings.pool_recycle,
            settings.pool_connect_timeout,
        ),
    )


def _parse_dsn(url: str) -> dict[str, object]:
    """Parse a ``mysql://user:pass@host:port/dbname`` DSN into connect kwargs."""
    parsed = urlparse(url)
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 3306,
        "user": parsed.username or "",
        "password": parsed.password or "",
        "db": parsed.path.lstrip("/") if parsed.path else "",
    }


async def get_mysql_pool(settings: MySQLSettings) -> aiomysql.Pool:
    """
    Create or return the cached aiomysql connection pool.

    Creates the pool on first call for a given ``(database_url, pool config)``
    pair. Subsequent calls with matching URL and pool settings return the
    cached pool without re-connecting. A different pool configuration for
    the same URL gets its own pool rather than reusing the first one's.

    Args:
        settings: A fully-validated ``MySQLSettings`` instance.

    Returns:
        An ``aiomysql.Pool`` that is ready to use.

    Raises:
        AdapterConnectionError:    Pool creation failed — host unreachable,
                                   bad credentials, dropped socket, etc.
        AdapterConfigurationError: ``DATABASE_URL`` references a database
                                   that does not exist (error code 1049).
        AdapterTimeoutError:       Pool creation exceeded
                                   ``settings.connection_timeout``.
    """
    url = settings.database_url
    key = _cache_key(settings)
    if key in _pool_cache:
        return _pool_cache[key]

    conn_kwargs = _parse_dsn(url)

    try:
        async with asyncio.timeout(settings.connection_timeout):
            pool: aiomysql.Pool = await aiomysql.create_pool(
                minsize=settings.pool_size,
                maxsize=settings.pool_size,
                pool_recycle=settings.pool_recycle,
                connect_timeout=settings.pool_connect_timeout,
                **conn_kwargs,
            )
    except asyncio.TimeoutError as exc:
        raise AdapterTimeoutError(
            f"Pool creation exceeded {settings.connection_timeout}s connection_timeout",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except pymysql.err.OperationalError as exc:
        code = exc.args[0] if exc.args else None
        if code in _CONFIGURATION_ERROR_CODES:
            raise AdapterConfigurationError(
                f"DATABASE_URL references an invalid or non-existent database: {url!r}",
                adapter=settings.adapter_name,
                operation="init",
                cause=exc,
            ) from exc
        # Any other OperationalError at connect time — unreachable host,
        # auth failure, dropped socket — is a connection-class failure.
        raise AdapterConnectionError(
            f"Cannot connect to MySQL at {url!r}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except OSError as exc:
        raise AdapterConnectionError(
            f"Cannot connect to MySQL at {url!r}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc

    _pool_cache[key] = pool
    return pool
