"""
openframe/adapters/db/falkordb/connection.py
===============================================
Async factory that creates and caches a ``falkordb.asyncio.FalkorDB`` client.

FalkorDB's Python driver (PyPI ``FalkorDB``, import name ``falkordb``,
researched against v1.7.1) wraps ``redis-py``: ``falkordb.asyncio.FalkorDB``
is confirmed via ``inspect.iscoroutinefunction`` to be a genuinely async
client — ``AsyncGraph.query``, ``AsyncGraph.create_node_range_index``, and
``AsyncGraph.create_node_unique_constraint`` are all real coroutines — so
this adapter uses it natively, the same shape as every other
``openframe-adapters`` package (Postgres/Redis/Qdrant/Milvus/Chroma), not
an executor-wrapped sync fallback.

``FalkorDB.select_graph(graph_id)`` is confirmed synchronous and lazy (via
``inspect.iscoroutinefunction`` returning ``False``, and reading its
source — it only constructs an ``AsyncGraph`` wrapper object, no I/O) —
so only the ``FalkorDB`` client itself is cached/connection-checked here;
``select_graph()`` is called fresh on every repository operation at near-
zero cost.

The client is shared across all ``FalkorDBRepository`` instances with
matching settings in the same process. Cache key covers every connection-
shape field (host/port/password/ssl/socket timeouts) — not just
host+port — so two ``FalkorDBSettings`` pointed at the same server but
with different socket timeouts get two distinct clients, rather than the
second one silently inheriting the first one's configuration.

Exception surface (independently verified against the installed driver,
not assumed):
    - Connection-level failures (unreachable host, auth failure, timeout)
      surface as ``redis.exceptions.ConnectionError``/``AuthenticationError``/
      ``TimeoutError`` — FalkorDB's client literally constructs a
      ``redis.Redis`` connection internally and does no translation.
    - ``falkordb.exceptions`` (confirmed via ``dir()``) only defines
      ``SchemaVersionMismatchException`` — there is no FalkorDB-specific
      query-error type. A malformed/failing Cypher query (syntax error,
      constraint violation) is detected by ``QueryResult.__check_for_errors``
      (confirmed by reading ``falkordb/query_result.py``), which re-raises
      the server's error response as a plain ``redis.exceptions.ResponseError``
      — because ``GRAPH.QUERY`` is itself a Redis command and redis-py turns
      any error reply into ``ResponseError``.
"""
from __future__ import annotations

import asyncio

import redis.exceptions
from falkordb.asyncio import FalkorDB

from openframe.core.exceptions import (
    AdapterConnectionError,
    AdapterTimeoutError,
)

from .config import FalkorDBSettings

__all__ = ["get_falkordb_client", "_client_cache", "_cache_key"]

_ClientCacheKey = tuple[str, int, str | None, bool, float, float]

# Module-level cache: connection-shape tuple → falkordb.asyncio.FalkorDB
_client_cache: dict[_ClientCacheKey, FalkorDB] = {}


def _cache_key(settings: FalkorDBSettings) -> _ClientCacheKey:
    """
    Cache key covering host/port/auth/TLS plus every socket-timeout
    setting.

    Two ``FalkorDBSettings`` pointed at the same ``(host, port)`` but with
    different socket timeouts (or a different password/ssl flag) must not
    share a client — a shared key here would mean the second caller
    silently gets the first caller's connection configuration.
    """
    return (
        settings.falkordb_host,
        settings.falkordb_port,
        settings.falkordb_password,
        settings.falkordb_ssl,
        settings.falkordb_socket_timeout,
        settings.falkordb_socket_connect_timeout,
    )


async def get_falkordb_client(settings: FalkorDBSettings) -> FalkorDB:
    """
    Create or return the cached ``falkordb.asyncio.FalkorDB`` client.

    Creates the client on first call. Subsequent calls with matching
    connection-shape settings return the cached instance. A different
    socket-timeout/auth/TLS configuration for the same host/port gets its
    own client rather than reusing the first one's.

    Args:
        settings: A ``FalkorDBSettings`` instance.

    Returns:
        ``falkordb.asyncio.FalkorDB`` — ready to use.

    Raises:
        AdapterConnectionError: FalkorDB server is unreachable or auth failed.
        AdapterTimeoutError:    Connection attempt exceeded ``connection_timeout``.
    """
    key = _cache_key(settings)
    if key in _client_cache:
        return _client_cache[key]

    try:
        async with asyncio.timeout(settings.connection_timeout):
            client = FalkorDB(
                host=settings.falkordb_host,
                port=settings.falkordb_port,
                password=settings.falkordb_password,
                ssl=settings.falkordb_ssl,
                socket_timeout=settings.falkordb_socket_timeout,
                socket_connect_timeout=settings.falkordb_socket_connect_timeout,
            )
            # FalkorDB(...) only builds the underlying redis.asyncio.Redis
            # connection object — no I/O happens until the first command.
            # Verify connectivity now (same pattern as get_redis_client).
            await client.connection.ping()
    except asyncio.TimeoutError as exc:
        raise AdapterTimeoutError(
            f"FalkorDB connection to "
            f"{settings.falkordb_host}:{settings.falkordb_port} timed out "
            f"after {settings.connection_timeout}s",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except redis.exceptions.AuthenticationError as exc:
        raise AdapterConnectionError(
            f"FalkorDB authentication failed for "
            f"{settings.falkordb_host}:{settings.falkordb_port}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except redis.exceptions.ConnectionError as exc:
        raise AdapterConnectionError(
            f"FalkorDB connection failed for "
            f"{settings.falkordb_host}:{settings.falkordb_port}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc

    _client_cache[key] = client
    return client
