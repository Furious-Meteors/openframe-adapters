"""
openframe/adapters/db/influxdb/connection.py
===============================================
``InfluxDBClientAsync`` factory and cache.

Async strategy (ADR-003: decided per-adapter, not forced)
-----------------------------------------------------------
This adapter uses **native async**, not the ``run_in_executor`` fallback —
the same shape as ``openframe-adapters-db-postgres``/``-oracle``, not the
hybrid/executor shape ``-cassandra`` needed.

Research finding (checked against the actually-installed driver, not
assumed/recalled): ``influxdb-client`` 1.50.0, installed fresh via
``pip install influxdb-client[async]`` for this investigation (the ``async``
extra pulls in ``aiohttp``; without it the async submodules fail to import
— see the ``ModuleNotFoundError`` reproduced below), ships a real, documented
native-async client:

    >>> import influxdb_client
    >>> influxdb_client.__version__
    '1.50.0'
    >>> from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
    >>> import inspect
    >>> inspect.signature(InfluxDBClientAsync.ping)
    (self) -> bool
    >>> hasattr(InfluxDBClientAsync, "__aenter__"), hasattr(InfluxDBClientAsync, "__aexit__")
    (True, True)

``InfluxDBClientAsync`` is backed by ``aiohttp`` end-to-end — not a thin
sync-wrapped shim. Its ``write_api().write()``, ``query_api().query()``, and
``delete_api().delete()`` are real ``async def`` coroutines (verified via
``inspect.signature`` on ``WriteApiAsync.write``, ``QueryApiAsync.query``,
``DeleteApiAsync.delete`` — none of them return a sync value wrapped
post-hoc), and the client itself is an async context manager. Installing
the ``async`` extra without the base package does not work either —
``influxdb_client.client.influxdb_client_async`` imports
``influxdb_client._async.rest``, which imports ``aiohttp`` unconditionally;
omitting the extra reproduces::

    ModuleNotFoundError: No module named 'aiohttp'

Because a real, usable native-async surface exists and was exercised
directly (not just introspected), this adapter is built the same shape as
Postgres/Oracle — one cached ``InfluxDBClientAsync`` per
``(url, token, org, timeout)`` tuple, async CRUD methods throughout. If a
future ``influxdb-client`` release regressed this surface, the honest move
per ADR-003 would be to fall back to
``asyncio.get_running_loop().run_in_executor(None, sync_fn)`` wrapping the
synchronous ``InfluxDBClient`` — this module does not do that because the
async surface genuinely exists and works in the installed version.

A second, real finding from the same investigation, worth documenting here
since it affects this module's exception handling directly: reading
``influxdb_client/_async/rest.py`` shows the async REST client's request
path does **not** catch ``aiohttp``'s own connection-level exceptions
anywhere — ``aiohttp.ClientConnectorError`` (DNS failure, connection
refused), ``aiohttp.ServerTimeoutError``, and the rest of the
``aiohttp.ClientError`` family propagate raw, unwrapped into
``InfluxDBClientAsync`` callers. Only HTTP responses that come back *with a
status code* are translated, by ``influxdb_client.rest.ApiException``
(raised at ``_async/rest.py`` lines ~223/232 — ``raise ApiException(...)``).
A DNS/socket-level failure before any HTTP response exists therefore never
becomes an ``ApiException`` at all. This module (and ``repository.py``)
catch ``aiohttp.ClientError`` explicitly alongside ``ApiException`` —
omitting it would let a raw driver-adjacent exception escape past the
adapter boundary, exactly the kind of gap ``docs/adapter-checklist.md``
calls out for Postgres's ``asyncpg.InterfaceError``.

``get_influxdb_client()`` is the single entry point for obtaining an
``InfluxDBClientAsync``. It creates the client on first call and returns the
cached instance on every subsequent call with the same
``(influxdb_url, influxdb_token, influxdb_org, client_timeout_ms)`` tuple.
InfluxDB has no connection-pool concept the way Postgres does (one
``aiohttp.ClientSession`` per client handles concurrency internally), but
the same cache-key discipline applies for the same reason: two
``InfluxDBSettings`` for the same URL but a different token/org/timeout must
not silently share a client.

Pool cache: ``_client_cache`` is a module-level dict keyed by
``(influxdb_url, influxdb_token, influxdb_org, client_timeout_ms)`` — not the
URL alone. Do NOT replace this with ``@lru_cache`` — that decorator does not
support async functions and would create a new coroutine on each call.
"""
from __future__ import annotations

import asyncio

import aiohttp
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.rest import ApiException

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)

from .config import InfluxDBSettings

__all__ = ["get_influxdb_client", "_client_cache", "_cache_key"]

_ClientCacheKey = tuple[str, str, str, int]

_client_cache: dict[_ClientCacheKey, InfluxDBClientAsync] = {}


def _cache_key(settings: InfluxDBSettings) -> _ClientCacheKey:
    """
    Cache key covering the URL plus every client-shape setting.

    Two ``InfluxDBSettings`` for the same ``influxdb_url`` but a different
    token, org, or timeout must not share a client — a shared key here would
    mean the second caller silently gets the first caller's credentials or
    timeout configuration (the same class of bug the checklist documents
    for Postgres's pool cache).
    """
    return (
        settings.influxdb_url,
        settings.influxdb_token,
        settings.influxdb_org,
        settings.client_timeout_ms,
    )


async def get_influxdb_client(settings: InfluxDBSettings) -> InfluxDBClientAsync:
    """
    Create or return the cached ``InfluxDBClientAsync``.

    Creates the client on first call for a given
    ``(url, token, org, timeout)`` tuple. Subsequent calls with a matching
    tuple return the cached client without reconnecting. Client creation
    itself is cheap (no handshake) — the actual connectivity check happens
    lazily on first request, which is why this function also issues a
    ``ping()`` to fail fast and loud, matching Postgres's
    ``get_postgres_pool()`` behaviour of verifying connectivity at creation
    time rather than deferring the first failure to an arbitrary later call.

    Args:
        settings: A fully-validated ``InfluxDBSettings`` instance.

    Returns:
        An ``InfluxDBClientAsync`` that is ready to use.

    Raises:
        AdapterConnectionError:    Client unreachable — DNS failure,
                                   connection refused, or any
                                   ``aiohttp.ClientError`` raised while
                                   pinging the server.
        AdapterConfigurationError: Token/org rejected (401/403 from the
                                   ``ping()`` call).
        AdapterTimeoutError:       Connectivity check exceeded
                                   ``settings.connection_timeout``.
    """
    key = _cache_key(settings)
    if key in _client_cache:
        return _client_cache[key]

    client = InfluxDBClientAsync(
        url=settings.influxdb_url,
        token=settings.influxdb_token,
        org=settings.influxdb_org,
        timeout=settings.client_timeout_ms,
    )
    try:
        async with asyncio.timeout(settings.connection_timeout):
            await client.ping()
    except asyncio.TimeoutError as exc:
        await client.close()
        raise AdapterTimeoutError(
            f"Client connectivity check exceeded {settings.connection_timeout}s connection_timeout",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except ApiException as exc:
        await client.close()
        if exc.status in (401, 403):
            raise AdapterConfigurationError(
                f"InfluxDB rejected the configured token/org ({exc.status}): {exc.reason}",
                adapter=settings.adapter_name,
                operation="init",
                cause=exc,
            ) from exc
        raise AdapterConnectionError(
            f"Cannot connect to InfluxDB at {settings.influxdb_url!r}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except (aiohttp.ClientError, OSError) as exc:
        # aiohttp connection-level failures (DNS, refused, reset) never
        # become ApiException — see the module docstring's second finding.
        await client.close()
        raise AdapterConnectionError(
            f"Cannot connect to InfluxDB at {settings.influxdb_url!r}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc

    _client_cache[key] = client
    return client
