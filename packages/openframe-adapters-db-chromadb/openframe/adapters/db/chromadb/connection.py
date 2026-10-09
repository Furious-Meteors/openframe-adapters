"""
openframe/adapters/db/chromadb/connection.py
==============================================
chromadb AsyncHttpClient factory and cache.

Async strategy (ADR-003: decided per-adapter, not forced)
-----------------------------------------------------------
This adapter uses **native async**, not the ``run_in_executor`` fallback.

Research finding (checked against the actually-installed driver, not
assumed): ``chromadb`` 1.5.9, installed fresh via ``pip install chromadb``
for this investigation, exposes a real, documented, genuinely-awaitable
async client-server API — not an experimental bolt-on over the sync
client. Verified directly:

    >>> import chromadb, inspect
    >>> chromadb.__version__
    '1.5.9'
    >>> hasattr(chromadb, "AsyncHttpClient")
    True
    >>> inspect.iscoroutinefunction(chromadb.AsyncHttpClient)
    True

``chromadb.AsyncHttpClient(host=..., port=...)`` itself is an ``async def``
factory that returns an ``AsyncClientAPI``, and every method this adapter
needs on the collection object it returns is a real coroutine function,
confirmed with ``inspect.iscoroutinefunction``:
``AsyncCollection.add``, ``.get``, ``.update``, ``.upsert``, ``.delete``,
``.query``, ``.count`` — all ``True``. ``AsyncClientAPI.get_or_create_collection``,
``.create_collection``, ``.get_collection`` are likewise real coroutines.

This is distinct from Chroma's embedded/persistent-local mode
(``chromadb.Client()``/``chromadb.PersistentClient()``), which has no
server to be async against and is intentionally out of scope for this
adapter — this ecosystem's adapters assume a real backend server
(see ``config.py``'s module docstring), matching how Postgres/Mongo/Redis
assume a running server rather than an embedded/local-file mode.

Because a real, usable native-async surface exists, this adapter is built
the same shape as ``openframe-adapters-db-postgres``/``-oracle`` (async
client cached by connection-parameter tuple, async CRUD methods) — not the
executor-wrapped-sync shape a backend with no native async client would
need. If a future ``chromadb`` release regressed this surface, the honest
move per ADR-003 would be to fall back to
``asyncio.get_running_loop().run_in_executor(None, sync_fn)`` with a plain
``chromadb.HttpClient`` here — this module does not do that because the
surface genuinely exists in the installed version.

A second finding worth documenting here since it affects this module
directly: connecting to an unreachable host raises a raw ``httpx``
exception, not a ``chromadb.errors.ChromaError`` — confirmed directly
against the installed version:

    >>> import asyncio, chromadb
    >>> async def f():
    ...     await chromadb.AsyncHttpClient(host="127.0.0.1", port=59999)
    >>> asyncio.run(f())
    httpx.ConnectError: All connection attempts failed

``chromadb``'s HTTP client is built on ``httpx`` (confirmed via
``pip show chromadb`` — ``httpx`` is a direct dependency; there is no
``requests`` dependency). ``httpx.TimeoutException`` is itself a subclass
of ``httpx.TransportError`` in the installed version, so this module (and
``repository.py``'s exception-wrapping helper) must check for
``httpx.TimeoutException`` *before* the broader ``httpx.TransportError``
catch, or every timeout would be misclassified as a generic connection
failure. See ``repository.py``'s ``_wrap_chromadb()`` for where this
distinction is actually applied.

``get_chromadb_client()`` is the single entry point for obtaining an
``AsyncClientAPI``. It creates the client on first call and returns the
cached instance on every subsequent call with the same connection
parameters (``chroma_host``, ``chroma_port``, ``chroma_ssl``,
``chroma_tenant``, ``chroma_database``). Multiple ``ChromaDBRepository``
instances constructed with matching settings share the same client.
Unlike Postgres/Oracle, Chroma's async client has no pool-shape knobs
(size, timeout, max-queries) of its own — the cache key here is simply
every connection-identity field, so two ``ChromaDBSettings`` pointing at
different tenants/databases on the same host:port get distinct clients
rather than one silently reusing the other's tenant/database scope (the
same category of bug the checklist's pool-cache-key item warns about,
applied to this driver's actual cache-worthy configuration surface).
"""
from __future__ import annotations

import asyncio

import chromadb
import httpx
from chromadb.api.async_api import AsyncClientAPI

from openframe.core.exceptions import (
    AdapterConnectionError,
    AdapterTimeoutError,
)

from .config import ChromaDBSettings

__all__ = ["get_chromadb_client", "_client_cache", "_cache_key"]

_ClientCacheKey = tuple[str, int, bool, str, str]

_client_cache: dict[_ClientCacheKey, AsyncClientAPI] = {}


def _cache_key(settings: ChromaDBSettings) -> _ClientCacheKey:
    """
    Cache key covering every connection-identity field.

    Two ``ChromaDBSettings`` pointing at the same host:port but a different
    tenant/database must not share a client — a shared key here would mean
    the second caller silently gets the first caller's tenant/database
    scope.
    """
    return (
        settings.chroma_host,
        settings.chroma_port,
        settings.chroma_ssl,
        settings.chroma_tenant,
        settings.chroma_database,
    )


async def get_chromadb_client(settings: ChromaDBSettings) -> AsyncClientAPI:
    """
    Create or return the cached ``AsyncClientAPI``.

    Creates the client on first call for a given connection-parameter
    tuple. Subsequent calls with matching settings return the cached
    client without reconnecting.

    Args:
        settings: A fully-validated ``ChromaDBSettings`` instance.

    Returns:
        An ``AsyncClientAPI`` that is ready to use.

    Raises:
        AdapterConnectionError: Client creation failed — host unreachable,
                                TLS error, auth rejected, etc. (surfaces as
                                a raw ``httpx.TransportError`` subclass from
                                the driver — see this module's docstring).
        AdapterTimeoutError:    Client creation exceeded
                                ``settings.connection_timeout``, whether via
                                this module's own ``asyncio.timeout`` or a
                                ``httpx.TimeoutException`` raised by the
                                driver itself.
    """
    key = _cache_key(settings)
    if key in _client_cache:
        return _client_cache[key]

    try:
        async with asyncio.timeout(settings.connection_timeout):
            client = await chromadb.AsyncHttpClient(
                host=settings.chroma_host,
                port=settings.chroma_port,
                ssl=settings.chroma_ssl,
                tenant=settings.chroma_tenant,
                database=settings.chroma_database,
            )
    except asyncio.TimeoutError as exc:
        raise AdapterTimeoutError(
            f"Client creation exceeded {settings.connection_timeout}s connection_timeout",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except httpx.TimeoutException as exc:
        # Must be caught before httpx.TransportError below — TimeoutException
        # is itself a TransportError subclass in the installed version.
        raise AdapterTimeoutError(
            f"Client creation to Chroma at "
            f"{settings.chroma_host}:{settings.chroma_port} timed out: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except httpx.TransportError as exc:
        raise AdapterConnectionError(
            f"Cannot connect to Chroma at "
            f"{settings.chroma_host}:{settings.chroma_port}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc

    _client_cache[key] = client
    return client
