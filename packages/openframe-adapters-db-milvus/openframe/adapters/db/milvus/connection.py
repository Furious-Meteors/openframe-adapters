"""
openframe/adapters/db/milvus/connection.py
=============================================
``pymilvus.AsyncMilvusClient`` factory and cache.

Async strategy (ADR-003: decided per-adapter, not forced)
-----------------------------------------------------------
This adapter uses **native async** — not the ``run_in_executor`` fallback.

Research finding (checked against the actually-installed driver, not
assumed/recalled): ``pymilvus`` 3.0.2, installed fresh via
``pip install pymilvus`` for this investigation, ships a real
``pymilvus.AsyncMilvusClient`` with genuine ``async def`` methods —
verified directly with ``inspect.iscoroutinefunction``::

    >>> import pymilvus, inspect
    >>> pymilvus.__version__
    '3.0.2'
    >>> from pymilvus import AsyncMilvusClient
    >>> for name in ("insert", "upsert", "get", "delete", "query", "search"):
    ...     print(name, inspect.iscoroutinefunction(getattr(AsyncMilvusClient, name)))
    insert True
    upsert True
    get True
    delete True
    query True
    search True

This is not an experimental/incomplete surface bolted onto the sync
client — every CRUD + search method this adapter needs is a real
coroutine function with a stable signature, e.g.::

    >>> inspect.signature(AsyncMilvusClient.search)
    (self, collection_name, data=None, filter='', limit=10, output_fields=None,
     search_params=None, timeout=None, partition_names=None, anns_field=None,
     ranker=None, function_chains=None, ids=None, search_aggregation=None, **kwargs)

Because a real, usable native-async surface exists, this adapter is built
the same shape as ``openframe-adapters-db-postgres``/``-oracle`` (async
client cached by connection-shape settings, async CRUD methods) — not the
executor-wrapped-sync shape ``openframe-adapters-db-cassandra`` needs for
query execution. If a future ``pymilvus`` release regressed this surface,
the honest fallback per ADR-003 would be
``asyncio.get_running_loop().run_in_executor(None, sync_fn)`` around the
synchronous ``pymilvus.MilvusClient`` — this module does not do that
because the native surface genuinely exists in the installed version.

A second research finding, worth documenting here since it drives this
module's and ``repository.py``'s exception-handling: ``AsyncMilvusClient.
__init__`` is **synchronous and lazy** — it only parses/stores the
connection config (``ConnectionConfig.from_uri(...)``); it does not open a
gRPC channel or touch the network. The first real connection attempt
happens on the first *awaited* call (``list_collections()``, ``get()``,
etc.). This means a bad host/port is never caught at construction time —
only on first use — confirmed directly::

    >>> AsyncMilvusClient(uri="http://127.0.0.1:19")   # port 19 refuses connections
    <AsyncMilvusClient object>   # constructs fine, no exception
    >>> await client.list_collections()                # raises here instead
    MilvusException: (code=2, message=Fail connecting to server on
    127.0.0.1:19, illegal connection params or server unavailable)

A third finding, the one that most affects ``repository.py``'s exception
mapping: the task brief (and a cursory reading of ``pymilvus.exceptions``)
suggests connection-class failures surface as ``ConnectError`` or
``MilvusUnavailableException``. Verified directly against the installed
driver that this is **not** what happens for ``AsyncMilvusClient`` — both
classes exist in ``pymilvus.exceptions`` but are never actually raised
anywhere reachable from ``AsyncMilvusClient`` in this version (grepped the
installed package source: zero non-definition references). A real
unreachable-server failure instead raises a bare ``pymilvus.
MilvusException`` with ``.code == 2`` (not a value in the public
``ErrorCode`` IntEnum, which only defines ``SUCCESS``/``UNEXPECTED_ERROR``/
``FORCE_DENY``/``RATE_LIMIT``/``COLLECTION_NOT_FOUND``/``INDEX_NOT_FOUND``)
and a ``.message`` containing ``"Fail connecting to server"``. See
``repository.py``'s ``_wrap_milvus()`` for how this is handled: the
specific query/config-class subclasses (``ParamError``,
``SchemaNotReadyException``, ``CollectionNotExistException``, etc.) are
matched first; ``ConnectError``/``MilvusUnavailableException`` are still
checked defensively in case a future driver version actually raises them;
and anything else is classified by inspecting the message text for
connection-failure phrasing, falling back to a non-retryable query error
if nothing connection-like is found. This is the honest state of the
driver's exception hierarchy in 3.0.2 — don't assume the subclass names
documented in pymilvus's own docstrings are what's actually thrown.

``get_milvus_client()`` is the single entry point for obtaining an
``AsyncMilvusClient``. It creates the client on first call and returns the
cached instance on every subsequent call with the same connection-shape
settings (``milvus_uri``, ``milvus_token``, ``milvus_db_name``). Multiple
``MilvusRepository`` instances constructed with matching settings share
the same client.

Client cache: ``_client_cache`` is a module-level dict keyed by
``(milvus_uri, milvus_token, milvus_db_name)`` — not the URI alone. Two
``MilvusSettings`` instances with the same ``milvus_uri`` but a different
``milvus_token``/``milvus_db_name`` get two distinct clients, rather than
the second one silently inheriting the first one's credentials/database
(the same cache-key-shape bug this checklist exists to prevent — see
``docs/adapter-checklist.md``).
Do NOT replace this with ``@lru_cache`` — that decorator does not support
async functions and would create a new coroutine on each call.
"""
from __future__ import annotations

import asyncio

from pymilvus import AsyncMilvusClient
from pymilvus.exceptions import MilvusException

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)

from .config import MilvusSettings

__all__ = ["get_milvus_client", "_client_cache", "_cache_key"]

_ClientCacheKey = tuple[str, str, str]

_client_cache: dict[_ClientCacheKey, AsyncMilvusClient] = {}

# Substrings observed in MilvusException.message for connection-class
# failures in the installed driver (3.0.2) — see this module's docstring
# for why subclass-based detection (ConnectError/MilvusUnavailableException)
# does not work: those classes are never actually raised.
_CONNECTION_FAILURE_PHRASES = (
    "fail connecting",
    "failed to connect",
    "unavailable",
    "deadline exceeded",
    "connection reset",
    "connection refused",
)


def _cache_key(settings: MilvusSettings) -> _ClientCacheKey:
    """
    Cache key covering the URI plus every connection-identity setting.

    Two ``MilvusSettings`` for the same ``milvus_uri`` but a different
    token or database name must not share a client — a shared key here
    would mean the second caller silently gets the first caller's
    credentials/database.
    """
    return (settings.milvus_uri, settings.milvus_token, settings.milvus_db_name)


def _looks_like_connection_failure(exc: MilvusException) -> bool:
    message = (getattr(exc, "message", "") or str(exc)).lower()
    return any(phrase in message for phrase in _CONNECTION_FAILURE_PHRASES)


async def get_milvus_client(settings: MilvusSettings) -> AsyncMilvusClient:
    """
    Create or return the cached ``AsyncMilvusClient``.

    Creates the client on first call for a given connection-identity key
    and returns the cached instance on every subsequent call with matching
    settings. A different URI/token/db_name for otherwise-matching settings
    gets its own client rather than reusing the first one's.

    Construction itself (``AsyncMilvusClient(...)``) is synchronous and
    lazy — it never touches the network (see module docstring) — so this
    function performs an explicit ``list_collections()`` probe under
    ``settings.connection_timeout`` to verify connectivity eagerly, the
    same contract ``get_postgres_pool()``/``get_oracle_pool()`` provide.

    Args:
        settings: A fully-validated ``MilvusSettings`` instance.

    Returns:
        An ``AsyncMilvusClient`` that is ready to use.

    Raises:
        AdapterConnectionError:    Server unreachable, auth failure, etc.
        AdapterConfigurationError: ``milvus_uri`` is syntactically invalid.
        AdapterTimeoutError:       Connectivity probe exceeded
                                   ``settings.connection_timeout``.
    """
    key = _cache_key(settings)
    if key in _client_cache:
        return _client_cache[key]

    try:
        client = AsyncMilvusClient(
            uri=settings.milvus_uri,
            token=settings.milvus_token,
            db_name=settings.milvus_db_name,
        )
    except MilvusException as exc:
        raise AdapterConfigurationError(
            f"Invalid Milvus connection configuration for {settings.milvus_uri!r}: {exc}",
            adapter=settings.adapter_name,
            operation="init",
            cause=exc,
        ) from exc

    try:
        async with asyncio.timeout(settings.connection_timeout):
            await client.list_collections()
    except asyncio.TimeoutError as exc:
        raise AdapterTimeoutError(
            f"Client connectivity probe exceeded {settings.connection_timeout}s connection_timeout",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except MilvusException as exc:
        raise AdapterConnectionError(
            f"Cannot connect to Milvus at {settings.milvus_uri!r}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc

    _client_cache[key] = client
    return client
