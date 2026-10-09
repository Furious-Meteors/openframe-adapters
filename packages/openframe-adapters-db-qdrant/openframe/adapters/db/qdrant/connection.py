"""
openframe/adapters/db/qdrant/connection.py
=============================================
AsyncQdrantClient factory and cache.

Async client research (checked against the actually-installed driver, not
assumed)
---------------------------------------------------------------------------
``qdrant-client`` 1.19.1, installed fresh via ``pip install qdrant-client``
for this investigation, exposes ``qdrant_client.AsyncQdrantClient`` with
real ``async def`` methods — verified directly with
``inspect.iscoroutinefunction()`` on ``upsert``, ``retrieve``, ``delete``,
``scroll``, and the method that replaced ``search`` (see below). All are
genuinely awaitable native coroutines, not a ``run_in_executor`` wrapper
around the sync client. This adapter therefore uses ``AsyncQdrantClient``
natively, matching Postgres's/Oracle's async shape.

Two real surprises found during the same investigation, both load-bearing
for this module and for ``repository.py``:

1. ``AsyncQdrantClient.search()`` **does not exist** in 1.19.1::

       >>> hasattr(qdrant_client.AsyncQdrantClient, "search")
       False

   ``search`` was removed in favour of the "universal query" endpoint,
   ``query_points(collection_name, query=<vector>, limit=k)``, which
   returns a ``QueryResponse`` whose ``.points`` attribute is the
   ``list[ScoredPoint]`` that ``search()`` used to return directly. This
   port's own ``search()`` method (see ``repository.py``) is implemented
   against ``query_points``, not ``search`` — the spec that assumed
   ``client.search(...)`` existed was written from general knowledge of
   the Qdrant API, not the installed version; this module corrects that
   assumption against the real client rather than propagating it.

2. ``AsyncQdrantClient(...)`` is a **synchronous, lazy constructor** — it
   performs no network I/O on instantiation (confirmed: no ``await``
   needed to construct it, and the first real round-trip only happens on
   the first actual operation, e.g. ``get_collections()``). This is
   unlike ``asyncpg.create_pool()``, which eagerly connects. To still
   surface connection failures at "factory" time (the same contract
   ``get_postgres_pool()``/``get_oracle_pool()`` provide — a connection
   problem is caught and translated before the caller's first real
   operation, not on it), ``get_qdrant_client()`` performs one cheap
   connectivity probe (``get_collections()``) immediately after
   constructing a *new* (cache-miss) client, wrapped in the same
   timeout/exception-translation logic as every other operation. This is
   a deliberate choice this module makes to match the rest of the
   ecosystem's factory contract — it is not something the driver forces.

Transport: ``prefer_grpc`` defaults to ``False`` on the installed client,
so the default transport is REST over ``httpx`` (confirmed via
``inspect.signature(AsyncQdrantClient.__init__)``). Verified behaviourally
by pointing a client at an unreachable host and awaiting
``get_collections()``: the failure surfaces as
``qdrant_client.http.exceptions.ResponseHandlingException``, whose
``.source`` attribute is the underlying ``httpx.ConnectError`` — i.e.
httpx-level connection errors are not raised directly, they are wrapped.
See ``repository.py``'s ``_wrap_qdrant()`` for the exception classification
built on this finding. gRPC transport (``prefer_grpc=True``) is supported
by the settings/cache key below but its own error surface
(``grpc.aio.AioRpcError``) was not exercised in this investigation since
the default, and the transport this adapter is tested against, is REST.

``get_qdrant_client()`` is the single entry point for obtaining an
``AsyncQdrantClient``. It creates the client on first call and returns the
cached instance on every subsequent call with the same ``qdrant_url`` AND
the same client-shape settings (``qdrant_api_key``, ``qdrant_prefer_grpc``,
``qdrant_https``). Multiple ``QdrantVectorStore`` instances constructed
with matching settings share the same client.

Client cache: ``_client_cache`` is a module-level dict keyed by
``(qdrant_url, config-tuple)`` — not the URL alone. Two ``QdrantSettings``
instances with the same ``qdrant_url`` but different transport/auth
settings get two distinct clients, rather than the second one silently
inheriting the first one's configuration (the bug this key shape fixes —
see ``docs/adapter-checklist.md``).
Do NOT replace this with ``@lru_cache`` — that decorator does not support
async functions and would create a new coroutine on each call.
"""
from __future__ import annotations

import asyncio

from qdrant_client import AsyncQdrantClient
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)

from .config import QdrantSettings

__all__ = ["get_qdrant_client", "_client_cache", "_cache_key"]

_ClientCacheKey = tuple[str, tuple[str | None, bool, bool | None]]

_client_cache: dict[_ClientCacheKey, AsyncQdrantClient] = {}


def _cache_key(settings: QdrantSettings) -> _ClientCacheKey:
    """
    Cache key covering the URL plus every client-shape setting.

    Two ``QdrantSettings`` for the same ``qdrant_url`` but different
    auth/transport configuration must not share a client — a shared key
    here would mean the second caller silently gets the first caller's
    API key or transport choice.
    """
    return (
        settings.qdrant_url,
        (
            settings.qdrant_api_key,
            settings.qdrant_prefer_grpc,
            settings.qdrant_https,
        ),
    )


async def get_qdrant_client(settings: QdrantSettings) -> AsyncQdrantClient:
    """
    Create or return the cached ``AsyncQdrantClient``.

    Creates the client on first call for a given
    ``(qdrant_url, client config)`` pair and immediately probes
    connectivity with ``get_collections()`` (see this module's docstring
    for why that probe exists — the client constructor itself is lazy and
    performs no I/O). Subsequent calls with a matching URL and config
    return the cached client without re-probing.

    Args:
        settings: A fully-validated ``QdrantSettings`` instance.

    Returns:
        An ``AsyncQdrantClient`` that has been probed for connectivity.

    Raises:
        AdapterConnectionError:    Server unreachable, TLS error, auth
                                   failure, or any 5xx/unrecognised
                                   response during the connectivity probe.
        AdapterConfigurationError: ``qdrant_url`` is syntactically invalid
                                   (rejected by the client itself before
                                   any network call).
        AdapterTimeoutError:      Probe exceeded ``settings.connection_timeout``.
    """
    key = _cache_key(settings)
    if key in _client_cache:
        return _client_cache[key]

    try:
        client = AsyncQdrantClient(
            url=settings.qdrant_url,
            api_key=settings.qdrant_api_key,
            prefer_grpc=settings.qdrant_prefer_grpc,
            https=settings.qdrant_https,
        )
    except ValueError as exc:
        raise AdapterConfigurationError(
            f"Invalid qdrant_url {settings.qdrant_url!r}: {exc}",
            adapter=settings.adapter_name,
            operation="init",
            cause=exc,
        ) from exc

    try:
        async with asyncio.timeout(settings.connection_timeout):
            await client.get_collections()
    except asyncio.TimeoutError as exc:
        raise AdapterTimeoutError(
            f"Client connectivity probe exceeded {settings.connection_timeout}s connection_timeout",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except ResponseHandlingException as exc:
        raise AdapterConnectionError(
            f"Cannot connect to Qdrant at {settings.qdrant_url!r}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except UnexpectedResponse as exc:
        status = exc.status_code
        if status is not None and 400 <= status < 500:
            raise AdapterConfigurationError(
                f"Qdrant rejected the connectivity probe ({status}): {exc}",
                adapter=settings.adapter_name,
                operation="connect",
                cause=exc,
            ) from exc
        raise AdapterConnectionError(
            f"Qdrant connectivity probe failed ({status}): {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc

    _client_cache[key] = client
    return client
