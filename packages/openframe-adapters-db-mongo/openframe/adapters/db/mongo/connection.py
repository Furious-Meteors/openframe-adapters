"""
openframe/adapters/db/mongo/connection.py
==========================================
Motor client factory and cache.

``get_mongo_client()`` is the single entry point for obtaining an
``AsyncIOMotorClient``. Unlike asyncpg, motor's client constructor is
**synchronous** and **lazy** — it does not open a connection at creation time.
The actual TCP handshake happens on the first operation. This means:

- ``get_mongo_client()`` is a plain synchronous function (not async).
- Connection errors (wrong credentials, unreachable host) surface in the
  repository methods, not here.
- Only syntactic URL errors raise immediately (``pymongo.errors.ConfigurationError``).

Cache: ``_client_cache`` is a module-level dict keyed by ``mongo_url``.
Multiple ``MongoRepository`` instances in the same process share one client.
"""
from __future__ import annotations

import pymongo.errors
from motor.motor_asyncio import AsyncIOMotorClient

from openframe.core.exceptions import AdapterConfigurationError

from .config import MongoSettings

__all__ = ["get_mongo_client", "_client_cache"]

_client_cache: dict[str, AsyncIOMotorClient] = {}  # type: ignore[type-arg]


def get_mongo_client(settings: MongoSettings) -> AsyncIOMotorClient:  # type: ignore[type-arg]
    """
    Create or return the cached ``AsyncIOMotorClient``.

    Motor's client constructor is synchronous and does not connect
    immediately — the connection is established lazily on the first
    operation. This function therefore requires no ``await``.

    The client is cached per ``mongo_url``. Multiple ``MongoRepository``
    instances in the same process share the same underlying client and its
    connection pool.

    Args:
        settings: A fully-validated ``MongoSettings`` instance.

    Returns:
        An ``AsyncIOMotorClient`` ready for use (lazy connection).

    Raises:
        AdapterConfigurationError: If ``MONGO_URL`` is syntactically invalid
                                   (``pymongo.errors.ConfigurationError``).
    """
    url = settings.mongo_url
    if url in _client_cache:
        return _client_cache[url]

    try:
        client: AsyncIOMotorClient = AsyncIOMotorClient(  # type: ignore[assignment]
            url,
            serverSelectionTimeoutMS=settings.mongo_server_selection_timeout_ms,
            minPoolSize=settings.mongo_min_pool_size,
            maxPoolSize=settings.mongo_max_pool_size,
            tls=settings.mongo_tls,
            tlsAllowInvalidCertificates=settings.mongo_tls_allow_invalid_certs,
        )
    except pymongo.errors.ConfigurationError as exc:
        raise AdapterConfigurationError(
            f"MONGO_URL is invalid or unsupported: {url!r} — {exc}",
            adapter=settings.adapter_name,
            operation="init",
            cause=exc,
        ) from exc

    _client_cache[url] = client
    return client
