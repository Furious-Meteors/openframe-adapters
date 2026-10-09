"""
openframe/adapters/db/dynamodb/connection.py
===============================================
aioboto3 DynamoDB resource/Table factory and cache.

``get_dynamodb_table()`` is the single entry point for obtaining a DynamoDB
``Table`` resource. It creates the underlying ``aioboto3.Session`` and
DynamoDB resource on first call and returns the cached ``Table`` on every
subsequent call with a matching ``(aws_region, endpoint_url,
dynamodb_table_name, credential-tuple)``.

Research spike finding — what "connection" means for this driver:
    ``aioboto3`` wraps ``aiobotocore``, which replaces botocore's blocking
    ``urllib3``/httpconnection stack with real ``aiohttp``-backed async I/O
    (see ``aiobotocore.httpsession.AIOHTTPSession`` — it opens genuine
    ``aiohttp.ClientSession`` objects, not a thread pool wrapping sync boto3
    calls). So unlike a naive "wrapper" driver that just runs blocking calls
    in an executor, calls made through the resource/client this module
    caches really do perform non-blocking network I/O.

    However, DynamoDB itself has no concept of a persistent TCP "connection
    pool" the way Postgres/MySQL do — it is a managed, stateless, HTTP-based
    AWS service. ``aioboto3.Session().resource("dynamodb", ...)`` returns an
    async context manager whose ``__aenter__`` sets up an internal
    ``aiohttp.ClientSession`` (itself pooling HTTP connections under the
    hood via its own connector), but does **not** perform any network call —
    entering it is cheap and does not fail even if the endpoint is
    unreachable. The first real network round-trip happens on the first
    actual operation (``get_item``, ``put_item``, etc.).

    What this module's cache holds is therefore a **session/resource/Table
    object**, not a connection pool in the Postgres/MySQL sense. It still
    exists for the same reason the Postgres/MySQL pool cache exists —
    avoiding the (small but real) overhead of re-creating the aiohttp
    session and re-resolving credentials/region config on every call — but
    callers should not expect "pool exhaustion" or "pool size" semantics
    here; aiohttp's own connector handles concurrent-request pooling
    transparently underneath the single cached ``Table`` object.

Cache: ``_table_cache`` is a module-level dict keyed by
``(aws_region, endpoint_url, dynamodb_table_name, aws_access_key_id,
aws_secret_access_key, aws_session_token)`` — not just region+table. Two
``DynamoDBSettings`` instances for the same region and table but pointed at
different endpoints (e.g. one at local DynamoDB, one at real AWS) or
different credentials must get two distinct sessions/resources, not the
second one silently inheriting the first one's configuration. Do NOT
replace this with ``@lru_cache`` — that decorator does not support async
functions and would create a new coroutine on each call.

Error-code gotcha (verified against the installed ``botocore`` exception
model — see this module's ``_wrap_botocore`` sibling in ``repository.py``
for the full table):
    ``aioboto3``/``botocore`` raise ``botocore.exceptions.ClientError`` for
    essentially every *service-side* DynamoDB error (table not found, bad
    input, throttling, conditional check failures — all distinguished only
    by ``exc.response["Error"]["Code"]``, never by a distinct Python
    exception subclass per error). Genuine *local*/network-level failures
    — the request never reached AWS at all — raise
    ``botocore.exceptions.EndpointConnectionError`` or a plain
    ``ConnectionError`` instead, which is a *different* exception hierarchy
    entirely. Missing/invalid credentials or region raise
    ``botocore.exceptions.NoCredentialsError``/``NoRegionError``/
    ``PartialCredentialsError`` at resource-creation time.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import aioboto3
import botocore.exceptions

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)

from .config import DynamoDBSettings

__all__ = ["get_dynamodb_table", "close_dynamodb_table", "_table_cache", "_cache_key"]

_TableCacheKey = tuple[str, str | None, str, str | None, str | None, str | None]


@dataclass
class _CachedTable:
    """Everything needed to later tear the cached Table down cleanly."""

    resource_cm: Any  # the aioboto3 ResourceCreatorContext (async CM)
    resource: Any      # the entered DynamoDB ServiceResource
    table: Any          # resource.Table(name) — what callers actually use


_table_cache: dict[_TableCacheKey, _CachedTable] = {}

# Configuration-class botocore exceptions raised at resource-creation time —
# the request never left the local process because required config/creds
# are missing or malformed.
_CONFIGURATION_EXCEPTIONS = (
    botocore.exceptions.NoCredentialsError,
    botocore.exceptions.PartialCredentialsError,
    botocore.exceptions.NoRegionError,
)


def _cache_key(settings: DynamoDBSettings) -> _TableCacheKey:
    """
    Cache key covering region, endpoint, table name, and credentials.

    Two ``DynamoDBSettings`` for the same region+table but a different
    ``endpoint_url`` (local DynamoDB vs. real AWS) or different credentials
    must not share a resource/Table — a shared key here would mean the
    second caller silently gets the first caller's connection target.
    """
    return (
        settings.aws_region,
        settings.endpoint_url,
        settings.dynamodb_table_name,
        settings.aws_access_key_id,
        settings.aws_secret_access_key,
        settings.aws_session_token,
    )


async def get_dynamodb_table(settings: DynamoDBSettings) -> Any:
    """
    Create or return the cached DynamoDB ``Table`` resource.

    Creates the underlying ``aioboto3`` session/resource on first call for a
    given cache key (see ``_cache_key``). Subsequent calls with a matching
    key return the cached ``Table`` without re-creating the session.

    Args:
        settings: A fully-validated ``DynamoDBSettings`` instance.

    Returns:
        An ``aioboto3`` DynamoDB ``Table`` resource, ready to use.

    Raises:
        AdapterConnectionError:    Resource creation failed for a
                                   connection-class reason (rare at this
                                   stage — most DynamoDB failures surface on
                                   the first real operation, not here).
        AdapterConfigurationError: Region/credentials are missing or
                                   malformed.
        AdapterTimeoutError:       Resource creation exceeded
                                   ``settings.connection_timeout``.
    """
    key = _cache_key(settings)
    cached = _table_cache.get(key)
    if cached is not None:
        return cached.table

    session = aioboto3.Session()
    resource_kwargs: dict[str, Any] = {"region_name": settings.aws_region}
    if settings.endpoint_url is not None:
        resource_kwargs["endpoint_url"] = settings.endpoint_url
    if settings.aws_access_key_id is not None:
        resource_kwargs["aws_access_key_id"] = settings.aws_access_key_id
    if settings.aws_secret_access_key is not None:
        resource_kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
    if settings.aws_session_token is not None:
        resource_kwargs["aws_session_token"] = settings.aws_session_token

    resource_cm = session.resource("dynamodb", **resource_kwargs)
    try:
        async with asyncio.timeout(settings.connection_timeout):
            resource = await resource_cm.__aenter__()
    except asyncio.TimeoutError as exc:
        raise AdapterTimeoutError(
            f"DynamoDB resource creation exceeded {settings.connection_timeout}s "
            "connection_timeout",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except _CONFIGURATION_EXCEPTIONS as exc:
        raise AdapterConfigurationError(
            f"DynamoDB resource creation failed — missing/invalid region or "
            f"credentials: {exc}",
            adapter=settings.adapter_name,
            operation="init",
            cause=exc,
        ) from exc
    except (botocore.exceptions.EndpointConnectionError, ConnectionError, OSError) as exc:
        raise AdapterConnectionError(
            f"Cannot reach DynamoDB endpoint for region {settings.aws_region!r}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc

    table = resource.Table(settings.dynamodb_table_name)
    _table_cache[key] = _CachedTable(resource_cm=resource_cm, resource=resource, table=table)
    return table


async def close_dynamodb_table(settings: DynamoDBSettings) -> None:
    """
    Close the cached resource for ``settings`` and remove it from the cache.

    Calls ``__aexit__`` on the underlying ``aioboto3`` resource context
    manager, which closes its internal ``aiohttp.ClientSession``. Safe to
    call even if nothing is cached for ``settings`` (no-op).
    """
    key = _cache_key(settings)
    cached = _table_cache.pop(key, None)
    if cached is not None:
        await cached.resource_cm.__aexit__(None, None, None)
