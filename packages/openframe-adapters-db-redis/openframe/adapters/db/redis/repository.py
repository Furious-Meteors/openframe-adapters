"""
openframe/adapters/db/redis/repository.py
==========================================
Generic Redis key-value repository implementing ``BaseRepository[T]``
from ``openframe-core`` via structural subtyping.

The base class works with raw ``dict[str, Any]`` values. Domain adapters
subclass it and override ``_entity_to_dict()`` and ``_dict_to_entity()`` to
map between Redis JSON strings and typed domain objects.

Key format:
    Every entity is stored under the key ``{prefix}:{entity_id}``. The
    prefix defaults to ``"openframe"`` and is configurable via
    ``RedisSettings.redis_key_prefix``. This prevents key collisions when
    multiple repositories share one Redis instance.

Timeout strategy:
    ``asyncio.timeout(settings.operation_timeout)`` wraps every Redis command.
    This cancels the operation if the Python event loop is blocked.

Error handling:
    Every ``redis.exceptions.*`` is caught and re-raised as the appropriate
    ``AdapterError`` subclass with cause chaining.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Generic, TypeVar

import redis.exceptions

from openframe.core.ports import Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import (
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository

from .config import RedisSettings
from .connection import _client_cache, get_redis_client

__all__ = ["RedisRepository"]

T = TypeVar("T")


class RedisRepository(Generic[T]):
    """
    Generic Redis key-value repository.

    Implements ``BaseRepository[T]`` structurally — no
    inheritance from the Protocol. All Redis exceptions are caught and
    re-raised as ``AdapterError`` subclasses. Every operation wraps its
    Redis command in ``asyncio.timeout(settings.operation_timeout)``.

    Health check: ``health()`` is the canonical — and only — health check.
    It returns a ``PluginHealth`` snapshot and never raises. ``ping()`` and
    ``is_ready()`` were removed in v2.0; use ``health()``.

    Entities are stored as JSON strings under prefixed keys:
        ``{settings.redis_key_prefix}:{entity_id}``

    Base implementation works with ``dict[str, Any]``. Subclasses override
    ``_entity_to_dict()`` and ``_dict_to_entity()`` for typed domain objects.

    Usage — raw dict mode::

        repo = RedisRepository(settings)
        item = await repo.get("abc-123")   # dict | None

    Usage — typed domain mode::

        class ItemRepository(RedisRepository[Item]):
            def _dict_to_entity(self, data: dict) -> Item:
                return Item(**data)
            def _entity_to_dict(self, entity: Item) -> dict:
                return entity.model_dump()

    Structural conformance::

        assert isinstance(repo, BaseRepository)
    """

    name:       str = "openframe-redis-repository"
    version:    str = "1.2.0"
    capability: Capability = Capability.CACHE

    def __init__(self, settings: RedisSettings) -> None:
        self._settings = settings

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    def _make_key(self, entity_id: str) -> str:
        """Return the full Redis key for an entity: ``{prefix}:{entity_id}``."""
        return f"{self._settings.redis_key_prefix}:{entity_id}"

    # ------------------------------------------------------------------
    # Entity ↔ dict mapping (override in typed subclasses)
    # ------------------------------------------------------------------

    def _entity_to_dict(self, entity: T) -> dict[str, Any]:
        """
        Convert the entity type ``T`` to a plain dict for JSON serialisation.

        Base implementation returns the entity unchanged if it is already a
        dict, or falls back to ``vars(entity)`` for simple objects. Subclasses
        override this to serialise typed domain objects correctly.
        """
        if isinstance(entity, dict):
            return dict(entity)
        return vars(entity)

    def _dict_to_entity(self, data: dict[str, Any]) -> T:
        """
        Convert a deserialised JSON dict to the entity type ``T``.

        Base implementation returns the dict unchanged. Subclasses override
        this to return typed domain objects.
        """
        return data  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Exception mapping helper
    # ------------------------------------------------------------------

    def _wrap_redis(self, exc: Exception, operation: str) -> AdapterQueryError | AdapterConnectionError | AdapterTimeoutError:
        """
        Map a redis exception to the appropriate ``AdapterError`` subclass.

        Caller must ``raise ... from exc`` at the call site.
        """
        if isinstance(exc, asyncio.TimeoutError):
            return AdapterTimeoutError(
                f"{operation} exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        if isinstance(exc, (redis.exceptions.ConnectionError, redis.exceptions.AuthenticationError)):
            return AdapterConnectionError(
                f"{operation} failed — cannot reach Redis: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        return AdapterQueryError(
            f"{operation} failed on Redis: {exc}",
            adapter=self._settings.adapter_name,
            operation=operation,
            cause=exc,
        )

    # ------------------------------------------------------------------
    # BaseRepository[T] interface
    # ------------------------------------------------------------------

    async def get(self, entity_id: str) -> T | None:
        """
        Retrieve a single entity by its ID.

        Fetches the JSON string stored at ``{prefix}:{entity_id}`` and
        deserialises it. Returns ``None`` if the key does not exist.

        Args:
            entity_id: The entity's string identifier.

        Returns:
            The entity if the key exists, ``None`` otherwise.

        Raises:
            AdapterConnectionError: Redis is unreachable.
            AdapterQueryError:      Redis command failed.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        client = await get_redis_client(self._settings)
        key = self._make_key(entity_id)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                value: str | None = await client.get(key)
        except (
            asyncio.TimeoutError,
            redis.exceptions.ConnectionError,
            redis.exceptions.AuthenticationError,
            redis.exceptions.TimeoutError,
            redis.exceptions.ResponseError,
            redis.exceptions.DataError,
            redis.exceptions.RedisError,
        ) as exc:
            raise self._wrap_redis(exc, "get") from exc

        if value is None:
            return None
        return self._dict_to_entity(json.loads(value))

    async def list(self, limit: int, offset: int) -> tuple[list[T], int]:
        """
        Return a paginated slice of entities and the total count.

        Uses ``SCAN`` with ``MATCH {prefix}:*`` to enumerate all keys for
        this repository's prefix — never ``KEYS``. Keys are sorted for
        deterministic ordering before slicing.

        Args:
            limit:  Maximum number of entities to return.
            offset: Number of entities to skip.

        Returns:
            A 2-tuple ``(entities, total_count)`` where ``total_count`` is
            the total number of keys matching the prefix pattern.

        Raises:
            AdapterConnectionError: Redis is unreachable.
            AdapterQueryError:      Redis command failed.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        client = await get_redis_client(self._settings)
        pattern = f"{self._settings.redis_key_prefix}:*"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                all_keys: list[str] = []
                async for key in client.scan_iter(match=pattern, count=100):
                    all_keys.append(key)

                total = len(all_keys)
                all_keys.sort()
                page_keys = all_keys[offset: offset + limit]

                if not page_keys:
                    return [], total

                values: list[str | None] = await client.mget(*page_keys)
        except (
            asyncio.TimeoutError,
            redis.exceptions.ConnectionError,
            redis.exceptions.AuthenticationError,
            redis.exceptions.TimeoutError,
            redis.exceptions.ResponseError,
            redis.exceptions.DataError,
            redis.exceptions.RedisError,
        ) as exc:
            raise self._wrap_redis(exc, "list") from exc

        entities = [
            self._dict_to_entity(json.loads(v))
            for v in values
            if v is not None
        ]
        return entities, total

    async def create(self, entity: T) -> T:
        """
        Insert a new entity.

        Serialises the entity to JSON and stores it using ``SET NX``
        (set-if-not-exists) so that duplicate creates raise an error
        instead of silently overwriting existing data.

        Args:
            entity: The entity to insert. Must have an ``id`` field.

        Returns:
            The entity as passed in.

        Raises:
            AdapterQueryError:      Entity has no ``id`` field, or the key
                                    already exists.
            AdapterConnectionError: Redis is unreachable.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        client = await get_redis_client(self._settings)
        data = self._entity_to_dict(entity)
        entity_id = str(data.get("id", ""))
        if not entity_id:
            raise AdapterQueryError(
                "Entity must have an 'id' field to create",
                adapter=self._settings.adapter_name,
                operation="create",
            )

        key = self._make_key(entity_id)
        value = json.dumps(data)
        ttl = self._settings.redis_default_ttl or None

        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                set_ok: bool | None = await client.set(key, value, nx=True, ex=ttl)
        except (
            asyncio.TimeoutError,
            redis.exceptions.ConnectionError,
            redis.exceptions.AuthenticationError,
            redis.exceptions.TimeoutError,
            redis.exceptions.ResponseError,
            redis.exceptions.DataError,
            redis.exceptions.RedisError,
        ) as exc:
            raise self._wrap_redis(exc, "create") from exc

        if not set_ok:
            raise AdapterQueryError(
                f"Key {key!r} already exists — use update() to overwrite",
                adapter=self._settings.adapter_name,
                operation="create",
            )
        return entity

    async def update(self, entity: T) -> T | None:
        """
        Update an existing entity.

        Serialises the entity to JSON and stores it using ``SET XX``
        (set-only-if-exists). Returns ``None`` if no key matched (entity
        was never created).

        Args:
            entity: The entity with updated fields. Must have an ``id`` field.

        Returns:
            The entity as passed in, or ``None`` if the key did not exist.

        Raises:
            AdapterConnectionError: Redis is unreachable.
            AdapterQueryError:      Redis command failed.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        client = await get_redis_client(self._settings)
        data = self._entity_to_dict(entity)
        entity_id = str(data.get("id", ""))
        key = self._make_key(entity_id)
        value = json.dumps(data)
        ttl = self._settings.redis_default_ttl or None

        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                set_ok: bool | None = await client.set(key, value, xx=True, ex=ttl)
        except (
            asyncio.TimeoutError,
            redis.exceptions.ConnectionError,
            redis.exceptions.AuthenticationError,
            redis.exceptions.TimeoutError,
            redis.exceptions.ResponseError,
            redis.exceptions.DataError,
            redis.exceptions.RedisError,
        ) as exc:
            raise self._wrap_redis(exc, "update") from exc

        if not set_ok:
            return None
        return entity

    async def delete(self, entity_id: str) -> bool:
        """
        Delete an entity by its ID.

        Args:
            entity_id: The entity's string identifier.

        Returns:
            ``True`` if the key was deleted, ``False`` if it did not exist.

        Raises:
            AdapterConnectionError: Redis is unreachable.
            AdapterQueryError:      Redis command failed.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        client = await get_redis_client(self._settings)
        key = self._make_key(entity_id)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                deleted: int = await client.delete(key)
        except (
            asyncio.TimeoutError,
            redis.exceptions.ConnectionError,
            redis.exceptions.AuthenticationError,
            redis.exceptions.TimeoutError,
            redis.exceptions.ResponseError,
            redis.exceptions.DataError,
            redis.exceptions.RedisError,
        ) as exc:
            raise self._wrap_redis(exc, "delete") from exc

        return deleted > 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """
        Close the Redis client and remove it from the cache.

        Call once at application shutdown. After this returns the client
        is closed; a subsequent operation will create a new client.
        """
        client = _client_cache.pop(self._settings.redis_url, None)
        if client is not None:
            await client.aclose()

    # ------------------------------------------------------------------
    # BasePort (Identity + Lifecycle) interface
    # ------------------------------------------------------------------

    async def initialize(self, context: PluginContext) -> None:
        """
        Establish the Redis client and verify connectivity.

        BasePort lifecycle entry point. Reuses the same cached client as
        every other method on this repository.

        Args:
            context: Plugin context. Unused — settings are provided at
                     construction time.

        Raises:
            AdapterConnectionError: Redis is unreachable.
        """
        await get_redis_client(self._settings)
        health = await self.health()
        if health.status != PluginStatus.READY:
            raise AdapterConnectionError(
                health.message or "Redis connectivity check failed during initialize()",
                adapter=self._settings.adapter_name,
                operation="initialize",
            )

    async def shutdown(self) -> None:
        """BasePort lifecycle entry point — alias for close(). Never raises."""
        await self.close()

    async def health(self) -> PluginHealth:
        """
        BasePort lifecycle entry point — returns a PluginHealth snapshot.

        The sole connectivity check on this repository — the Redis ``PING``
        command with a 5-second timeout. Never raises.
        """
        try:
            client = await get_redis_client(self._settings)
            await asyncio.wait_for(client.ping(), timeout=5.0)
            return PluginHealth(status=PluginStatus.READY, message="")
        except Exception as exc:  # noqa: BLE001
            return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
