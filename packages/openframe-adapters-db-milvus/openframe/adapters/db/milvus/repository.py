"""
openframe/adapters/db/milvus/repository.py
=============================================
Generic Milvus vector-store repository implementing ``BaseVectorStore[T]``
from ``openframe-core`` via structural subtyping.

Uses ``AsyncMilvusClient``'s simplified API (not the older ``Collection``
ORM API) — see ``connection.py``'s module docstring for why this adapter
uses the native async client rather than wrapping the sync one in an
executor.

API mapping — simplified-client calls used by each port method:

    get(id)              -> client.get(collection_name, ids=[id])
    list(limit, offset)  -> client.query(collection_name, filter="",
                                          limit=limit, offset=offset)
    create(entity)        -> client.upsert(collection_name, data=[row])
    update(entity)        -> client.upsert(collection_name, data=[row])
                              (see _upsert_row — create/update share the
                              same call; Milvus upsert is idempotent by
                              primary key, there is no separate insert-only
                              path exposed that also returns the previous
                              row, so "did this update an existing row"
                              is answered with a pre-check get() in
                              update() specifically, to satisfy the port's
                              "None if the entity did not exist" contract)
    delete(id)            -> client.delete(collection_name, ids=[id])
    search(vector, k)     -> client.search(collection_name, data=[vector],
                                            limit=k)

``query()``'s ``limit``/``offset`` are not named parameters on
``AsyncMilvusClient.query`` itself (verified via
``inspect.signature``/``inspect.getsource`` against the installed 3.0.2
client — the method's own signature is
``query(collection_name, filter, output_fields, timeout, ids,
partition_names, **kwargs)``); they flow through ``**kwargs`` down to the
gRPC query request as real integer query parameters, which is Milvus's
documented pagination mechanism for ``query()`` — unlike some vector DBs
which only support a cursor/token, Milvus's query-by-filter path takes a
genuine integer offset. This adapter passes them as kwargs accordingly.

Row <-> entity mapping is overridable via ``_entity_to_row``/``_row_to_entity``,
matching ``PostgresRepository``'s pattern. The base implementation works
with ``dict`` rows shaped like
``{"id": ..., "vector": [...], **metadata}``.

Usage — raw dict mode::

    repo = MilvusRepository(settings, collection_name="items")
    item = await repo.get("abc-123")           # dict | None

Usage — typed domain mode::

    class ItemRepository(MilvusRepository[Item]):
        _collection_name = "items"

        def _row_to_entity(self, row: dict) -> Item:
            return Item(**row)

        def _entity_to_row(self, entity: Item) -> dict:
            return entity.model_dump()

Structural conformance (no inheritance from Protocols required)::

    assert isinstance(repo, BaseVectorStore)
"""
from __future__ import annotations

import asyncio
from typing import Any, Generic, TypeVar

from pymilvus.exceptions import MilvusException

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import Capability, PluginContext, PluginHealth, PluginStatus

from .config import MilvusSettings
from .connection import (
    _cache_key,
    _client_cache,
    _looks_like_connection_failure,
    get_milvus_client,
)

__all__ = ["MilvusRepository"]

T = TypeVar("T")

# Query/config-class pymilvus exceptions — bad params, missing collection,
# schema issues. Classified as AdapterQueryError (non-retryable), matching
# the connection-vs-query distinction every adapter in this checklist makes.
# Verified against the installed pymilvus 3.0.2 exception hierarchy — see
# connection.py's module docstring for why ConnectError/MilvusUnavailableException
# (the names the task brief/pymilvus docstrings suggest) are checked
# defensively rather than relied on; they are never actually raised by
# AsyncMilvusClient in this version.
_QUERY_CLASS_EXCEPTION_NAMES = (
    "ParamError",
    "SchemaNotReadyException",
    "CollectionNotExistException",
    "DataNotMatchException",
    "DataTypeNotMatchException",
    "FieldTypeException",
    "FieldsTypeException",
    "PrimaryKeyException",
    "AutoIDException",
    "DescribeCollectionException",
    "AmbiguousIndexName",
)

# Connection-class names, checked defensively — see module docstring.
_CONNECTION_CLASS_EXCEPTION_NAMES = (
    "ConnectError",
    "ConnectionNotExistException",
    "MilvusUnavailableException",
)


class MilvusRepository(Generic[T]):
    """
    Generic Milvus vector-store repository.

    Implements ``BaseVectorStore[T]`` structurally — no inheritance from
    the Protocol. All ``pymilvus`` exceptions are caught and re-raised as
    ``AdapterError`` subclasses. Every operation wraps its
    ``AsyncMilvusClient`` call in ``asyncio.timeout(settings.operation_timeout)``.

    Health check: ``health()`` is the sole health check on this repository.
    It verifies backend connectivity and returns a ``PluginHealth``
    snapshot describing the result — never raises.

    Class attributes (override in subclass):
        _collection_name: Collection name used when no ``collection_name``
                          argument is passed.
        _id_field:        Primary-key field name. Default ``"id"``.
        _vector_field:    Vector field name. Default ``"vector"``.

    Args:
        settings:        A ``MilvusSettings`` instance.
        collection_name: Collection name. Overrides the
                         ``_collection_name`` class attribute.
        id_field:        PK field name. Overrides ``_id_field``.
        vector_field:    Vector field name. Overrides ``_vector_field``.

    Raises:
        AdapterConfigurationError: If neither ``collection_name`` param
                                   nor ``_collection_name`` class attribute
                                   is set.
    """

    _collection_name: str = ""
    _id_field: str = "id"
    _vector_field: str = "vector"

    name:       str = "openframe-milvus-repository"
    version:    str = "0.1.0"
    capability: Capability = Capability.SEARCH

    def __init__(
        self,
        settings: MilvusSettings,
        collection_name: str | None = None,
        id_field: str | None = None,
        vector_field: str | None = None,
    ) -> None:
        self._settings = settings
        self._collection_name = collection_name or self.__class__._collection_name
        self._id_field = id_field or self.__class__._id_field
        self._vector_field = vector_field or self.__class__._vector_field

        if not self._collection_name:
            raise AdapterConfigurationError(
                "MilvusRepository requires a collection name. "
                "Pass collection_name= to __init__ or set _collection_name "
                "on the subclass.",
                adapter=settings.adapter_name,
                operation="init",
            )

    # ------------------------------------------------------------------
    # Row <-> entity mapping (override in typed subclasses)
    # ------------------------------------------------------------------

    def _row_to_entity(self, row: dict[str, Any]) -> T:
        """
        Convert a Milvus result row (``dict``) to the entity type ``T``.

        Base implementation returns the row unchanged. Subclasses override
        this to return typed domain objects.
        """
        return row  # type: ignore[return-value]

    def _entity_to_row(self, entity: T) -> dict[str, Any]:
        """
        Convert the entity type ``T`` to a Milvus row dict
        (``{"id": ..., "vector": [...], **metadata}``).

        Base implementation returns the entity unchanged if it is already
        a dict, or falls back to ``vars(entity)`` for simple objects.
        Subclasses override this to serialise typed domain objects
        correctly.
        """
        if isinstance(entity, dict):
            return entity
        return vars(entity)

    # ------------------------------------------------------------------
    # Exception mapping helper
    # ------------------------------------------------------------------

    def _wrap_milvus(
        self, exc: MilvusException, operation: str
    ) -> AdapterQueryError | AdapterConnectionError:
        """
        Map a ``pymilvus.MilvusException`` (or subclass) to the
        appropriate ``AdapterError`` subclass.

        Classification order (see module + connection.py docstrings for
        why, verified against the installed driver rather than assumed):

        1. A known query/config-class subclass name -> AdapterQueryError.
        2. A known connection-class subclass name (checked defensively;
           never actually observed raised by AsyncMilvusClient in 3.0.2)
           -> AdapterConnectionError.
        3. Otherwise, inspect ``exc.message``/``str(exc)`` for
           connection-failure phrasing (the actual real-world shape of a
           mid-call connection loss in this driver version: a bare
           ``MilvusException`` with ``code=2`` and a "Fail connecting to
           server" message) -> AdapterConnectionError.
        4. Anything else -> AdapterQueryError (non-retryable default).

        Caller must ``raise ... from exc`` at the call site.
        """
        exc_name = type(exc).__name__
        if exc_name in _QUERY_CLASS_EXCEPTION_NAMES:
            return AdapterQueryError(
                f"{operation} failed on {self._collection_name}: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        if exc_name in _CONNECTION_CLASS_EXCEPTION_NAMES:
            return AdapterConnectionError(
                f"{operation} failed — connection to Milvus was lost: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        if _looks_like_connection_failure(exc):
            return AdapterConnectionError(
                f"{operation} failed — connection to Milvus was lost: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        return AdapterQueryError(
            f"{operation} failed on {self._collection_name}: {exc}",
            adapter=self._settings.adapter_name,
            operation=operation,
            cause=exc,
        )

    def _upsert_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Ensure the row carries the configured id/vector field names unchanged."""
        return row

    # ------------------------------------------------------------------
    # BaseRepository[T] interface
    # ------------------------------------------------------------------

    async def get(self, entity_id: str) -> T | None:
        """
        Retrieve a single entity by primary key.

        Args:
            entity_id: Value of the ``_id_field`` to look up.

        Returns:
            The entity if a matching row exists, ``None`` otherwise.

        Raises:
            AdapterQueryError:   Query failed after connection was established.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        client = await get_milvus_client(self._settings)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                rows = await client.get(self._collection_name, ids=[entity_id])
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"get exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="get",
                cause=exc,
            ) from exc
        except MilvusException as exc:
            raise self._wrap_milvus(exc, "get") from exc

        if not rows:
            return None
        return self._row_to_entity(rows[0])

    async def list(self, limit: int, offset: int) -> tuple[list[T], int]:
        """
        Return a paginated slice of entities and the total entity count.

        Pagination is a real integer ``limit``/``offset`` passed through
        to ``client.query()`` — see module docstring for why this works
        against ``AsyncMilvusClient`` despite not being a named parameter
        on its own signature.

        Args:
            limit:  Maximum number of entities to return.
            offset: Number of entities to skip.

        Returns:
            A 2-tuple ``(entities, total_count)`` where ``total_count`` is
            the number of all entities in the collection (not just the
            slice).

        Raises:
            AdapterQueryError:   Query failed after connection was established.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        client = await get_milvus_client(self._settings)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                rows = await client.query(
                    self._collection_name,
                    filter="",
                    limit=limit,
                    offset=offset,
                )
                all_rows = await client.query(self._collection_name, filter="")
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"list exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="list",
                cause=exc,
            ) from exc
        except MilvusException as exc:
            raise self._wrap_milvus(exc, "list") from exc

        entities = [self._row_to_entity(r) for r in rows]
        return entities, len(all_rows)

    async def create(self, entity: T) -> T:
        """
        Upsert a new entity and return it as stored.

        Milvus's simplified client has no insert-only path distinct from
        upsert that also enforces non-existence, so ``create()`` upserts
        by primary key (idempotent) — matching this port's documented
        "most vector DBs treat insert as upsert-by-id natively" contract.

        Args:
            entity: The entity to create.

        Returns:
            The entity as stored.

        Raises:
            AdapterQueryError:   Upsert failed.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        client = await get_milvus_client(self._settings)
        row = self._entity_to_row(entity)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await client.upsert(self._collection_name, data=[row])
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"create exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="create",
                cause=exc,
            ) from exc
        except MilvusException as exc:
            raise self._wrap_milvus(exc, "create") from exc

        return entity

    async def update(self, entity: T) -> T | None:
        """
        Upsert changes to an existing entity.

        Milvus upsert does not report whether a row previously existed, so
        this method performs a ``get()`` first to honour this port's
        "``None`` if the entity did not exist" contract — the same
        existence-check-then-upsert pattern an id-addressable-but-upsert-
        only backend requires.

        Args:
            entity: The entity with updated fields. Must contain the
                    ``_id_field`` value.

        Returns:
            The updated entity as stored, or ``None`` if no entity with
            that id existed.

        Raises:
            AdapterQueryError:   Upsert failed.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        row = self._entity_to_row(entity)
        entity_id = row.get(self._id_field)
        existing = await self.get(entity_id)
        if existing is None:
            return None

        client = await get_milvus_client(self._settings)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await client.upsert(self._collection_name, data=[row])
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"update exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="update",
                cause=exc,
            ) from exc
        except MilvusException as exc:
            raise self._wrap_milvus(exc, "update") from exc

        return entity

    async def delete(self, entity_id: str) -> bool:
        """
        Delete an entity by primary key.

        Performs a ``get()`` first, since ``client.delete()`` does not
        itself report whether a matching row existed.

        Args:
            entity_id: Value of the ``_id_field`` to delete.

        Returns:
            ``True`` if an entity was deleted, ``False`` if none matched.

        Raises:
            AdapterQueryError:   Deletion failed.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        existing = await self.get(entity_id)
        if existing is None:
            return False

        client = await get_milvus_client(self._settings)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await client.delete(self._collection_name, ids=[entity_id])
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"delete exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="delete",
                cause=exc,
            ) from exc
        except MilvusException as exc:
            raise self._wrap_milvus(exc, "delete") from exc

        return True

    # ------------------------------------------------------------------
    # BaseVectorStore[T] interface
    # ------------------------------------------------------------------

    async def search(self, query_vector: list[float], k: int) -> list[T]:
        """
        Return the ``k`` entities whose vectors are most similar to
        ``query_vector``, using the collection's configured metric
        (``settings.metric_type`` — ``COSINE`` by default).

        Args:
            query_vector: The embedding to search against.
            k: Maximum number of results to return.

        Returns:
            Up to ``k`` entities, ordered from most to least similar.
            Empty list if nothing matches.

        Raises:
            AdapterQueryError:   Search failed (e.g. dimension mismatch).
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        client = await get_milvus_client(self._settings)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                results = await client.search(
                    self._collection_name,
                    data=[query_vector],
                    limit=k,
                    output_fields=["*"],
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"search exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="search",
                cause=exc,
            ) from exc
        except MilvusException as exc:
            raise self._wrap_milvus(exc, "search") from exc

        if not results:
            return []
        # search() returns one hit-list per query vector; we sent exactly one.
        hits = results[0]
        rows = [hit.get("entity", hit) for hit in hits]
        return [self._row_to_entity(r) for r in rows]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """
        Close the Milvus client connection and remove it from the cache.

        Call once at application shutdown. After this returns the client
        is closed and a subsequent operation will create a new one.
        """
        key = _cache_key(self._settings)
        client = _client_cache.get(key)
        if client is not None:
            close = getattr(client, "close", None)
            if close is not None:
                result = close()
                if asyncio.iscoroutine(result):
                    await result
            _client_cache.pop(key, None)

    # ------------------------------------------------------------------
    # BasePort (Identity + Lifecycle) interface
    # ------------------------------------------------------------------

    async def initialize(self, context: PluginContext) -> None:
        """
        Establish the Milvus client and verify connectivity.

        BasePort lifecycle entry point. Reuses the same cached client as
        every other method on this repository.

        Args:
            context: Plugin context. Unused — settings are provided at
                     construction time.

        Raises:
            AdapterConnectionError: Milvus is unreachable.
        """
        await get_milvus_client(self._settings)
        health = await self.health()
        if health.status != PluginStatus.READY:
            raise AdapterConnectionError(
                health.message or "Milvus connectivity check failed during initialize()",
                adapter=self._settings.adapter_name,
                operation="initialize",
            )

    async def shutdown(self) -> None:
        """BasePort lifecycle entry point — alias for close(). Never raises."""
        try:
            await self.close()
        except Exception:  # noqa: BLE001
            pass

    async def health(self) -> PluginHealth:
        """
        BasePort lifecycle entry point — returns a PluginHealth snapshot.

        The sole connectivity check on this repository — a low-cost
        ``list_collections()`` call with a 5-second timeout. Never raises.
        """
        try:
            client = await get_milvus_client(self._settings)
            await asyncio.wait_for(client.list_collections(), timeout=5.0)
            return PluginHealth(status=PluginStatus.READY, message="")
        except Exception as exc:  # noqa: BLE001
            return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
