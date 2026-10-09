"""
openframe/adapters/db/qdrant/repository.py
=============================================
Generic Qdrant vector store implementing ``BaseVectorStore[T]`` from
``openframe-core`` (``openframe-core>=3.5``) via structural subtyping.

The base class works with raw ``dict[str, Any]`` entities shaped like
``{"id": ..., "vector": [...], **payload}``. Domain adapters subclass it
and override ``_entity_to_point()`` / ``_point_to_entity()`` to map
between Qdrant's point shapes and typed domain objects — the same
override pattern ``PostgresRepository`` uses for
``_row_to_entity``/``_entity_to_row``.

Mapping ``BaseVectorStore`` onto Qdrant's actual API
-----------------------------------------------------
``create()``/``update()`` — Qdrant's native write primitive is
``upsert(collection_name, points=[PointStruct(...)])``, which is
unconditional: it inserts if the id is absent, overwrites if present.
Both methods funnel through the same private helper, ``_upsert_point()``
— there is exactly one upsert code path, not two near-duplicate ones, and
that sharing is why ``create`` and ``update`` read almost identically
below. The one behavioural difference the port's own contract requires:
``update()`` on a *missing* entity must return ``None`` (per
``RepositoryContractTests.test_update_returns_none_for_missing_entity``),
but Qdrant's upsert has no concept of "fail because it didn't already
exist" — it would happily create it. So ``update()`` does one extra
``retrieve()`` round-trip first to check existence before upserting;
``create()`` does not, since "create" has no not-found case to honour.
This existence check is the real, user-visible cost of satisfying
``BaseRepository``'s documented update semantics on top of a backend
whose native operation is an unconditional upsert — not an arbitrary
inefficiency.

``get(entity_id)`` — ``client.retrieve(collection_name, ids=[entity_id],
with_vectors=True)``, first result or ``None``. ``with_vectors=True`` is
required — the client defaults ``with_vectors=False``, which would
silently return entities with no vector, breaking round-tripping through
``create()`` → ``get()``.

``list(limit, offset)`` — **does not map cleanly, document honestly
rather than pretend it does (same caveat InfluxDB's package documents
for its own ``offset`` mismatch).** Qdrant's ``scroll()`` has no
"skip N rows" offset — its own ``offset`` parameter is a point-id cursor
("skip points with ids less than this id"), normally chained from the
``next_page_offset`` a previous ``scroll()`` call returned, not an
arbitrary integer position. ``scroll()`` always returns points sorted by
id ascending. To still honour ``BaseRepository.list(limit, offset)``'s
documented integer-offset contract, this method implements a
**compatibility shim**: it scrolls up to ``limit + offset`` points from
the start (id-ascending) and skips the first ``offset`` of them in
Python. This is correctness-preserving for the sizes any test or small
collection will hit, but is not performance-sensible for a large
``offset`` — there is no server-side "skip N" cursor, so every call
re-fetches and discards the skipped prefix. A caller who needs efficient
pagination over a large collection should page by Qdrant's own
id-cursor ``next_page_offset`` directly via the raw driver
(``repo.client`` / ``get_qdrant_client()``), not via this method's
``offset``. The total count comes from a separate ``client.count()``
call.

``delete(entity_id)`` — ``client.delete(collection_name,
points_selector=[entity_id])``. Qdrant's delete is idempotent — deleting
a missing id is not an error — so a ``retrieve()`` existence check runs
first to produce the ``True``/``False`` result this port's ``delete()``
must return.

``search(query_vector, k)`` — Qdrant 1.19's ``AsyncQdrantClient`` has no
``search()`` method; it was replaced by the "universal query" endpoint
``query_points(collection_name, query=query_vector, limit=k)``, whose
``QueryResponse.points`` is the ``list[ScoredPoint]`` ``search()`` used
to return directly. See ``connection.py``'s module docstring for how this
was verified against the actually-installed client rather than assumed.
Similarity metric: whatever metric the target collection was created
with (Qdrant stores the metric per-collection at creation time, not
per-query) — this adapter does not create collections or pick a metric
itself, so document the metric your own collection-creation code chose.
"""
from __future__ import annotations

import asyncio
from typing import Any, Generic, TypeVar

from qdrant_client import AsyncQdrantClient
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse
from qdrant_client.http.models import PointStruct, Record, ScoredPoint

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseVectorStore, Capability, PluginContext, PluginHealth, PluginStatus

from .config import QdrantSettings
from .connection import _cache_key, _client_cache, get_qdrant_client

__all__ = ["QdrantVectorStore"]

T = TypeVar("T")


class QdrantVectorStore(Generic[T]):
    """
    Generic Qdrant vector store.

    Implements ``BaseVectorStore[T]`` structurally — no inheritance from
    the Protocol. All ``qdrant_client`` exceptions are caught and
    re-raised as ``AdapterError`` subclasses. Every operation wraps its
    client call in ``asyncio.timeout(settings.operation_timeout)``.

    Health check: ``health()`` is the sole health check on this store. It
    verifies backend connectivity and returns a ``PluginHealth`` snapshot
    describing the result — never raises.

    Class attributes (override in subclass):
        _collection: Collection name used when no ``collection`` argument
                     is passed.

    Args:
        settings:   A ``QdrantSettings`` instance.
        collection: Collection name. Overrides the ``_collection`` class
                    attribute.

    Raises:
        AdapterConfigurationError: If neither ``collection`` param nor
                                   ``_collection`` class attribute is set.
    """

    _collection: str = ""

    name:       str = "openframe-qdrant-vector-store"
    version:    str = "0.1.0"
    capability: Capability = Capability.SEARCH

    def __init__(
        self,
        settings: QdrantSettings,
        collection: str | None = None,
    ) -> None:
        self._settings = settings
        self._collection = collection or self.__class__._collection

        if not self._collection:
            raise AdapterConfigurationError(
                "QdrantVectorStore requires a collection name. "
                "Pass collection= to __init__ or set _collection on the subclass.",
                adapter=settings.adapter_name,
                operation="init",
            )

    # ------------------------------------------------------------------
    # Entity <-> point mapping (override in typed subclasses)
    # ------------------------------------------------------------------

    def _entity_to_point(self, entity: T) -> PointStruct:
        """
        Convert the entity type ``T`` to a ``PointStruct`` for upsert.

        Base implementation expects a dict with ``"id"`` and ``"vector"``
        keys; every other key becomes payload. Subclasses override this
        to serialise typed domain objects correctly.
        """
        if isinstance(entity, dict):
            data = entity
        else:
            data = vars(entity)
        payload = {k: v for k, v in data.items() if k not in ("id", "vector")}
        return PointStruct(id=data["id"], vector=data["vector"], payload=payload)

    def _point_to_entity(self, point: Record | ScoredPoint) -> T:
        """
        Convert a Qdrant ``Record``/``ScoredPoint`` to the entity type ``T``.

        Base implementation returns a dict merging ``id``, ``vector``, and
        the point's payload. Subclasses override this to return typed
        domain objects.
        """
        result: dict[str, Any] = {"id": point.id, "vector": point.vector}
        result.update(point.payload or {})
        return result  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Exception mapping helper
    # ------------------------------------------------------------------

    def _wrap_qdrant(
        self, exc: Exception, operation: str
    ) -> AdapterQueryError | AdapterConnectionError | AdapterConfigurationError:
        """
        Map a ``qdrant_client`` exception to the appropriate ``AdapterError``
        subclass.

        Distinguishes a transport/connection-class failure
        (``ResponseHandlingException`` — wraps an underlying ``httpx``
        connection/timeout error, confirmed by pointing a client at an
        unreachable host; see ``connection.py``'s module docstring) from
        an in-band HTTP response the server actually sent back
        (``UnexpectedResponse``, which carries a real ``status_code``):
        a 5xx/unrecognised status is treated as connection-class
        (retryable — the server-side failure, not a client mistake); a
        4xx is treated as a query-class failure (not retryable — e.g. a
        query-vector dimensionality mismatch against the collection).
        Caller must ``raise ... from exc`` at the call site.
        """
        if isinstance(exc, ResponseHandlingException):
            return AdapterConnectionError(
                f"{operation} failed — connection to Qdrant was lost: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        if isinstance(exc, UnexpectedResponse):
            status = exc.status_code
            if status is not None and 400 <= status < 500:
                return AdapterQueryError(
                    f"{operation} failed on {self._collection} ({status}): {exc}",
                    adapter=self._settings.adapter_name,
                    operation=operation,
                    cause=exc,
                )
            return AdapterConnectionError(
                f"{operation} failed — Qdrant server error ({status}): {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        return AdapterQueryError(
            f"{operation} failed on {self._collection}: {exc}",
            adapter=self._settings.adapter_name,
            operation=operation,
            cause=exc,
        )

    # ------------------------------------------------------------------
    # Shared upsert helper — create() and update() both funnel through it.
    # ------------------------------------------------------------------

    async def _upsert_point(self, entity: T, operation: str) -> None:
        client = await get_qdrant_client(self._settings)
        point = self._entity_to_point(entity)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await client.upsert(collection_name=self._collection, points=[point])
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"{operation} exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            ) from exc
        except (ResponseHandlingException, UnexpectedResponse) as exc:
            raise self._wrap_qdrant(exc, operation) from exc

    # ------------------------------------------------------------------
    # BaseVectorStore[T] interface
    # ------------------------------------------------------------------

    async def get(self, entity_id: str) -> T | None:
        """
        Retrieve a single entity by id.

        Args:
            entity_id: The point id to look up.

        Returns:
            The entity if found, ``None`` otherwise.

        Raises:
            AdapterQueryError:   Query failed after connection was established.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        client = await get_qdrant_client(self._settings)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                records = await client.retrieve(
                    collection_name=self._collection,
                    ids=[entity_id],
                    with_vectors=True,
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"get exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="get",
                cause=exc,
            ) from exc
        except (ResponseHandlingException, UnexpectedResponse) as exc:
            raise self._wrap_qdrant(exc, "get") from exc

        if not records:
            return None
        return self._point_to_entity(records[0])

    async def list(self, limit: int, offset: int) -> tuple[list[T], int]:
        """
        Return a paginated slice of entities plus the total count.

        See this module's docstring for the honest offset caveat: Qdrant's
        ``scroll()`` offset is a point-id cursor, not an integer skip
        count, so this is implemented as a compatibility shim (scroll
        ``limit + offset`` points id-ascending from the start, then slice
        in Python) rather than a performant server-side skip.

        Args:
            limit:  Maximum number of entities to return.
            offset: Number of entities to skip from the beginning.

        Returns:
            A 2-tuple ``(entities, total_count)``.

        Raises:
            AdapterQueryError:   Query failed after connection was established.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        client = await get_qdrant_client(self._settings)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                records, _next = await client.scroll(
                    collection_name=self._collection,
                    limit=limit + offset,
                    with_vectors=True,
                )
                count_result = await client.count(collection_name=self._collection)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"list exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="list",
                cause=exc,
            ) from exc
        except (ResponseHandlingException, UnexpectedResponse) as exc:
            raise self._wrap_qdrant(exc, "list") from exc

        page = records[offset:]
        entities = [self._point_to_entity(r) for r in page]
        return entities, count_result.count

    async def create(self, entity: T) -> T:
        """
        Upsert a new entity and return it unchanged.

        Funnels through ``_upsert_point()`` (see this module's docstring
        for why ``create``/``update`` share one upsert path). Qdrant's
        write API returns no server-generated fields the way SQL's
        ``RETURNING *`` does, so there is nothing to merge back in — the
        entity is returned exactly as passed.

        Args:
            entity: The entity to create.

        Returns:
            The entity as passed in.

        Raises:
            AdapterQueryError:   Upsert failed (e.g. vector dimensionality
                                 mismatch against the collection).
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        await self._upsert_point(entity, "create")
        return entity

    async def update(self, entity: T) -> T | None:
        """
        Upsert changes to an existing entity.

        Checks existence via ``get()`` first — Qdrant's upsert alone
        cannot distinguish "update an existing entity" from "create a new
        one", but this port's contract requires ``update()`` on a missing
        entity to return ``None`` rather than silently creating it. See
        this module's docstring for why this extra round-trip exists.

        Args:
            entity: The entity with updated fields. Must have a valid id.

        Returns:
            The updated entity, or ``None`` if the entity did not exist.

        Raises:
            AdapterQueryError:   Upsert failed.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        if isinstance(entity, dict):
            entity_id = entity.get("id")
        else:
            entity_id = getattr(entity, "id", None)

        existing = await self.get(entity_id) if entity_id is not None else None
        if existing is None:
            return None

        await self._upsert_point(entity, "update")
        return entity

    async def delete(self, entity_id: str) -> bool:
        """
        Remove an entity by id.

        Checks existence via ``get()`` first — Qdrant's delete is
        idempotent (deleting a missing id is not an error), but this
        port's contract requires a ``True``/``False`` existed-and-removed
        result.

        Args:
            entity_id: The point id to delete.

        Returns:
            ``True`` if the entity was deleted, ``False`` if it did not exist.

        Raises:
            AdapterQueryError:   Deletion failed.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        existing = await self.get(entity_id)
        if existing is None:
            return False

        client = await get_qdrant_client(self._settings)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await client.delete(
                    collection_name=self._collection,
                    points_selector=[entity_id],
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"delete exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="delete",
                cause=exc,
            ) from exc
        except (ResponseHandlingException, UnexpectedResponse) as exc:
            raise self._wrap_qdrant(exc, "delete") from exc

        return True

    async def search(self, query_vector: list[float], k: int) -> list[T]:
        """
        Return the ``k`` entities whose vectors are most similar to
        ``query_vector``.

        Implemented against ``client.query_points()`` — the installed
        ``AsyncQdrantClient`` has no ``search()`` method. See this
        module's docstring for how this was verified.

        Args:
            query_vector: The embedding to search against.
            k: Maximum number of results to return.

        Returns:
            Up to ``k`` entities, most similar first. Empty list if
            nothing matches.

        Raises:
            AdapterQueryError:   Query failed (e.g. dimensionality mismatch).
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        client = await get_qdrant_client(self._settings)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                response = await client.query_points(
                    collection_name=self._collection,
                    query=query_vector,
                    limit=k,
                    with_vectors=True,
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"search exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="search",
                cause=exc,
            ) from exc
        except (ResponseHandlingException, UnexpectedResponse) as exc:
            raise self._wrap_qdrant(exc, "search") from exc

        return [self._point_to_entity(p) for p in response.points]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """
        Close the Qdrant client and remove it from the cache.

        Call once at application shutdown. After this returns, a
        subsequent operation will create a new client.
        """
        key = _cache_key(self._settings)
        client = _client_cache.get(key)
        if client is not None:
            await client.close()
            _client_cache.pop(key, None)

    # ------------------------------------------------------------------
    # BasePort (Identity + Lifecycle) interface
    # ------------------------------------------------------------------

    async def initialize(self, context: PluginContext) -> None:
        """
        Establish the Qdrant client and verify connectivity.

        BasePort lifecycle entry point. Reuses the same cached client as
        every other method on this store.

        Args:
            context: Plugin context. Unused — settings are provided at
                     construction time.

        Raises:
            AdapterConnectionError: Qdrant is unreachable.
        """
        await get_qdrant_client(self._settings)
        health = await self.health()
        if health.status != PluginStatus.READY:
            raise AdapterConnectionError(
                health.message or "Qdrant connectivity check failed during initialize()",
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

        The sole connectivity check on this store — a low-cost
        ``get_collections()`` call with a 5-second timeout. Never raises.
        """
        try:
            client = await get_qdrant_client(self._settings)
            await asyncio.wait_for(client.get_collections(), timeout=5.0)
            return PluginHealth(status=PluginStatus.READY, message="")
        except Exception as exc:  # noqa: BLE001
            return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
