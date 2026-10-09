"""
openframe/adapters/db/chromadb/repository.py
================================================
Generic ChromaDB vector-store repository implementing ``BaseVectorStore[T]``
from ``openframe-core`` via structural subtyping.

ChromaDB stores "rows" as four parallel lists keyed by position — ids,
embeddings, metadatas, documents — not as row/record objects the way
asyncpg/pymongo return them. Both ``collection.get(...)`` and
``collection.query(...)`` return a dict of parallel lists (``GetResult``/
``QueryResult``), and ``query()`` nests each list one level deeper (one
list-of-lists per query embedding, since Chroma's query API supports
batched multi-vector queries — this adapter always sends exactly one query
embedding, so it always unwraps index ``[0]``). ``_rows_to_entities()``/
``_entity_to_row()`` below transpose between that columnar shape and the
plain ``dict[str, Any]`` row shape (``id``/``vector``/``metadata``/
``document``) this base class's entity mapping hooks operate on — matching
the row↔entity override pattern ``PostgresRepository`` uses, just with a
transpose step in front of it because Chroma's wire shape is columnar
rather than row-oriented.

The base class works with raw ``dict[str, Any]`` rows shaped
``{"id": ..., "vector": [...], "metadata": {...}, "document": ...}``.
Domain adapters subclass it and override ``_row_to_entity()``/
``_entity_to_row()`` to map between that row shape and typed domain
objects, exactly like ``PostgresRepository``.

create() vs upsert() — ``create()`` uses ``collection.add()``, which fails
on a duplicate id (``IDAlreadyExistsError``/``DuplicateIDError`` from the
server) — the SQL-``INSERT``-like, non-idempotent behaviour that matches
``BaseRepository.create()``'s "persist a NEW entity" contract and surfaces
a duplicate id as the same kind of error a unique-constraint violation
would be in Postgres. ``update()`` uses ``collection.update()`` after a
``collection.get(ids=[id])`` existence check — Chroma's own ``update()``
has no documented "raise if missing" behaviour (unlike ``add()``'s
documented duplicate-id error), so this adapter establishes existence
itself and returns ``None`` for a missing id, matching
``BaseRepository.update()``'s contract. ``collection.upsert()`` is
intentionally unused by either method — it would make ``create()``
silently overwrite on a duplicate id, hiding the exact bug class
``BaseRepository.create()``'s contract exists to catch.

Usage — raw dict mode (no subclassing needed):

    repo = ChromaDBRepository(settings, collection="items")
    item: dict | None = await repo.get("abc-123")

Usage — typed domain mode (subclass):

    class ItemRepository(ChromaDBRepository[Item]):
        _collection = "items"

        def _row_to_entity(self, row: dict) -> Item:
            return Item(id=row["id"], vector=row["vector"], **row["metadata"])

        def _entity_to_row(self, entity: Item) -> dict:
            return {
                "id": entity.id,
                "vector": entity.vector,
                "metadata": {"name": entity.name},
            }

Structural conformance (no inheritance from Protocols required):

    assert isinstance(repo, BaseVectorStore)
    assert isinstance(repo, BaseRepository)
"""
from __future__ import annotations

import asyncio
from typing import Any, Generic, TypeVar

import chromadb.errors as chromadb_errors
import httpx

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import Capability, PluginContext, PluginHealth, PluginStatus

from .config import ChromaDBSettings
from .connection import get_chromadb_client

__all__ = ["ChromaDBRepository"]

T = TypeVar("T")

# Every exception chromadb.errors defines is a flat subclass of
# ChromaError (confirmed against the installed 1.5.9 — there is no
# connection-vs-query split within this hierarchy the way asyncpg splits
# PostgresError from InterfaceError). These are all semantic/query-class
# failures returned by the Chroma server itself (bad id, bad dimension,
# auth rejected on a request, etc.) — never a transport-level failure.
# Transport-level failures (host unreachable, connection dropped mid-call,
# timed out) surface as raw httpx exceptions instead, handled separately
# below. See connection.py's module docstring for the full research
# finding this split is based on.
_QUERY_ERRORS = (chromadb_errors.ChromaError,)


class ChromaDBRepository(Generic[T]):
    """
    Generic ChromaDB vector-store repository.

    Implements ``BaseVectorStore[T]`` structurally — no inheritance from
    the Protocol. All ``chromadb``/``httpx`` exceptions are caught and
    re-raised as ``AdapterError`` subclasses. Every operation wraps its
    call in ``asyncio.timeout(settings.operation_timeout)``.

    Similarity metric: whatever the Chroma collection was created with
    (default ``"l2"`` — squared L2 distance — unless the collection's
    ``hnsw:space`` metadata says otherwise at creation time). This adapter
    does not set a metric itself; it uses ``get_or_create_collection``,
    so an existing collection's configured metric is always respected.
    ``search()``'s results are ordered by Chroma's own ``distances``
    output — ascending distance, nearest first — regardless of which
    metric is active.

    Health check: ``health()`` is the sole health check on this
    repository. It verifies backend connectivity and returns a
    ``PluginHealth`` snapshot describing the result — never raises.

    Class attributes (override in subclass):
        _collection: Collection name used when no ``collection`` argument
                     is passed.

    Args:
        settings:   A ``ChromaDBSettings`` instance.
        collection: Collection name. Overrides the ``_collection`` class
                    attribute and ``settings.chroma_collection``.

    Raises:
        AdapterConfigurationError: If no collection name is available from
                                    any of: ``collection`` param, ``_collection``
                                    class attribute, ``settings.chroma_collection``.
    """

    _collection: str = ""

    name:       str = "openframe-chromadb-repository"
    version:    str = "0.1.0"
    capability: Capability = Capability.SEARCH

    def __init__(
        self,
        settings: ChromaDBSettings,
        collection: str | None = None,
    ) -> None:
        self._settings = settings
        self._collection_name = (
            collection or self.__class__._collection or settings.chroma_collection
        )

        if not self._collection_name:
            raise AdapterConfigurationError(
                "ChromaDBRepository requires a collection name. Pass "
                "collection= to __init__, set _collection on the subclass, "
                "or set CHROMA_COLLECTION.",
                adapter=settings.adapter_name,
                operation="init",
            )
        self._collection_cache: Any = None

    # ------------------------------------------------------------------
    # Row ↔ entity mapping (override in typed subclasses)
    # ------------------------------------------------------------------

    def _row_to_entity(self, row: dict[str, Any]) -> T:
        """
        Convert a plain row dict to the entity type ``T``.

        Base implementation returns the row unchanged. Subclasses override
        this to return typed domain objects from
        ``{"id": ..., "vector": [...], "metadata": {...}, "document": ...}``.
        """
        return row  # type: ignore[return-value]

    def _entity_to_row(self, entity: T) -> dict[str, Any]:
        """
        Convert the entity type ``T`` to a row dict shaped
        ``{"id": ..., "vector": [...], "metadata": {...}, "document": ...}``.

        Base implementation returns the entity unchanged if it is already
        shaped that way. Subclasses override this to serialise typed
        domain objects correctly.
        """
        if isinstance(entity, dict):
            return entity
        return vars(entity)

    # ------------------------------------------------------------------
    # Columnar (parallel-list) ↔ row transpose
    # ------------------------------------------------------------------
    # Chroma's wire format for get()/query() is NOT row-oriented — it is
    # a dict of parallel lists, one list per field, all the same length
    # (or, for query(), one list of lists — the outer index is "which
    # query embedding", which this adapter always collapses to [0] since
    # it only ever sends one). These helpers transpose that columnar
    # shape into the plain per-row dicts _row_to_entity() expects, the
    # single most unusual part of this adapter relative to a row-oriented
    # driver like asyncpg/pymongo.

    @staticmethod
    def _get_result_to_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
        """Transpose a ``collection.get()`` GetResult (columnar) into row dicts."""
        ids = result.get("ids") or []
        embeddings = result.get("embeddings")
        metadatas = result.get("metadatas")
        documents = result.get("documents")
        rows = []
        for i, entity_id in enumerate(ids):
            rows.append(
                {
                    "id": entity_id,
                    "vector": list(embeddings[i]) if embeddings is not None else None,
                    "metadata": dict(metadatas[i]) if metadatas is not None and metadatas[i] is not None else {},
                    "document": documents[i] if documents is not None else None,
                }
            )
        return rows

    @staticmethod
    def _query_result_to_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Transpose a ``collection.query()`` QueryResult (doubly-nested
        columnar — outer index is "which query embedding") into row dicts
        for the single query embedding this adapter always sends (index 0).
        """
        ids = (result.get("ids") or [[]])[0]
        embeddings_outer = result.get("embeddings")
        metadatas_outer = result.get("metadatas")
        documents_outer = result.get("documents")
        embeddings = embeddings_outer[0] if embeddings_outer is not None else None
        metadatas = metadatas_outer[0] if metadatas_outer is not None else None
        documents = documents_outer[0] if documents_outer is not None else None
        rows = []
        for i, entity_id in enumerate(ids):
            rows.append(
                {
                    "id": entity_id,
                    "vector": list(embeddings[i]) if embeddings is not None else None,
                    "metadata": dict(metadatas[i]) if metadatas is not None and metadatas[i] is not None else {},
                    "document": documents[i] if documents is not None else None,
                }
            )
        return rows

    # ------------------------------------------------------------------
    # Exception mapping helper
    # ------------------------------------------------------------------

    def _wrap_chromadb(
        self, exc: Exception, operation: str
    ) -> AdapterQueryError | AdapterConnectionError | AdapterTimeoutError:
        """
        Map a ``chromadb``/``httpx`` exception to the appropriate
        ``AdapterError`` subclass.

        ``httpx.TimeoutException`` must be checked before the broader
        ``httpx.TransportError`` — it is itself a ``TransportError``
        subclass in the installed version, so checking order matters (see
        ``connection.py``'s module docstring for the same finding applied
        to client creation). ``chromadb.errors.ChromaError`` and its
        subclasses are always query-class (semantic failures the Chroma
        server itself returned), never connection-class — there is no
        asyncpg-style split within that hierarchy. Caller must
        ``raise ... from exc`` at the call site.
        """
        if isinstance(exc, httpx.TimeoutException):
            return AdapterTimeoutError(
                f"{operation} timed out: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        if isinstance(exc, httpx.TransportError):
            return AdapterConnectionError(
                f"{operation} failed — connection to Chroma was lost: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        return AdapterQueryError(
            f"{operation} failed on collection {self._collection_name}: {exc}",
            adapter=self._settings.adapter_name,
            operation=operation,
            cause=exc,
        )

    # ------------------------------------------------------------------
    # Collection handle
    # ------------------------------------------------------------------

    async def _get_collection(self) -> Any:
        """Return the cached ``AsyncCollection`` handle, creating it on first use."""
        if self._collection_cache is not None:
            return self._collection_cache
        client = await get_chromadb_client(self._settings)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                collection = await client.get_or_create_collection(name=self._collection_name)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"get_or_create_collection exceeded {self._settings.operation_timeout}s "
                "operation_timeout",
                adapter=self._settings.adapter_name,
                operation="get_or_create_collection",
                cause=exc,
            ) from exc
        except httpx.TimeoutException as exc:
            raise self._wrap_chromadb(exc, "get_or_create_collection") from exc
        except httpx.TransportError as exc:
            raise self._wrap_chromadb(exc, "get_or_create_collection") from exc
        except _QUERY_ERRORS as exc:
            raise self._wrap_chromadb(exc, "get_or_create_collection") from exc
        self._collection_cache = collection
        return collection

    # ------------------------------------------------------------------
    # BaseVectorStore[T] / BaseRepository[T] interface
    # ------------------------------------------------------------------

    async def get(self, entity_id: str) -> T | None:
        """
        Retrieve a single entity by id.

        Args:
            entity_id: The id to look up.

        Returns:
            The entity if found, ``None`` otherwise.

        Raises:
            AdapterConnectionError: Backend unreachable mid-call.
            AdapterQueryError:      Query failed for any other reason.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        collection = await self._get_collection()
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                result = await collection.get(
                    ids=[entity_id], include=["embeddings", "metadatas", "documents"]
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"get exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="get",
                cause=exc,
            ) from exc
        except (httpx.TimeoutException, httpx.TransportError, *_QUERY_ERRORS) as exc:
            raise self._wrap_chromadb(exc, "get") from exc

        rows = self._get_result_to_rows(result)
        if not rows:
            return None
        return self._row_to_entity(rows[0])

    async def list(self, limit: int, offset: int) -> tuple[list[T], int]:
        """
        Return a paginated slice of entities plus the total count.

        Args:
            limit:  Maximum number of entities to return.
            offset: Number of entities to skip.

        Returns:
            A 2-tuple ``(entities, total_count)``.

        Raises:
            AdapterConnectionError: Backend unreachable mid-call.
            AdapterQueryError:      Query failed.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        collection = await self._get_collection()
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                result = await collection.get(
                    limit=limit,
                    offset=offset,
                    include=["embeddings", "metadatas", "documents"],
                )
                total = await collection.count()
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"list exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="list",
                cause=exc,
            ) from exc
        except (httpx.TimeoutException, httpx.TransportError, *_QUERY_ERRORS) as exc:
            raise self._wrap_chromadb(exc, "list") from exc

        rows = self._get_result_to_rows(result)
        entities = [self._row_to_entity(r) for r in rows]
        return entities, total

    async def create(self, entity: T) -> T:
        """
        Add a new entity. Fails if the id already exists.

        Uses ``collection.add()`` (not ``upsert()``) — see this module's
        docstring for why ``create()`` must be non-idempotent to honour
        ``BaseRepository.create()``'s "persist a NEW entity" contract.

        Args:
            entity: The entity to create.

        Returns:
            The entity as passed in (Chroma's ``add()`` has no server-
            generated fields to merge back).

        Raises:
            AdapterQueryError:      Insert failed (e.g. duplicate id).
            AdapterConnectionError: Backend unreachable mid-call.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        collection = await self._get_collection()
        row = self._entity_to_row(entity)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await collection.add(
                    ids=[row["id"]],
                    embeddings=[row["vector"]],
                    metadatas=[row.get("metadata") or None],
                    documents=[row.get("document")] if row.get("document") is not None else None,
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"create exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="create",
                cause=exc,
            ) from exc
        except (httpx.TimeoutException, httpx.TransportError, *_QUERY_ERRORS) as exc:
            raise self._wrap_chromadb(exc, "create") from exc
        except ValueError as exc:
            # Chroma's client-side validation (e.g. duplicate id caught
            # before the request is even sent) raises plain ValueError —
            # must still surface as AdapterQueryError, not propagate raw.
            raise AdapterQueryError(
                f"create failed on collection {self._collection_name}: {exc}",
                adapter=self._settings.adapter_name,
                operation="create",
                cause=exc,
            ) from exc

        return entity

    async def update(self, entity: T) -> T | None:
        """
        Update an existing entity's vector/metadata/document.

        Checks existence via ``collection.get(ids=[id])`` first (Chroma's
        own ``update()`` has no documented raise-if-missing behaviour), so
        this adapter establishes "does it exist" itself and returns
        ``None`` for a missing id, matching ``BaseRepository.update()``'s
        contract.

        Args:
            entity: The entity with updated fields. Must have a valid id.

        Returns:
            The updated entity as passed in, or ``None`` if it did not exist.

        Raises:
            AdapterQueryError:      Update failed.
            AdapterConnectionError: Backend unreachable mid-call.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        collection = await self._get_collection()
        row = self._entity_to_row(entity)
        entity_id = row["id"]
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                existing = await collection.get(ids=[entity_id])
                if not existing.get("ids"):
                    return None
                await collection.update(
                    ids=[entity_id],
                    embeddings=[row["vector"]] if row.get("vector") is not None else None,
                    metadatas=[row.get("metadata") or None],
                    documents=[row.get("document")] if row.get("document") is not None else None,
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"update exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="update",
                cause=exc,
            ) from exc
        except (httpx.TimeoutException, httpx.TransportError, *_QUERY_ERRORS) as exc:
            raise self._wrap_chromadb(exc, "update") from exc

        return entity

    async def delete(self, entity_id: str) -> bool:
        """
        Delete an entity by id.

        Args:
            entity_id: The id to delete.

        Returns:
            ``True`` if the entity existed and was deleted, ``False`` otherwise.

        Raises:
            AdapterQueryError:      Deletion failed.
            AdapterConnectionError: Backend unreachable mid-call.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        collection = await self._get_collection()
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                existing = await collection.get(ids=[entity_id])
                if not existing.get("ids"):
                    return False
                await collection.delete(ids=[entity_id])
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"delete exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="delete",
                cause=exc,
            ) from exc
        except (httpx.TimeoutException, httpx.TransportError, *_QUERY_ERRORS) as exc:
            raise self._wrap_chromadb(exc, "delete") from exc

        return True

    async def search(self, query_vector: list[float], k: int) -> list[T]:
        """
        Return the ``k`` entities nearest to ``query_vector``.

        Uses ``collection.query(query_embeddings=[query_vector], n_results=k)``
        — always exactly one query embedding, so the doubly-nested
        QueryResult is always unwrapped at index ``[0]``
        (``_query_result_to_rows()``). Results are ordered by Chroma's own
        ``distances`` output (ascending — nearest first), under whichever
        metric the collection was created with (see this class's own
        docstring).

        Args:
            query_vector: The embedding to search against.
            k: Maximum number of results to return.

        Returns:
            Up to ``k`` entities, nearest first. Empty list if the
            collection has no entities — never raises, never returns
            ``None``.

        Raises:
            AdapterConnectionError: Backend unreachable mid-call.
            AdapterQueryError:      Query failed (e.g. dimension mismatch).
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        collection = await self._get_collection()
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                result = await collection.query(
                    query_embeddings=[query_vector],
                    n_results=k,
                    include=["embeddings", "metadatas", "documents", "distances"],
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"search exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="search",
                cause=exc,
            ) from exc
        except (httpx.TimeoutException, httpx.TransportError, *_QUERY_ERRORS) as exc:
            raise self._wrap_chromadb(exc, "search") from exc

        rows = self._query_result_to_rows(result)
        return [self._row_to_entity(r) for r in rows]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """
        Drop the cached collection handle reference for this repository.

        The underlying ``AsyncClientAPI`` is shared via ``connection.py``'s
        module-level cache and is not torn down here — matching
        ``PostgresRepository.close()``'s "pool stays cached, repository
        just drops its own handle" shape is not applicable here since
        Chroma's client has no explicit close(); this is a no-op beyond
        clearing the local cache so a subsequent operation re-resolves the
        collection.
        """
        self._collection_cache = None

    # ------------------------------------------------------------------
    # BasePort (Identity + Lifecycle) interface
    # ------------------------------------------------------------------

    async def initialize(self, context: PluginContext) -> None:
        """
        Establish the client/collection and verify connectivity.

        BasePort lifecycle entry point.

        Args:
            context: Plugin context. Unused — settings are provided at
                     construction time.

        Raises:
            AdapterConnectionError: Chroma is unreachable.
        """
        await self._get_collection()
        health = await self.health()
        if health.status != PluginStatus.READY:
            raise AdapterConnectionError(
                health.message or "Chroma connectivity check failed during initialize()",
                adapter=self._settings.adapter_name,
                operation="initialize",
            )

    async def shutdown(self) -> None:
        """BasePort lifecycle entry point — alias for close(). Never raises."""
        await self.close()

    async def health(self) -> PluginHealth:
        """
        BasePort lifecycle entry point — returns a PluginHealth snapshot.

        The sole connectivity check on this repository — a low-cost
        ``collection.count()`` call. Never raises.
        """
        try:
            collection = await self._get_collection()
            await asyncio.wait_for(collection.count(), timeout=5.0)
            return PluginHealth(status=PluginStatus.READY, message="")
        except Exception as exc:  # noqa: BLE001
            return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
