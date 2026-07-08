"""
openframe/adapters/db/mongo/repository.py
==========================================
Generic MongoDB document repository implementing ``BaseRepository[T]``
from ``openframe-core`` via structural subtyping.

The base class works with raw ``dict[str, Any]`` documents. Domain adapters
subclass it and override ``_doc_to_entity()`` / ``_entity_to_doc()`` to map
between documents and typed domain objects.

``_id`` handling:
    MongoDB uses ``_id`` as the document identifier; ``BaseRepository`` uses
    ``entity_id: str``. The adapter bridges this by:

    - Accepting any string ``entity_id`` and trying to parse it as an
      ``ObjectId``. If parsing succeeds the ObjectId is used for the filter
      (enabling fast indexed lookups); otherwise the raw string is used.
    - Always serialising ``_id`` to ``str`` before it leaves the adapter
      boundary. Raw ``ObjectId`` values are never returned to callers.
    - Adding a convenience ``id`` key mirroring ``_id`` in every returned
      document so callers can use either key.

Error handling:
    Every pymongo exception is caught and re-raised as the appropriate
    ``AdapterError`` subclass. Motor exceptions are subclasses of pymongo
    exceptions — always import from ``pymongo.errors``.

Timeout strategy (defence in depth):
    1. ``asyncio.timeout(settings.operation_timeout)`` on every await —
       cancels if the Python event loop is blocked.
    2. ``max_time_ms=int(settings.operation_timeout * 1000)`` passed to
       motor query methods — instructs the MongoDB server to abort the query.
"""
from __future__ import annotations

import asyncio
from typing import Any, Generic, TypeVar

import pymongo.errors
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection

from openframe.core.ports import Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository

from .config import MongoSettings
from .connection import _client_cache, get_mongo_client

__all__ = ["MongoRepository"]

T = TypeVar("T")

# pymongo exceptions that indicate a broken connection rather than a query error
_CONNECTION_ERRORS = (
    pymongo.errors.ServerSelectionTimeoutError,
    pymongo.errors.ConnectionFailure,
)

# pymongo exceptions that indicate a failed document operation
_QUERY_ERRORS = (
    pymongo.errors.DuplicateKeyError,
    pymongo.errors.WriteError,
    pymongo.errors.OperationFailure,
    pymongo.errors.PyMongoError,
)


class MongoRepository(Generic[T]):
    """
    Generic MongoDB document repository.

    Implements ``BaseRepository[T]`` structurally — no
    inheritance from the Protocol. All pymongo/motor exceptions are caught
    and re-raised as ``AdapterError`` subclasses. Every operation wraps its
    motor call in ``asyncio.timeout(settings.operation_timeout)`` and also
    passes ``max_time_ms`` to the motor method for server-side enforcement.

    Health check: ``health()`` is the canonical — and only — health check.
    It returns a ``PluginHealth`` snapshot and never raises. ``ping()`` and
    ``is_ready()`` were removed in v2.0; use ``health()``.

    Class attributes (override in subclass):
        _collection: Collection name used when no ``collection`` arg is passed.

    Args:
        settings:   A ``MongoSettings`` instance.
        collection: MongoDB collection name. Overrides ``_collection`` attribute.

    Raises:
        AdapterConfigurationError: If neither ``collection`` param nor
                                   ``_collection`` class attribute is set.
    """

    _collection: str = ""

    name:       str = "openframe-mongo-repository"
    version:    str = "1.3.0"
    capability: Capability = Capability.PERSISTENCE

    def __init__(
        self,
        settings: MongoSettings,
        collection: str | None = None,
    ) -> None:
        self._settings = settings
        self._coll_name = collection or self.__class__._collection

        if not self._coll_name:
            raise AdapterConfigurationError(
                "MongoRepository requires a collection name. "
                "Pass collection= to __init__ or set _collection on the subclass.",
                adapter=settings.adapter_name,
                operation="init",
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_collection(self) -> AsyncIOMotorCollection:  # type: ignore[type-arg]
        """Return the motor collection object for this repository."""
        client = get_mongo_client(self._settings)
        db = client[self._settings.mongo_database]
        return db[self._coll_name]

    def _normalise_id(self, entity_id: str) -> ObjectId | str:
        """
        Convert a string entity_id to ``ObjectId`` when it is a valid 24-char
        hex string, otherwise return it as a plain string.

        ObjectId lookup is index-optimised on Atlas and most deployments.
        Plain-string ``_id`` values are returned unchanged.
        """
        try:
            return ObjectId(entity_id)
        except Exception:  # bson.errors.InvalidId or any other parse failure
            return entity_id

    def _serialise_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        """
        Normalise a raw MongoDB document for the adapter boundary.

        - Converts ``_id`` (``ObjectId`` or ``str``) to ``str``.
        - Adds a convenience ``id`` key mirroring ``_id`` so callers can use
          either key without knowing MongoDB internals.
        """
        result = dict(doc)
        if "_id" in result:
            result["_id"] = str(result["_id"])
            if "id" not in result:
                result["id"] = result["_id"]
        return result

    def _max_time_ms(self) -> int:
        """Return operation_timeout as integer milliseconds for motor methods."""
        return int(self._settings.operation_timeout * 1000)

    # ------------------------------------------------------------------
    # Row ↔ entity mapping (override in typed subclasses)
    # ------------------------------------------------------------------

    def _doc_to_entity(self, doc: dict[str, Any]) -> T:
        """
        Convert a normalised MongoDB document dict to the entity type ``T``.

        Base implementation returns the document unchanged. Subclasses
        override this to return typed domain objects.
        """
        return doc  # type: ignore[return-value]

    def _entity_to_doc(self, entity: T) -> dict[str, Any]:
        """
        Convert the entity type ``T`` to a MongoDB document dict.

        Base implementation returns the entity unchanged if it is already a
        dict, or falls back to ``vars(entity)`` for simple objects. Subclasses
        override this to serialise typed domain objects correctly.
        """
        if isinstance(entity, dict):
            return dict(entity)
        return vars(entity)

    # ------------------------------------------------------------------
    # Exception mapping helper
    # ------------------------------------------------------------------

    def _wrap_pymongo(self, exc: Exception, operation: str) -> AdapterQueryError | AdapterConnectionError | AdapterTimeoutError:
        """
        Map a pymongo exception to the appropriate ``AdapterError`` subclass.

        Caller must ``raise ... from exc`` at the call site.
        """
        if isinstance(exc, asyncio.TimeoutError):
            return AdapterTimeoutError(
                f"{operation} exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        if isinstance(exc, _CONNECTION_ERRORS):
            return AdapterConnectionError(
                f"{operation} failed — cannot reach MongoDB: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        return AdapterQueryError(
            f"{operation} failed on collection {self._coll_name!r}: {exc}",
            adapter=self._settings.adapter_name,
            operation=operation,
            cause=exc,
        )

    # ------------------------------------------------------------------
    # BaseRepository[T] interface
    # ------------------------------------------------------------------

    async def get(self, entity_id: str) -> T | None:
        """
        Retrieve a single document by ``_id``.

        Args:
            entity_id: String representation of the document ``_id``.
                       Parsed as ``ObjectId`` when valid, plain string otherwise.

        Returns:
            The entity if a matching document exists, ``None`` otherwise.

        Raises:
            AdapterConnectionError: Backend is unreachable.
            AdapterQueryError:      Query failed post-connection.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        col = self._get_collection()
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                doc = await col.find_one(
                    {"_id": self._normalise_id(entity_id)},
                    max_time_ms=self._max_time_ms(),
                )
        except (asyncio.TimeoutError, *_CONNECTION_ERRORS, *_QUERY_ERRORS) as exc:
            raise self._wrap_pymongo(exc, "get") from exc

        if doc is None:
            return None
        return self._doc_to_entity(self._serialise_doc(dict(doc)))

    async def list(self, limit: int, offset: int) -> tuple[list[T], int]:
        """
        Return a paginated slice of documents and the total document count.

        Both the ``find()`` cursor scan and the ``count_documents()`` call
        run inside a single ``asyncio.timeout()`` block.

        Args:
            limit:  Maximum number of documents to return.
            offset: Number of documents to skip.

        Returns:
            A 2-tuple ``(entities, total_count)`` where ``total_count`` is
            the total number of documents in the collection.

        Raises:
            AdapterConnectionError: Backend is unreachable.
            AdapterQueryError:      Query failed post-connection.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        col = self._get_collection()
        max_ms = self._max_time_ms()
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                cursor = col.find({}, max_time_ms=max_ms).skip(offset).limit(limit)
                docs = await cursor.to_list(length=limit)
                total: int = await col.count_documents({}, maxTimeMS=max_ms)
        except (asyncio.TimeoutError, *_CONNECTION_ERRORS, *_QUERY_ERRORS) as exc:
            raise self._wrap_pymongo(exc, "list") from exc

        entities = [self._doc_to_entity(self._serialise_doc(dict(d))) for d in docs]
        return entities, total

    async def create(self, entity: T) -> T:
        """
        Insert a new document and return the stored document.

        If the entity dict contains an ``id`` key but no ``_id`` key, ``id``
        is mapped to ``_id`` before insertion. This lets callers use either
        convention. After insertion the stored document is fetched so that
        any server-assigned fields (e.g. default ``_id``) are reflected in
        the return value.

        Args:
            entity: The entity to insert.

        Returns:
            The entity as stored, with ``_id`` converted to ``str``.

        Raises:
            AdapterConnectionError: Backend is unreachable.
            AdapterQueryError:      Insert failed (e.g. duplicate key).
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        col = self._get_collection()
        doc = self._entity_to_doc(entity)

        if "id" in doc and "_id" not in doc:
            doc["_id"] = self._normalise_id(str(doc.pop("id")))

        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                result = await col.insert_one(doc)
                inserted = await col.find_one(
                    {"_id": result.inserted_id},
                    max_time_ms=self._max_time_ms(),
                )
        except (asyncio.TimeoutError, *_CONNECTION_ERRORS, *_QUERY_ERRORS) as exc:
            raise self._wrap_pymongo(exc, "create") from exc

        return self._doc_to_entity(self._serialise_doc(dict(inserted)))  # type: ignore[arg-type]

    async def update(self, entity: T) -> T | None:
        """
        Update an existing document and return the updated document.

        The ``_id`` or ``id`` field is extracted from the entity dict and used
        in the filter. All remaining fields are applied via ``$set``.
        Uses ``find_one_and_update`` with ``return_document=True`` for an
        atomic update-and-return.

        Args:
            entity: The entity with updated fields. Must contain ``_id`` or ``id``.

        Returns:
            The updated entity as stored, or ``None`` if no document matched.

        Raises:
            AdapterQueryError:      Entity has no ``_id`` / ``id``, or update failed.
            AdapterConnectionError: Backend is unreachable.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        col = self._get_collection()
        doc = self._entity_to_doc(entity)

        entity_id = doc.pop("_id", None) or doc.pop("id", None)
        if entity_id is None:
            raise AdapterQueryError(
                "Entity must have '_id' or 'id' to update",
                adapter=self._settings.adapter_name,
                operation="update",
            )

        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                result = await col.find_one_and_update(
                    {"_id": self._normalise_id(str(entity_id))},
                    {"$set": doc},
                    return_document=True,
                    max_time_ms=self._max_time_ms(),
                )
        except (asyncio.TimeoutError, *_CONNECTION_ERRORS, *_QUERY_ERRORS) as exc:
            raise self._wrap_pymongo(exc, "update") from exc

        if result is None:
            return None
        return self._doc_to_entity(self._serialise_doc(dict(result)))

    async def delete(self, entity_id: str) -> bool:
        """
        Delete a document by ``_id``.

        Args:
            entity_id: String representation of the document ``_id``.

        Returns:
            ``True`` if a document was deleted, ``False`` if none matched.

        Raises:
            AdapterConnectionError: Backend is unreachable.
            AdapterQueryError:      Deletion failed.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        col = self._get_collection()
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                result = await col.delete_one(
                    {"_id": self._normalise_id(entity_id)},
                )
        except (asyncio.TimeoutError, *_CONNECTION_ERRORS, *_QUERY_ERRORS) as exc:
            raise self._wrap_pymongo(exc, "delete") from exc

        return result.deleted_count == 1

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """
        Close the MongoDB client and remove it from the cache.

        Motor's ``close()`` is synchronous. Call once at application shutdown.
        After this returns the client is closed; a subsequent operation will
        create a new client.
        """
        client = _client_cache.pop(self._settings.mongo_url, None)
        if client is not None:
            client.close()

    # ------------------------------------------------------------------
    # BasePort (Identity + Lifecycle) interface
    # ------------------------------------------------------------------

    async def initialize(self, context: PluginContext) -> None:
        """
        Establish the Motor client and verify connectivity.

        BasePort lifecycle entry point. Reuses the same cached client as
        every other method on this repository.

        Args:
            context: Plugin context. Unused — settings are provided at
                     construction time.

        Raises:
            AdapterConnectionError: MongoDB is unreachable.
        """
        get_mongo_client(self._settings)
        health = await self.health()
        if health.status != PluginStatus.READY:
            raise AdapterConnectionError(
                health.message or "MongoDB connectivity check failed during initialize()",
                adapter=self._settings.adapter_name,
                operation="initialize",
            )

    async def shutdown(self) -> None:
        """BasePort lifecycle entry point — alias for close(). Never raises."""
        await self.close()

    async def health(self) -> PluginHealth:
        """
        BasePort lifecycle entry point — returns a PluginHealth snapshot.

        The sole connectivity check on this repository — ``admin.command
        ("ping")`` with a 5-second timeout. Never raises.
        """
        try:
            client = get_mongo_client(self._settings)
            await asyncio.wait_for(client.admin.command("ping"), timeout=5.0)
            return PluginHealth(status=PluginStatus.READY, message="")
        except Exception as exc:  # noqa: BLE001
            return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
