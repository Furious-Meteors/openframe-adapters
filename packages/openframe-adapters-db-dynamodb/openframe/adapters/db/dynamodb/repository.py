"""
openframe/adapters/db/dynamodb/repository.py
===============================================
Generic DynamoDB repository implementing ``BaseRepository[T]``
from ``openframe-core`` via structural subtyping.

Uses the aioboto3 DynamoDB **resource** interface (the high-level ``Table``
object), not the lower-level **client** interface. The resource interface
works with plain Python dicts — boto3's built-in type serializer/
deserializer converts them to/from DynamoDB's verbose
``{"S": "value"}``-style attribute-value maps automatically. The client
interface would require constructing those attribute-value maps by hand on
every call, which maps far more awkwardly onto ``BaseRepository[T]``'s
plain-dict CRUD shape. This is a deliberate choice, not an oversight — see
``connection.py``'s module docstring for the full reasoning.

The base class works with raw ``dict[str, Any]`` rows/items. Domain adapters
subclass it and override ``_row_to_entity()`` / ``_entity_to_row()`` to map
between items and typed domain objects.

Usage — raw dict mode (no subclassing needed):

    repo = DynamoDBRepository(settings, id_column="id")
    item: dict | None = await repo.get("abc-123")

Usage — typed domain mode (subclass):

    class ItemRepository(DynamoDBRepository[Item]):
        _id_column = "id"

        def _row_to_entity(self, row: dict) -> Item:
            return Item(**row)

        def _entity_to_row(self, entity: Item) -> dict[str, Any]:
            return entity.model_dump()

Structural conformance (no inheritance from Protocols required):

    assert isinstance(repo, BaseRepository)

DynamoDB-specific semantics worth calling out:

    ``list(limit, offset)``: DynamoDB has no native integer offset — a
    ``Scan`` paginates via an opaque ``LastEvaluatedKey`` token, not a skip
    count. This implementation paginates internally through the full table
    (honouring ``BaseRepository``'s ``(limit, offset)`` contract exactly)
    and discards the first ``offset`` items. This is correct but, unlike a
    SQL ``OFFSET``, its cost scales with ``offset + limit`` scanned items —
    acceptable for the modest page sizes ``BaseRepository`` is designed for,
    but not a substitute for DynamoDB's own key-based (``ExclusiveStartKey``)
    pagination in a latency-sensitive path.

    ``update(entity)``: DynamoDB's ``put_item`` would happily *create* a new
    item when the key doesn't already exist — there is no native "update
    only if present" short of a conditional write. To honour
    ``BaseRepository``'s "update() returns None for a missing entity"
    contract, every ``update()`` issues a conditional ``put_item`` with
    ``ConditionExpression="attribute_exists(...)"`` on the id column, and a
    ``ConditionalCheckFailedException`` (the item didn't exist) is
    translated to ``None`` rather than propagated as ``AdapterQueryError`` —
    this is the one place a DynamoDB error code changes control flow instead
    of just exception type.

Exception-classification gotcha (verified against the installed
``botocore`` exception model, see ``connection.py``'s module docstring and
this module's ``_wrap_botocore``): ``botocore.exceptions.ClientError`` is
the single exception class DynamoDB funnels nearly every *service-side*
error through — the only way to tell a conditional-check failure apart from
a missing table or a throttled request is ``exc.response["Error"]["Code"]``,
never the Python exception type. Genuine *local*/network-level failures
(the request never reached AWS) raise a completely different hierarchy —
``botocore.exceptions.EndpointConnectionError``/``ConnectionError`` — and
must be checked separately; a single ``except ClientError`` would never see
them.
"""
from __future__ import annotations

import asyncio
from typing import Any, Generic, TypeVar

import botocore.exceptions

from openframe.core.exceptions import (
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository, Capability, PluginContext, PluginHealth, PluginStatus

from .config import DynamoDBSettings
from .connection import _cache_key, _table_cache, close_dynamodb_table, get_dynamodb_table

__all__ = ["DynamoDBRepository"]

T = TypeVar("T")

# DynamoDB error codes (exc.response["Error"]["Code"] on ClientError) that
# indicate the request reached AWS and was rejected for a reason that isn't
# "the backend is unreachable" — a true query-class failure. See AWS's
# documented DynamoDB error codes.
_VALIDATION_ERROR_CODES = frozenset({
    "ValidationException",
})

# Conditional-write failures. Classified as query-class, except update()
# special-cases this to mean "entity not found" per BaseRepository's
# contract rather than raising at all.
_CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailedException"

# Table-not-found. The table name in settings doesn't exist — this is a
# configuration problem (wrong DYNAMODB_TABLE_NAME), not a transient
# connection issue, but it only ever surfaces on the first real operation
# (resource creation itself never touches the network — see connection.py).
_RESOURCE_NOT_FOUND = "ResourceNotFoundException"

# Transient-capacity errors. The request reached AWS and was explicitly
# rejected because of a capacity/rate limit, not a permanent failure —
# retrying later is the correct response. openframe-core's AdapterError
# taxonomy has no dedicated "throttled" subclass, so these map onto the
# closest semantic fit: AdapterConnectionError, whose retryable=True default
# communicates exactly what callers need to know (back off and retry),
# even though no actual connection was lost.
_THROTTLING_ERROR_CODES = frozenset({
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
    "RequestLimitExceeded",
})


class DynamoDBRepository(Generic[T]):
    """
    Generic DynamoDB repository.

    Implements ``BaseRepository[T]`` structurally — no inheritance from the
    Protocol. All ``botocore``/``aioboto3`` exceptions are caught and
    re-raised as ``AdapterError`` subclasses. Every operation wraps its
    aioboto3 call in ``asyncio.timeout(settings.operation_timeout)``.

    Health check: ``health()`` is the sole health check on this repository.
    It verifies backend connectivity and returns a ``PluginHealth``
    snapshot describing the result — never raises.

    Class attributes (override in subclass):
        _id_column: Partition key attribute name. Default ``"id"``.

    Args:
        settings:  A ``DynamoDBSettings`` instance.
        id_column: Partition key attribute name. Overrides ``_id_column``.
    """

    _id_column: str = "id"

    name:       str = "openframe-dynamodb-repository"
    version:    str = "0.1.0"
    capability: Capability = Capability.PERSISTENCE

    def __init__(
        self,
        settings: DynamoDBSettings,
        id_column: str | None = None,
    ) -> None:
        self._settings = settings
        self._id_column = id_column or self.__class__._id_column

    # ------------------------------------------------------------------
    # Row <-> entity mapping (override in typed subclasses)
    # ------------------------------------------------------------------

    def _row_to_entity(self, row: dict[str, Any]) -> T:
        """
        Convert a raw DynamoDB item dict to the entity type ``T``.

        Base implementation returns the dict unchanged. Subclasses override
        this to return typed domain objects.
        """
        return row  # type: ignore[return-value]

    def _entity_to_row(self, entity: T) -> dict[str, Any]:
        """
        Convert the entity type ``T`` to an item dict for DynamoDB.

        Base implementation returns the entity unchanged if it is already a
        dict, or falls back to ``vars(entity)`` for simple objects.
        Subclasses override this to serialise typed domain objects
        correctly.
        """
        if isinstance(entity, dict):
            return entity
        return vars(entity)

    # ------------------------------------------------------------------
    # Exception mapping helper
    # ------------------------------------------------------------------

    def _wrap_botocore(
        self, exc: Exception, operation: str
    ) -> AdapterQueryError | AdapterConnectionError:
        """
        Map a ``botocore``/``aioboto3`` exception to the appropriate
        ``AdapterError`` subclass.

        ``botocore.exceptions.ClientError`` is dispatched purely on
        ``exc.response["Error"]["Code"]`` — DynamoDB funnels essentially
        every service-side failure through this one exception type.
        Genuine local/network-level failures
        (``EndpointConnectionError``/``ConnectionError``) are a separate
        hierarchy checked first. Caller must ``raise ... from exc`` at the
        call site.
        """
        if isinstance(exc, (botocore.exceptions.EndpointConnectionError, ConnectionError)):
            return AdapterConnectionError(
                f"{operation} failed — cannot reach DynamoDB: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        if isinstance(exc, botocore.exceptions.ClientError):
            code = exc.response.get("Error", {}).get("Code", "")
            if code in _THROTTLING_ERROR_CODES:
                return AdapterConnectionError(
                    f"{operation} failed — DynamoDB throttled the request "
                    f"({code}): {exc}",
                    adapter=self._settings.adapter_name,
                    operation=operation,
                    cause=exc,
                )
            # ResourceNotFoundException, ValidationException,
            # ConditionalCheckFailedException (when not special-cased by the
            # caller), and any other service-side rejection: query-class.
            return AdapterQueryError(
                f"{operation} failed ({code or 'unknown'}): {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        return AdapterQueryError(
            f"{operation} failed: {exc}",
            adapter=self._settings.adapter_name,
            operation=operation,
            cause=exc,
        )

    # ------------------------------------------------------------------
    # BaseRepository[T] interface
    # ------------------------------------------------------------------

    async def get(self, entity_id: str) -> T | None:
        """
        Retrieve a single item by partition key.

        Args:
            entity_id: Value of the ``_id_column`` to look up.

        Returns:
            The entity if a matching item exists, ``None`` otherwise.

        Raises:
            AdapterQueryError:      Query failed after the request reached
                                    DynamoDB (bad input, missing table, etc).
            AdapterConnectionError: DynamoDB was unreachable, or the request
                                    was throttled.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        table = await get_dynamodb_table(self._settings)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                response = await table.get_item(Key={self._id_column: entity_id})
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"get exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="get",
                cause=exc,
            ) from exc
        except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as exc:
            raise self._wrap_botocore(exc, "get") from exc

        item = response.get("Item")
        if item is None:
            return None
        return self._row_to_entity(item)

    async def list(self, limit: int, offset: int) -> tuple[list[T], int]:
        """
        Return a paginated slice of items and the total item count.

        DynamoDB has no native integer offset (see module docstring) — this
        scans the full table, paginating via ``LastEvaluatedKey``, and
        slices the accumulated items in Python. The total count comes from
        a separate ``Select="COUNT"`` scan.

        Args:
            limit:  Maximum number of items to return.
            offset: Number of items to skip.

        Returns:
            A 2-tuple ``(entities, total_count)`` where ``total_count`` is
            the number of all items in the table (not just the slice).

        Raises:
            AdapterQueryError:      Scan failed.
            AdapterConnectionError: DynamoDB was unreachable, or the request
                                    was throttled.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        table = await get_dynamodb_table(self._settings)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                items: list[dict[str, Any]] = []
                scan_kwargs: dict[str, Any] = {}
                needed = offset + limit
                while True:
                    response = await table.scan(**scan_kwargs)
                    items.extend(response.get("Items", []))
                    last_key = response.get("LastEvaluatedKey")
                    if last_key is None or len(items) >= needed:
                        break
                    scan_kwargs["ExclusiveStartKey"] = last_key

                count = 0
                count_kwargs: dict[str, Any] = {"Select": "COUNT"}
                while True:
                    count_response = await table.scan(**count_kwargs)
                    count += count_response.get("Count", 0)
                    last_key = count_response.get("LastEvaluatedKey")
                    if last_key is None:
                        break
                    count_kwargs["ExclusiveStartKey"] = last_key
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"list exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="list",
                cause=exc,
            ) from exc
        except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as exc:
            raise self._wrap_botocore(exc, "list") from exc

        page = items[offset: offset + limit]
        entities = [self._row_to_entity(i) for i in page]
        return entities, count

    async def create(self, entity: T) -> T:
        """
        Insert a new item and return it.

        ``put_item`` returns no body on success, so unlike SQL adapters
        there are no backend-generated fields to re-fetch — the entity
        passed in is returned unchanged (DynamoDB has no auto-increment
        equivalent).

        Args:
            entity: The entity to insert.

        Returns:
            The entity as passed in.

        Raises:
            AdapterQueryError:      Insert failed (e.g. bad input).
            AdapterConnectionError: DynamoDB was unreachable, or the request
                                    was throttled.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        table = await get_dynamodb_table(self._settings)
        row_dict = self._entity_to_row(entity)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await table.put_item(Item=row_dict)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"create exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="create",
                cause=exc,
            ) from exc
        except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as exc:
            raise self._wrap_botocore(exc, "create") from exc

        return entity

    async def update(self, entity: T) -> T | None:
        """
        Update an existing item and return it, or ``None`` if it doesn't exist.

        Issues a conditional ``put_item`` requiring the partition key to
        already exist. A ``ConditionalCheckFailedException`` means no
        matching item was present — translated to ``None`` per
        ``BaseRepository``'s contract, not raised as ``AdapterQueryError``.

        Args:
            entity: The entity with updated fields. Must contain
                    ``_id_column``.

        Returns:
            The updated entity as stored, or ``None`` if no item matched.

        Raises:
            AdapterQueryError:      Update failed for a reason other than
                                    "item doesn't exist" (e.g. bad input).
            AdapterConnectionError: DynamoDB was unreachable, or the request
                                    was throttled.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        table = await get_dynamodb_table(self._settings)
        row_dict = self._entity_to_row(entity)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await table.put_item(
                    Item=row_dict,
                    ConditionExpression="attribute_exists(#id)",
                    ExpressionAttributeNames={"#id": self._id_column},
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"update exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="update",
                cause=exc,
            ) from exc
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == _CONDITIONAL_CHECK_FAILED:
                return None
            raise self._wrap_botocore(exc, "update") from exc
        except botocore.exceptions.BotoCoreError as exc:
            raise self._wrap_botocore(exc, "update") from exc

        return entity

    async def delete(self, entity_id: str) -> bool:
        """
        Delete an item by partition key.

        Args:
            entity_id: Value of the ``_id_column`` to delete.

        Returns:
            ``True`` if an item was deleted, ``False`` if no item matched.

        Raises:
            AdapterQueryError:      Deletion failed.
            AdapterConnectionError: DynamoDB was unreachable, or the request
                                    was throttled.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        table = await get_dynamodb_table(self._settings)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                response = await table.delete_item(
                    Key={self._id_column: entity_id},
                    ReturnValues="ALL_OLD",
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"delete exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="delete",
                cause=exc,
            ) from exc
        except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as exc:
            raise self._wrap_botocore(exc, "delete") from exc

        return response.get("Attributes") is not None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """
        Close the cached DynamoDB resource and remove it from the cache.

        Call once at application shutdown. After this returns, a
        subsequent operation will create a new resource/session.
        """
        await close_dynamodb_table(self._settings)

    # ------------------------------------------------------------------
    # BasePort (Identity + Lifecycle) interface
    # ------------------------------------------------------------------

    async def initialize(self, context: PluginContext) -> None:
        """
        Establish the DynamoDB resource and verify connectivity.

        BasePort lifecycle entry point. Reuses the same cached resource as
        every other method on this repository.

        Args:
            context: Plugin context. Unused — settings are provided at
                     construction time.

        Raises:
            AdapterConnectionError: DynamoDB is unreachable or the
                                    configured table does not exist.
        """
        await get_dynamodb_table(self._settings)
        health = await self.health()
        if health.status != PluginStatus.READY:
            raise AdapterConnectionError(
                health.message or "DynamoDB connectivity check failed during initialize()",
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
        ``describe_table`` call (via the resource's ``meta.client``) with a
        5-second timeout, confirming the configured table exists and is
        reachable. Never raises.
        """
        try:
            table = await get_dynamodb_table(self._settings)

            async def _ping() -> None:
                await table.meta.client.describe_table(TableName=self._settings.dynamodb_table_name)

            await asyncio.wait_for(_ping(), timeout=5.0)
            return PluginHealth(status=PluginStatus.READY, message="")
        except Exception as exc:  # noqa: BLE001
            return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
