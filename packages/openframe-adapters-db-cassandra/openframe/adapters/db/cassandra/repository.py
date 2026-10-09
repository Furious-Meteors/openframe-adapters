"""
openframe/adapters/db/cassandra/repository.py
=================================================
Generic Cassandra repository implementing ``BaseRepository[T]`` from
``openframe-core`` via structural subtyping.

The base class works with raw ``dict[str, Any]`` rows (built from
``cassandra.cluster.ResultSet`` rows, which are already mapping-like). Domain
adapters subclass it and override ``_row_to_entity()`` / ``_entity_to_row()``
to map between rows and typed domain objects.

Every CRUD method dispatches through ``Session.execute_async()`` and
``connection._response_future_to_asyncio()`` — see ``connection.py``'s
module docstring for why this does not consume a thread-pool thread for the
query's duration, unlike a naive ``run_in_executor(None, session.execute)``.

Usage — raw dict mode (no subclassing needed):

    repo = CassandraRepository(settings, table="items", id_column="id")
    item: dict | None = await repo.get("abc-123")

Usage — typed domain mode (subclass):

    class ItemRepository(CassandraRepository[Item]):
        _table = "items"
        _id_column = "id"

        def _row_to_entity(self, row) -> Item:
            return Item(**dict(row._asdict()))

        def _entity_to_row(self, entity: Item) -> dict[str, Any]:
            return entity.model_dump()

Structural conformance (no inheritance from Protocols required):

    assert isinstance(repo, BaseRepository)
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Generic, TypeVar

import cassandra
from cassandra.cluster import ConnectionException, NoHostAvailable

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository, Capability, PluginContext, PluginHealth, PluginStatus

from .config import CassandraSettings
from .connection import _cache_key, _response_future_to_asyncio, _session_cache, get_cassandra_session

__all__ = ["CassandraRepository"]

_logger = logging.getLogger(__name__)

T = TypeVar("T")

# Connection-class driver exceptions: the cluster/session itself is
# unreachable or authentication is broken. Retryable, except for
# AuthenticationFailed (see _wrap_cassandra() — retrying with the same bad
# credentials cannot succeed).
#
# cassandra.Unavailable is deliberately included here rather than treated
# as a query-class failure: it means the coordinator could not muster
# enough replicas to satisfy the requested consistency level *right now* —
# a transient cluster-health condition much closer in spirit to "the
# backend is temporarily unreachable" than to "this specific query is
# malformed." It is safe to retry (especially for idempotent reads), same
# as AdapterConnectionError's default.
_CONNECTION_ERRORS = (
    NoHostAvailable,
    ConnectionException,
    cassandra.Unavailable,
)

# Timeout-class driver exceptions: the request reached the coordinator (or
# the client gave up waiting) but did not complete within the cluster's own
# window. Distinct from a query that is simply wrong (AdapterQueryError) —
# these are retried the same way AdapterTimeoutError already is by
# convention across this ecosystem.
_TIMEOUT_ERRORS = (
    cassandra.OperationTimedOut,
    cassandra.ReadTimeout,
    cassandra.WriteTimeout,
)

# Query-class driver exceptions: the request was rejected or a replica
# explicitly failed to process it. Not retryable — identical input fails
# identically again.
_QUERY_ERRORS = (
    cassandra.InvalidRequest,
    cassandra.ReadFailure,
    cassandra.WriteFailure,
)


class CassandraRepository(Generic[T]):
    """
    Generic Cassandra repository.

    Implements ``BaseRepository[T]`` structurally — no inheritance from the
    Protocol. All cassandra-driver exceptions are caught and re-raised as
    ``AdapterError`` subclasses. Every operation wraps its
    ``execute_async()``-bridged call in ``asyncio.timeout(operation_timeout)``.

    Health check: ``health()`` is the sole health check on this repository.
    It verifies backend connectivity and returns a ``PluginHealth`` snapshot
    describing the result — never raises.

    Class attributes (override in subclass):
        _table:     Table name used when no ``table`` argument is passed.
        _id_column: Primary key column. Default ``"id"``.

    Args:
        settings:  A ``CassandraSettings`` instance.
        table:     Table name. Overrides the ``_table`` class attribute.
        id_column: PK column name. Overrides the ``_id_column`` class
                   attribute.

    Raises:
        AdapterConfigurationError: If neither ``table`` param nor ``_table``
                                   class attribute is set.
    """

    _table: str = ""
    _id_column: str = "id"

    name:       str = "openframe-cassandra-repository"
    version:    str = "0.1.0"
    capability: Capability = Capability.PERSISTENCE

    def __init__(
        self,
        settings: CassandraSettings,
        table: str | None = None,
        id_column: str | None = None,
    ) -> None:
        self._settings = settings
        self._table = table or self.__class__._table
        self._id_column = id_column or self.__class__._id_column

        if not self._table:
            raise AdapterConfigurationError(
                "CassandraRepository requires a table name. "
                "Pass table= to __init__ or set _table on the subclass.",
                adapter=settings.adapter_name,
                operation="init",
            )

    # ------------------------------------------------------------------
    # Row <-> entity mapping (override in typed subclasses)
    # ------------------------------------------------------------------

    def _row_to_entity(self, row: Any) -> T:
        """
        Convert a cassandra-driver result row to the entity type ``T``.

        Base implementation returns ``dict(row._asdict())`` for a
        ``namedtuple``-shaped row (the driver's default row factory). Mocks
        in tests use plain dicts, which this also handles. Subclasses
        override this to return typed domain objects.
        """
        if isinstance(row, dict):
            return dict(row)  # type: ignore[return-value]
        return dict(row._asdict())  # type: ignore[return-value]

    def _entity_to_row(self, entity: T) -> dict[str, Any]:
        """
        Convert the entity type ``T`` to a column->value dict for CQL.

        Base implementation returns the entity unchanged if it is already a
        dict, or falls back to ``vars(entity)`` for simple objects.
        Subclasses override this to serialise typed domain objects correctly.
        """
        if isinstance(entity, dict):
            return entity
        return vars(entity)

    # ------------------------------------------------------------------
    # Exception mapping helper
    # ------------------------------------------------------------------

    def _wrap_cassandra(
        self, exc: Exception, operation: str
    ) -> AdapterQueryError | AdapterConnectionError | AdapterTimeoutError:
        """
        Map a cassandra-driver exception to the appropriate ``AdapterError``
        subclass.

        Classification (see the module-level comments above each tuple for
        the reasoning behind each bucket):
            AuthenticationFailed             -> AdapterConnectionError, retryable=False
            NoHostAvailable / ConnectionException / Unavailable
                                              -> AdapterConnectionError, retryable=True
            OperationTimedOut / ReadTimeout / WriteTimeout
                                              -> AdapterTimeoutError, retryable=True
            InvalidRequest / ReadFailure / WriteFailure
                                              -> AdapterQueryError, retryable=False
            anything else (incl. generic cassandra.DriverException)
                                              -> AdapterQueryError, retryable=False

        Caller must ``raise ... from exc`` at the call site.
        """
        if isinstance(exc, cassandra.AuthenticationFailed):
            # AdapterConnectionError defaults retryable=True; overridden
            # below since retrying with the same bad credentials cannot
            # succeed (__init__ has a fixed signature, no retryable= kwarg).
            auth_error = AdapterConnectionError(
                f"{operation} failed — Cassandra authentication rejected: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
            auth_error.retryable = False
            return auth_error
        if isinstance(exc, _CONNECTION_ERRORS):
            return AdapterConnectionError(
                f"{operation} failed — connection to Cassandra was lost or "
                f"the cluster could not satisfy the request: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        if isinstance(exc, _TIMEOUT_ERRORS):
            return AdapterTimeoutError(
                f"{operation} timed out at the Cassandra coordinator: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        return AdapterQueryError(
            f"{operation} failed on {self._table}: {exc}",
            adapter=self._settings.adapter_name,
            operation=operation,
            cause=exc,
        )

    # ------------------------------------------------------------------
    # Query execution helper — the bridge every CRUD method goes through
    # ------------------------------------------------------------------

    async def _execute(self, query: str, params: tuple[Any, ...] | None, operation: str) -> Any:
        """
        Execute a CQL statement via ``execute_async()`` bridged to asyncio.

        Never calls the blocking ``Session.execute()`` — see
        ``connection.py``'s module docstring for why.
        """
        session = await get_cassandra_session(self._settings)
        loop = asyncio.get_running_loop()
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                response_future = session.execute_async(query, params)
                aio_future = _response_future_to_asyncio(response_future, loop)
                result = await aio_future
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"{operation} exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            ) from exc
        except Exception as exc:
            raise self._wrap_cassandra(exc, operation) from exc
        return result

    # ------------------------------------------------------------------
    # BaseRepository[T] interface
    # ------------------------------------------------------------------

    async def get(self, entity_id: str) -> T | None:
        """
        Retrieve a single row by primary key.

        Args:
            entity_id: Value of the ``_id_column`` to look up.

        Returns:
            The entity if a matching row exists, ``None`` otherwise.

        Raises:
            AdapterQueryError:      Query failed after connection was established.
            AdapterConnectionError: Connection to Cassandra was lost.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        query = f"SELECT * FROM {self._table} WHERE {self._id_column} = %s LIMIT 1"
        rows = await self._execute(query, (entity_id,), "get")
        row = next(iter(rows), None)
        if row is None:
            return None
        return self._row_to_entity(row)

    async def list(self, limit: int, offset: int) -> tuple[list[T], int]:
        """
        Return a paginated slice of rows and the total row count.

        Cassandra has no native ``OFFSET`` — this implementation fetches up
        to ``limit + offset`` rows and slices client-side. This is adequate
        for the bounded, admin-style pagination this repository targets; it
        is not meant for deep pagination over very large partitions (use a
        paging-state-aware cursor for that, outside this base repository's
        scope).

        Args:
            limit:  Maximum number of rows to return.
            offset: Number of rows to skip.

        Returns:
            A 2-tuple ``(entities, total_count)`` where ``total_count`` is
            the number of all rows in the table (not just the slice).

        Raises:
            AdapterQueryError:      Query failed after connection was established.
            AdapterConnectionError: Connection to Cassandra was lost.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        rows_query = f"SELECT * FROM {self._table} LIMIT %s"
        count_query = f"SELECT COUNT(*) FROM {self._table}"

        all_rows = await self._execute(rows_query, (limit + offset,), "list")
        count_rows = await self._execute(count_query, None, "list")
        count_row = next(iter(count_rows), None)
        count = self._extract_count(count_row)

        rows = list(all_rows)[offset : offset + limit]
        entities = [self._row_to_entity(r) for r in rows]
        return entities, count

    @staticmethod
    def _extract_count(count_row: Any) -> int:
        if count_row is None:
            return 0
        if isinstance(count_row, dict):
            return int(next(iter(count_row.values())))
        return int(count_row[0])

    async def create(self, entity: T) -> T:
        """
        Insert a new row and return the row as submitted.

        Cassandra's ``INSERT`` has no ``RETURNING`` clause, so unlike the
        Postgres/Oracle adapters, this returns the entity as submitted
        rather than a database-generated row. Callers that need
        server-generated fields (e.g. a ``TIMEUUID``) should set them
        client-side before calling ``create()``.

        Args:
            entity: The entity to insert.

        Returns:
            The entity as submitted.

        Raises:
            AdapterQueryError:      Insert failed (e.g. malformed statement).
            AdapterConnectionError: Connection to Cassandra was lost.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        row_dict = self._entity_to_row(entity)
        columns = list(row_dict.keys())
        values = tuple(row_dict.values())
        placeholders = ", ".join("%s" for _ in columns)
        col_list = ", ".join(columns)
        query = f"INSERT INTO {self._table} ({col_list}) VALUES ({placeholders})"
        await self._execute(query, values, "create")
        return self._row_to_entity(row_dict)

    async def update(self, entity: T) -> T | None:
        """
        Update an existing row in place.

        The ``_id_column`` value is extracted from the row dict and used in
        the ``WHERE`` clause. All other columns form the ``SET`` clause.

        Cassandra's ``UPDATE`` is an upsert and reports no "rows matched"
        count at the protocol level, so — unlike Postgres's/Oracle's
        ``UPDATE ... RETURNING``/``RETURNING INTO`` — there is no single
        statement that both performs the write and tells us whether a row
        already existed. This method does an explicit existence check
        (``get(entity_id)``) before issuing the ``UPDATE`` so it can honor
        the same ``BaseRepository`` contract as every other adapter in this
        ecosystem (``update()`` of a non-existent id returns ``None``,
        not an upsert) — at the cost of one extra round trip per call.

        Args:
            entity: The entity with updated fields. Must contain
                    ``_id_column``.

        Returns:
            The updated entity, or ``None`` if no row with this id exists.

        Raises:
            AdapterQueryError:      Update statement is malformed (e.g. no
                                    columns to update besides the id).
            AdapterConnectionError: Connection to Cassandra was lost.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        row_dict = self._entity_to_row(entity)
        entity_id = row_dict.get(self._id_column)

        existing = await self.get(entity_id)
        if existing is None:
            return None

        update_cols = {k: v for k, v in row_dict.items() if k != self._id_column}
        if not update_cols:
            raise AdapterQueryError(
                f"update on {self._table}: entity has no columns to update "
                f"(only id column {self._id_column!r} found).",
                adapter=self._settings.adapter_name,
                operation="update",
            )

        set_clause = ", ".join(f"{col} = %s" for col in update_cols.keys())
        query = f"UPDATE {self._table} SET {set_clause} WHERE {self._id_column} = %s"
        values = tuple(update_cols.values()) + (entity_id,)
        await self._execute(query, values, "update")
        return self._row_to_entity(row_dict)

    async def delete(self, entity_id: str) -> bool:
        """
        Delete a row by primary key.

        Cassandra's ``DELETE`` reports no "rows matched" count — the
        statement succeeds whether or not a matching row existed. This
        method does an explicit existence check (``get(entity_id)``) before
        issuing the ``DELETE`` so it can honor the same ``BaseRepository``
        contract as every other adapter in this ecosystem (``delete()`` of
        a non-existent id returns ``False``) — at the cost of one extra
        round trip per call.

        Args:
            entity_id: Value of the ``_id_column`` to delete.

        Returns:
            ``True`` if a row existed and was deleted, ``False`` if no row
            matched.

        Raises:
            AdapterQueryError:      Deletion failed.
            AdapterConnectionError: Connection to Cassandra was lost.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        existing = await self.get(entity_id)
        if existing is None:
            return False

        query = f"DELETE FROM {self._table} WHERE {self._id_column} = %s"
        await self._execute(query, (entity_id,), "delete")
        return True

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """
        Shut down the cluster/session and remove it from the cache.

        Call once at application shutdown. After this returns the session
        is closed and a subsequent operation will create a new one.
        """
        key = _cache_key(self._settings)
        session = _session_cache.get(key)
        if session is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, session.cluster.shutdown)
            _session_cache.pop(key, None)

    # ------------------------------------------------------------------
    # BasePort (Identity + Lifecycle) interface
    # ------------------------------------------------------------------

    async def initialize(self, context: PluginContext) -> None:
        """
        Establish the session and verify connectivity.

        BasePort lifecycle entry point. Reuses the same cached session as
        every other method on this repository.

        Args:
            context: Plugin context. Unused — settings are provided at
                     construction time.

        Raises:
            AdapterConnectionError: Cassandra is unreachable.
        """
        await get_cassandra_session(self._settings)
        health = await self.health()
        if health.status != PluginStatus.READY:
            raise AdapterConnectionError(
                health.message or "Cassandra connectivity check failed during initialize()",
                adapter=self._settings.adapter_name,
                operation="initialize",
            )

    async def shutdown(self) -> None:
        """BasePort lifecycle entry point — alias for close(). Never raises."""
        try:
            await self.close()
        except Exception as exc:  # noqa: BLE001
            _logger.error("CassandraRepository shutdown error (ignored): %s", exc)

    async def health(self) -> PluginHealth:
        """
        BasePort lifecycle entry point — returns a PluginHealth snapshot.

        The sole connectivity check on this repository — a low-cost
        ``SELECT now() FROM system.local`` with a 5-second timeout. Never
        raises.
        """
        try:
            session = await get_cassandra_session(self._settings)
            loop = asyncio.get_running_loop()
            response_future = session.execute_async("SELECT now() FROM system.local")
            aio_future = _response_future_to_asyncio(response_future, loop)
            await asyncio.wait_for(aio_future, timeout=5.0)
            return PluginHealth(status=PluginStatus.READY, message="")
        except Exception as exc:  # noqa: BLE001
            return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
