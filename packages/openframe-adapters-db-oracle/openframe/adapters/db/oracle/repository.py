"""
openframe/adapters/db/oracle/repository.py
============================================
Generic Oracle repository implementing ``BaseRepository[T]`` from
``openframe-core`` via structural subtyping.

The base class works with raw ``dict[str, Any]`` rows. Domain adapters
subclass it and override ``_row_to_entity()`` / ``_entity_to_row()`` to map
between rows and typed domain objects.

Usage — raw dict mode (no subclassing needed):

    repo = OracleRepository(settings, table="items", id_column="id")
    item: dict | None = await repo.get("abc-123")

Usage — typed domain mode (subclass):

    class ItemRepository(OracleRepository[Item]):
        _table = "items"
        _id_column = "id"

        def _row_to_entity(self, row: dict) -> Item:
            return Item(**row)

        def _entity_to_row(self, entity: Item) -> dict[str, Any]:
            return entity.model_dump()

Structural conformance (no inheritance from Protocols required):

    assert isinstance(repo, BaseRepository)
"""
from __future__ import annotations

import asyncio
from typing import Any, Generic, TypeVar

import oracledb

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository, Capability, PluginContext, PluginHealth, PluginStatus

from .config import OracleSettings
from .connection import _cache_key, _pool_cache, get_oracle_pool

__all__ = ["OracleRepository"]

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Connection-vs-query exception classification
# ---------------------------------------------------------------------------
# Every exception oracledb raises that this repository is expected to catch
# derives from oracledb.Error EXCEPT oracledb.InterfaceError, which — like
# asyncpg.InterfaceError in the Postgres adapter — is NOT a subclass of
# oracledb.DatabaseError. Verified directly against the installed driver
# (oracledb 26.0.1):
#
#   >>> oracledb.InterfaceError.__mro__
#   (InterfaceError, Error, Exception, BaseException, object)
#   >>> oracledb.DatabaseError.__mro__
#   (DatabaseError, Error, Exception, BaseException, object)
#
# So `except oracledb.DatabaseError` alone would let a raw, untranslated
# oracledb.InterfaceError escape past the adapter boundary. Every call site
# below catches `(oracledb.DatabaseError, oracledb.InterfaceError, OSError)`
# instead of assuming DatabaseError covers everything.
#
# ORA/DPY codes classified as connection-class (broken/lost connection,
# retryable) rather than query-class (bad SQL, constraint violation, not
# retryable):
#
#   - DPY-6xxx: driver-side connection failures. Verified: connecting to a
#     refused port raises oracledb.OperationalError, full_code "DPY-6005".
#   - ORA codes oracledb's OWN internal error table (oracledb.errors) maps
#     to OperationalError: 22, 378, 600, 602, 603, 604, 609, 1012, 1013,
#     1033, 1034, 1041, 1043, 1089, 1090, 1092, 3111, 3113, 3114, 3122,
#     3135, 12153, 12203, 12500, 12571, 27146, 28511 (source:
#     oracledb.errors.ERR_OPERATIONAL_ERROR_CODES) plus the "recoverable"
#     set 376, 1033, 1034, 1090, 1115, 12514, 12571, 12757, 16456 (source:
#     oracledb.errors.ERR_RECOVERABLE_ERROR_CODES). Verified two of these by
#     inspecting oracledb.errors' internal code->constant map: 3113 and 3114
#     ("end-of-file on communication channel" / "not connected to ORACLE")
#     both map to ERR_CONNECTION_CLOSED.
#   - ORA-12541 (TNS:no listener), ORA-12154 (TNS:could not resolve connect
#     identifier), ORA-12170 (TNS:connect timeout occurred), ORA-12537
#     (TNS:connection closed) — standard, well-documented Oracle networking
#     error codes. NOT verified against a live Oracle listener in this
#     investigation (no server was available) and NOT present in oracledb's
#     own internal error-code table (that table only covers errors the
#     Python driver itself raises/reinterprets; these originate from the
#     network/listener layer and would arrive as-is in a DatabaseError's
#     full_code when a real server/listener sends them). Included because
#     they are genuine, real ORA codes with an unambiguous connection-class
#     meaning — flagged here so a reviewer knows this part is documented
#     Oracle knowledge, not driver-introspection like the rest of this list.
#   - isinstance(exc, oracledb.OperationalError) as a class-based fallback
#     net, since the driver itself already raises OperationalError for
#     every code above that it recognizes.
_CONNECTION_DPY_PREFIXES = ("DPY-6",)

_CONNECTION_ORA_CODES = frozenset(
    {
        "ORA-00022", "ORA-00378", "ORA-00600", "ORA-00602", "ORA-00603",
        "ORA-00604", "ORA-00609", "ORA-01012", "ORA-01013", "ORA-01033",
        "ORA-01034", "ORA-01041", "ORA-01043", "ORA-01089", "ORA-01090",
        "ORA-01092", "ORA-03111", "ORA-03113", "ORA-03114", "ORA-03122",
        "ORA-03135", "ORA-12153", "ORA-12203", "ORA-12500", "ORA-12571",
        "ORA-27146", "ORA-28511", "ORA-01115", "ORA-12514", "ORA-12757",
        "ORA-16456",
        # Not in oracledb's own table — standard TNS/listener codes, see
        # docstring note above.
        "ORA-12541", "ORA-12154", "ORA-12170", "ORA-12537",
    }
)


class OracleRepository(Generic[T]):
    """
    Generic Oracle repository.

    Implements ``BaseRepository[T]`` structurally — no inheritance from the
    Protocol. All ``oracledb`` exceptions are caught and re-raised as
    ``AdapterError`` subclasses. Every operation wraps its call in
    ``asyncio.timeout(settings.operation_timeout)``.

    Health check: ``health()`` is the sole health check on this repository.
    It verifies backend connectivity and returns a ``PluginHealth`` snapshot
    describing the result — never raises.

    Class attributes (override in subclass):
        _table:     Table name used when no ``table`` argument is passed.
        _id_column: Primary key column. Default ``"id"``.

    Args:
        settings:  An ``OracleSettings`` instance.
        table:     Table name. Overrides the ``_table`` class attribute.
        id_column: PK column name. Overrides the ``_id_column`` class attribute.

    Raises:
        AdapterConfigurationError: If neither ``table`` param nor ``_table``
                                   class attribute is set.
    """

    _table: str = ""
    _id_column: str = "id"

    name:       str = "openframe-oracle-repository"
    version:    str = "0.1.0"
    capability: Capability = Capability.PERSISTENCE

    def __init__(
        self,
        settings: OracleSettings,
        table: str | None = None,
        id_column: str | None = None,
    ) -> None:
        self._settings = settings
        self._table = table or self.__class__._table
        self._id_column = id_column or self.__class__._id_column

        if not self._table:
            raise AdapterConfigurationError(
                "OracleRepository requires a table name. "
                "Pass table= to __init__ or set _table on the subclass.",
                adapter=settings.adapter_name,
                operation="init",
            )

    # ------------------------------------------------------------------
    # Row <-> entity mapping (override in typed subclasses)
    # ------------------------------------------------------------------

    def _row_to_entity(self, row: dict[str, Any]) -> T:
        """
        Convert a column-name-keyed dict to the entity type ``T``.

        Base implementation returns the dict unchanged. Subclasses override
        this to return typed domain objects.
        """
        return row  # type: ignore[return-value]

    def _entity_to_row(self, entity: T) -> dict[str, Any]:
        """
        Convert the entity type ``T`` to a column->value dict for SQL.

        Base implementation returns the entity unchanged if it is already a
        dict, or falls back to ``vars(entity)`` for simple objects. Subclasses
        override this to serialise typed domain objects correctly.
        """
        if isinstance(entity, dict):
            return entity
        return vars(entity)

    @staticmethod
    def _cursor_row_to_dict(cursor: oracledb.AsyncCursor, row: tuple[Any, ...]) -> dict[str, Any]:
        """
        Convert a raw oracledb cursor row (a plain tuple — oracledb has no
        dict-like Record type the way asyncpg does) to a column-name-keyed
        dict, using ``cursor.description``.
        """
        columns = [d[0] for d in cursor.description]
        return dict(zip(columns, row))

    # ------------------------------------------------------------------
    # Exception mapping helper
    # ------------------------------------------------------------------

    def _wrap_oracledb(
        self, exc: Exception, operation: str
    ) -> AdapterQueryError | AdapterConnectionError:
        """
        Map an ``oracledb`` exception (or ``OSError``) to the appropriate
        ``AdapterError`` subclass.

        Distinguishes a lost/broken connection (``AdapterConnectionError`` —
        retryable) from an in-band query failure such as a constraint
        violation or bad SQL (``AdapterQueryError`` — not retryable by
        default). See the module-level comment above for the exact
        classification rules and what was verified against the installed
        driver vs. documented-but-unverified Oracle knowledge.

        Caller must ``raise ... from exc`` at the call site.
        """
        if isinstance(exc, OSError):
            return AdapterConnectionError(
                f"{operation} failed — network error talking to Oracle: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )

        is_connection_class = False
        if isinstance(exc, oracledb.Error) and exc.args:
            err = exc.args[0]
            full_code = getattr(err, "full_code", "") or ""
            if full_code.startswith(_CONNECTION_DPY_PREFIXES):
                is_connection_class = True
            elif full_code in _CONNECTION_ORA_CODES:
                is_connection_class = True
        if isinstance(exc, oracledb.OperationalError):
            is_connection_class = True
        if isinstance(exc, oracledb.InterfaceError):
            # oracledb.InterfaceError is not a DatabaseError subclass (see
            # module-level comment above) and in practice signals a
            # connection/cursor lifecycle problem ("not connected", "cursor
            # is closed", pool shutting down) rather than a query-shape
            # problem — classified as connection-class by default, matching
            # how the Postgres adapter treats asyncpg.InterfaceError.
            is_connection_class = True

        if is_connection_class:
            return AdapterConnectionError(
                f"{operation} failed — connection to Oracle was lost: {exc}",
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
            AdapterQueryError:   Query failed after connection was established.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        pool = await get_oracle_pool(self._settings)
        query = f"SELECT * FROM {self._table} WHERE {self._id_column} = :1"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                conn = await pool.acquire()
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(query, [entity_id])
                        row = await cur.fetchone()
                        if row is None:
                            return None
                        row_dict = self._cursor_row_to_dict(cur, row)
                finally:
                    await pool.release(conn)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"get exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="get",
                cause=exc,
            ) from exc
        except (oracledb.DatabaseError, oracledb.InterfaceError, OSError) as exc:
            raise self._wrap_oracledb(exc, "get") from exc

        return self._row_to_entity(row_dict)

    async def list(self, limit: int, offset: int) -> tuple[list[T], int]:
        """
        Return a paginated slice of rows and the total row count.

        Both queries run on a single connection acquired from the pool.

        Args:
            limit:  Maximum number of rows to return.
            offset: Number of rows to skip.

        Returns:
            A 2-tuple ``(entities, total_count)`` where ``total_count`` is
            the number of all rows in the table (not just the slice).

        Raises:
            AdapterQueryError:   Query failed after connection was established.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        pool = await get_oracle_pool(self._settings)
        rows_query = (
            f"SELECT * FROM {self._table} ORDER BY {self._id_column} "
            f"OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY"
        )
        count_query = f"SELECT COUNT(*) FROM {self._table}"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                conn = await pool.acquire()
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(rows_query, [offset, limit])
                        rows = await cur.fetchall()
                        entities = [self._row_to_entity(self._cursor_row_to_dict(cur, r)) for r in rows]
                        await cur.execute(count_query)
                        count_row = await cur.fetchone()
                        count = int(count_row[0]) if count_row is not None else 0
                finally:
                    await pool.release(conn)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"list exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="list",
                cause=exc,
            ) from exc
        except (oracledb.DatabaseError, oracledb.InterfaceError, OSError) as exc:
            raise self._wrap_oracledb(exc, "list") from exc

        return entities, count

    async def create(self, entity: T) -> T:
        """
        Insert a new row and return the stored row.

        Calls ``_entity_to_row(entity)`` to obtain the column dict, then
        executes an ``INSERT`` followed by a ``SELECT`` of the freshly
        inserted row (Oracle's ``INSERT ... RETURNING`` requires binding
        output variables per-column, so a follow-up ``SELECT`` on the
        primary key is used instead, matching what a caller can rely on
        for any table shape).

        Args:
            entity: The entity to insert.

        Returns:
            The entity as stored, including any backend-assigned fields
            (as long as the id was supplied — auto-generated identity
            columns must be set by the caller or a subclass override).

        Raises:
            AdapterQueryError:   Insert failed (e.g. unique-constraint violation).
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        pool = await get_oracle_pool(self._settings)
        row_dict = self._entity_to_row(entity)
        columns = list(row_dict.keys())
        values = list(row_dict.values())
        placeholders = ", ".join(f":{i + 1}" for i in range(len(columns)))
        col_list = ", ".join(columns)
        insert_query = f"INSERT INTO {self._table} ({col_list}) VALUES ({placeholders})"
        entity_id = row_dict.get(self._id_column)
        select_query = f"SELECT * FROM {self._table} WHERE {self._id_column} = :1"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                conn = await pool.acquire()
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(insert_query, values)
                        await cur.execute(select_query, [entity_id])
                        row = await cur.fetchone()
                        row_dict_stored = (
                            self._cursor_row_to_dict(cur, row) if row is not None else row_dict
                        )
                    await conn.commit()
                finally:
                    await pool.release(conn)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"create exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="create",
                cause=exc,
            ) from exc
        except (oracledb.DatabaseError, oracledb.InterfaceError, OSError) as exc:
            raise self._wrap_oracledb(exc, "create") from exc

        return self._row_to_entity(row_dict_stored)

    async def update(self, entity: T) -> T | None:
        """
        Update an existing row and return the stored row.

        The ``_id_column`` value is extracted from the row dict and used in
        the ``WHERE`` clause. All other columns form the ``SET`` clause.

        Args:
            entity: The entity with updated fields. Must contain ``_id_column``.

        Returns:
            The updated entity as stored, or ``None`` if no row was matched.

        Raises:
            AdapterQueryError:   Update failed (e.g. constraint violation).
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        pool = await get_oracle_pool(self._settings)
        row_dict = self._entity_to_row(entity)
        entity_id = row_dict.get(self._id_column)

        update_cols = {k: v for k, v in row_dict.items() if k != self._id_column}
        if not update_cols:
            raise AdapterQueryError(
                f"update on {self._table}: entity has no columns to update "
                f"(only id column {self._id_column!r} found).",
                adapter=self._settings.adapter_name,
                operation="update",
            )

        set_parts = [f"{col} = :{i + 1}" for i, col in enumerate(update_cols.keys())]
        set_clause = ", ".join(set_parts)
        id_placeholder = f":{len(update_cols) + 1}"
        update_query = f"UPDATE {self._table} SET {set_clause} WHERE {self._id_column} = {id_placeholder}"
        select_query = f"SELECT * FROM {self._table} WHERE {self._id_column} = :1"
        values = list(update_cols.values()) + [entity_id]
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                conn = await pool.acquire()
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(update_query, values)
                        if cur.rowcount == 0:
                            await conn.commit()
                            return None
                        await cur.execute(select_query, [entity_id])
                        row = await cur.fetchone()
                        row_dict_stored = self._cursor_row_to_dict(cur, row) if row is not None else None
                    await conn.commit()
                finally:
                    await pool.release(conn)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"update exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="update",
                cause=exc,
            ) from exc
        except (oracledb.DatabaseError, oracledb.InterfaceError, OSError) as exc:
            raise self._wrap_oracledb(exc, "update") from exc

        if row_dict_stored is None:
            return None
        return self._row_to_entity(row_dict_stored)

    async def delete(self, entity_id: str) -> bool:
        """
        Delete a row by primary key.

        Args:
            entity_id: Value of the ``_id_column`` to delete.

        Returns:
            ``True`` if a row was deleted, ``False`` if no row matched.

        Raises:
            AdapterQueryError:   Deletion failed.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        pool = await get_oracle_pool(self._settings)
        query = f"DELETE FROM {self._table} WHERE {self._id_column} = :1"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                conn = await pool.acquire()
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(query, [entity_id])
                        deleted = cur.rowcount > 0
                    await conn.commit()
                finally:
                    await pool.release(conn)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"delete exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="delete",
                cause=exc,
            ) from exc
        except (oracledb.DatabaseError, oracledb.InterfaceError, OSError) as exc:
            raise self._wrap_oracledb(exc, "delete") from exc

        return deleted

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """
        Close the connection pool and remove it from the cache.

        Call once at application shutdown. After this returns the pool is
        closed and a subsequent operation will create a new pool.
        """
        key = _cache_key(self._settings)
        pool = _pool_cache.get(key)
        if pool is not None:
            await pool.close()
            _pool_cache.pop(key, None)

    # ------------------------------------------------------------------
    # BasePort (Identity + Lifecycle) interface
    # ------------------------------------------------------------------

    async def initialize(self, context: PluginContext) -> None:
        """
        Establish the connection pool and verify connectivity.

        BasePort lifecycle entry point. Reuses the same cached pool as
        every other method on this repository.

        Args:
            context: Plugin context. Unused — settings are provided at
                     construction time.

        Raises:
            AdapterConnectionError: Oracle is unreachable.
        """
        await get_oracle_pool(self._settings)
        health = await self.health()
        if health.status != PluginStatus.READY:
            raise AdapterConnectionError(
                health.message or "Oracle connectivity check failed during initialize()",
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
        ``SELECT 1 FROM DUAL`` against the pool with a 5-second timeout.
        Never raises.
        """
        try:
            pool = await get_oracle_pool(self._settings)
            conn = await asyncio.wait_for(pool.acquire(), timeout=5.0)
            try:
                async with asyncio.timeout(5.0):
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT 1 FROM DUAL")
                        await cur.fetchone()
            finally:
                await pool.release(conn)
            return PluginHealth(status=PluginStatus.READY, message="")
        except Exception as exc:  # noqa: BLE001
            return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
