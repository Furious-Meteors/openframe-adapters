"""
openframe/adapters/db/mariadb/repository.py
==============================================
Generic MariaDB repository implementing ``BaseRepository[T]``
from ``openframe-core`` via structural subtyping.

The base class works with raw ``dict[str, Any]`` rows. Domain adapters
subclass it and override ``_row_to_entity()`` / ``_entity_to_row()`` to map
between rows and typed domain objects.

Usage — raw dict mode (no subclassing needed):

    repo = MariadbRepository(settings, table="items", id_column="id")
    item: dict | None = await repo.get("abc-123")

Usage — typed domain mode (subclass):

    class ItemRepository(MariadbRepository[Item]):
        _table = "items"
        _id_column = "id"

        def _row_to_entity(self, row: dict) -> Item:
            return Item(**row)

        def _entity_to_row(self, entity: Item) -> dict[str, Any]:
            return entity.model_dump()

Structural conformance (no inheritance from Protocols required):

    assert isinstance(repo, BaseRepository)

Driver note: MariaDB is wire-protocol-compatible with MySQL, so this adapter
uses the exact same ``aiomysql`` driver (which wraps ``PyMySQL``) as
``openframe-adapters-db-mysql`` — there is no MariaDB-specific Python driver
involved.

Exception-classification gotcha (verified against the installed driver, see
``connection.py``'s module docstring and this module's ``_wrap_pymysql``):
PyMySQL's ``pymysql.err.MySQLError`` IS the common base of every exception
this driver can raise — ``InterfaceError``, ``DatabaseError``,
``OperationalError``, ``IntegrityError``, ``ProgrammingError``,
``DataError``, ``InternalError`` and ``NotSupportedError`` all subclass it
(``pymysql.err.Error`` sits directly under it, and every other exception
sits under ``Error`` or ``DatabaseError``). Unlike asyncpg — where
``InterfaceError`` is NOT a subclass of ``PostgresError`` and must be caught
separately — a single ``except pymysql.err.MySQLError`` at each call site is
verified sufficient here. What still requires care is
``pymysql.err.OperationalError`` itself: it is overloaded to represent BOTH
genuine connection failures (error codes 2002/2003/2006/2013 — unreachable
host, dropped socket, server gone away) AND purely in-band query failures
that have nothing to do with connectivity (error code 1205 — "Lock wait
timeout exceeded"). ``_wrap_pymysql`` inspects ``exc.args[0]`` (the numeric
error code PyMySQL always puts first) to tell these apart instead of
trusting ``isinstance(exc, OperationalError)`` alone.

MySQL/MariaDB error-code parity: all the codes this classifier keys off
(2002/2003/2006/2013/1205) predate the MySQL/MariaDB fork and are shared
between both server families, so this classification behaves identically
against a MySQL or a MariaDB server. This is worth stating explicitly rather
than assuming it holds forever — newer, MySQL-version-specific error codes
introduced after the fork (and MariaDB-only extensions such as its Aria
storage engine or MariaDB-specific system variables) are not covered by, or
guaranteed to match, this code list.
"""
from __future__ import annotations

import asyncio
from typing import Any, Generic, TypeVar

import aiomysql
import pymysql.err

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository, Capability, PluginContext, PluginHealth, PluginStatus

from .config import MariadbSettings
from .connection import _cache_key, _pool_cache, get_mariadb_pool

__all__ = ["MariadbRepository"]

T = TypeVar("T")

# Error codes (exc.args[0] on pymysql.err.OperationalError) that indicate a
# broken/lost/unreachable connection rather than an in-band query failure.
# Raised mid-query (e.g. the connection drops while a statement is in
# flight), distinct from the connect-time errors handled in
# connection.get_mariadb_pool(). Pre-fork, shared codes — identical on
# MySQL and MariaDB servers.
_CONNECTION_ERROR_CODES = frozenset({
    2002,  # Can't connect through socket
    2003,  # Can't connect to MySQL server
    2006,  # MySQL server has gone away
    2013,  # Lost connection to MySQL server during query
})


class MariadbRepository(Generic[T]):
    """
    Generic MariaDB repository.

    Implements ``BaseRepository[T]`` structurally — no
    inheritance from the Protocol. All PyMySQL exceptions are caught and
    re-raised as ``AdapterError`` subclasses. Every operation wraps its
    aiomysql call in ``asyncio.timeout(settings.operation_timeout)``.

    Health check: ``health()`` is the sole health check on this repository.
    It verifies backend connectivity and returns a ``PluginHealth``
    snapshot describing the result — never raises.

    Class attributes (override in subclass):
        _table:     Table name used when no ``table`` argument is passed.
        _id_column: Primary key column. Default ``"id"``.

    Args:
        settings:  A ``MariadbSettings`` instance.
        table:     Table name. Overrides the ``_table`` class attribute.
        id_column: PK column name. Overrides the ``_id_column`` class attribute.

    Raises:
        AdapterConfigurationError: If neither ``table`` param nor ``_table``
                                   class attribute is set.
    """

    _table: str = ""
    _id_column: str = "id"

    name:       str = "openframe-mariadb-repository"
    version:    str = "0.1.0"
    capability: Capability = Capability.PERSISTENCE

    def __init__(
        self,
        settings: MariadbSettings,
        table: str | None = None,
        id_column: str | None = None,
    ) -> None:
        self._settings = settings
        self._table = table or self.__class__._table
        self._id_column = id_column or self.__class__._id_column

        if not self._table:
            raise AdapterConfigurationError(
                "MariadbRepository requires a table name. "
                "Pass table= to __init__ or set _table on the subclass.",
                adapter=settings.adapter_name,
                operation="init",
            )

    # ------------------------------------------------------------------
    # Row <-> entity mapping (override in typed subclasses)
    # ------------------------------------------------------------------

    def _row_to_entity(self, row: dict[str, Any]) -> T:
        """
        Convert a raw cursor row dict to the entity type ``T``.

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

    # ------------------------------------------------------------------
    # Exception mapping helper
    # ------------------------------------------------------------------

    def _wrap_pymysql(
        self, exc: Exception, operation: str
    ) -> AdapterQueryError | AdapterConnectionError:
        """
        Map a ``pymysql.err.MySQLError`` to the appropriate ``AdapterError``
        subclass.

        Distinguishes a lost/broken connection (``AdapterConnectionError`` —
        retryable) from an in-band query failure such as a constraint
        violation or lock-wait timeout (``AdapterQueryError`` — not
        retryable by default), the same distinction
        ``PostgresRepository``/``MongoRepository``/``MySQLRepository``
        already make. ``pymysql.err.OperationalError`` is overloaded to
        cover both cases, so its numeric error code is inspected rather than
        trusting the exception's Python type alone. Caller must
        ``raise ... from exc`` at the call site.
        """
        if isinstance(exc, pymysql.err.OperationalError):
            code = exc.args[0] if exc.args else None
            if code in _CONNECTION_ERROR_CODES:
                return AdapterConnectionError(
                    f"{operation} failed — connection to MariaDB was lost: {exc}",
                    adapter=self._settings.adapter_name,
                    operation=operation,
                    cause=exc,
                )
            # e.g. code 1205 "Lock wait timeout exceeded" — a query-level
            # failure, not a broken connection.
            return AdapterQueryError(
                f"{operation} failed on {self._table}: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        if isinstance(exc, pymysql.err.InterfaceError):
            # e.g. "cursor closed", "pool is closed" — treated as a
            # connection-class failure even though it has no numeric code.
            return AdapterConnectionError(
                f"{operation} failed — MariaDB interface error: {exc}",
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
            AdapterQueryError:      Query failed after connection was established.
            AdapterConnectionError: Connection to MariaDB was lost mid-query.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        pool = await get_mariadb_pool(self._settings)
        query = (
            f"SELECT * FROM {self._table} "
            f"WHERE {self._id_column} = %s LIMIT 1"
        )
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                async with pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:  # type: ignore[name-defined]
                        await cur.execute(query, (entity_id,))
                        row = await cur.fetchone()
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"get exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="get",
                cause=exc,
            ) from exc
        except pymysql.err.MySQLError as exc:
            raise self._wrap_pymysql(exc, "get") from exc

        if row is None:
            return None
        return self._row_to_entity(row)

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
            AdapterQueryError:      Query failed after connection was established.
            AdapterConnectionError: Connection to MariaDB was lost mid-query.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        pool = await get_mariadb_pool(self._settings)
        rows_query = (
            f"SELECT * FROM {self._table} "
            f"ORDER BY {self._id_column} LIMIT %s OFFSET %s"
        )
        count_query = f"SELECT COUNT(*) AS count FROM {self._table}"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                async with pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:  # type: ignore[name-defined]
                        await cur.execute(rows_query, (limit, offset))
                        rows = await cur.fetchall()
                        await cur.execute(count_query)
                        count_row = await cur.fetchone()
                        count: int = count_row["count"] if count_row else 0
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"list exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="list",
                cause=exc,
            ) from exc
        except pymysql.err.MySQLError as exc:
            raise self._wrap_pymysql(exc, "list") from exc

        entities = [self._row_to_entity(r) for r in rows]
        return entities, count

    async def create(self, entity: T) -> T:
        """
        Insert a new row and return the stored row (with DB-generated fields).

        Calls ``_entity_to_row(entity)`` to obtain the column dict, executes
        an ``INSERT``, then re-fetches the row by primary key (falling back
        to the driver's ``lastrowid`` when the entity itself supplies no id,
        e.g. an auto-increment column) so database-generated fields are
        included in the returned entity.

        Args:
            entity: The entity to insert.

        Returns:
            The entity as stored, including any backend-assigned fields.

        Raises:
            AdapterQueryError:      Insert failed (e.g. unique-constraint violation).
            AdapterConnectionError: Connection to MariaDB was lost mid-query.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        pool = await get_mariadb_pool(self._settings)
        row_dict = self._entity_to_row(entity)
        columns = list(row_dict.keys())
        values = list(row_dict.values())
        placeholders = ", ".join(["%s"] * len(columns))
        col_list = ", ".join(columns)
        query = f"INSERT INTO {self._table} ({col_list}) VALUES ({placeholders})"
        select_query = f"SELECT * FROM {self._table} WHERE {self._id_column} = %s LIMIT 1"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                async with pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:  # type: ignore[name-defined]
                        await cur.execute(query, values)
                        entity_id = row_dict.get(self._id_column, cur.lastrowid)
                        await cur.execute(select_query, (entity_id,))
                        row = await cur.fetchone()
                    await conn.commit()
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"create exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="create",
                cause=exc,
            ) from exc
        except pymysql.err.MySQLError as exc:
            raise self._wrap_pymysql(exc, "create") from exc

        return self._row_to_entity(row)  # type: ignore[arg-type]

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
            AdapterQueryError:      Update failed (e.g. constraint violation).
            AdapterConnectionError: Connection to MariaDB was lost mid-query.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        pool = await get_mariadb_pool(self._settings)
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

        set_clause = ", ".join(f"{col} = %s" for col in update_cols.keys())
        query = f"UPDATE {self._table} SET {set_clause} WHERE {self._id_column} = %s"
        select_query = f"SELECT * FROM {self._table} WHERE {self._id_column} = %s LIMIT 1"
        values = list(update_cols.values()) + [entity_id]
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                async with pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:  # type: ignore[name-defined]
                        await cur.execute(query, values)
                        matched = cur.rowcount
                        if matched:
                            await cur.execute(select_query, (entity_id,))
                            row = await cur.fetchone()
                        else:
                            row = None
                    await conn.commit()
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"update exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="update",
                cause=exc,
            ) from exc
        except pymysql.err.MySQLError as exc:
            raise self._wrap_pymysql(exc, "update") from exc

        if row is None:
            return None
        return self._row_to_entity(row)

    async def delete(self, entity_id: str) -> bool:
        """
        Delete a row by primary key.

        Args:
            entity_id: Value of the ``_id_column`` to delete.

        Returns:
            ``True`` if a row was deleted, ``False`` if no row matched.

        Raises:
            AdapterQueryError:      Deletion failed.
            AdapterConnectionError: Connection to MariaDB was lost mid-query.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        pool = await get_mariadb_pool(self._settings)
        query = f"DELETE FROM {self._table} WHERE {self._id_column} = %s"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(query, (entity_id,))
                        deleted = cur.rowcount
                    await conn.commit()
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"delete exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="delete",
                cause=exc,
            ) from exc
        except pymysql.err.MySQLError as exc:
            raise self._wrap_pymysql(exc, "delete") from exc

        return deleted == 1

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
            pool.close()
            await pool.wait_closed()
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
            AdapterConnectionError: MariaDB is unreachable.
        """
        await get_mariadb_pool(self._settings)
        health = await self.health()
        if health.status != PluginStatus.READY:
            raise AdapterConnectionError(
                health.message or "MariaDB connectivity check failed during initialize()",
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
        ``SELECT 1`` against the pool with a 5-second timeout. Never raises.
        """
        try:
            pool = await get_mariadb_pool(self._settings)

            async def _ping() -> None:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT 1")
                        await cur.fetchone()

            await asyncio.wait_for(_ping(), timeout=5.0)
            return PluginHealth(status=PluginStatus.READY, message="")
        except Exception as exc:  # noqa: BLE001
            return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
