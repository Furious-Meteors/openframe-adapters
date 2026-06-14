"""
openframe/adapters/db/postgres/repository.py
==============================================
Generic PostgreSQL repository implementing ``BaseRepository[T]`` and
``HealthCheck`` from ``openframe-core`` via structural subtyping.

The base class works with raw ``dict[str, Any]`` rows. Domain adapters
subclass it and override ``_row_to_entity()`` / ``_entity_to_row()`` to map
between rows and typed domain objects.

Usage — raw dict mode (no subclassing needed):

    repo = PostgresRepository(settings, table="items", id_column="id")
    item: dict | None = await repo.get("abc-123")

Usage — typed domain mode (subclass):

    class ItemRepository(PostgresRepository[Item]):
        _table = "items"
        _id_column = "id"

        def _row_to_entity(self, row: asyncpg.Record) -> Item:
            return Item(**dict(row))

        def _entity_to_row(self, entity: Item) -> dict[str, Any]:
            return entity.model_dump()

Structural conformance (no inheritance from Protocols required):

    assert isinstance(repo, BaseRepository)
    assert isinstance(repo, HealthCheck)
"""
from __future__ import annotations

import asyncio
from typing import Any, Generic, TypeVar

import asyncpg

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterQueryError,
    AdapterTimeoutError,
)

from .config import PostgresSettings
from .connection import _pool_cache, get_postgres_pool

__all__ = ["PostgresRepository"]

T = TypeVar("T")


class PostgresRepository(Generic[T]):
    """
    Generic PostgreSQL repository.

    Implements ``BaseRepository[T]`` and ``HealthCheck`` structurally — no
    inheritance from either Protocol. All asyncpg exceptions are caught and
    re-raised as ``AdapterError`` subclasses. Every operation wraps its
    asyncpg call in ``asyncio.timeout(settings.operation_timeout)``.

    Class attributes (override in subclass):
        _table:     Table name used when no ``table`` argument is passed.
        _id_column: Primary key column. Default ``"id"``.

    Args:
        settings:  A ``PostgresSettings`` instance.
        table:     Table name. Overrides the ``_table`` class attribute.
        id_column: PK column name. Overrides the ``_id_column`` class attribute.

    Raises:
        AdapterConfigurationError: If neither ``table`` param nor ``_table``
                                   class attribute is set.
    """

    _table: str = ""
    _id_column: str = "id"

    def __init__(
        self,
        settings: PostgresSettings,
        table: str | None = None,
        id_column: str | None = None,
    ) -> None:
        self._settings = settings
        self._table = table or self.__class__._table
        self._id_column = id_column or self.__class__._id_column

        if not self._table:
            raise AdapterConfigurationError(
                "PostgresRepository requires a table name. "
                "Pass table= to __init__ or set _table on the subclass.",
                adapter=settings.adapter_name,
                operation="init",
            )

    # ------------------------------------------------------------------
    # Row ↔ entity mapping (override in typed subclasses)
    # ------------------------------------------------------------------

    def _row_to_entity(self, row: asyncpg.Record) -> T:  # type: ignore[type-arg]
        """
        Convert an asyncpg Record to the entity type ``T``.

        Base implementation returns ``dict(row)``. Subclasses override this
        to return typed domain objects.
        """
        return dict(row)  # type: ignore[return-value]

    def _entity_to_row(self, entity: T) -> dict[str, Any]:
        """
        Convert the entity type ``T`` to a column→value dict for SQL.

        Base implementation returns the entity unchanged if it is already a
        dict, or falls back to ``vars(entity)`` for simple objects. Subclasses
        override this to serialise typed domain objects correctly.
        """
        if isinstance(entity, dict):
            return entity
        return vars(entity)

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
        pool = await get_postgres_pool(self._settings)
        query = (
            f"SELECT * FROM {self._table} "
            f"WHERE {self._id_column} = $1 LIMIT 1"
        )
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                row = await pool.fetchrow(query, entity_id)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"get exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="get",
                cause=exc,
            ) from exc
        except asyncpg.PostgresError as exc:
            raise AdapterQueryError(
                f"get failed on {self._table}: {exc}",
                adapter=self._settings.adapter_name,
                operation="get",
                cause=exc,
            ) from exc

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
            AdapterQueryError:   Query failed after connection was established.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        pool = await get_postgres_pool(self._settings)
        rows_query = (
            f"SELECT * FROM {self._table} "
            f"ORDER BY {self._id_column} LIMIT $1 OFFSET $2"
        )
        count_query = f"SELECT COUNT(*) FROM {self._table}"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                async with pool.acquire() as conn:
                    rows = await conn.fetch(rows_query, limit, offset)
                    count: int = await conn.fetchval(count_query)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"list exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="list",
                cause=exc,
            ) from exc
        except asyncpg.PostgresError as exc:
            raise AdapterQueryError(
                f"list failed on {self._table}: {exc}",
                adapter=self._settings.adapter_name,
                operation="list",
                cause=exc,
            ) from exc

        entities = [self._row_to_entity(r) for r in rows]
        return entities, count

    async def create(self, entity: T) -> T:
        """
        Insert a new row and return the stored row (with DB-generated fields).

        Calls ``_entity_to_row(entity)`` to obtain the column dict, then
        executes an ``INSERT … RETURNING *`` so that database-generated
        fields (e.g. auto-increment PK, ``created_at``) are included in the
        returned entity.

        Args:
            entity: The entity to insert.

        Returns:
            The entity as stored, including any backend-assigned fields.

        Raises:
            AdapterQueryError:   Insert failed (e.g. unique-constraint violation).
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        pool = await get_postgres_pool(self._settings)
        row_dict = self._entity_to_row(entity)
        columns = list(row_dict.keys())
        values = list(row_dict.values())
        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
        col_list = ", ".join(columns)
        query = (
            f"INSERT INTO {self._table} ({col_list}) "
            f"VALUES ({placeholders}) RETURNING *"
        )
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                row = await pool.fetchrow(query, *values)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"create exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="create",
                cause=exc,
            ) from exc
        except asyncpg.PostgresError as exc:
            raise AdapterQueryError(
                f"create failed on {self._table}: {exc}",
                adapter=self._settings.adapter_name,
                operation="create",
                cause=exc,
            ) from exc

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
            AdapterQueryError:   Update failed (e.g. constraint violation).
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        pool = await get_postgres_pool(self._settings)
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

        set_parts = [
            f"{col} = ${i + 1}" for i, col in enumerate(update_cols.keys())
        ]
        set_clause = ", ".join(set_parts)
        id_placeholder = f"${len(update_cols) + 1}"
        query = (
            f"UPDATE {self._table} SET {set_clause} "
            f"WHERE {self._id_column} = {id_placeholder} RETURNING *"
        )
        values = list(update_cols.values()) + [entity_id]
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                row = await pool.fetchrow(query, *values)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"update exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="update",
                cause=exc,
            ) from exc
        except asyncpg.PostgresError as exc:
            raise AdapterQueryError(
                f"update failed on {self._table}: {exc}",
                adapter=self._settings.adapter_name,
                operation="update",
                cause=exc,
            ) from exc

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
            AdapterQueryError:   Deletion failed.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        pool = await get_postgres_pool(self._settings)
        query = f"DELETE FROM {self._table} WHERE {self._id_column} = $1"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                status: str = await pool.execute(query, entity_id)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"delete exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="delete",
                cause=exc,
            ) from exc
        except asyncpg.PostgresError as exc:
            raise AdapterQueryError(
                f"delete failed on {self._table}: {exc}",
                adapter=self._settings.adapter_name,
                operation="delete",
                cause=exc,
            ) from exc

        # asyncpg returns "DELETE N" where N is the number of deleted rows.
        return status == "DELETE 1"

    # ------------------------------------------------------------------
    # HealthCheck interface
    # ------------------------------------------------------------------

    async def ping(self) -> bool:
        """
        Low-cost liveness check — ``SELECT 1`` with a 5-second timeout.

        Returns:
            ``True`` if the backend responded, ``False`` on any failure.
            Never raises.
        """
        try:
            pool = await get_postgres_pool(self._settings)
            await asyncio.wait_for(pool.fetchval("SELECT 1"), timeout=5.0)
            return True
        except Exception:  # noqa: BLE001
            return False

    async def is_ready(self) -> bool:
        """
        Full readiness check — verifies public schema tables are accessible.

        Queries ``pg_tables`` to confirm the database connection is healthy
        and the schema is queryable. Uses a 10-second timeout.

        Returns:
            ``True`` if the database is ready, ``False`` on any failure.
            Never raises.
        """
        try:
            pool = await get_postgres_pool(self._settings)
            await asyncio.wait_for(
                pool.fetchval(
                    "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public'"
                ),
                timeout=10.0,
            )
            return True
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """
        Close the connection pool and remove it from the cache.

        Call once at application shutdown. After this returns the pool is
        closed and a subsequent operation will create a new pool.
        """
        pool = _pool_cache.get(self._settings.database_url)
        if pool is not None:
            await pool.close()
            _pool_cache.pop(self._settings.database_url, None)
