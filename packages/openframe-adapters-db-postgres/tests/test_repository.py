"""
tests/test_repository.py — openframe-adapters-db-postgres
===========================================================
Contract tests (RepositoryContractTests) run first, then adapter-specific
unit tests covering PostgreSQL error mapping and driver behaviour.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from openframe.adapters.db.postgres import PostgresRepository
from openframe.core.exceptions import AdapterConfigurationError, AdapterQueryError, AdapterTimeoutError
from openframe.core.ports import BaseRepository
from openframe.core.testing import RepositoryContractTests


# ── Contract tests — must pass for every BaseRepository implementation ─────

class TestPostgresRepositoryContracts(RepositoryContractTests):
    """
    PostgresRepository passes the full openframe contract suite.

    All 18 RepositoryContractTests run against a mocked asyncpg pool.
    No real database required.
    """

    @pytest.fixture
    def repository(self, mock_settings, mock_pool):
        """
        PostgresRepository backed by a stateful in-memory mock pool.

        The mock pool tracks insertions, updates, and deletions so that the
        RepositoryContractTests behavioural assertions (create → get, list
        pagination, etc.) pass without a real database.
        """
        import re
        from unittest.mock import AsyncMock

        import openframe.adapters.db.postgres.connection as conn_module

        conn_module._pool_cache[mock_settings.database_url] = mock_pool

        # In-memory store that simulates the database table.
        _store: dict[str, dict] = {}

        def _fetchrow(query: str, *args):
            q = query.upper()
            if "INSERT" in q:
                # Parse column list from: INSERT INTO tbl (col1, col2) VALUES ...
                m = re.search(r"\(([^)]+)\)\s*VALUES", query, re.IGNORECASE)
                cols = [c.strip() for c in m.group(1).split(",")] if m else []
                row = dict(zip(cols, args))
                _store[str(row.get("id", ""))] = row
                return row
            if "UPDATE" in q:
                # Last arg is the WHERE id value.
                entity_id = str(args[-1])
                if entity_id not in _store:
                    return None
                # Parse SET columns: UPDATE tbl SET col=$1,... WHERE id=$N
                m = re.search(r"SET\s+(.+?)\s+WHERE", query, re.IGNORECASE)
                set_cols = (
                    [p.strip().split("=")[0].strip() for p in m.group(1).split(",")]
                    if m
                    else []
                )
                row = dict(_store[entity_id])
                for i, col in enumerate(set_cols):
                    row[col] = args[i]
                _store[entity_id] = row
                return row
            # SELECT — first arg is entity_id.
            entity_id = str(args[0]) if args else ""
            return _store.get(entity_id)

        def _execute(query: str, *args):
            if "DELETE" in query.upper():
                entity_id = str(args[0]) if args else ""
                if entity_id in _store:
                    del _store[entity_id]
                    return "DELETE 1"
                return "DELETE 0"
            return ""

        def _conn_fetch(query: str, limit: int, offset: int):
            items = list(_store.values())
            return items[offset : offset + limit]

        def _conn_fetchval(query: str):
            return len(_store)

        mock_pool.fetchrow = AsyncMock(side_effect=_fetchrow)
        mock_pool.execute = AsyncMock(side_effect=_execute)
        mock_pool.fetchval = AsyncMock(return_value=1)  # ping / is_ready
        conn = mock_pool.acquire.return_value
        conn.fetch = AsyncMock(side_effect=_conn_fetch)
        conn.fetchval = AsyncMock(side_effect=_conn_fetchval)

        r = PostgresRepository(mock_settings, table="items", id_column="id")
        yield r
        conn_module._pool_cache.clear()

    @pytest.fixture
    def port(self, repository):
        return repository

    @pytest.fixture
    def make_entity(self):
        def _make(id: str, name: str = "test") -> dict:
            return {"id": id, "name": name}
        return _make


# ── Adapter-specific tests — beyond what the contract covers ───────────────


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_record(data: dict) -> MagicMock:
    """Simulate an asyncpg.Record — dict-castable MagicMock."""
    record = MagicMock()
    record.__iter__ = MagicMock(return_value=iter(data.items()))
    record.keys = MagicMock(return_value=list(data.keys()))
    record.items = MagicMock(return_value=data.items())
    # dict(record) calls keys() then __getitem__
    record.__getitem__ = MagicMock(side_effect=data.__getitem__)
    return record


class TestProtocolConformance:
    def test_isinstance_base_repository(self, repo: PostgresRepository) -> None:
        assert isinstance(repo, BaseRepository)


class TestInit:
    def test_missing_table_raises_configuration_error(self, mock_settings) -> None:
        with pytest.raises(AdapterConfigurationError):
            PostgresRepository(mock_settings)

    def test_table_from_init_arg(self, mock_settings) -> None:
        repo = PostgresRepository(mock_settings, table="orders")
        assert repo._table == "orders"

    def test_table_from_class_attribute(self, mock_settings) -> None:
        class OrderRepo(PostgresRepository):
            _table = "orders"

        repo = OrderRepo(mock_settings)
        assert repo._table == "orders"


class TestGet:
    async def test_get_found_returns_dict(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        row_data = {"id": "1", "name": "widget"}
        mock_pool.fetchrow.return_value = row_data
        result = await repo.get("1")
        mock_pool.fetchrow.assert_called_once()
        assert result == row_data

    async def test_get_not_found_returns_none(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.fetchrow.return_value = None
        result = await repo.get("missing")
        assert result is None

    async def test_get_postgres_error_raises_adapter_query_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncpg
        mock_pool.fetchrow.side_effect = asyncpg.PostgresError("boom")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_timeout_raises_adapter_timeout_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncio
        mock_pool.fetchrow.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"


class TestList:
    async def test_list_returns_rows_and_count(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        row_data = [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]
        conn = mock_pool.acquire.return_value
        conn.fetch.return_value = row_data
        conn.fetchval.return_value = 42

        entities, count = await repo.list(limit=10, offset=0)
        assert entities == row_data
        assert count == 42

    async def test_list_postgres_error_raises_adapter_query_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncpg
        conn = mock_pool.acquire.return_value
        conn.fetch.side_effect = asyncpg.PostgresError("oops")
        with pytest.raises(AdapterQueryError):
            await repo.list(limit=10, offset=0)

    async def test_list_timeout_raises_adapter_timeout_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncio
        conn = mock_pool.acquire.return_value
        conn.fetch.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.list(limit=10, offset=0)


class TestCreate:
    async def test_create_returns_stored_row(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        stored = {"id": "99", "name": "thing"}
        mock_pool.fetchrow.return_value = stored
        result = await repo.create({"name": "thing"})
        mock_pool.fetchrow.assert_called_once()
        assert result == stored

    async def test_create_postgres_error_raises_adapter_query_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncpg
        mock_pool.fetchrow.side_effect = asyncpg.UniqueViolationError("dup")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.create({"id": "1", "name": "x"})
        assert exc_info.value.operation == "create"

    async def test_create_timeout_raises_adapter_timeout_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncio
        mock_pool.fetchrow.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.create({"id": "1", "name": "x"})


class TestUpdate:
    async def test_update_found_returns_updated_row(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        updated = {"id": "1", "name": "updated"}
        mock_pool.fetchrow.return_value = updated
        result = await repo.update({"id": "1", "name": "updated"})
        assert result == updated

    async def test_update_not_found_returns_none(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.fetchrow.return_value = None
        result = await repo.update({"id": "999", "name": "ghost"})
        assert result is None

    async def test_update_postgres_error_raises_adapter_query_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncpg
        mock_pool.fetchrow.side_effect = asyncpg.PostgresError("fail")
        with pytest.raises(AdapterQueryError):
            await repo.update({"id": "1", "name": "x"})

    async def test_update_timeout_raises_adapter_timeout_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncio
        mock_pool.fetchrow.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.update({"id": "1", "name": "x"})


class TestDelete:
    async def test_delete_existing_returns_true(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.execute.return_value = "DELETE 1"
        result = await repo.delete("1")
        assert result is True

    async def test_delete_missing_returns_false(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.execute.return_value = "DELETE 0"
        result = await repo.delete("missing")
        assert result is False

    async def test_delete_postgres_error_raises_adapter_query_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncpg
        mock_pool.execute.side_effect = asyncpg.PostgresError("nope")
        with pytest.raises(AdapterQueryError):
            await repo.delete("1")

    async def test_delete_timeout_raises_adapter_timeout_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncio
        mock_pool.execute.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.delete("1")
