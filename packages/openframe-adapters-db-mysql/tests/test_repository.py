"""
tests/test_repository.py — openframe-adapters-db-mysql
==========================================================
Contract tests (RepositoryContractTests) run first, then adapter-specific
unit tests covering MySQL error mapping and driver behaviour.
"""
from __future__ import annotations

import re
from unittest.mock import AsyncMock, MagicMock

import pytest

from openframe.adapters.db.mysql import MySQLRepository
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository
from openframe.core.testing import RepositoryContractTests


# ── In-memory fake cursor — simulates a MySQL table for contract tests ─────

class _FakeCursor:
    """
    A stateful fake aiomysql cursor backed by an in-memory dict "table".

    Supports exactly the query shapes MySQLRepository issues: single-row
    SELECT by id, paginated SELECT with LIMIT/OFFSET, COUNT(*), INSERT,
    UPDATE ... SET ... WHERE, and DELETE.
    """

    def __init__(self, store: dict[str, dict]) -> None:
        self._store = store
        self._result: dict | list | None = None
        self.rowcount = 0
        self.lastrowid = 0

    async def __aenter__(self) -> "_FakeCursor":
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    async def execute(self, query: str, args=None) -> None:
        args = tuple(args) if args else ()
        q = query.strip().upper()

        if q.startswith("INSERT"):
            m = re.search(r"\(([^)]+)\)\s*VALUES", query, re.IGNORECASE)
            cols = [c.strip() for c in m.group(1).split(",")] if m else []
            row = dict(zip(cols, args))
            self._store[str(row.get("id", ""))] = row
            self.rowcount = 1
            self.lastrowid = row.get("id") or 0
        elif q.startswith("UPDATE"):
            entity_id = str(args[-1])
            if entity_id in self._store:
                m = re.search(r"SET\s+(.+?)\s+WHERE", query, re.IGNORECASE)
                set_cols = (
                    [p.strip().split("=")[0].strip() for p in m.group(1).split(",")]
                    if m
                    else []
                )
                row = dict(self._store[entity_id])
                for i, col in enumerate(set_cols):
                    row[col] = args[i]
                self._store[entity_id] = row
                self.rowcount = 1
            else:
                self.rowcount = 0
        elif q.startswith("DELETE"):
            entity_id = str(args[0]) if args else ""
            if entity_id in self._store:
                del self._store[entity_id]
                self.rowcount = 1
            else:
                self.rowcount = 0
        elif "COUNT(*)" in q:
            self._result = {"count": len(self._store)}
        elif "LIMIT" in q and "OFFSET" in q:
            limit, offset = args[0], args[1]
            items = list(self._store.values())
            self._result = items[offset : offset + limit]
        elif q.startswith("SELECT"):
            entity_id = str(args[0]) if args else ""
            self._result = self._store.get(entity_id)
        else:
            self._result = None

    async def fetchone(self):
        if isinstance(self._result, list):
            return self._result[0] if self._result else None
        return self._result

    async def fetchall(self):
        if isinstance(self._result, list):
            return self._result
        return []


# ── Contract tests — must pass for every BaseRepository implementation ─────

class TestMySQLRepositoryContracts(RepositoryContractTests):
    """
    MySQLRepository passes the full openframe contract suite.

    All RepositoryContractTests run against a mocked aiomysql pool backed
    by an in-memory dict "table". No real database required.
    """

    @pytest.fixture
    def repository(self, mock_settings, mock_pool):
        import openframe.adapters.db.mysql.connection as conn_module

        conn_module._pool_cache[conn_module._cache_key(mock_settings)] = mock_pool

        _store: dict[str, dict] = {}
        conn = mock_pool.acquire.return_value
        conn.cursor = MagicMock(side_effect=lambda *a, **kw: _FakeCursor(_store))
        conn.commit = AsyncMock()

        r = MySQLRepository(mock_settings, table="items", id_column="id")
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


class TestProtocolConformance:
    def test_isinstance_base_repository(self, repo: MySQLRepository) -> None:
        assert isinstance(repo, BaseRepository)


class TestInit:
    def test_missing_table_raises_configuration_error(self, mock_settings) -> None:
        with pytest.raises(AdapterConfigurationError):
            MySQLRepository(mock_settings)

    def test_table_from_init_arg(self, mock_settings) -> None:
        repo = MySQLRepository(mock_settings, table="orders")
        assert repo._table == "orders"

    def test_table_from_class_attribute(self, mock_settings) -> None:
        class OrderRepo(MySQLRepository):
            _table = "orders"

        repo = OrderRepo(mock_settings)
        assert repo._table == "orders"


class TestGet:
    async def test_get_found_returns_dict(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        row_data = {"id": "1", "name": "widget"}
        mock_cursor.fetchone.return_value = row_data
        result = await repo.get("1")
        mock_cursor.execute.assert_called_once()
        assert result == row_data

    async def test_get_not_found_returns_none(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.fetchone.return_value = None
        result = await repo.get("missing")
        assert result is None

    async def test_get_query_error_raises_adapter_query_error(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        import pymysql.err
        mock_cursor.execute.side_effect = pymysql.err.ProgrammingError("bad syntax")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_timeout_raises_adapter_timeout_error(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        import asyncio
        mock_cursor.execute.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_lock_wait_timeout_raises_adapter_query_error(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        """
        Regression test for the classification gotcha: MySQL error code 1205
        ("Lock wait timeout exceeded") is raised as pymysql.err.OperationalError
        — the SAME exception class used for genuine connection failures — but
        it is a query-level failure, not a broken connection. A naive
        isinstance(exc, OperationalError) check would misclassify it as
        AdapterConnectionError (retryable), which is wrong: retrying the same
        query immediately is likely to hit the same lock contention again,
        and callers need AdapterQueryError's non-retryable semantics here.
        """
        import pymysql.err
        mock_cursor.execute.side_effect = pymysql.err.OperationalError(
            1205, "Lock wait timeout exceeded; try restarting transaction"
        )
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    @pytest.mark.parametrize("error_code", [2002, 2003, 2006, 2013])
    async def test_get_lost_connection_raises_adapter_connection_error(
        self, repo: MySQLRepository, mock_cursor: MagicMock, error_code: int
    ) -> None:
        """
        A connection dropped mid-query must surface as AdapterConnectionError
        (retryable), not AdapterQueryError — matching PostgresRepository's/
        MongoRepository's connection-vs-query distinction. Regression test
        for the classification gotcha: these codes share
        pymysql.err.OperationalError with query-class code 1205, so the
        classifier must inspect the numeric code, not just the exception type.
        """
        import pymysql.err
        mock_cursor.execute.side_effect = pymysql.err.OperationalError(
            error_code, "connection lost"
        )
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"
        assert exc_info.value.retryable is True

    async def test_get_interface_error_raises_adapter_connection_error(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        import pymysql.err
        mock_cursor.execute.side_effect = pymysql.err.InterfaceError("cursor closed")
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"


class TestList:
    async def test_list_returns_rows_and_count(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        row_data = [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]
        mock_cursor.fetchall.return_value = row_data
        mock_cursor.fetchone.return_value = {"count": 42}

        entities, count = await repo.list(limit=10, offset=0)
        assert entities == row_data
        assert count == 42

    async def test_list_query_error_raises_adapter_query_error(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        import pymysql.err
        mock_cursor.execute.side_effect = pymysql.err.ProgrammingError("oops")
        with pytest.raises(AdapterQueryError):
            await repo.list(limit=10, offset=0)

    async def test_list_timeout_raises_adapter_timeout_error(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        import asyncio
        mock_cursor.execute.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.list(limit=10, offset=0)


class TestCreate:
    async def test_create_returns_stored_row(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        stored = {"id": "99", "name": "thing"}
        mock_cursor.fetchone.return_value = stored
        result = await repo.create({"name": "thing"})
        assert mock_cursor.execute.call_count == 2
        assert result == stored

    async def test_create_integrity_error_raises_adapter_query_error(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        import pymysql.err
        mock_cursor.execute.side_effect = pymysql.err.IntegrityError(1062, "dup")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.create({"id": "1", "name": "x"})
        assert exc_info.value.operation == "create"

    async def test_create_timeout_raises_adapter_timeout_error(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        import asyncio
        mock_cursor.execute.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.create({"id": "1", "name": "x"})


class TestUpdate:
    async def test_update_found_returns_updated_row(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        updated = {"id": "1", "name": "updated"}
        mock_cursor.rowcount = 1
        mock_cursor.fetchone.return_value = updated
        result = await repo.update({"id": "1", "name": "updated"})
        assert result == updated

    async def test_update_not_found_returns_none(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.rowcount = 0
        result = await repo.update({"id": "999", "name": "ghost"})
        assert result is None

    async def test_update_query_error_raises_adapter_query_error(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        import pymysql.err
        mock_cursor.execute.side_effect = pymysql.err.ProgrammingError("fail")
        with pytest.raises(AdapterQueryError):
            await repo.update({"id": "1", "name": "x"})

    async def test_update_timeout_raises_adapter_timeout_error(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        import asyncio
        mock_cursor.execute.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.update({"id": "1", "name": "x"})


class TestDelete:
    async def test_delete_existing_returns_true(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.rowcount = 1
        result = await repo.delete("1")
        assert result is True

    async def test_delete_missing_returns_false(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.rowcount = 0
        result = await repo.delete("missing")
        assert result is False

    async def test_delete_query_error_raises_adapter_query_error(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        import pymysql.err
        mock_cursor.execute.side_effect = pymysql.err.ProgrammingError("nope")
        with pytest.raises(AdapterQueryError):
            await repo.delete("1")

    async def test_delete_timeout_raises_adapter_timeout_error(
        self, repo: MySQLRepository, mock_cursor: MagicMock
    ) -> None:
        import asyncio
        mock_cursor.execute.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.delete("1")


class TestClose:
    async def test_close_removes_pool_from_cache(
        self, repo: MySQLRepository, mock_settings, mock_pool
    ) -> None:
        import openframe.adapters.db.mysql.connection as conn_module

        key = conn_module._cache_key(mock_settings)
        assert key in conn_module._pool_cache
        await repo.close()
        assert key not in conn_module._pool_cache
        mock_pool.close.assert_called_once()
        mock_pool.wait_closed.assert_awaited_once()
