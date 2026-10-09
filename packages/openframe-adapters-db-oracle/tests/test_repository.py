"""
tests/test_repository.py — openframe-adapters-db-oracle
===========================================================
Contract tests (RepositoryContractTests) run first, then adapter-specific
unit tests covering Oracle error mapping and driver behaviour.
"""
from __future__ import annotations

import re
from unittest.mock import AsyncMock, MagicMock

import pytest
import oracledb

from openframe.adapters.db.oracle import OracleRepository
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository
from openframe.core.testing import RepositoryContractTests


def make_oracle_error(exc_cls, full_code: str, message: str = "boom"):
    """See tests/test_connection.py for the shape this mirrors."""
    err = MagicMock()
    err.full_code = full_code
    err.code = 0
    err.message = message
    return exc_cls(err)


class _FakeCursor:
    """
    Simulates oracledb.AsyncCursor against an in-memory dict store, parsing
    the exact SQL shapes OracleRepository generates. Supports `async with`.
    """

    def __init__(self, store: dict, id_column: str) -> None:
        self._store = store
        self._id_column = id_column
        self.description: list[tuple] = []
        self.rowcount = 0
        self._result_rows: list[tuple] = []

    async def __aenter__(self) -> "_FakeCursor":
        return self

    async def __aexit__(self, *exc_info) -> bool:
        return False

    async def execute(self, query: str, params: list | None = None):
        params = params or []
        q = query.upper()
        if "INSERT" in q:
            m = re.search(r"\(([^)]+)\)\s*VALUES", query, re.IGNORECASE)
            cols = [c.strip() for c in m.group(1).split(",")] if m else []
            row = dict(zip(cols, params))
            self._store[str(row.get(self._id_column, ""))] = row
            self.rowcount = 1
        elif "UPDATE" in q:
            entity_id = str(params[-1])
            if entity_id in self._store:
                m = re.search(r"SET\s+(.+?)\s+WHERE", query, re.IGNORECASE)
                set_cols = (
                    [p.strip().split("=")[0].strip() for p in m.group(1).split(",")]
                    if m
                    else []
                )
                row = dict(self._store[entity_id])
                for i, col in enumerate(set_cols):
                    row[col] = params[i]
                self._store[entity_id] = row
                self.rowcount = 1
            else:
                self.rowcount = 0
        elif "DELETE" in q:
            entity_id = str(params[0]) if params else ""
            if entity_id in self._store:
                del self._store[entity_id]
                self.rowcount = 1
            else:
                self.rowcount = 0
        elif "COUNT" in q:
            self._result_rows = [(len(self._store),)]
            self.description = [("COUNT(*)",)]
        elif "OFFSET" in q and "FETCH NEXT" in q:
            offset, limit = params[0], params[1]
            items = list(self._store.values())
            page = items[offset : offset + limit]
            self._result_rows = [tuple(r.values()) for r in page]
            self.description = [(k,) for k in (page[0].keys() if page else [])]
        elif "SELECT" in q:
            entity_id = str(params[0]) if params else ""
            row = self._store.get(entity_id)
            if row:
                self._result_rows = [tuple(row.values())]
                self.description = [(k,) for k in row.keys()]
            else:
                self._result_rows = []
                self.description = []
        return self

    async def fetchone(self):
        if self._result_rows:
            return self._result_rows.pop(0)
        return None

    async def fetchall(self):
        rows = self._result_rows
        self._result_rows = []
        return rows


# ── Contract tests — must pass for every BaseRepository implementation ─────

class TestOracleRepositoryContracts(RepositoryContractTests):
    """
    OracleRepository passes the full openframe contract suite.

    All RepositoryContractTests run against a mocked oracledb pool.
    No real Oracle database required.
    """

    @pytest.fixture
    def repository(self, mock_settings, mock_pool):
        import openframe.adapters.db.oracle.connection as conn_module

        conn_module._pool_cache[conn_module._cache_key(mock_settings)] = mock_pool

        store: dict[str, dict] = {}
        conn = mock_pool._conn
        conn.cursor = MagicMock(side_effect=lambda: _FakeCursor(store, "id"))

        r = OracleRepository(mock_settings, table="items", id_column="id")
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
    def test_isinstance_base_repository(self, repo: OracleRepository) -> None:
        assert isinstance(repo, BaseRepository)


class TestInit:
    def test_missing_table_raises_configuration_error(self, mock_settings) -> None:
        with pytest.raises(AdapterConfigurationError):
            OracleRepository(mock_settings)

    def test_table_from_init_arg(self, mock_settings) -> None:
        repo = OracleRepository(mock_settings, table="orders")
        assert repo._table == "orders"

    def test_table_from_class_attribute(self, mock_settings) -> None:
        class OrderRepo(OracleRepository):
            _table = "orders"

        repo = OrderRepo(mock_settings)
        assert repo._table == "orders"


def _set_cursor_get_result(mock_pool, row_dict: dict | None) -> None:
    """Wire the mock pool's cursor to return `row_dict` from a `get`-style query."""
    cur = MagicMock()
    cur.execute = AsyncMock(return_value=cur)
    if row_dict is not None:
        cur.description = [(k,) for k in row_dict.keys()]
        cur.fetchone = AsyncMock(return_value=tuple(row_dict.values()))
    else:
        cur.description = []
        cur.fetchone = AsyncMock(return_value=None)
    cur.__aenter__ = AsyncMock(return_value=cur)
    cur.__aexit__ = AsyncMock(return_value=False)
    mock_pool._conn.cursor = MagicMock(return_value=cur)
    return cur


class TestGet:
    async def test_get_found_returns_dict(self, repo: OracleRepository, mock_pool: MagicMock) -> None:
        row_data = {"ID": "1", "NAME": "widget"}
        _set_cursor_get_result(mock_pool, row_data)
        result = await repo.get("1")
        assert result == row_data

    async def test_get_not_found_returns_none(self, repo: OracleRepository, mock_pool: MagicMock) -> None:
        _set_cursor_get_result(mock_pool, None)
        result = await repo.get("missing")
        assert result is None

    async def test_get_database_error_raises_adapter_query_error(
        self, repo: OracleRepository, mock_pool: MagicMock
    ) -> None:
        cur = _set_cursor_get_result(mock_pool, None)
        cur.execute = AsyncMock(side_effect=oracledb.DatabaseError("boom"))
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_timeout_raises_adapter_timeout_error(
        self, repo: OracleRepository, mock_pool: MagicMock
    ) -> None:
        import asyncio

        cur = _set_cursor_get_result(mock_pool, None)
        cur.execute = AsyncMock(side_effect=asyncio.TimeoutError())
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_interface_error_raises_adapter_connection_error(
        self, repo: OracleRepository, mock_pool: MagicMock
    ) -> None:
        """
        oracledb.InterfaceError is NOT a subclass of oracledb.DatabaseError
        (verified against the installed driver — see repository.py's
        module-level comment), so it must be explicitly caught alongside it
        or it would propagate as a raw, untranslated driver exception.
        """
        cur = _set_cursor_get_result(mock_pool, None)
        cur.execute = AsyncMock(side_effect=oracledb.InterfaceError("not connected"))
        with pytest.raises(AdapterConnectionError):
            await repo.get("1")

    async def test_get_operational_error_raises_adapter_connection_error(
        self, repo: OracleRepository, mock_pool: MagicMock
    ) -> None:
        """
        Regression test for the connection-vs-query classification bug this
        checklist guards against: oracledb.OperationalError (e.g. ORA-03113
        "end-of-file on communication channel" — a genuine connection-class
        ORA code) must surface as AdapterConnectionError (retryable), not
        AdapterQueryError.
        """
        cur = _set_cursor_get_result(mock_pool, None)
        exc = make_oracle_error(oracledb.OperationalError, "ORA-03113")
        cur.execute = AsyncMock(side_effect=exc)
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"
        assert exc_info.value.retryable is True

    async def test_get_dpy6_error_raises_adapter_connection_error(
        self, repo: OracleRepository, mock_pool: MagicMock
    ) -> None:
        """DPY-6xxx classified as connection-class regardless of raised class."""
        cur = _set_cursor_get_result(mock_pool, None)
        exc = make_oracle_error(oracledb.DatabaseError, "DPY-6005")
        cur.execute = AsyncMock(side_effect=exc)
        with pytest.raises(AdapterConnectionError):
            await repo.get("1")

    async def test_get_ora_12541_raises_adapter_connection_error(
        self, repo: OracleRepository, mock_pool: MagicMock
    ) -> None:
        """ORA-12541 (TNS:no listener) — see repository.py docstring for provenance."""
        cur = _set_cursor_get_result(mock_pool, None)
        exc = make_oracle_error(oracledb.DatabaseError, "ORA-12541")
        cur.execute = AsyncMock(side_effect=exc)
        with pytest.raises(AdapterConnectionError):
            await repo.get("1")

    async def test_get_generic_database_error_raises_adapter_query_error(
        self, repo: OracleRepository, mock_pool: MagicMock
    ) -> None:
        """A non-connection ORA code (e.g. ORA-00001 unique constraint) → AdapterQueryError."""
        cur = _set_cursor_get_result(mock_pool, None)
        exc = make_oracle_error(oracledb.IntegrityError, "ORA-00001")
        cur.execute = AsyncMock(side_effect=exc)
        with pytest.raises(AdapterQueryError):
            await repo.get("1")

    async def test_get_dns_failure_raises_adapter_connection_error(
        self, repo: OracleRepository, mock_pool: MagicMock
    ) -> None:
        """Raw OSError (e.g. socket.gaierror) must also be caught — see connection.py."""
        import socket

        cur = _set_cursor_get_result(mock_pool, None)
        cur.execute = AsyncMock(side_effect=socket.gaierror("dns failure"))
        with pytest.raises(AdapterConnectionError):
            await repo.get("1")


class TestDelete:
    async def test_delete_existing_returns_true(self, repo: OracleRepository, mock_pool: MagicMock) -> None:
        cur = MagicMock()
        cur.execute = AsyncMock(return_value=cur)
        cur.rowcount = 1
        cur.__aenter__ = AsyncMock(return_value=cur)
        cur.__aexit__ = AsyncMock(return_value=False)
        mock_pool._conn.cursor = MagicMock(return_value=cur)

        result = await repo.delete("1")
        assert result is True

    async def test_delete_missing_returns_false(self, repo: OracleRepository, mock_pool: MagicMock) -> None:
        cur = MagicMock()
        cur.execute = AsyncMock(return_value=cur)
        cur.rowcount = 0
        cur.__aenter__ = AsyncMock(return_value=cur)
        cur.__aexit__ = AsyncMock(return_value=False)
        mock_pool._conn.cursor = MagicMock(return_value=cur)

        result = await repo.delete("missing")
        assert result is False

    async def test_delete_database_error_raises_adapter_query_error(
        self, repo: OracleRepository, mock_pool: MagicMock
    ) -> None:
        cur = MagicMock()
        cur.execute = AsyncMock(side_effect=oracledb.ProgrammingError("bad sql"))
        cur.__aenter__ = AsyncMock(return_value=cur)
        cur.__aexit__ = AsyncMock(return_value=False)
        mock_pool._conn.cursor = MagicMock(return_value=cur)

        with pytest.raises(AdapterQueryError):
            await repo.delete("1")
