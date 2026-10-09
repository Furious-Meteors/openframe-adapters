"""
tests/test_repository.py — openframe-adapters-db-cassandra
===============================================================
Contract tests (RepositoryContractTests) run first, then adapter-specific
unit tests covering Cassandra error mapping and driver behaviour.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import cassandra
import pytest

from openframe.adapters.db.cassandra import CassandraRepository
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository
from openframe.core.testing import RepositoryContractTests


class FakeResponseFuture:
    """
    Local copy of the minimal ResponseFuture stand-in from conftest.py.

    Duplicated here (rather than imported) because this test file has no
    package __init__.py to make a relative import of conftest work, and
    this class is trivial enough that duplication is clearer than adding
    sys.path machinery.
    """

    def __init__(self, result: object = None, error: Exception | None = None) -> None:
        self._result = result
        self._error = error

    def add_callbacks(
        self,
        callback,
        errback,
        callback_args=(),
        callback_kwargs=None,
        errback_args=(),
        errback_kwargs=None,
    ) -> None:
        if self._error is not None:
            errback(self._error, *errback_args, **(errback_kwargs or {}))
        else:
            callback(self._result, *callback_args, **(callback_kwargs or {}))


# ── Contract tests — must pass for every BaseRepository implementation ─────

class TestCassandraRepositoryContracts(RepositoryContractTests):
    """
    CassandraRepository passes the full openframe contract suite.

    All RepositoryContractTests run against a mocked cassandra-driver
    Session. No real cluster required.
    """

    @pytest.fixture
    def repository(self, mock_settings, mock_session):
        """
        CassandraRepository backed by a stateful in-memory mock session.

        The mock session tracks insertions, updates, and deletions so that
        the RepositoryContractTests behavioural assertions (create -> get,
        list pagination, etc.) pass without a real database.
        """
        import re

        import openframe.adapters.db.cassandra.connection as conn_module

        conn_module._session_cache[conn_module._cache_key(mock_settings)] = mock_session

        _store: dict[str, dict] = {}

        def _execute_async(query: str, params=None):
            q = query.upper()
            params = params or ()
            if "INSERT" in q:
                m = re.search(r"\(([^)]+)\)\s*VALUES", query, re.IGNORECASE)
                cols = [c.strip() for c in m.group(1).split(",")] if m else []
                row = dict(zip(cols, params))
                _store[str(row.get("id", ""))] = row
                return FakeResponseFuture(result=[row])
            if "UPDATE" in q:
                entity_id = str(params[-1])
                m = re.search(r"SET\s+(.+?)\s+WHERE", query, re.IGNORECASE)
                set_cols = (
                    [p.strip().split("=")[0].strip() for p in m.group(1).split(",")]
                    if m
                    else []
                )
                row = dict(_store.get(entity_id, {}))
                for i, col in enumerate(set_cols):
                    row[col] = params[i]
                row.setdefault("id", entity_id)
                _store[entity_id] = row
                return FakeResponseFuture(result=[row])
            if "DELETE" in q:
                entity_id = str(params[0]) if params else ""
                _store.pop(entity_id, None)
                return FakeResponseFuture(result=[])
            if "COUNT" in q:
                return FakeResponseFuture(result=[{"count": len(_store)}])
            if "LIMIT" in q and "WHERE" not in q:
                # list()
                limit = params[0] if params else len(_store)
                return FakeResponseFuture(result=list(_store.values())[:limit])
            # get() — SELECT ... WHERE id = %s
            entity_id = str(params[0]) if params else ""
            row = _store.get(entity_id)
            return FakeResponseFuture(result=[row] if row is not None else [])

        mock_session.execute_async = MagicMock(side_effect=_execute_async)

        r = CassandraRepository(mock_settings, table="items", id_column="id")
        yield r
        conn_module._session_cache.clear()

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
    def test_isinstance_base_repository(self, repo: CassandraRepository) -> None:
        assert isinstance(repo, BaseRepository)


class TestInit:
    def test_missing_table_raises_configuration_error(self, mock_settings) -> None:
        with pytest.raises(AdapterConfigurationError):
            CassandraRepository(mock_settings)

    def test_table_from_init_arg(self, mock_settings) -> None:
        repo = CassandraRepository(mock_settings, table="orders")
        assert repo._table == "orders"

    def test_table_from_class_attribute(self, mock_settings) -> None:
        class OrderRepo(CassandraRepository):
            _table = "orders"

        repo = OrderRepo(mock_settings)
        assert repo._table == "orders"


class TestGet:
    async def test_get_found_returns_dict(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        row_data = {"id": "1", "name": "widget"}
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(result=[row_data])
        )
        result = await repo.get("1")
        assert result == row_data

    async def test_get_not_found_returns_none(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(result=[])
        )
        result = await repo.get("missing")
        assert result is None

    async def test_get_invalid_request_raises_adapter_query_error(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(error=cassandra.InvalidRequest("bad cql"))
        )
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"
        assert exc_info.value.retryable is False

    async def test_get_timeout_raises_adapter_timeout_error(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        import asyncio

        def _raise_timeout(*_a, **_kw):
            raise asyncio.TimeoutError()

        mock_session.execute_async = MagicMock(side_effect=_raise_timeout)
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    @pytest.mark.parametrize(
        "exc_factory",
        [
            lambda: cassandra.cluster.NoHostAvailable("no hosts", {}),
            lambda: cassandra.cluster.ConnectionException("conn lost"),
            lambda: cassandra.Unavailable("not enough replicas"),
        ],
    )
    async def test_get_connection_class_errors_raise_adapter_connection_error(
        self, repo: CassandraRepository, mock_session: MagicMock, exc_factory
    ) -> None:
        """
        Regression test for the connection-vs-query distinction: a
        connection-class driver exception (lost connection, unreachable
        cluster, or an unsatisfiable consistency level — treated as a
        transient cluster-health signal, not a malformed query) must
        surface as AdapterConnectionError (retryable), not AdapterQueryError.
        """
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(error=exc_factory())
        )
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"
        assert exc_info.value.retryable is True

    async def test_get_authentication_failed_is_not_retryable(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(error=cassandra.AuthenticationFailed("bad creds"))
        )
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get("1")
        assert exc_info.value.retryable is False

    @pytest.mark.parametrize(
        "exc_factory",
        [
            lambda: cassandra.OperationTimedOut("client gave up"),
            lambda: cassandra.ReadTimeout("coordinator read timeout"),
            lambda: cassandra.WriteTimeout("coordinator write timeout", write_type=0),
        ],
    )
    async def test_get_timeout_class_errors_raise_adapter_timeout_error(
        self, repo: CassandraRepository, mock_session: MagicMock, exc_factory
    ) -> None:
        """
        OperationTimedOut/ReadTimeout/WriteTimeout represent the cluster (or
        client) giving up waiting for completion — distinct from a
        malformed query, so these map to AdapterTimeoutError rather than
        AdapterQueryError.
        """
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(error=exc_factory())
        )
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    @pytest.mark.parametrize(
        "exc_factory",
        [
            lambda: cassandra.ReadFailure("replica failed", 0, 1, 1, 1, False),
            lambda: cassandra.WriteFailure("replica failed", 0, 1, 1, "SIMPLE", 1),
        ],
    )
    async def test_get_replica_failure_raises_adapter_query_error(
        self, repo: CassandraRepository, mock_session: MagicMock, exc_factory
    ) -> None:
        """
        ReadFailure/WriteFailure mean a replica explicitly failed to process
        the request (not just a timeout) — not retryable.
        """
        try:
            exc = exc_factory()
        except TypeError:
            # Constructor signature differs across driver versions — fall
            # back to a bare instantiation for the purposes of this test.
            exc = cassandra.ReadFailure.__new__(cassandra.ReadFailure)
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(error=exc)
        )
        with pytest.raises(AdapterQueryError):
            await repo.get("1")


class TestList:
    async def test_list_returns_rows_and_count(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        rows = [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]

        def _execute_async(query, params=None):
            if "COUNT" in query.upper():
                return FakeResponseFuture(result=[{"count": 2}])
            return FakeResponseFuture(result=rows)

        mock_session.execute_async = MagicMock(side_effect=_execute_async)
        entities, count = await repo.list(limit=10, offset=0)
        assert entities == rows
        assert count == 2

    async def test_list_invalid_request_raises_adapter_query_error(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(error=cassandra.InvalidRequest("bad"))
        )
        with pytest.raises(AdapterQueryError):
            await repo.list(limit=10, offset=0)


class TestCreate:
    async def test_create_returns_entity(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(result=[])
        )
        result = await repo.create({"id": "99", "name": "thing"})
        assert result == {"id": "99", "name": "thing"}

    async def test_create_invalid_request_raises_adapter_query_error(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(error=cassandra.InvalidRequest("dup"))
        )
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.create({"id": "1", "name": "x"})
        assert exc_info.value.operation == "create"


class TestUpdate:
    async def test_update_returns_entity_when_existing(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        """update() does an existence check (get) before the UPDATE statement."""
        existing_row = {"id": "1", "name": "original"}

        def _execute_async(query, params=None):
            if query.upper().startswith("SELECT"):
                return FakeResponseFuture(result=[existing_row])
            return FakeResponseFuture(result=[])

        mock_session.execute_async = MagicMock(side_effect=_execute_async)
        result = await repo.update({"id": "1", "name": "updated"})
        assert result == {"id": "1", "name": "updated"}

    async def test_update_returns_none_when_missing(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        """
        REGRESSION target: the contract requires update() of a missing id
        to return None, not silently upsert — Cassandra's UPDATE itself
        cannot distinguish the two, so the repository does an explicit
        existence check first.
        """
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(result=[])
        )
        result = await repo.update({"id": "missing", "name": "updated"})
        assert result is None

    async def test_update_no_columns_raises_adapter_query_error(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        existing_row = {"id": "1"}
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(result=[existing_row])
        )
        with pytest.raises(AdapterQueryError):
            await repo.update({"id": "1"})


class TestDelete:
    async def test_delete_returns_true_when_existing(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        """delete() does an existence check (get) before the DELETE statement."""
        existing_row = {"id": "1", "name": "x"}

        def _execute_async(query, params=None):
            if query.upper().startswith("SELECT"):
                return FakeResponseFuture(result=[existing_row])
            return FakeResponseFuture(result=[])

        mock_session.execute_async = MagicMock(side_effect=_execute_async)
        result = await repo.delete("1")
        assert result is True

    async def test_delete_returns_false_when_missing(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        """REGRESSION target: delete() of a missing id must return False."""
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(result=[])
        )
        result = await repo.delete("missing")
        assert result is False

    async def test_delete_connection_error_raises_adapter_connection_error(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(
                error=cassandra.cluster.NoHostAvailable("gone", {})
            )
        )
        with pytest.raises(AdapterConnectionError):
            await repo.delete("1")


class TestHealth:
    async def test_health_returns_ready_on_success(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(result=[{"now": 1}])
        )
        from openframe.core.ports import PluginStatus

        health = await repo.health()
        assert health.status == PluginStatus.READY

    async def test_health_never_raises_on_failure(
        self, repo: CassandraRepository, mock_session: MagicMock
    ) -> None:
        mock_session.execute_async = MagicMock(
            return_value=FakeResponseFuture(error=cassandra.cluster.NoHostAvailable("x", {}))
        )
        from openframe.core.ports import PluginStatus

        health = await repo.health()
        assert health.status == PluginStatus.FAILED
