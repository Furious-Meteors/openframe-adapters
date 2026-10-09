"""
tests/test_repository.py — openframe-adapters-db-milvus
===========================================================
Contract tests (VectorStoreContractTests) run first, then adapter-specific
unit tests covering Milvus error mapping and driver behaviour.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from pymilvus.exceptions import (
    CollectionNotExistException,
    ConnectError,
    MilvusException,
    ParamError,
)

from openframe.adapters.db.milvus import MilvusRepository
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository, BaseVectorStore
from openframe.core.testing import VectorStoreContractTests


def _euclidean(a: list[float], b: list[float]) -> float:
    return sum((x - y) ** 2 for x, y in zip(a, b)) ** 0.5


# ── Contract tests — must pass for every BaseVectorStore implementation ────


class TestMilvusRepositoryContracts(VectorStoreContractTests):
    """
    MilvusRepository passes the full openframe VectorStoreContractTests
    suite against a stateful in-memory mock client. No real Milvus server
    required.
    """

    @pytest.fixture
    def repository(self, mock_settings, mock_client):
        import openframe.adapters.db.milvus.connection as conn_module

        conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client

        _store: dict[str, dict] = {}

        async def _get(collection_name, ids, **kwargs):
            key = str(ids[0])
            if key in _store:
                return [dict(_store[key])]
            return []

        async def _query(collection_name, filter="", **kwargs):
            rows = [dict(r) for r in _store.values()]
            if "limit" in kwargs:
                offset = kwargs.get("offset", 0)
                limit = kwargs["limit"]
                rows = rows[offset : offset + limit]
            return rows

        async def _upsert(collection_name, data, **kwargs):
            for row in data:
                _store[str(row["id"])] = dict(row)
            return {"upsert_count": len(data)}

        async def _delete(collection_name, ids, **kwargs):
            key = str(ids[0])
            if key in _store:
                del _store[key]
                return {"delete_count": 1}
            return {"delete_count": 0}

        async def _search(collection_name, data, limit, **kwargs):
            query_vec = data[0]
            scored = sorted(
                _store.values(),
                key=lambda r: _euclidean(query_vec, r["vector"]),
            )
            hits = [{"id": r["id"], "entity": dict(r)} for r in scored[:limit]]
            return [hits]

        mock_client.get = AsyncMock(side_effect=_get)
        mock_client.query = AsyncMock(side_effect=_query)
        mock_client.upsert = AsyncMock(side_effect=_upsert)
        mock_client.delete = AsyncMock(side_effect=_delete)
        mock_client.search = AsyncMock(side_effect=_search)
        mock_client.list_collections = AsyncMock(return_value=["items"])

        r = MilvusRepository(mock_settings, collection_name="items")
        yield r
        conn_module._client_cache.clear()

    @pytest.fixture
    def port(self, repository):
        return repository

    @pytest.fixture
    def make_entity(self):
        def _make(id: str, name: str = "test", vector: list[float] | None = None) -> dict:
            return {"id": id, "name": name, "vector": vector or [1.0, 0.0, 0.0]}

        return _make

    @pytest.fixture
    def entity_vector(self):
        return lambda entity: entity["vector"]


# ── Adapter-specific tests — beyond what the contract covers ───────────────


class TestProtocolConformance:
    def test_isinstance_base_repository(self, repo: MilvusRepository) -> None:
        assert isinstance(repo, BaseRepository)

    def test_isinstance_base_vector_store(self, repo: MilvusRepository) -> None:
        assert isinstance(repo, BaseVectorStore)


class TestInit:
    def test_missing_collection_name_raises_configuration_error(self, mock_settings) -> None:
        with pytest.raises(AdapterConfigurationError):
            MilvusRepository(mock_settings)

    def test_collection_name_from_init_arg(self, mock_settings) -> None:
        repo = MilvusRepository(mock_settings, collection_name="orders")
        assert repo._collection_name == "orders"

    def test_collection_name_from_class_attribute(self, mock_settings) -> None:
        class OrderRepo(MilvusRepository):
            _collection_name = "orders"

        repo = OrderRepo(mock_settings)
        assert repo._collection_name == "orders"


class TestGet:
    async def test_get_found_returns_dict(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        row_data = {"id": "1", "name": "widget", "vector": [1.0, 0.0, 0.0]}
        mock_client.get.return_value = [row_data]
        result = await repo.get("1")
        mock_client.get.assert_called_once()
        assert result == row_data

    async def test_get_not_found_returns_none(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.get.return_value = []
        result = await repo.get("missing")
        assert result is None

    async def test_get_query_class_error_raises_adapter_query_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.get.side_effect = ParamError(message="bad id type")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_timeout_raises_adapter_timeout_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        import asyncio

        mock_client.get.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_collection_not_exist_raises_adapter_query_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.get.side_effect = CollectionNotExistException(message="no such collection")
        with pytest.raises(AdapterQueryError):
            await repo.get("1")

    async def test_get_connect_error_raises_adapter_connection_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        """
        ConnectError is checked defensively even though the installed
        driver never actually raises it from AsyncMilvusClient — see
        connection.py's module docstring. If it ever is raised, it must
        still be classified correctly.
        """
        mock_client.get.side_effect = ConnectError(message="cannot connect")
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"
        assert exc_info.value.retryable is True

    async def test_get_bare_milvus_exception_with_connection_phrasing_raises_adapter_connection_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        """
        REGRESSION: a real unreachable-server failure in the installed
        driver (3.0.2) surfaces as a bare MilvusException (code=2,
        "Fail connecting to server") rather than ConnectError/
        MilvusUnavailableException — verified directly against the
        installed driver, not assumed from pymilvus's own docstrings.
        Must still classify as AdapterConnectionError via message-phrase
        matching, not fall through to AdapterQueryError.
        """
        mock_client.get.side_effect = MilvusException(
            code=2, message="Fail connecting to server on 127.0.0.1:19"
        )
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_unrelated_milvus_exception_raises_adapter_query_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        """A MilvusException with no connection-like phrasing and no known
        subclass name falls back to the non-retryable AdapterQueryError."""
        mock_client.get.side_effect = MilvusException(message="some other failure")
        with pytest.raises(AdapterQueryError):
            await repo.get("1")


class TestList:
    async def test_list_returns_rows_and_count(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        page = [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]
        all_rows = page + [{"id": "3", "name": "c"}]
        mock_client.query = AsyncMock(side_effect=[page, all_rows])

        entities, count = await repo.list(limit=2, offset=0)
        assert entities == page
        assert count == 3

    async def test_list_passes_limit_and_offset_as_kwargs(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.query = AsyncMock(side_effect=[[], []])
        await repo.list(limit=5, offset=10)
        first_call = mock_client.query.call_args_list[0]
        assert first_call.kwargs.get("limit") == 5
        assert first_call.kwargs.get("offset") == 10

    async def test_list_query_error_raises_adapter_query_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.query.side_effect = ParamError(message="bad filter")
        with pytest.raises(AdapterQueryError):
            await repo.list(limit=10, offset=0)

    async def test_list_timeout_raises_adapter_timeout_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        import asyncio

        mock_client.query.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.list(limit=10, offset=0)


class TestCreate:
    async def test_create_calls_upsert_and_returns_entity(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        entity = {"id": "99", "name": "thing", "vector": [1.0, 0.0, 0.0]}
        result = await repo.create(entity)
        mock_client.upsert.assert_called_once_with("items", data=[entity])
        assert result == entity

    async def test_create_query_error_raises_adapter_query_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.upsert.side_effect = ParamError(message="dup")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.create({"id": "1", "name": "x", "vector": [1.0, 0.0, 0.0]})
        assert exc_info.value.operation == "create"

    async def test_create_timeout_raises_adapter_timeout_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        import asyncio

        mock_client.upsert.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.create({"id": "1", "name": "x", "vector": [1.0, 0.0, 0.0]})


class TestUpdate:
    async def test_update_found_returns_updated_entity(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.get.return_value = [{"id": "1", "name": "original", "vector": [1.0, 0.0, 0.0]}]
        updated = {"id": "1", "name": "updated", "vector": [1.0, 0.0, 0.0]}
        result = await repo.update(updated)
        assert result == updated
        mock_client.upsert.assert_called_once_with("items", data=[updated])

    async def test_update_not_found_returns_none(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.get.return_value = []
        result = await repo.update({"id": "999", "name": "ghost", "vector": [1.0, 0.0, 0.0]})
        assert result is None
        mock_client.upsert.assert_not_called()

    async def test_update_query_error_raises_adapter_query_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.get.return_value = [{"id": "1", "name": "x", "vector": [1.0, 0.0, 0.0]}]
        mock_client.upsert.side_effect = ParamError(message="fail")
        with pytest.raises(AdapterQueryError):
            await repo.update({"id": "1", "name": "x", "vector": [1.0, 0.0, 0.0]})

    async def test_update_timeout_raises_adapter_timeout_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        import asyncio

        mock_client.get.return_value = [{"id": "1", "name": "x", "vector": [1.0, 0.0, 0.0]}]
        mock_client.upsert.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.update({"id": "1", "name": "x", "vector": [1.0, 0.0, 0.0]})


class TestDelete:
    async def test_delete_existing_returns_true(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.get.return_value = [{"id": "1", "name": "x", "vector": [1.0, 0.0, 0.0]}]
        result = await repo.delete("1")
        assert result is True
        mock_client.delete.assert_called_once_with("items", ids=["1"])

    async def test_delete_missing_returns_false(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.get.return_value = []
        result = await repo.delete("missing")
        assert result is False
        mock_client.delete.assert_not_called()

    async def test_delete_query_error_raises_adapter_query_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.get.return_value = [{"id": "1", "name": "x", "vector": [1.0, 0.0, 0.0]}]
        mock_client.delete.side_effect = ParamError(message="nope")
        with pytest.raises(AdapterQueryError):
            await repo.delete("1")

    async def test_delete_timeout_raises_adapter_timeout_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        import asyncio

        mock_client.get.return_value = [{"id": "1", "name": "x", "vector": [1.0, 0.0, 0.0]}]
        mock_client.delete.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.delete("1")


class TestSearch:
    async def test_search_returns_mapped_entities(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        hit = {"id": "1", "entity": {"id": "1", "name": "x", "vector": [1.0, 0.0, 0.0]}}
        mock_client.search.return_value = [[hit]]
        results = await repo.search(query_vector=[1.0, 0.0, 0.0], k=1)
        assert results == [{"id": "1", "name": "x", "vector": [1.0, 0.0, 0.0]}]

    async def test_search_empty_results_returns_empty_list(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.search.return_value = []
        results = await repo.search(query_vector=[1.0, 0.0, 0.0], k=5)
        assert results == []

    async def test_search_query_error_raises_adapter_query_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        mock_client.search.side_effect = ParamError(message="dim mismatch")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.search(query_vector=[1.0, 0.0], k=5)
        assert exc_info.value.operation == "search"

    async def test_search_timeout_raises_adapter_timeout_error(
        self, repo: MilvusRepository, mock_client: MagicMock
    ) -> None:
        import asyncio

        mock_client.search.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.search(query_vector=[1.0, 0.0, 0.0], k=5)


class TestClose:
    async def test_close_closes_client_and_clears_cache(
        self, repo: MilvusRepository, mock_client: MagicMock, mock_settings
    ) -> None:
        import openframe.adapters.db.milvus.connection as conn_module

        key = conn_module._cache_key(mock_settings)
        assert key in conn_module._client_cache
        await repo.close()
        mock_client.close.assert_called_once()
        assert key not in conn_module._client_cache
