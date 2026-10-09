"""
tests/test_repository.py — openframe-adapters-db-qdrant
===========================================================
Contract tests (VectorStoreContractTests) run first, then adapter-specific
unit tests covering Qdrant error mapping and driver behaviour.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse
from qdrant_client.http.models import CountResult, PointStruct, QueryResponse, Record, ScoredPoint

from openframe.adapters.db.qdrant import QdrantVectorStore
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository, BaseVectorStore
from openframe.core.testing import VectorStoreContractTests


# ── Contract tests — must pass for every BaseVectorStore implementation ────


class TestQdrantVectorStoreContracts(VectorStoreContractTests):
    """
    QdrantVectorStore passes the full openframe VectorStoreContractTests
    suite (RepositoryContractTests + search assertions) against a mocked
    AsyncQdrantClient. No real Qdrant server required.
    """

    @pytest.fixture
    def repository(self, mock_settings, mock_client):
        """
        QdrantVectorStore backed by a stateful in-memory mock client.

        The mock client tracks upserted points so that the contract tests'
        behavioural assertions (create -> get, list pagination, search,
        etc.) pass without a real Qdrant server.
        """
        import openframe.adapters.db.qdrant.connection as conn_module

        conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client

        _store: dict[str, dict] = {}

        def _to_record(point_id: str) -> Record:
            data = _store[point_id]
            return Record(id=point_id, payload=data["payload"], vector=data["vector"])

        async def _upsert(collection_name, points):
            for p in points:
                _store[str(p.id)] = {"vector": p.vector, "payload": p.payload or {}}

        async def _retrieve(collection_name, ids, with_vectors=False):
            return [_to_record(str(i)) for i in ids if str(i) in _store]

        async def _scroll(collection_name, limit, offset=None, with_vectors=False):
            ordered_ids = sorted(_store.keys())
            page = ordered_ids[:limit]
            return [_to_record(i) for i in page], None

        async def _count(collection_name):
            return CountResult(count=len(_store))

        async def _delete(collection_name, points_selector):
            for i in points_selector:
                _store.pop(str(i), None)

        def _distance(a: list[float], b: list[float]) -> float:
            return sum((x - y) ** 2 for x, y in zip(a, b)) ** 0.5

        async def _query_points(collection_name, query, limit, with_vectors=False):
            scored = [
                (
                    _distance(query, data["vector"]),
                    ScoredPoint(
                        id=point_id,
                        version=0,
                        score=1.0,
                        payload=data["payload"],
                        vector=data["vector"],
                    ),
                )
                for point_id, data in _store.items()
            ]
            scored.sort(key=lambda pair: pair[0])
            return QueryResponse(points=[p for _, p in scored[:limit]])

        mock_client.upsert = AsyncMock(side_effect=_upsert)
        mock_client.retrieve = AsyncMock(side_effect=_retrieve)
        mock_client.scroll = AsyncMock(side_effect=_scroll)
        mock_client.count = AsyncMock(side_effect=_count)
        mock_client.delete = AsyncMock(side_effect=_delete)
        mock_client.query_points = AsyncMock(side_effect=_query_points)

        r = QdrantVectorStore(mock_settings, collection="items")
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
    def test_isinstance_base_vector_store(self, repo: QdrantVectorStore) -> None:
        assert isinstance(repo, BaseVectorStore)

    def test_isinstance_base_repository(self, repo: QdrantVectorStore) -> None:
        assert isinstance(repo, BaseRepository)


class TestInit:
    def test_missing_collection_raises_configuration_error(self, mock_settings) -> None:
        with pytest.raises(AdapterConfigurationError):
            QdrantVectorStore(mock_settings)

    def test_collection_from_init_arg(self, mock_settings) -> None:
        store = QdrantVectorStore(mock_settings, collection="documents")
        assert store._collection == "documents"

    def test_collection_from_class_attribute(self, mock_settings) -> None:
        class DocumentStore(QdrantVectorStore):
            _collection = "documents"

        store = DocumentStore(mock_settings)
        assert store._collection == "documents"


class TestGet:
    async def test_get_found_returns_entity(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        record = Record(id="1", payload={"name": "widget"}, vector=[1.0, 0.0, 0.0])
        mock_client.retrieve.return_value = [record]
        result = await repo.get("1")
        mock_client.retrieve.assert_called_once()
        assert result == {"id": "1", "vector": [1.0, 0.0, 0.0], "name": "widget"}

    async def test_get_requests_with_vectors(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.retrieve.return_value = []
        await repo.get("1")
        _, kwargs = mock_client.retrieve.call_args
        assert kwargs["with_vectors"] is True

    async def test_get_not_found_returns_none(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.retrieve.return_value = []
        result = await repo.get("missing")
        assert result is None

    async def test_get_response_handling_exception_raises_adapter_connection_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        """
        A dropped connection mid-call must surface as AdapterConnectionError
        (retryable), not AdapterQueryError.
        """
        mock_client.retrieve.side_effect = ResponseHandlingException(ConnectionError("boom"))
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_unexpected_response_4xx_raises_adapter_query_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        exc = UnexpectedResponse(status_code=400, reason_phrase="bad", content=b"", headers={})
        mock_client.retrieve.side_effect = exc
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_unexpected_response_5xx_raises_adapter_connection_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        """
        REGRESSION: a 5xx/unrecognised response is a server-side failure,
        not a client mistake — it must be retryable (AdapterConnectionError),
        not misclassified as a non-retryable AdapterQueryError the way a
        naive "every UnexpectedResponse is a query error" mapping would.
        """
        exc = UnexpectedResponse(status_code=500, reason_phrase="err", content=b"", headers={})
        mock_client.retrieve.side_effect = exc
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_timeout_raises_adapter_timeout_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.retrieve.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"


class TestList:
    async def test_list_returns_entities_and_count(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        records = [
            Record(id="1", payload={"name": "a"}, vector=[1.0, 0.0, 0.0]),
            Record(id="2", payload={"name": "b"}, vector=[0.0, 1.0, 0.0]),
        ]
        mock_client.scroll.return_value = (records, None)
        mock_client.count.return_value = CountResult(count=42)

        entities, count = await repo.list(limit=10, offset=0)
        assert entities == [
            {"id": "1", "vector": [1.0, 0.0, 0.0], "name": "a"},
            {"id": "2", "vector": [0.0, 1.0, 0.0], "name": "b"},
        ]
        assert count == 42

    async def test_list_offset_is_applied_as_python_slice(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        """
        Qdrant's scroll() offset is a point-id cursor, not an integer skip
        count — this adapter's list() must request limit+offset points and
        slice the first `offset` off itself, rather than passing offset
        straight through to scroll() as if it were a skip count.
        """
        records = [
            Record(id=str(i), payload={}, vector=[0.0, 0.0, 0.0]) for i in range(5)
        ]
        mock_client.scroll.return_value = (records, None)
        mock_client.count.return_value = CountResult(count=5)

        entities, _ = await repo.list(limit=2, offset=3)

        _, kwargs = mock_client.scroll.call_args
        assert kwargs["limit"] == 5  # limit + offset
        assert [e["id"] for e in entities] == ["3", "4"]

    async def test_list_response_handling_exception_raises_adapter_connection_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.scroll.side_effect = ResponseHandlingException(ConnectionError("boom"))
        with pytest.raises(AdapterConnectionError):
            await repo.list(limit=10, offset=0)

    async def test_list_timeout_raises_adapter_timeout_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.scroll.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.list(limit=10, offset=0)


class TestCreate:
    async def test_create_returns_entity_unchanged(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        entity = {"id": "99", "vector": [1.0, 0.0, 0.0], "name": "thing"}
        result = await repo.create(entity)
        mock_client.upsert.assert_called_once()
        assert result is entity

    async def test_create_builds_point_struct(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        entity = {"id": "99", "vector": [1.0, 0.0, 0.0], "name": "thing"}
        await repo.create(entity)
        _, kwargs = mock_client.upsert.call_args
        point = kwargs["points"][0]
        assert isinstance(point, PointStruct)
        assert point.id == "99"
        assert point.vector == [1.0, 0.0, 0.0]
        assert point.payload == {"name": "thing"}

    async def test_create_response_handling_exception_raises_adapter_connection_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.upsert.side_effect = ResponseHandlingException(ConnectionError("boom"))
        with pytest.raises(AdapterConnectionError):
            await repo.create({"id": "1", "vector": [1.0, 0.0, 0.0]})

    async def test_create_unexpected_response_4xx_raises_adapter_query_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        exc = UnexpectedResponse(status_code=400, reason_phrase="bad", content=b"", headers={})
        mock_client.upsert.side_effect = exc
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.create({"id": "1", "vector": [1.0, 0.0, 0.0]})
        assert exc_info.value.operation == "create"

    async def test_create_timeout_raises_adapter_timeout_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.upsert.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.create({"id": "1", "vector": [1.0, 0.0, 0.0]})


class TestUpdate:
    async def test_update_found_upserts_and_returns_entity(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.retrieve.return_value = [
            Record(id="1", payload={"name": "old"}, vector=[1.0, 0.0, 0.0])
        ]
        entity = {"id": "1", "vector": [1.0, 0.0, 0.0], "name": "updated"}
        result = await repo.update(entity)
        assert result is entity
        mock_client.upsert.assert_called_once()

    async def test_update_not_found_returns_none(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.retrieve.return_value = []
        result = await repo.update({"id": "999", "vector": [0.0, 0.0, 0.0], "name": "ghost"})
        assert result is None
        mock_client.upsert.assert_not_called()

    async def test_update_response_handling_exception_raises_adapter_connection_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.retrieve.return_value = [
            Record(id="1", payload={}, vector=[1.0, 0.0, 0.0])
        ]
        mock_client.upsert.side_effect = ResponseHandlingException(ConnectionError("boom"))
        with pytest.raises(AdapterConnectionError):
            await repo.update({"id": "1", "vector": [1.0, 0.0, 0.0]})

    async def test_update_timeout_raises_adapter_timeout_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.retrieve.return_value = [
            Record(id="1", payload={}, vector=[1.0, 0.0, 0.0])
        ]
        mock_client.upsert.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.update({"id": "1", "vector": [1.0, 0.0, 0.0]})


class TestDelete:
    async def test_delete_existing_returns_true(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.retrieve.return_value = [
            Record(id="1", payload={}, vector=[1.0, 0.0, 0.0])
        ]
        result = await repo.delete("1")
        assert result is True
        mock_client.delete.assert_called_once()

    async def test_delete_missing_returns_false(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.retrieve.return_value = []
        result = await repo.delete("missing")
        assert result is False
        mock_client.delete.assert_not_called()

    async def test_delete_response_handling_exception_raises_adapter_connection_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.retrieve.return_value = [
            Record(id="1", payload={}, vector=[1.0, 0.0, 0.0])
        ]
        mock_client.delete.side_effect = ResponseHandlingException(ConnectionError("boom"))
        with pytest.raises(AdapterConnectionError):
            await repo.delete("1")

    async def test_delete_timeout_raises_adapter_timeout_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.retrieve.return_value = [
            Record(id="1", payload={}, vector=[1.0, 0.0, 0.0])
        ]
        mock_client.delete.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.delete("1")


class TestSearch:
    async def test_search_uses_query_points_not_search(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        """
        REGRESSION: AsyncQdrantClient 1.19 has no search() method — it was
        replaced by query_points(). search() must call query_points(), not
        a nonexistent search() attribute on the mock (which MagicMock would
        otherwise silently auto-create, masking the bug).
        """
        mock_client.query_points.return_value = QueryResponse(points=[])
        await repo.search(query_vector=[1.0, 0.0, 0.0], k=5)
        mock_client.query_points.assert_called_once()
        _, kwargs = mock_client.query_points.call_args
        assert kwargs["query"] == [1.0, 0.0, 0.0]
        assert kwargs["limit"] == 5

    async def test_search_returns_entities_from_scored_points(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        points = [
            ScoredPoint(id="1", version=0, score=0.99, payload={"name": "a"}, vector=[1.0, 0.0, 0.0]),
        ]
        mock_client.query_points.return_value = QueryResponse(points=points)
        results = await repo.search(query_vector=[1.0, 0.0, 0.0], k=5)
        assert results == [{"id": "1", "vector": [1.0, 0.0, 0.0], "name": "a"}]

    async def test_search_empty_response_returns_empty_list(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.query_points.return_value = QueryResponse(points=[])
        results = await repo.search(query_vector=[1.0, 0.0, 0.0], k=5)
        assert results == []

    async def test_search_response_handling_exception_raises_adapter_connection_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.query_points.side_effect = ResponseHandlingException(ConnectionError("boom"))
        with pytest.raises(AdapterConnectionError):
            await repo.search(query_vector=[1.0, 0.0, 0.0], k=5)

    async def test_search_unexpected_response_4xx_raises_adapter_query_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        exc = UnexpectedResponse(status_code=400, reason_phrase="bad", content=b"", headers={})
        mock_client.query_points.side_effect = exc
        with pytest.raises(AdapterQueryError):
            await repo.search(query_vector=[1.0, 0.0, 0.0], k=5)

    async def test_search_timeout_raises_adapter_timeout_error(
        self, repo: QdrantVectorStore, mock_client: MagicMock
    ) -> None:
        mock_client.query_points.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.search(query_vector=[1.0, 0.0, 0.0], k=5)


class TestClose:
    async def test_close_removes_client_from_cache(
        self, repo: QdrantVectorStore, mock_client: MagicMock, mock_settings
    ) -> None:
        import openframe.adapters.db.qdrant.connection as conn_module

        key = conn_module._cache_key(mock_settings)
        assert key in conn_module._client_cache
        await repo.close()
        assert key not in conn_module._client_cache
        mock_client.close.assert_called_once()

    async def test_close_is_safe_when_not_cached(self, mock_settings) -> None:
        from openframe.adapters.db.qdrant import QdrantVectorStore as QVS

        store = QVS(mock_settings, collection="items")
        await store.close()  # must not raise
