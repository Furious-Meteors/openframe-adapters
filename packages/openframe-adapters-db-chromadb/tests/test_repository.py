"""
tests/test_repository.py — openframe-adapters-db-chromadb
=============================================================
Contract tests (VectorStoreContractTests) run first, then adapter-specific
unit tests covering ChromaDB error mapping, the columnar-to-row transpose,
and create()-vs-upsert() semantics.
"""
from __future__ import annotations

import asyncio

import chromadb.errors as chromadb_errors
import httpx
import pytest

from openframe.adapters.db.chromadb import ChromaDBRepository
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository, BaseVectorStore
from openframe.core.testing import VectorStoreContractTests


# ── Contract tests — must pass for every BaseVectorStore implementation ────

class TestChromaDBRepositoryContracts(VectorStoreContractTests):
    """
    ChromaDBRepository passes the full openframe VectorStoreContractTests
    suite against the stateful fake collection in conftest.py. No real
    Chroma server required.
    """

    @pytest.fixture
    def repository(self, mock_settings, mock_client):
        import openframe.adapters.db.chromadb.connection as conn_module

        conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client
        r = ChromaDBRepository(mock_settings, collection="items")
        yield r
        conn_module._client_cache.clear()

    @pytest.fixture
    def port(self, repository):
        return repository

    @pytest.fixture
    def make_entity(self):
        def _make(id: str, name: str = "test", vector: list[float] | None = None) -> dict:
            return {
                "id": id,
                "vector": vector or [1.0, 0.0, 0.0],
                "metadata": {"name": name},
                "document": None,
            }
        return _make

    @pytest.fixture
    def entity_vector(self):
        return lambda entity: entity["vector"]


# ── Adapter-specific tests — beyond what the contract covers ───────────────


class TestProtocolConformance:
    def test_isinstance_base_vector_store(self, repo: ChromaDBRepository) -> None:
        assert isinstance(repo, BaseVectorStore)

    def test_isinstance_base_repository(self, repo: ChromaDBRepository) -> None:
        assert isinstance(repo, BaseRepository)


class TestInit:
    def test_missing_collection_raises_configuration_error(self) -> None:
        from openframe.adapters.db.chromadb import ChromaDBSettings

        settings_no_collection = ChromaDBSettings()
        with pytest.raises(AdapterConfigurationError):
            ChromaDBRepository(settings_no_collection)

    def test_collection_from_init_arg(self, mock_settings) -> None:
        repo = ChromaDBRepository(mock_settings, collection="orders")
        assert repo._collection_name == "orders"

    def test_collection_from_class_attribute(self, mock_settings) -> None:
        class OrderRepo(ChromaDBRepository):
            _collection = "orders"

        repo = OrderRepo(mock_settings)
        assert repo._collection_name == "orders"

    def test_collection_from_settings(self) -> None:
        from openframe.adapters.db.chromadb import ChromaDBSettings

        settings = ChromaDBSettings(chroma_collection="from-settings")
        repo = ChromaDBRepository(settings)
        assert repo._collection_name == "from-settings"


class TestGet:
    async def test_get_found_returns_row(self, repo: ChromaDBRepository) -> None:
        await repo.create({"id": "1", "vector": [1.0, 2.0, 3.0], "metadata": {"name": "widget"}})
        result = await repo.get("1")
        assert result["id"] == "1"
        assert result["vector"] == [1.0, 2.0, 3.0]
        assert result["metadata"] == {"name": "widget"}

    async def test_get_not_found_returns_none(self, repo: ChromaDBRepository) -> None:
        result = await repo.get("missing")
        assert result is None

    async def test_get_chroma_error_raises_adapter_query_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        async def _raise(*args, **kwargs):
            raise chromadb_errors.InternalError("boom")

        mock_collection.get = _raise
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_timeout_raises_adapter_timeout_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        async def _raise(*args, **kwargs):
            raise asyncio.TimeoutError()

        mock_collection.get = _raise
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_connection_lost_raises_adapter_connection_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        """
        A connection dropped mid-call must surface as AdapterConnectionError
        (retryable), not AdapterQueryError.
        """
        async def _raise(*args, **kwargs):
            raise httpx.ReadError("connection lost")

        mock_collection.get = _raise
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"
        assert exc_info.value.retryable is True

    async def test_get_httpx_timeout_raises_adapter_timeout_error_not_connection_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        """
        REGRESSION-shaped test: httpx.TimeoutException is itself a
        httpx.TransportError subclass. If the exception-mapping helper
        checked TransportError before TimeoutException, every timeout
        would be misclassified as AdapterConnectionError instead of
        AdapterTimeoutError. _wrap_chromadb() must check
        httpx.TimeoutException first.
        """
        async def _raise(*args, **kwargs):
            raise httpx.ReadTimeout("timed out")

        mock_collection.get = _raise
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"


class TestList:
    async def test_list_returns_rows_and_count(self, repo: ChromaDBRepository) -> None:
        await repo.create({"id": "1", "vector": [1.0, 0.0, 0.0], "metadata": {"name": "a"}})
        await repo.create({"id": "2", "vector": [0.0, 1.0, 0.0], "metadata": {"name": "b"}})
        entities, count = await repo.list(limit=10, offset=0)
        assert count == 2
        assert {e["id"] for e in entities} == {"1", "2"}

    async def test_list_chroma_error_raises_adapter_query_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        async def _raise(*args, **kwargs):
            raise chromadb_errors.InternalError("oops")

        mock_collection.get = _raise
        with pytest.raises(AdapterQueryError):
            await repo.list(limit=10, offset=0)

    async def test_list_timeout_raises_adapter_timeout_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        async def _raise(*args, **kwargs):
            raise asyncio.TimeoutError()

        mock_collection.get = _raise
        with pytest.raises(AdapterTimeoutError):
            await repo.list(limit=10, offset=0)


class TestCreate:
    async def test_create_returns_entity(self, repo: ChromaDBRepository) -> None:
        entity = {"id": "99", "vector": [1.0, 2.0, 3.0], "metadata": {"name": "thing"}}
        result = await repo.create(entity)
        assert result == entity

    async def test_create_duplicate_id_raises_adapter_query_error(
        self, repo: ChromaDBRepository
    ) -> None:
        """
        create() uses collection.add() — fails on a duplicate id, matching
        BaseRepository.create()'s "persist a NEW entity" contract. This is
        the behaviour that distinguishes create() from upsert()-based
        idempotent writes.
        """
        entity = {"id": "dup", "vector": [1.0, 0.0, 0.0], "metadata": {}}
        await repo.create(entity)
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.create(entity)
        assert exc_info.value.operation == "create"

    async def test_create_timeout_raises_adapter_timeout_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        async def _raise(*args, **kwargs):
            raise asyncio.TimeoutError()

        mock_collection.add = _raise
        with pytest.raises(AdapterTimeoutError):
            await repo.create({"id": "1", "vector": [1.0, 0.0, 0.0], "metadata": {}})

    async def test_create_connection_lost_raises_adapter_connection_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        async def _raise(*args, **kwargs):
            raise httpx.ConnectError("gone")

        mock_collection.add = _raise
        with pytest.raises(AdapterConnectionError):
            await repo.create({"id": "1", "vector": [1.0, 0.0, 0.0], "metadata": {}})


class TestUpdate:
    async def test_update_found_returns_updated_entity(self, repo: ChromaDBRepository) -> None:
        await repo.create({"id": "1", "vector": [1.0, 0.0, 0.0], "metadata": {"name": "original"}})
        updated = {"id": "1", "vector": [0.0, 1.0, 0.0], "metadata": {"name": "modified"}}
        result = await repo.update(updated)
        assert result == updated
        fetched = await repo.get("1")
        assert fetched["metadata"]["name"] == "modified"
        assert fetched["vector"] == [0.0, 1.0, 0.0]

    async def test_update_not_found_returns_none(self, repo: ChromaDBRepository) -> None:
        result = await repo.update({"id": "999", "vector": [1.0, 0.0, 0.0], "metadata": {}})
        assert result is None

    async def test_update_chroma_error_raises_adapter_query_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        await repo.create({"id": "1", "vector": [1.0, 0.0, 0.0], "metadata": {}})

        async def _raise(*args, **kwargs):
            raise chromadb_errors.InternalError("fail")

        mock_collection.update = _raise
        with pytest.raises(AdapterQueryError):
            await repo.update({"id": "1", "vector": [1.0, 0.0, 0.0], "metadata": {}})

    async def test_update_timeout_raises_adapter_timeout_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        async def _raise(*args, **kwargs):
            raise asyncio.TimeoutError()

        mock_collection.get = _raise
        with pytest.raises(AdapterTimeoutError):
            await repo.update({"id": "1", "vector": [1.0, 0.0, 0.0], "metadata": {}})


class TestDelete:
    async def test_delete_existing_returns_true(self, repo: ChromaDBRepository) -> None:
        await repo.create({"id": "1", "vector": [1.0, 0.0, 0.0], "metadata": {}})
        result = await repo.delete("1")
        assert result is True
        assert await repo.get("1") is None

    async def test_delete_missing_returns_false(self, repo: ChromaDBRepository) -> None:
        result = await repo.delete("missing")
        assert result is False

    async def test_delete_chroma_error_raises_adapter_query_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        async def _raise(*args, **kwargs):
            raise chromadb_errors.InternalError("nope")

        mock_collection.get = _raise
        with pytest.raises(AdapterQueryError):
            await repo.delete("1")

    async def test_delete_timeout_raises_adapter_timeout_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        async def _raise(*args, **kwargs):
            raise asyncio.TimeoutError()

        mock_collection.get = _raise
        with pytest.raises(AdapterTimeoutError):
            await repo.delete("1")


class TestSearch:
    async def test_search_orders_by_distance(self, repo: ChromaDBRepository) -> None:
        await repo.create({"id": "far", "vector": [10.0, 10.0, 10.0], "metadata": {}})
        await repo.create({"id": "near", "vector": [1.0, 0.0, 0.0], "metadata": {}})
        results = await repo.search(query_vector=[1.0, 0.0, 0.0], k=2)
        assert results[0]["id"] == "near"

    async def test_search_chroma_error_raises_adapter_query_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        async def _raise(*args, **kwargs):
            raise chromadb_errors.InvalidDimensionException("dimension mismatch")

        mock_collection.query = _raise
        with pytest.raises(AdapterQueryError):
            await repo.search(query_vector=[1.0, 0.0, 0.0], k=5)

    async def test_search_timeout_raises_adapter_timeout_error(
        self, repo: ChromaDBRepository, mock_collection
    ) -> None:
        async def _raise(*args, **kwargs):
            raise asyncio.TimeoutError()

        mock_collection.query = _raise
        with pytest.raises(AdapterTimeoutError):
            await repo.search(query_vector=[1.0, 0.0, 0.0], k=5)


class TestColumnarTranspose:
    """
    Unit tests for the parallel-list-to-row transpose helpers — the most
    unusual part of this adapter relative to a row-oriented driver.
    """

    def test_get_result_to_rows_transposes_correctly(self) -> None:
        get_result = {
            "ids": ["a", "b"],
            "embeddings": [[1.0, 2.0], [3.0, 4.0]],
            "metadatas": [{"x": 1}, {"y": 2}],
            "documents": [None, "doc-b"],
        }
        rows = ChromaDBRepository._get_result_to_rows(get_result)
        assert rows == [
            {"id": "a", "vector": [1.0, 2.0], "metadata": {"x": 1}, "document": None},
            {"id": "b", "vector": [3.0, 4.0], "metadata": {"y": 2}, "document": "doc-b"},
        ]

    def test_get_result_to_rows_handles_empty(self) -> None:
        assert ChromaDBRepository._get_result_to_rows({"ids": []}) == []

    def test_query_result_to_rows_unwraps_outer_query_index(self) -> None:
        query_result = {
            "ids": [["a", "b"]],
            "embeddings": [[[1.0, 2.0], [3.0, 4.0]]],
            "metadatas": [[{"x": 1}, None]],
            "documents": [[None, "doc-b"]],
            "distances": [[0.0, 1.5]],
        }
        rows = ChromaDBRepository._query_result_to_rows(query_result)
        assert rows == [
            {"id": "a", "vector": [1.0, 2.0], "metadata": {"x": 1}, "document": None},
            {"id": "b", "vector": [3.0, 4.0], "metadata": {}, "document": "doc-b"},
        ]

    def test_query_result_to_rows_handles_empty(self) -> None:
        assert ChromaDBRepository._query_result_to_rows({"ids": [[]]}) == []
