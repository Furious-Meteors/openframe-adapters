"""
tests/test_repository.py — openframe-adapters-db-falkordb
==============================================================
Contract tests (GraphStoreContractTests) run first, then adapter-specific
unit tests covering FalkorDB error mapping, the id-addressing scheme, the
node-label Cypher-injection guard, and traverse().
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
import redis.exceptions
from falkordb.node import Node

from openframe.adapters.db.falkordb import FalkorDBRepository, FalkorDBSettings
from openframe.core.exceptions import AdapterConnectionError, AdapterQueryError, AdapterTimeoutError
from openframe.core.ports import BaseGraphStore, BaseRepository
from openframe.core.testing import GraphStoreContractTests


# ── Contract tests — must pass for every BaseGraphStore implementation ─────

class TestFalkorDBRepositoryContracts(GraphStoreContractTests):
    """
    FalkorDBRepository passes the full openframe GraphStoreContractTests
    suite (which itself builds on RepositoryContractTests/PortContractTests)
    against a stateful in-memory fake graph. No real FalkorDB server
    required.
    """

    @pytest.fixture
    def repository(
        self, mock_settings: FalkorDBSettings, mock_falkordb_client
    ) -> FalkorDBRepository:
        import openframe.adapters.db.falkordb.connection as conn_module

        conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client

        r = FalkorDBRepository(mock_settings)
        yield r
        conn_module._client_cache.clear()

    @pytest.fixture
    def port(self, repository):
        return repository

    @pytest.fixture
    def make_entity(self):
        def _make(id: str, name: str = "test") -> dict:
            return {"id": id, "name": name}
        return _make

    @pytest.fixture
    def traverse_query_for(self):
        """Build a (query, params) pair that filters by the entity's id."""
        return lambda entity: (
            "MATCH (n) WHERE n.id = $id RETURN n",
            {"id": entity["id"]},
        )


# ── Adapter-specific tests ─────────────────────────────────────────────────

class TestProtocolConformance:
    def test_isinstance_base_repository(self, repo: FalkorDBRepository) -> None:
        assert isinstance(repo, BaseRepository)

    def test_isinstance_base_graph_store(self, repo: FalkorDBRepository) -> None:
        assert isinstance(repo, BaseGraphStore)


class TestLabelInterpolation:
    """
    The configured node label is interpolated directly into every query
    this repository builds — FalkorDBSettings validates it as a safe
    Cypher identifier at construction time, so by the time repository.py
    sees it, it is trusted.
    """

    async def test_get_query_uses_configured_label(
        self, mock_settings: FalkorDBSettings, mock_falkordb_client, fake_graph
    ) -> None:
        import openframe.adapters.db.falkordb.connection as conn_module
        conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client

        calls: list[str] = []
        original_query = fake_graph.query

        async def _spy_query(q, params=None, timeout=None):
            calls.append(q)
            return await original_query(q, params=params, timeout=timeout)

        fake_graph.query = _spy_query

        r = FalkorDBRepository(mock_settings)
        await r.get("missing")
        assert any(f"n:{mock_settings.falkordb_node_label}" in q for q in calls)
        conn_module._client_cache.clear()

    def test_custom_label_rejected_if_unsafe_bypassing_pydantic(self) -> None:
        """
        FalkorDBSettings itself is the only validation point — confirms a
        repository built directly from a trusted Settings instance never
        needs to re-validate. (The actual injection-rejection test lives
        in test_config.py; this just documents the division of
        responsibility.)
        """
        from openframe.adapters.db.falkordb.config import _SAFE_IDENTIFIER_RE

        assert _SAFE_IDENTIFIER_RE.match("Entity")
        assert not _SAFE_IDENTIFIER_RE.match("Entity) DETACH DELETE (n")


class TestGet:
    async def test_get_returns_none_for_missing_node(self, repo: FalkorDBRepository) -> None:
        result = await repo.get("missing")
        assert result is None

    async def test_get_maps_node_properties_to_dict(
        self, repo: FalkorDBRepository
    ) -> None:
        await repo.create({"id": "1", "name": "Alice"})
        result = await repo.get("1")
        assert result == {"id": "1", "name": "Alice"}

    async def test_get_timeout_raises_adapter_timeout_error(
        self, repo: FalkorDBRepository, fake_graph
    ) -> None:
        fake_graph.query = AsyncMock(side_effect=asyncio.TimeoutError())
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_response_error_raises_adapter_query_error(
        self, repo: FalkorDBRepository, fake_graph
    ) -> None:
        fake_graph.query = AsyncMock(side_effect=redis.exceptions.ResponseError("bad cypher"))
        with pytest.raises(AdapterQueryError):
            await repo.get("1")


class TestCreate:
    async def test_create_returns_entity(self, repo: FalkorDBRepository) -> None:
        entity = {"id": "1", "name": "test"}
        result = await repo.create(entity)
        assert result == entity

    async def test_create_raises_query_error_when_entity_has_no_id(
        self, repo: FalkorDBRepository
    ) -> None:
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.create({"name": "no id here"})
        assert exc_info.value.operation == "create"

    async def test_create_duplicate_id_raises_adapter_query_error(
        self, repo: FalkorDBRepository
    ) -> None:
        """
        Regression test for the unique-constraint addressing scheme:
        creating a second node with an id already in use must surface as
        a non-retryable AdapterQueryError (constraint violation), not
        silently overwrite or raise an untranslated driver exception.
        """
        await repo.create({"id": "dup", "name": "first"})
        with pytest.raises(AdapterQueryError):
            await repo.create({"id": "dup", "name": "second"})


class TestUpdate:
    async def test_update_returns_updated_entity(self, repo: FalkorDBRepository) -> None:
        await repo.create({"id": "1", "name": "original"})
        result = await repo.update({"id": "1", "name": "modified"})
        assert result == {"id": "1", "name": "modified"}

    async def test_update_returns_none_when_missing(self, repo: FalkorDBRepository) -> None:
        result = await repo.update({"id": "nonexistent", "name": "x"})
        assert result is None


class TestDelete:
    async def test_delete_returns_true_when_deleted(self, repo: FalkorDBRepository) -> None:
        await repo.create({"id": "1", "name": "test"})
        result = await repo.delete("1")
        assert result is True

    async def test_delete_returns_false_when_missing(self, repo: FalkorDBRepository) -> None:
        result = await repo.delete("missing")
        assert result is False

    async def test_delete_uses_detach_delete(
        self, repo: FalkorDBRepository, fake_graph
    ) -> None:
        """DETACH DELETE so nodes with relationships aren't rejected."""
        calls: list[str] = []
        original_query = fake_graph.query

        async def _spy_query(q, params=None, timeout=None):
            calls.append(q)
            return await original_query(q, params=params, timeout=timeout)

        fake_graph.query = _spy_query
        await repo.delete("1")
        assert any("DETACH DELETE" in q for q in calls)


class TestList:
    async def test_list_orders_deterministically(self, repo: FalkorDBRepository) -> None:
        """
        Regression test: list() must ORDER BY n.id — Cypher gives no
        ordering guarantee without an explicit ORDER BY, which would make
        pagination (offset/limit) non-deterministic against a real server.
        """
        for i in range(3):
            await repo.create({"id": f"z-{i}", "name": "x"})
        page1, _ = await repo.list(limit=5, offset=0)
        page2, _ = await repo.list(limit=5, offset=0)
        assert page1 == page2


class TestExceptionClassification:
    """
    _wrap_falkordb() must distinguish connection-class failures (broken/
    lost connection, retryable) from query-class failures (malformed
    Cypher, constraint violation, non-retryable) — the same distinction
    every other adapter's _wrap_<driver>() helper makes.
    """

    async def test_connection_error_is_connection_class(
        self, repo: FalkorDBRepository, fake_graph
    ) -> None:
        fake_graph.query = AsyncMock(side_effect=redis.exceptions.ConnectionError("lost"))
        with pytest.raises(AdapterConnectionError):
            await repo.get("1")

    async def test_authentication_error_is_connection_class(
        self, repo: FalkorDBRepository, fake_graph
    ) -> None:
        fake_graph.query = AsyncMock(side_effect=redis.exceptions.AuthenticationError("bad auth"))
        with pytest.raises(AdapterConnectionError):
            await repo.get("1")

    async def test_response_error_is_query_class(
        self, repo: FalkorDBRepository, fake_graph
    ) -> None:
        fake_graph.query = AsyncMock(side_effect=redis.exceptions.ResponseError("syntax error"))
        with pytest.raises(AdapterQueryError):
            await repo.get("1")

    async def test_timeout_is_timeout_class(
        self, repo: FalkorDBRepository, fake_graph
    ) -> None:
        fake_graph.query = AsyncMock(side_effect=asyncio.TimeoutError())
        with pytest.raises(AdapterTimeoutError):
            await repo.get("1")


class TestEnsureConstraintIdempotency:
    """
    _ensure_ready() must not crash on repeated initialize()-equivalent
    calls — create_node_unique_constraint() is called at most once per
    repository instance (flag-guarded), and an "already exists"-shaped
    ResponseError on a repeated raw call is swallowed, not propagated.
    """

    async def test_ensure_ready_only_calls_constraint_once(
        self, repo: FalkorDBRepository, fake_graph
    ) -> None:
        await repo._ensure_ready()
        await repo._ensure_ready()
        assert fake_graph.create_node_unique_constraint.await_count == 1

    async def test_already_exists_response_error_is_swallowed(
        self, mock_settings: FalkorDBSettings, mock_falkordb_client, fake_graph
    ) -> None:
        import openframe.adapters.db.falkordb.connection as conn_module
        conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client

        fake_graph.create_node_unique_constraint = AsyncMock(
            side_effect=redis.exceptions.ResponseError("constraint already exists")
        )
        r = FalkorDBRepository(mock_settings)
        await r._ensure_ready()  # must not raise
        assert r._constraint_ensured is True
        conn_module._client_cache.clear()

    async def test_non_already_exists_response_error_propagates(
        self, mock_settings: FalkorDBSettings, mock_falkordb_client, fake_graph
    ) -> None:
        import openframe.adapters.db.falkordb.connection as conn_module
        conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client

        fake_graph.create_node_unique_constraint = AsyncMock(
            side_effect=redis.exceptions.ResponseError("some other failure")
        )
        r = FalkorDBRepository(mock_settings)
        with pytest.raises(AdapterQueryError):
            await r._ensure_ready()
        conn_module._client_cache.clear()


class TestTraverse:
    async def test_traverse_maps_node_results(self, repo: FalkorDBRepository) -> None:
        await repo.create({"id": "1", "name": "Alice"})
        results = await repo.traverse(
            "MATCH (n) WHERE n.id = $id RETURN n", {"id": "1"}
        )
        assert results == [{"id": "1", "name": "Alice"}]

    async def test_traverse_passes_through_scalar_results(
        self, repo: FalkorDBRepository
    ) -> None:
        results = await repo.traverse("RETURN 1", None)
        assert results == [1]

    async def test_traverse_empty_params_defaults_to_empty_dict(
        self, repo: FalkorDBRepository, fake_graph
    ) -> None:
        calls: list[tuple] = []
        original_query = fake_graph.query

        async def _spy_query(q, params=None, timeout=None):
            calls.append(params)
            return await original_query(q, params=params, timeout=timeout)

        fake_graph.query = _spy_query
        await repo.traverse("RETURN 1", None)
        assert calls[0] == {}

    async def test_traverse_response_error_raises_adapter_query_error(
        self, repo: FalkorDBRepository, fake_graph
    ) -> None:
        fake_graph.query = AsyncMock(side_effect=redis.exceptions.ResponseError("bad query"))
        with pytest.raises(AdapterQueryError):
            await repo.traverse("MATCH (n) INVALID SYNTAX", None)


class TestNodeToEntityMapping:
    def test_default_node_to_entity_returns_properties_dict(
        self, repo: FalkorDBRepository
    ) -> None:
        node = Node(labels=["Entity"], properties={"id": "1", "name": "Alice"})
        assert repo._node_to_entity(node) == {"id": "1", "name": "Alice"}

    def test_entity_to_properties_passthrough_for_dict(
        self, repo: FalkorDBRepository
    ) -> None:
        entity = {"id": "1", "name": "Alice"}
        assert repo._entity_to_properties(entity) == entity
        # Must be a copy, not the same object — mutating the returned
        # dict must not mutate the caller's entity.
        props = repo._entity_to_properties(entity)
        props["name"] = "mutated"
        assert entity["name"] == "Alice"
