"""
tests/conftest.py — openframe-adapters-db-falkordb
======================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.

All tests run with zero network calls. The falkordb.asyncio.FalkorDB
client is mocked at the ``openframe.adapters.db.falkordb.connection``
import level — a fake, in-memory ``AsyncGraph`` stand-in interprets the
small, fixed set of Cypher query *shapes* this adapter's own
``repository.py`` actually generates (it is not a general Cypher
interpreter — it does not need to be, since it only ever receives
queries this package itself builds, plus whatever ``traverse_query_for``
supplies in the contract tests below). No real FalkorDB/Redis server is
needed.
"""
from __future__ import annotations

# Canonical OTel reset fixtures from openframe-core v3.0.
# Provides (autouse): reset_telemetry_state
# Provides (on-demand): span_exporter, metric_reader
from openframe.core.testing.fixtures import *  # noqa: F401, F403

import pytest
from unittest.mock import AsyncMock, MagicMock

import redis.exceptions
from falkordb.node import Node


class FakeQueryResult:
    """Minimal stand-in for ``falkordb.query_result.QueryResult``."""

    def __init__(self, result_set=None, nodes_deleted: int = 0) -> None:
        self.result_set = result_set or []
        self.nodes_deleted = nodes_deleted


class FakeGraph:
    """
    In-memory stand-in for ``falkordb.asyncio.graph.AsyncGraph``.

    Dispatches on substrings of the query text that uniquely identify
    which of ``FalkorDBRepository``'s fixed query shapes is being run —
    this is legitimate because this fake only ever needs to understand
    queries this adapter package itself generates (plus a handful of
    contract-test-supplied queries using the same shapes), never
    arbitrary Cypher.
    """

    def __init__(self, label: str = "Entity") -> None:
        self._label = label
        self._nodes: dict[str, dict] = {}
        self.create_node_unique_constraint = AsyncMock(return_value=None)
        self.create_node_range_index = AsyncMock(return_value=None)

    def _node(self, props: dict) -> Node:
        return Node(labels=[self._label], properties=dict(props))

    async def query(self, q: str, params=None, timeout=None) -> FakeQueryResult:
        params = params or {}

        if "CREATE (n:" in q and "SET n = $props" in q:
            props = dict(params["props"])
            node_id = str(props.get("id", ""))
            if node_id in self._nodes:
                raise redis.exceptions.ResponseError(
                    f"unique constraint violation on id {node_id!r}"
                )
            self._nodes[node_id] = props
            return FakeQueryResult(result_set=[[self._node(props)]])

        if "SET n += $props" in q:
            node_id = str(params.get("id", ""))
            if node_id not in self._nodes:
                return FakeQueryResult(result_set=[])
            self._nodes[node_id].update(dict(params["props"]))
            return FakeQueryResult(result_set=[[self._node(self._nodes[node_id])]])

        if "DETACH DELETE" in q:
            node_id = str(params.get("id", ""))
            if node_id in self._nodes:
                del self._nodes[node_id]
                return FakeQueryResult(nodes_deleted=1)
            return FakeQueryResult(nodes_deleted=0)

        if "SKIP $offset" in q:
            ordered = sorted(self._nodes.items(), key=lambda kv: kv[0])
            offset = int(params.get("offset", 0))
            limit = int(params.get("limit", len(ordered)))
            page = ordered[offset: offset + limit]
            return FakeQueryResult(result_set=[[self._node(props)] for _, props in page])

        if "count(n)" in q:
            return FakeQueryResult(result_set=[[len(self._nodes)]])

        if "WHERE n.id = $id RETURN n" in q:
            node_id = str(params.get("id", ""))
            if node_id in self._nodes:
                return FakeQueryResult(result_set=[[self._node(self._nodes[node_id])]])
            return FakeQueryResult(result_set=[])

        if q.strip() == "RETURN 1":
            return FakeQueryResult(result_set=[[1]])

        return FakeQueryResult(result_set=[])


class FakeFalkorDBClient:
    """In-memory stand-in for ``falkordb.asyncio.FalkorDB``."""

    def __init__(self, graph: FakeGraph) -> None:
        self._graph = graph
        self.connection = MagicMock()
        self.connection.ping = AsyncMock(return_value=True)
        self.aclose = AsyncMock()

    def select_graph(self, graph_id: str) -> FakeGraph:
        return self._graph


@pytest.fixture
def fake_graph() -> FakeGraph:
    """A fresh, empty FakeGraph for each test."""
    return FakeGraph()


@pytest.fixture
def mock_falkordb_client(fake_graph: FakeGraph) -> FakeFalkorDBClient:
    """A fully mocked falkordb.asyncio.FalkorDB client. No network calls."""
    return FakeFalkorDBClient(fake_graph)


@pytest.fixture
def mock_settings():
    """A FalkorDBSettings instance with default connection details."""
    from openframe.adapters.db.falkordb import FalkorDBSettings
    return FalkorDBSettings()


@pytest.fixture
def repo(mock_settings, mock_falkordb_client):
    """A FalkorDBRepository wired to a mocked FalkorDB client."""
    from openframe.adapters.db.falkordb import FalkorDBRepository
    import openframe.adapters.db.falkordb.connection as conn_module

    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client
    r = FalkorDBRepository(mock_settings)
    yield r
    conn_module._client_cache.clear()
