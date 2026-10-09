"""
tests/conftest.py — openframe-adapters-db-qdrant
====================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.

All tests run with zero network calls. ``AsyncQdrantClient`` is mocked at
the ``openframe.adapters.db.qdrant.connection`` import level so no real
Qdrant server is needed.
"""
from __future__ import annotations

# Canonical OTel reset fixtures from openframe-core v3.0.
# Provides (autouse): reset_telemetry_state
# Provides (on-demand): span_exporter, metric_reader
from openframe.core.testing.fixtures import *  # noqa: F401, F403

import pytest
from unittest.mock import AsyncMock, MagicMock

from qdrant_client.http.models import CountResult


@pytest.fixture
def mock_client() -> MagicMock:
    """A fully mocked AsyncQdrantClient."""
    client = MagicMock()
    client.get_collections = AsyncMock()
    client.upsert = AsyncMock()
    client.retrieve = AsyncMock(return_value=[])
    client.scroll = AsyncMock(return_value=([], None))
    client.count = AsyncMock(return_value=CountResult(count=0))
    client.delete = AsyncMock()
    client.query_points = AsyncMock()
    client.close = AsyncMock()
    return client


@pytest.fixture
def mock_settings() -> object:
    """A QdrantSettings instance with a dummy URL."""
    from openframe.adapters.db.qdrant import QdrantSettings

    return QdrantSettings(qdrant_url="http://localhost:6333")


@pytest.fixture
def repo(mock_settings: object, mock_client: MagicMock):
    """
    A QdrantVectorStore wired to a mocked client.

    The mock client is injected directly into ``_client_cache`` so
    ``get_qdrant_client()`` never attempts a real connection.
    """
    from openframe.adapters.db.qdrant import QdrantVectorStore
    import openframe.adapters.db.qdrant.connection as conn_module

    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client  # type: ignore[attr-defined]
    r = QdrantVectorStore(mock_settings, collection="items")  # type: ignore[arg-type]
    yield r
    conn_module._client_cache.clear()
