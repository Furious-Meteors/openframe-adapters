"""
tests/conftest.py — openframe-adapters-db-milvus
====================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.

All tests run with zero network calls. ``pymilvus.AsyncMilvusClient`` is
mocked at the ``openframe.adapters.db.milvus.connection`` import level so
no real Milvus server is needed.
"""
from __future__ import annotations

# Canonical OTel reset fixtures from openframe-core v3.0.
# Provides (autouse): reset_telemetry_state
# Provides (on-demand): span_exporter, metric_reader
from openframe.core.testing.fixtures import *  # noqa: F401, F403

import pytest
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def mock_client() -> MagicMock:
    """A fully mocked AsyncMilvusClient."""
    client = MagicMock()
    client.get = AsyncMock()
    client.query = AsyncMock()
    client.upsert = AsyncMock()
    client.insert = AsyncMock()
    client.delete = AsyncMock()
    client.search = AsyncMock()
    client.list_collections = AsyncMock(return_value=[])
    client.close = AsyncMock()
    return client


@pytest.fixture
def mock_settings() -> object:
    """A MilvusSettings instance with a dummy URI."""
    from openframe.adapters.db.milvus import MilvusSettings

    return MilvusSettings(milvus_uri="http://localhost:19530")


@pytest.fixture
def repo(mock_settings: object, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch):
    """
    A MilvusRepository wired to a mocked client.

    The mock client is injected directly into ``_client_cache`` so
    ``get_milvus_client()`` never attempts a real connection.
    """
    from openframe.adapters.db.milvus import MilvusRepository
    import openframe.adapters.db.milvus.connection as conn_module

    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client  # type: ignore[attr-defined]
    r = MilvusRepository(mock_settings, collection_name="items")  # type: ignore[arg-type]
    yield r
    conn_module._client_cache.clear()
