"""
tests/conftest.py — openframe-adapters-db-chromadb
=====================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.

All tests run with zero network calls. ``chromadb.AsyncHttpClient`` is
mocked at the ``openframe.adapters.db.chromadb.connection`` import level —
no real Chroma server is needed. ``mock_collection`` is a stateful fake
backed by a plain dict, reproducing Chroma's actual columnar
(parallel-list) wire format for ``get()``/``query()`` closely enough for
the contract-test suite's behavioural assertions (create -> get, list
pagination, search ranking, etc.) to pass without a real backend.
"""
from __future__ import annotations

import math
from unittest.mock import AsyncMock, MagicMock

import pytest

# Canonical OTel reset fixtures from openframe-core v3.0.
# Provides (autouse): reset_telemetry_state
# Provides (on-demand): span_exporter, metric_reader
from openframe.core.testing.fixtures import *  # noqa: F401, F403

import chromadb.errors as chromadb_errors


def _euclidean(a: list[float], b: list[float]) -> float:
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


class _FakeAsyncCollection:
    """
    Stateful fake reproducing chromadb's AsyncCollection behaviour closely
    enough for contract + unit tests — no real Chroma server involved.
    """

    def __init__(self) -> None:
        self._store: dict[str, dict] = {}

    async def add(self, ids, embeddings=None, metadatas=None, documents=None, images=None, uris=None):
        embeddings = embeddings or [None] * len(ids)
        metadatas = metadatas or [None] * len(ids)
        documents = documents or [None] * len(ids)
        for i, entity_id in enumerate(ids):
            if entity_id in self._store:
                raise chromadb_errors.IDAlreadyExistsError(f"ID {entity_id} already exists")
        for i, entity_id in enumerate(ids):
            self._store[entity_id] = {
                "embedding": list(embeddings[i]) if embeddings[i] is not None else None,
                "metadata": dict(metadatas[i]) if metadatas[i] is not None else None,
                "document": documents[i],
            }

    async def get(self, ids=None, where=None, limit=None, offset=None, where_document=None, include=None):
        if ids is not None:
            if isinstance(ids, str):
                ids = [ids]
            found = [i for i in ids if i in self._store]
        else:
            all_ids = list(self._store.keys())
            offset = offset or 0
            found = all_ids[offset: offset + limit] if limit is not None else all_ids[offset:]
        return {
            "ids": found,
            "embeddings": [self._store[i]["embedding"] for i in found],
            "metadatas": [self._store[i]["metadata"] for i in found],
            "documents": [self._store[i]["document"] for i in found],
        }

    async def update(self, ids, embeddings=None, metadatas=None, documents=None, images=None, uris=None):
        if isinstance(ids, str):
            ids = [ids]
        for i, entity_id in enumerate(ids):
            if entity_id not in self._store:
                continue
            row = self._store[entity_id]
            if embeddings is not None and embeddings[i] is not None:
                row["embedding"] = list(embeddings[i])
            if metadatas is not None and metadatas[i] is not None:
                row["metadata"] = dict(metadatas[i])
            if documents is not None and documents[i] is not None:
                row["document"] = documents[i]

    async def upsert(self, ids, embeddings=None, metadatas=None, documents=None, images=None, uris=None):
        if isinstance(ids, str):
            ids = [ids]
        for i, entity_id in enumerate(ids):
            self._store[entity_id] = {
                "embedding": list(embeddings[i]) if embeddings is not None and embeddings[i] is not None else None,
                "metadata": dict(metadatas[i]) if metadatas is not None and metadatas[i] is not None else None,
                "document": documents[i] if documents is not None else None,
            }

    async def delete(self, ids=None, where=None, where_document=None, limit=None):
        if ids:
            for entity_id in ids:
                self._store.pop(entity_id, None)
        return {}

    async def query(self, query_embeddings, query_texts=None, query_images=None, query_uris=None,
                     ids=None, n_results=10, where=None, where_document=None, include=None):
        query_vector = list(query_embeddings[0])
        scored = sorted(
            self._store.items(),
            key=lambda kv: _euclidean(query_vector, kv[1]["embedding"]),
        )
        top = scored[:n_results]
        return {
            "ids": [[k for k, _ in top]],
            "embeddings": [[v["embedding"] for _, v in top]],
            "metadatas": [[v["metadata"] for _, v in top]],
            "documents": [[v["document"] for _, v in top]],
            "distances": [[_euclidean(query_vector, v["embedding"]) for _, v in top]],
        }

    async def count(self) -> int:
        return len(self._store)


@pytest.fixture
def mock_collection() -> _FakeAsyncCollection:
    """A stateful fake AsyncCollection — no real Chroma server needed."""
    return _FakeAsyncCollection()


@pytest.fixture
def mock_client(mock_collection: _FakeAsyncCollection) -> MagicMock:
    """A fully mocked AsyncClientAPI that always returns mock_collection."""
    client = MagicMock()
    client.get_or_create_collection = AsyncMock(return_value=mock_collection)
    client.create_collection = AsyncMock(return_value=mock_collection)
    client.get_collection = AsyncMock(return_value=mock_collection)
    return client


@pytest.fixture
def mock_settings() -> object:
    """A ChromaDBSettings instance with a dummy collection name."""
    from openframe.adapters.db.chromadb import ChromaDBSettings

    return ChromaDBSettings(chroma_collection="items")


@pytest.fixture
def repo(mock_settings: object, mock_client: MagicMock):
    """
    A ChromaDBRepository wired to a mocked AsyncClientAPI.

    The mock client is injected directly into ``_client_cache`` so
    ``get_chromadb_client()`` never attempts a real connection.
    """
    from openframe.adapters.db.chromadb import ChromaDBRepository
    import openframe.adapters.db.chromadb.connection as conn_module

    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client  # type: ignore[attr-defined]
    r = ChromaDBRepository(mock_settings, collection="items")  # type: ignore[arg-type]
    yield r
    conn_module._client_cache.clear()
