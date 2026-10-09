"""
openframe.adapters.db.qdrant
==============================
Qdrant vector-store adapter for the OpenFrame Microservice Suite.

Public API:

    QdrantSettings    — Pydantic Settings subclass for connection config.
    QdrantVectorStore — Generic async vector store (BaseVectorStore).
    get_qdrant_client — Async factory that creates / returns the cached client.
    QdrantPlugin      — BasePort-satisfying plugin wrapper.

Quick start::

    from openframe.adapters.db.qdrant import (
        QdrantSettings,
        QdrantVectorStore,
        get_qdrant_client,
    )

    settings = QdrantSettings(qdrant_url="http://localhost:6333")

    # Raw dict mode
    store = QdrantVectorStore(settings, collection="documents")
    item = await store.get("abc-123")               # dict | None
    results = await store.search([0.1, 0.2, 0.3], k=5)

    # Typed mode — subclass and override mapping methods
    class DocumentStore(QdrantVectorStore[Document]):
        _collection = "documents"

        def _point_to_entity(self, point):
            return Document(id=point.id, vector=point.vector, **point.payload)

        def _entity_to_point(self, entity):
            from qdrant_client.http.models import PointStruct
            return PointStruct(id=entity.id, vector=entity.vector, payload={"text": entity.text})
"""
from __future__ import annotations

from .config import QdrantSettings
from .connection import get_qdrant_client
from .plugin import QdrantPlugin
from .repository import QdrantVectorStore

__all__ = [
    "QdrantSettings",
    "QdrantVectorStore",
    "get_qdrant_client",
    "QdrantPlugin",
]
