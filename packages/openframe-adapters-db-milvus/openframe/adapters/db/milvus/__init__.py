"""
openframe.adapters.db.milvus
===============================
Milvus vector-database adapter for the OpenFrame Microservice Suite.

Public API:

    MilvusSettings    — Pydantic Settings subclass for connection config.
    MilvusRepository  — Generic async vector-store repository (BaseVectorStore).
    MilvusPlugin      — Plugin wrapper for the OpenFrame plugin registry.
    get_milvus_client — Async factory that creates / returns the cached client.

Quick start::

    from openframe.adapters.db.milvus import (
        MilvusSettings,
        MilvusRepository,
        get_milvus_client,
    )

    settings = MilvusSettings(milvus_uri="http://localhost:19530")

    # Raw dict mode
    repo = MilvusRepository(settings, collection_name="items")
    item = await repo.get("abc-123")           # dict | None
    results = await repo.search(query_vector=[0.1, 0.2, 0.3], k=5)

    # Typed mode — subclass and override mapping methods
    class ItemRepository(MilvusRepository[Item]):
        _collection_name = "items"

        def _row_to_entity(self, row):
            return Item(**row)

        def _entity_to_row(self, entity):
            return entity.model_dump()
"""
from __future__ import annotations

from .config import MilvusSettings
from .connection import get_milvus_client
from .plugin import MilvusPlugin
from .repository import MilvusRepository

__all__ = [
    "MilvusSettings",
    "MilvusRepository",
    "get_milvus_client",
    "MilvusPlugin",
]
