"""
openframe.adapters.db.chromadb
=================================
ChromaDB vector-store adapter for the OpenFrame Microservice Suite.

Public API:

    ChromaDBSettings    — Pydantic Settings subclass for connection config.
    ChromaDBRepository  — Generic async vector-store repository (BaseVectorStore).
    ChromaDBPlugin      — Plugin registry wrapper.
    get_chromadb_client — Async factory that creates / returns the cached client.

Quick start::

    from openframe.adapters.db.chromadb import (
        ChromaDBSettings,
        ChromaDBRepository,
        get_chromadb_client,
    )

    settings = ChromaDBSettings(chroma_host="localhost", chroma_port=8000)

    # Raw dict mode
    repo = ChromaDBRepository(settings, collection="items")
    item = await repo.get("abc-123")           # dict | None
    results = await repo.search(query_vector=[0.1, 0.2, 0.3], k=5)

    # Typed mode — subclass and override mapping methods
    class ItemRepository(ChromaDBRepository[Item]):
        _collection = "items"

        def _row_to_entity(self, row):
            return Item(id=row["id"], vector=row["vector"], **row["metadata"])

        def _entity_to_row(self, entity):
            return {"id": entity.id, "vector": entity.vector, "metadata": {"name": entity.name}}
"""
from __future__ import annotations

from .config import ChromaDBSettings
from .connection import get_chromadb_client
from .plugin import ChromaDBPlugin
from .repository import ChromaDBRepository

__all__ = [
    "ChromaDBSettings",
    "ChromaDBRepository",
    "get_chromadb_client",
    "ChromaDBPlugin",
]
