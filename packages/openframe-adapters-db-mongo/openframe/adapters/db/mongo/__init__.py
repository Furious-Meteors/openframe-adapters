"""
openframe.adapters.db.mongo
=============================
MongoDB document store adapter for the OpenFrame Microservice Suite.

Public API:

    MongoSettings      — Pydantic Settings subclass for connection config.
    MongoRepository    — Generic async repository (BaseRepository).
    get_mongo_client   — Synchronous factory that creates / returns the cached client.

Quick start::

    from openframe.adapters.db.mongo import (
        MongoSettings,
        MongoRepository,
        get_mongo_client,
    )

    settings = MongoSettings(
        mongo_url="mongodb://user:pw@localhost:27017",
        mongo_database="mydb",
    )

    # Raw dict mode
    repo = MongoRepository(settings, collection="artifacts")
    doc = await repo.get("507f1f77bcf86cd799439011")    # dict | None

    # Typed mode — subclass and override mapping methods
    class ArtifactRepository(MongoRepository[Artifact]):
        _collection = "artifacts"

        def _doc_to_entity(self, doc):
            return Artifact(**doc)

        def _entity_to_doc(self, entity):
            return entity.model_dump()
"""
from __future__ import annotations

from .config import MongoSettings
from .connection import get_mongo_client
from .plugin import MongoPlugin
from .repository import MongoRepository

__all__ = [
    "MongoSettings",
    "MongoRepository",
    "get_mongo_client",
    "MongoPlugin",
]
