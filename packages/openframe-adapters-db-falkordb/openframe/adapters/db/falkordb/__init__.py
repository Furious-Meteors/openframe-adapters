"""
openframe.adapters.db.falkordb
=================================
FalkorDB graph-database adapter for the OpenFrame Microservice Suite.

Implements ``openframe-core``'s ``BaseGraphStore[T]`` port (requires
openframe-core>=3.6) — id-addressable node CRUD (inherited from
``BaseRepository[T]``) plus ``traverse()`` for arbitrary Cypher
pattern-matching and relationship/edge creation.

Public API::

    FalkorDBSettings    — Pydantic Settings subclass for connection config.
    FalkorDBRepository  — Generic async graph repository (BaseGraphStore).
    get_falkordb_client — Async factory that creates / returns the cached client.
    FalkorDBPlugin      — BasePort implementation for PluginRegistry.

Quick start::

    from openframe.adapters.db.falkordb import (
        FalkorDBSettings,
        FalkorDBRepository,
    )

    settings = FalkorDBSettings(falkordb_host="localhost", falkordb_port=6379)

    # Raw dict mode
    repo = FalkorDBRepository(settings)
    await repo.create({"id": "1", "name": "Alice"})
    alice = await repo.get("1")                 # {"id": "1", "name": "Alice"}

    # Typed mode — subclass and override mapping methods
    class PersonRepository(FalkorDBRepository[Person]):
        def _node_to_entity(self, node):
            return Person(**node.properties)
        def _entity_to_properties(self, entity):
            return entity.model_dump()
"""
from __future__ import annotations

from .config import FalkorDBSettings
from .connection import get_falkordb_client
from .plugin import FalkorDBPlugin
from .repository import FalkorDBRepository

__all__ = [
    "FalkorDBSettings",
    "FalkorDBRepository",
    "get_falkordb_client",
    "FalkorDBPlugin",
]
