"""
openframe.adapters.db.postgres
================================
PostgreSQL database adapter for the OpenFrame Microservice Suite.

Public API:

    PostgresSettings    — Pydantic Settings subclass for connection config.
    PostgresRepository  — Generic async repository (BaseRepository + HealthCheck).
    get_postgres_pool   — Async factory that creates / returns the cached pool.

Quick start::

    from openframe.adapters.db.postgres import (
        PostgresSettings,
        PostgresRepository,
        get_postgres_pool,
    )

    settings = PostgresSettings(database_url="postgresql://user:pw@localhost/db")

    # Raw dict mode
    repo = PostgresRepository(settings, table="items", id_column="id")
    item = await repo.get("abc-123")           # dict | None

    # Typed mode — subclass and override mapping methods
    class ItemRepository(PostgresRepository[Item]):
        _table = "items"
        _id_column = "id"

        def _row_to_entity(self, row):
            return Item(**dict(row))

        def _entity_to_row(self, entity):
            return entity.model_dump()
"""
from __future__ import annotations

from .config import PostgresSettings
from .connection import get_postgres_pool
from .plugin import PostgresPlugin
from .repository import PostgresRepository

__all__ = [
    "PostgresSettings",
    "PostgresRepository",
    "get_postgres_pool",
    "PostgresPlugin",
]
