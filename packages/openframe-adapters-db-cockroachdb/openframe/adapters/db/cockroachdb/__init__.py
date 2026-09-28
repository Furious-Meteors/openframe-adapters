"""
openframe.adapters.db.cockroachdb
====================================
CockroachDB database adapter for the OpenFrame Microservice Suite.

CockroachDB speaks the PostgreSQL wire protocol, so this package uses the
same ``asyncpg`` driver as ``openframe-adapters-db-postgres`` and shares its
connection pooling and exception-classification design. See
``repository.py``'s module docstring and this package's README for the real
SQL-dialect and transaction-semantics differences from Postgres that matter
at this adapter's boundary (no ``SERIAL``, default ``SERIALIZABLE``
isolation with client-side retry requirements for explicit multi-statement
transactions).

Public API:

    CockroachdbSettings    — Pydantic Settings subclass for connection config.
    CockroachdbRepository  — Generic async repository (BaseRepository).
    get_cockroachdb_pool   — Async factory that creates / returns the cached pool.

Quick start::

    from openframe.adapters.db.cockroachdb import (
        CockroachdbSettings,
        CockroachdbRepository,
        get_cockroachdb_pool,
    )

    settings = CockroachdbSettings(cockroachdb_url="postgresql://user:pw@localhost:26257/db")

    # Raw dict mode
    repo = CockroachdbRepository(settings, table="items", id_column="id")
    item = await repo.get("abc-123")           # dict | None

    # Typed mode — subclass and override mapping methods
    class ItemRepository(CockroachdbRepository[Item]):
        _table = "items"
        _id_column = "id"

        def _row_to_entity(self, row):
            return Item(**dict(row))

        def _entity_to_row(self, entity):
            return entity.model_dump()
"""
from __future__ import annotations

from .config import CockroachdbSettings
from .connection import get_cockroachdb_pool
from .plugin import CockroachdbPlugin
from .repository import CockroachdbRepository

__all__ = [
    "CockroachdbSettings",
    "CockroachdbRepository",
    "get_cockroachdb_pool",
    "CockroachdbPlugin",
]
