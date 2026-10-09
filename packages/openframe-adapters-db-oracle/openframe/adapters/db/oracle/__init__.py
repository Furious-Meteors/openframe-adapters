"""
openframe.adapters.db.oracle
==============================
Oracle database adapter for the OpenFrame Microservice Suite.

Public API:

    OracleSettings    — Pydantic Settings subclass for connection config.
    OracleRepository  — Generic async repository (BaseRepository).
    get_oracle_pool   — Async factory that creates / returns the cached pool.
    OraclePlugin      — BasePort-satisfying plugin wrapper.

Quick start::

    from openframe.adapters.db.oracle import (
        OracleSettings,
        OracleRepository,
        get_oracle_pool,
    )

    settings = OracleSettings(oracle_dsn="app/secret@db.example.com:1521/orclpdb")

    # Raw dict mode
    repo = OracleRepository(settings, table="items", id_column="id")
    item = await repo.get("abc-123")           # dict | None

    # Typed mode — subclass and override mapping methods
    class ItemRepository(OracleRepository[Item]):
        _table = "items"
        _id_column = "id"

        def _row_to_entity(self, row):
            return Item(**row)

        def _entity_to_row(self, entity):
            return entity.model_dump()

Async strategy: native async via ``oracledb``'s ``connect_async()``/
``create_pool_async()`` thin-mode API — see ``connection.py``'s module
docstring for the research findings behind this choice.
"""
from __future__ import annotations

from .config import OracleSettings
from .connection import get_oracle_pool
from .plugin import OraclePlugin
from .repository import OracleRepository

__all__ = [
    "OracleSettings",
    "OracleRepository",
    "get_oracle_pool",
    "OraclePlugin",
]
