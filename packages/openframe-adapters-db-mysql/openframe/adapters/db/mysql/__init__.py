"""
openframe.adapters.db.mysql
==============================
MySQL database adapter for the OpenFrame Microservice Suite.

Public API:

    MySQLSettings    — Pydantic Settings subclass for connection config.
    MySQLRepository  — Generic async repository (BaseRepository).
    get_mysql_pool   — Async factory that creates / returns the cached pool.

Quick start::

    from openframe.adapters.db.mysql import (
        MySQLSettings,
        MySQLRepository,
        get_mysql_pool,
    )

    settings = MySQLSettings(database_url="mysql://user:pw@localhost/db")

    # Raw dict mode
    repo = MySQLRepository(settings, table="items", id_column="id")
    item = await repo.get("abc-123")           # dict | None

    # Typed mode — subclass and override mapping methods
    class ItemRepository(MySQLRepository[Item]):
        _table = "items"
        _id_column = "id"

        def _row_to_entity(self, row):
            return Item(**row)

        def _entity_to_row(self, entity):
            return entity.model_dump()
"""
from __future__ import annotations

from .config import MySQLSettings
from .connection import get_mysql_pool
from .plugin import MySQLPlugin
from .repository import MySQLRepository

__all__ = [
    "MySQLSettings",
    "MySQLRepository",
    "get_mysql_pool",
    "MySQLPlugin",
]
