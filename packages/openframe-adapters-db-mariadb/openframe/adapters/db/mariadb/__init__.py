"""
openframe.adapters.db.mariadb
==============================
MariaDB database adapter for the OpenFrame Microservice Suite.

MariaDB is a wire-protocol-compatible fork of MySQL, so this adapter is
built on the same ``aiomysql`` driver (which wraps ``PyMySQL``) as
``openframe-adapters-db-mysql`` and shares its exception-classification
logic — see ``connection.py`` and ``repository.py``'s module docstrings for
the specific MySQL/MariaDB error-code compatibility notes.

Public API:

    MariadbSettings    — Pydantic Settings subclass for connection config.
    MariadbRepository  — Generic async repository (BaseRepository).
    get_mariadb_pool   — Async factory that creates / returns the cached pool.

Quick start::

    from openframe.adapters.db.mariadb import (
        MariadbSettings,
        MariadbRepository,
        get_mariadb_pool,
    )

    settings = MariadbSettings(database_url="mysql://user:pw@localhost/db")

    # Raw dict mode
    repo = MariadbRepository(settings, table="items", id_column="id")
    item = await repo.get("abc-123")           # dict | None

    # Typed mode — subclass and override mapping methods
    class ItemRepository(MariadbRepository[Item]):
        _table = "items"
        _id_column = "id"

        def _row_to_entity(self, row):
            return Item(**row)

        def _entity_to_row(self, entity):
            return entity.model_dump()
"""
from __future__ import annotations

from .config import MariadbSettings
from .connection import get_mariadb_pool
from .plugin import MariadbPlugin
from .repository import MariadbRepository

__all__ = [
    "MariadbSettings",
    "MariadbRepository",
    "get_mariadb_pool",
    "MariadbPlugin",
]
