"""
openframe.adapters.db.cassandra
==================================
Cassandra database adapter for the OpenFrame Microservice Suite.

Public API:

    CassandraSettings     — Pydantic Settings subclass for connection config.
    CassandraRepository   — Generic async repository (BaseRepository).
    CassandraPlugin       — BasePort-satisfying plugin wrapper.
    get_cassandra_session — Async factory that creates / returns the cached
                            cassandra-driver Session.

Quick start::

    from openframe.adapters.db.cassandra import (
        CassandraSettings,
        CassandraRepository,
        get_cassandra_session,
    )

    settings = CassandraSettings(cassandra_contact_points=["10.0.0.1"])

    # Raw dict mode
    repo = CassandraRepository(settings, table="items", id_column="id")
    item = await repo.get("abc-123")           # dict | None

    # Typed mode — subclass and override mapping methods
    class ItemRepository(CassandraRepository[Item]):
        _table = "items"
        _id_column = "id"

        def _row_to_entity(self, row):
            return Item(**dict(row._asdict()))

        def _entity_to_row(self, entity):
            return entity.model_dump()

See ``connection.py``'s module docstring for the async-strategy research
finding this package is built on: query execution is bridged from
cassandra-driver's native ``ResponseFuture`` callbacks to asyncio (no
thread-pool thread held per query), while one-time cluster/session setup
is wrapped in ``run_in_executor`` (the driver has no async-native connect).
"""
from __future__ import annotations

from .config import CassandraSettings
from .connection import get_cassandra_session
from .plugin import CassandraPlugin
from .repository import CassandraRepository

__all__ = [
    "CassandraSettings",
    "CassandraRepository",
    "get_cassandra_session",
    "CassandraPlugin",
]
