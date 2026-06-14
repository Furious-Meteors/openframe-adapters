"""
tests/conftest.py
==================
Shared pytest fixtures for openframe-adapters-db-mongo.

All tests run with zero network calls. Motor is mocked at the
``openframe.adapters.db.mongo.connection`` import level so no real
MongoDB server is needed.
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def mock_collection() -> MagicMock:
    """A fully mocked AsyncIOMotorCollection."""
    col = MagicMock()
    col.find_one = AsyncMock()
    col.insert_one = AsyncMock()
    col.find_one_and_update = AsyncMock()
    col.delete_one = AsyncMock()
    col.count_documents = AsyncMock()

    cursor = MagicMock()
    cursor.skip = MagicMock(return_value=cursor)
    cursor.limit = MagicMock(return_value=cursor)
    cursor.to_list = AsyncMock(return_value=[])
    col.find = MagicMock(return_value=cursor)
    return col


@pytest.fixture
def mock_client(mock_collection: MagicMock) -> MagicMock:
    """A fully mocked AsyncIOMotorClient."""
    client = MagicMock()
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=mock_collection)
    db.list_collection_names = AsyncMock(return_value=["artifacts"])
    client.__getitem__ = MagicMock(return_value=db)
    client.admin = MagicMock()
    client.admin.command = AsyncMock(return_value={"ok": 1})
    client.close = MagicMock()
    return client


@pytest.fixture
def mock_settings() -> object:
    """A MongoSettings instance with dummy connection details."""
    from openframe.adapters.db.mongo import MongoSettings

    return MongoSettings(
        mongo_url="mongodb://test:test@localhost:27017",
        mongo_database="test_db",
    )


@pytest.fixture
def repo(
    mock_settings: object,
    mock_client: MagicMock,
) -> object:
    """
    A MongoRepository wired to a mocked client.

    The mock client is injected directly into ``_client_cache`` so
    ``get_mongo_client()`` never attempts a real connection.
    """
    from openframe.adapters.db.mongo import MongoRepository
    import openframe.adapters.db.mongo.connection as conn_module

    conn_module._client_cache[mock_settings.mongo_url] = mock_client  # type: ignore[attr-defined]
    r = MongoRepository(mock_settings, collection="artifacts")  # type: ignore[arg-type]
    yield r
    conn_module._client_cache.clear()
