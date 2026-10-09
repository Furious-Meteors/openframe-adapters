"""
tests/conftest.py — openframe-adapters-db-dynamodb
======================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.

All tests run with zero network calls. aioboto3 is mocked at the
``openframe.adapters.db.dynamodb.connection`` import level so no real
DynamoDB server (local or AWS) is needed.
"""
from __future__ import annotations

# Canonical OTel reset fixtures from openframe-core v3.0.
# Provides (autouse): reset_telemetry_state
# Provides (on-demand): span_exporter, metric_reader
from openframe.core.testing.fixtures import *  # noqa: F401, F403

import pytest
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def mock_table() -> MagicMock:
    """A fully mocked aioboto3 DynamoDB Table resource."""
    table = MagicMock()
    table.get_item = AsyncMock(return_value={})
    table.put_item = AsyncMock(return_value={})
    table.delete_item = AsyncMock(return_value={})
    table.scan = AsyncMock(return_value={"Items": [], "Count": 0})
    table.meta = MagicMock()
    table.meta.client = MagicMock()
    table.meta.client.describe_table = AsyncMock(return_value={})
    return table


@pytest.fixture
def mock_resource() -> MagicMock:
    """A fully mocked aioboto3 DynamoDB ServiceResource."""
    return MagicMock()


@pytest.fixture
def mock_resource_cm(mock_resource: MagicMock) -> MagicMock:
    """A fully mocked aioboto3 ResourceCreatorContext (async context manager)."""
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_resource)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


@pytest.fixture
def mock_settings() -> object:
    """A DynamoDBSettings instance with dummy region/table/credentials."""
    from openframe.adapters.db.dynamodb import DynamoDBSettings

    return DynamoDBSettings(
        aws_region="us-east-1",
        dynamodb_table_name="test-table",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture
def repo(mock_settings: object, mock_table: MagicMock, mock_resource_cm: MagicMock, monkeypatch: pytest.MonkeyPatch):
    """
    A DynamoDBRepository wired to a mocked Table.

    The mocked resource/table is injected directly into ``_table_cache`` so
    ``get_dynamodb_table()`` never attempts a real connection.
    """
    from openframe.adapters.db.dynamodb import DynamoDBRepository
    import openframe.adapters.db.dynamodb.connection as conn_module

    conn_module._table_cache[conn_module._cache_key(mock_settings)] = conn_module._CachedTable(  # type: ignore[attr-defined]
        resource_cm=mock_resource_cm,
        resource=mock_resource_cm.__aenter__.return_value,
        table=mock_table,
    )
    r = DynamoDBRepository(mock_settings, id_column="id")  # type: ignore[arg-type]
    yield r
    conn_module._table_cache.clear()
