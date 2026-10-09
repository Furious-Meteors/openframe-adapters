"""
tests/test_connection.py
==========================
Unit tests for get_dynamodb_table() and _table_cache.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import botocore.exceptions
import pytest

from openframe.adapters.db.dynamodb import DynamoDBSettings
from openframe.adapters.db.dynamodb.connection import (
    _cache_key,
    _table_cache,
    close_dynamodb_table,
    get_dynamodb_table,
)
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)


@pytest.fixture(autouse=True)
def clear_table_cache():
    """Ensure _table_cache is clean before and after every test."""
    _table_cache.clear()
    yield
    _table_cache.clear()


@pytest.fixture
def settings() -> DynamoDBSettings:
    return DynamoDBSettings(aws_region="us-east-1", dynamodb_table_name="items")


@pytest.fixture
def settings_alt_endpoint() -> DynamoDBSettings:
    return DynamoDBSettings(
        aws_region="us-east-1",
        dynamodb_table_name="items",
        endpoint_url="http://localhost:8000",
    )


def _make_session_mock(resource_obj: MagicMock) -> MagicMock:
    """A fake aioboto3.Session() whose .resource() is an async CM."""
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=resource_obj)
    cm.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.resource = MagicMock(return_value=cm)
    return session


class TestGetDynamoDBTable:
    async def test_returns_cached_table_on_second_call(self, settings: DynamoDBSettings) -> None:
        fake_resource = MagicMock()
        fake_table = MagicMock(name="table")
        fake_resource.Table = MagicMock(return_value=fake_table)
        session = _make_session_mock(fake_resource)

        with patch(
            "openframe.adapters.db.dynamodb.connection.aioboto3.Session",
            return_value=session,
        ):
            table1 = await get_dynamodb_table(settings)
            table2 = await get_dynamodb_table(settings)

        assert table1 is table2
        assert table1 is fake_table
        session.resource.assert_called_once()  # resource only created once

    async def test_different_endpoint_urls_produce_different_tables(
        self, settings: DynamoDBSettings, settings_alt_endpoint: DynamoDBSettings
    ) -> None:
        """
        Regression test: two Settings for the SAME region+table but
        different endpoint_url (e.g. local DynamoDB vs real AWS) must NOT
        share a resource/Table.
        """
        table_a = MagicMock(name="table_a")
        table_b = MagicMock(name="table_b")
        resource_a = MagicMock()
        resource_a.Table = MagicMock(return_value=table_a)
        resource_b = MagicMock()
        resource_b.Table = MagicMock(return_value=table_b)

        sessions = [_make_session_mock(resource_a), _make_session_mock(resource_b)]
        with patch(
            "openframe.adapters.db.dynamodb.connection.aioboto3.Session",
            side_effect=sessions,
        ):
            t1 = await get_dynamodb_table(settings)
            t2 = await get_dynamodb_table(settings_alt_endpoint)

        assert t1 is not t2
        assert t1 is table_a
        assert t2 is table_b

    async def test_table_stored_in_cache_after_creation(self, settings: DynamoDBSettings) -> None:
        fake_resource = MagicMock()
        fake_table = MagicMock()
        fake_resource.Table = MagicMock(return_value=fake_table)
        session = _make_session_mock(fake_resource)

        with patch(
            "openframe.adapters.db.dynamodb.connection.aioboto3.Session",
            return_value=session,
        ):
            await get_dynamodb_table(settings)

        assert _table_cache[_cache_key(settings)].table is fake_table

    async def test_no_credentials_raises_adapter_configuration_error(
        self, settings: DynamoDBSettings
    ) -> None:
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(side_effect=botocore.exceptions.NoCredentialsError())
        session = MagicMock()
        session.resource = MagicMock(return_value=cm)

        with patch(
            "openframe.adapters.db.dynamodb.connection.aioboto3.Session",
            return_value=session,
        ):
            with pytest.raises(AdapterConfigurationError) as exc_info:
                await get_dynamodb_table(settings)

        assert exc_info.value.operation == "init"

    async def test_no_region_raises_adapter_configuration_error(
        self, settings: DynamoDBSettings
    ) -> None:
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(side_effect=botocore.exceptions.NoRegionError())
        session = MagicMock()
        session.resource = MagicMock(return_value=cm)

        with patch(
            "openframe.adapters.db.dynamodb.connection.aioboto3.Session",
            return_value=session,
        ):
            with pytest.raises(AdapterConfigurationError):
                await get_dynamodb_table(settings)

    async def test_endpoint_connection_error_raises_adapter_connection_error(
        self, settings: DynamoDBSettings
    ) -> None:
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(
            side_effect=botocore.exceptions.EndpointConnectionError(endpoint_url="http://x")
        )
        session = MagicMock()
        session.resource = MagicMock(return_value=cm)

        with patch(
            "openframe.adapters.db.dynamodb.connection.aioboto3.Session",
            return_value=session,
        ):
            with pytest.raises(AdapterConnectionError):
                await get_dynamodb_table(settings)

    async def test_timeout_raises_adapter_timeout_error(self, settings: DynamoDBSettings) -> None:
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(side_effect=asyncio.TimeoutError())
        session = MagicMock()
        session.resource = MagicMock(return_value=cm)

        with patch(
            "openframe.adapters.db.dynamodb.connection.aioboto3.Session",
            return_value=session,
        ):
            with pytest.raises(AdapterTimeoutError) as exc_info:
                await get_dynamodb_table(settings)

        assert exc_info.value.operation == "connect"

    async def test_cache_key_includes_credentials(self) -> None:
        """
        Two Settings with the same region+table+endpoint but different
        credentials must not share a cache key.
        """
        s1 = DynamoDBSettings(
            aws_region="us-east-1", dynamodb_table_name="items", aws_access_key_id="a"
        )
        s2 = DynamoDBSettings(
            aws_region="us-east-1", dynamodb_table_name="items", aws_access_key_id="b"
        )
        assert _cache_key(s1) != _cache_key(s2)


class TestCloseDynamoDBTable:
    async def test_close_removes_from_cache_and_exits_cm(self, settings: DynamoDBSettings) -> None:
        fake_resource = MagicMock()
        fake_table = MagicMock()
        fake_resource.Table = MagicMock(return_value=fake_table)
        session = _make_session_mock(fake_resource)

        with patch(
            "openframe.adapters.db.dynamodb.connection.aioboto3.Session",
            return_value=session,
        ):
            await get_dynamodb_table(settings)

        key = _cache_key(settings)
        assert key in _table_cache
        await close_dynamodb_table(settings)
        assert key not in _table_cache

    async def test_close_is_noop_when_nothing_cached(self, settings: DynamoDBSettings) -> None:
        # Must not raise even though nothing was ever created for `settings`.
        await close_dynamodb_table(settings)
