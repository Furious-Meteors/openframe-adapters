"""
tests/test_repository.py — openframe-adapters-db-dynamodb
=============================================================
Contract tests (RepositoryContractTests) run first, then adapter-specific
unit tests covering DynamoDB error mapping and driver behaviour.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import botocore.exceptions
import pytest

from openframe.adapters.db.dynamodb import DynamoDBRepository
from openframe.core.exceptions import (
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository
from openframe.core.testing import RepositoryContractTests


def _client_error(code: str, message: str = "boom") -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        error_response={"Error": {"Code": code, "Message": message}},
        operation_name="Op",
    )


# ── In-memory fake Table — simulates a DynamoDB table for contract tests ───

class _FakeTable:
    """A stateful fake aioboto3 Table backed by an in-memory dict "table"."""

    def __init__(self, store: dict[str, dict]) -> None:
        self._store = store
        self.meta = MagicMock()
        self.meta.client = MagicMock()
        self.meta.client.describe_table = AsyncMock(return_value={})

    async def get_item(self, Key: dict) -> dict:
        entity_id = Key["id"]
        item = self._store.get(entity_id)
        return {"Item": item} if item is not None else {}

    async def put_item(self, Item: dict, ConditionExpression=None, ExpressionAttributeNames=None) -> dict:
        entity_id = Item["id"]
        if ConditionExpression is not None and entity_id not in self._store:
            raise _client_error("ConditionalCheckFailedException", "no such item")
        self._store[entity_id] = Item
        return {}

    async def delete_item(self, Key: dict, ReturnValues: str | None = None) -> dict:
        entity_id = Key["id"]
        old = self._store.pop(entity_id, None)
        if old is not None and ReturnValues == "ALL_OLD":
            return {"Attributes": old}
        return {}

    async def scan(self, **kwargs) -> dict:
        items = list(self._store.values())
        if kwargs.get("Select") == "COUNT":
            return {"Count": len(items)}
        return {"Items": items}


# ── Contract tests — must pass for every BaseRepository implementation ─────

class TestDynamoDBRepositoryContracts(RepositoryContractTests):
    """
    DynamoDBRepository passes the full openframe contract suite.

    All RepositoryContractTests run against a mocked aioboto3 Table backed
    by an in-memory dict "table". No real DynamoDB required.
    """

    @pytest.fixture
    def repository(self, mock_settings, mock_resource_cm):
        import openframe.adapters.db.dynamodb.connection as conn_module

        _store: dict[str, dict] = {}
        fake_table = _FakeTable(_store)
        conn_module._table_cache[conn_module._cache_key(mock_settings)] = conn_module._CachedTable(
            resource_cm=mock_resource_cm,
            resource=mock_resource_cm.__aenter__.return_value,
            table=fake_table,
        )

        r = DynamoDBRepository(mock_settings, id_column="id")
        yield r
        conn_module._table_cache.clear()

    @pytest.fixture
    def port(self, repository):
        return repository

    @pytest.fixture
    def make_entity(self):
        def _make(id: str, name: str = "test") -> dict:
            return {"id": id, "name": name}
        return _make


# ── Adapter-specific tests — beyond what the contract covers ───────────────


class TestProtocolConformance:
    def test_isinstance_base_repository(self, repo: DynamoDBRepository) -> None:
        assert isinstance(repo, BaseRepository)


class TestInit:
    def test_default_id_column(self, mock_settings) -> None:
        repo = DynamoDBRepository(mock_settings)
        assert repo._id_column == "id"

    def test_id_column_from_init_arg(self, mock_settings) -> None:
        repo = DynamoDBRepository(mock_settings, id_column="pk")
        assert repo._id_column == "pk"

    def test_id_column_from_class_attribute(self, mock_settings) -> None:
        class OrderRepo(DynamoDBRepository):
            _id_column = "order_id"

        repo = OrderRepo(mock_settings)
        assert repo._id_column == "order_id"


class TestGet:
    async def test_get_found_returns_dict(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        item = {"id": "1", "name": "widget"}
        mock_table.get_item.return_value = {"Item": item}
        result = await repo.get("1")
        mock_table.get_item.assert_called_once_with(Key={"id": "1"})
        assert result == item

    async def test_get_not_found_returns_none(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.get_item.return_value = {}
        result = await repo.get("missing")
        assert result is None

    async def test_get_validation_exception_raises_adapter_query_error(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.get_item.side_effect = _client_error("ValidationException")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_resource_not_found_raises_adapter_query_error(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.get_item.side_effect = _client_error("ResourceNotFoundException")
        with pytest.raises(AdapterQueryError):
            await repo.get("1")

    @pytest.mark.parametrize(
        "code", ["ProvisionedThroughputExceededException", "ThrottlingException", "RequestLimitExceeded"]
    )
    async def test_get_throttling_raises_adapter_connection_error(
        self, repo: DynamoDBRepository, mock_table: MagicMock, code: str
    ) -> None:
        """
        Regression test for the classification gotcha: throttling-class
        errors share botocore.exceptions.ClientError with every other
        DynamoDB service error, so the classifier must inspect
        exc.response["Error"]["Code"], not just the exception type. These
        are transient capacity errors and must surface as retryable.
        """
        mock_table.get_item.side_effect = _client_error(code)
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get("1")
        assert exc_info.value.retryable is True

    async def test_get_endpoint_connection_error_raises_adapter_connection_error(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.get_item.side_effect = botocore.exceptions.EndpointConnectionError(
            endpoint_url="http://x"
        )
        with pytest.raises(AdapterConnectionError):
            await repo.get("1")

    async def test_get_timeout_raises_adapter_timeout_error(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.get_item.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"


class TestList:
    async def test_list_returns_items_and_count(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        items = [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]
        mock_table.scan.side_effect = [
            {"Items": items},
            {"Count": 2},
        ]
        entities, count = await repo.list(limit=10, offset=0)
        assert entities == items
        assert count == 2

    async def test_list_respects_offset_and_limit(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        items = [{"id": str(i)} for i in range(5)]
        mock_table.scan.side_effect = [
            {"Items": items},
            {"Count": 5},
        ]
        entities, count = await repo.list(limit=2, offset=2)
        assert entities == items[2:4]
        assert count == 5

    async def test_list_query_error_raises_adapter_query_error(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.scan.side_effect = _client_error("ValidationException")
        with pytest.raises(AdapterQueryError):
            await repo.list(limit=10, offset=0)

    async def test_list_timeout_raises_adapter_timeout_error(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.scan.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.list(limit=10, offset=0)


class TestCreate:
    async def test_create_returns_entity(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        entity = {"id": "99", "name": "thing"}
        result = await repo.create(entity)
        mock_table.put_item.assert_called_once_with(Item=entity)
        assert result == entity

    async def test_create_validation_error_raises_adapter_query_error(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.put_item.side_effect = _client_error("ValidationException")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.create({"id": "1", "name": "x"})
        assert exc_info.value.operation == "create"

    async def test_create_timeout_raises_adapter_timeout_error(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.put_item.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.create({"id": "1", "name": "x"})


class TestUpdate:
    async def test_update_returns_entity_on_success(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        entity = {"id": "1", "name": "updated"}
        result = await repo.update(entity)
        assert result == entity
        mock_table.put_item.assert_called_once_with(
            Item=entity,
            ConditionExpression="attribute_exists(#id)",
            ExpressionAttributeNames={"#id": "id"},
        )

    async def test_update_returns_none_on_conditional_check_failed(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        """
        Regression test: ConditionalCheckFailedException means "item doesn't
        exist" and must be translated to None per BaseRepository's contract,
        not raised as AdapterQueryError.
        """
        mock_table.put_item.side_effect = _client_error("ConditionalCheckFailedException")
        result = await repo.update({"id": "999", "name": "ghost"})
        assert result is None

    async def test_update_other_client_error_raises_adapter_query_error(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.put_item.side_effect = _client_error("ValidationException")
        with pytest.raises(AdapterQueryError):
            await repo.update({"id": "1", "name": "x"})

    async def test_update_timeout_raises_adapter_timeout_error(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.put_item.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.update({"id": "1", "name": "x"})


class TestDelete:
    async def test_delete_existing_returns_true(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.delete_item.return_value = {"Attributes": {"id": "1"}}
        result = await repo.delete("1")
        assert result is True

    async def test_delete_missing_returns_false(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.delete_item.return_value = {}
        result = await repo.delete("missing")
        assert result is False

    async def test_delete_query_error_raises_adapter_query_error(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.delete_item.side_effect = _client_error("ValidationException")
        with pytest.raises(AdapterQueryError):
            await repo.delete("1")

    async def test_delete_timeout_raises_adapter_timeout_error(
        self, repo: DynamoDBRepository, mock_table: MagicMock
    ) -> None:
        mock_table.delete_item.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.delete("1")


class TestClose:
    async def test_close_removes_table_from_cache(
        self, repo: DynamoDBRepository, mock_settings, mock_resource_cm
    ) -> None:
        import openframe.adapters.db.dynamodb.connection as conn_module

        key = conn_module._cache_key(mock_settings)
        assert key in conn_module._table_cache
        await repo.close()
        assert key not in conn_module._table_cache
        mock_resource_cm.__aexit__.assert_awaited_once()
