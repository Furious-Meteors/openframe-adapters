"""
tests/test_repository.py
==========================
Unit tests for PostgresRepository CRUD operations and Protocol conformance.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from openframe.adapters.db.postgres import PostgresRepository
from openframe.core.exceptions import AdapterConfigurationError, AdapterQueryError, AdapterTimeoutError
from openframe.core.health import HealthCheck
from openframe.core.ports import BaseRepository


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_record(data: dict) -> MagicMock:
    """Simulate an asyncpg.Record — dict-castable MagicMock."""
    record = MagicMock()
    record.__iter__ = MagicMock(return_value=iter(data.items()))
    record.keys = MagicMock(return_value=list(data.keys()))
    record.items = MagicMock(return_value=data.items())
    # dict(record) calls keys() then __getitem__
    record.__getitem__ = MagicMock(side_effect=data.__getitem__)
    return record


class TestProtocolConformance:
    def test_isinstance_base_repository(self, repo: PostgresRepository) -> None:
        assert isinstance(repo, BaseRepository)

    def test_isinstance_health_check(self, repo: PostgresRepository) -> None:
        assert isinstance(repo, HealthCheck)


class TestInit:
    def test_missing_table_raises_configuration_error(self, mock_settings) -> None:
        with pytest.raises(AdapterConfigurationError):
            PostgresRepository(mock_settings)

    def test_table_from_init_arg(self, mock_settings) -> None:
        repo = PostgresRepository(mock_settings, table="orders")
        assert repo._table == "orders"

    def test_table_from_class_attribute(self, mock_settings) -> None:
        class OrderRepo(PostgresRepository):
            _table = "orders"

        repo = OrderRepo(mock_settings)
        assert repo._table == "orders"


class TestGet:
    async def test_get_found_returns_dict(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        row_data = {"id": "1", "name": "widget"}
        mock_pool.fetchrow.return_value = row_data
        result = await repo.get("1")
        mock_pool.fetchrow.assert_called_once()
        assert result == row_data

    async def test_get_not_found_returns_none(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.fetchrow.return_value = None
        result = await repo.get("missing")
        assert result is None

    async def test_get_postgres_error_raises_adapter_query_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncpg
        mock_pool.fetchrow.side_effect = asyncpg.PostgresError("boom")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_timeout_raises_adapter_timeout_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncio
        mock_pool.fetchrow.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"


class TestList:
    async def test_list_returns_rows_and_count(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        row_data = [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]
        conn = mock_pool.acquire.return_value
        conn.fetch.return_value = row_data
        conn.fetchval.return_value = 42

        entities, count = await repo.list(limit=10, offset=0)
        assert entities == row_data
        assert count == 42

    async def test_list_postgres_error_raises_adapter_query_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncpg
        conn = mock_pool.acquire.return_value
        conn.fetch.side_effect = asyncpg.PostgresError("oops")
        with pytest.raises(AdapterQueryError):
            await repo.list(limit=10, offset=0)

    async def test_list_timeout_raises_adapter_timeout_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncio
        conn = mock_pool.acquire.return_value
        conn.fetch.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.list(limit=10, offset=0)


class TestCreate:
    async def test_create_returns_stored_row(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        stored = {"id": "99", "name": "thing"}
        mock_pool.fetchrow.return_value = stored
        result = await repo.create({"name": "thing"})
        mock_pool.fetchrow.assert_called_once()
        assert result == stored

    async def test_create_postgres_error_raises_adapter_query_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncpg
        mock_pool.fetchrow.side_effect = asyncpg.UniqueViolationError("dup")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.create({"id": "1", "name": "x"})
        assert exc_info.value.operation == "create"

    async def test_create_timeout_raises_adapter_timeout_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncio
        mock_pool.fetchrow.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.create({"id": "1", "name": "x"})


class TestUpdate:
    async def test_update_found_returns_updated_row(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        updated = {"id": "1", "name": "updated"}
        mock_pool.fetchrow.return_value = updated
        result = await repo.update({"id": "1", "name": "updated"})
        assert result == updated

    async def test_update_not_found_returns_none(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.fetchrow.return_value = None
        result = await repo.update({"id": "999", "name": "ghost"})
        assert result is None

    async def test_update_postgres_error_raises_adapter_query_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncpg
        mock_pool.fetchrow.side_effect = asyncpg.PostgresError("fail")
        with pytest.raises(AdapterQueryError):
            await repo.update({"id": "1", "name": "x"})

    async def test_update_timeout_raises_adapter_timeout_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncio
        mock_pool.fetchrow.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.update({"id": "1", "name": "x"})


class TestDelete:
    async def test_delete_existing_returns_true(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.execute.return_value = "DELETE 1"
        result = await repo.delete("1")
        assert result is True

    async def test_delete_missing_returns_false(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.execute.return_value = "DELETE 0"
        result = await repo.delete("missing")
        assert result is False

    async def test_delete_postgres_error_raises_adapter_query_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncpg
        mock_pool.execute.side_effect = asyncpg.PostgresError("nope")
        with pytest.raises(AdapterQueryError):
            await repo.delete("1")

    async def test_delete_timeout_raises_adapter_timeout_error(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        import asyncio
        mock_pool.execute.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.delete("1")
