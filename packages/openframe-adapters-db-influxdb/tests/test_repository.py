"""
tests/test_repository.py — openframe-adapters-db-influxdb
=============================================================
Contract tests (RepositoryContractTests) run first, then adapter-specific
unit tests covering InfluxDB error mapping and driver behaviour.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
from influxdb_client.rest import ApiException

from openframe.adapters.db.influxdb import InfluxDBRepository
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository
from openframe.core.testing import RepositoryContractTests


# ── Contract tests — must pass for every BaseRepository implementation ─────

class TestInfluxDBRepositoryContracts(RepositoryContractTests):
    """
    InfluxDBRepository passes the full openframe contract suite against a
    stateful mocked InfluxDBClientAsync. No real InfluxDB server required.
    """

    @pytest.fixture
    def repository(self, mock_settings, stateful_mock_client):
        import openframe.adapters.db.influxdb.connection as conn_module

        conn_module._client_cache[conn_module._cache_key(mock_settings)] = stateful_mock_client
        r = InfluxDBRepository(mock_settings, measurement="readings", id_tag="id")
        yield r
        conn_module._client_cache.clear()

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
    def test_isinstance_base_repository(self, repo: InfluxDBRepository) -> None:
        assert isinstance(repo, BaseRepository)


class TestInit:
    def test_missing_measurement_raises_configuration_error(self, mock_settings) -> None:
        with pytest.raises(AdapterConfigurationError):
            InfluxDBRepository(mock_settings)

    def test_measurement_from_init_arg(self, mock_settings) -> None:
        repo = InfluxDBRepository(mock_settings, measurement="orders")
        assert repo._measurement == "orders"

    def test_measurement_from_class_attribute(self, mock_settings) -> None:
        class OrderRepo(InfluxDBRepository):
            _measurement = "orders"

        repo = OrderRepo(mock_settings)
        assert repo._measurement == "orders"

    def test_bucket_defaults_to_settings_bucket(self, mock_settings) -> None:
        repo = InfluxDBRepository(mock_settings, measurement="orders")
        assert repo._bucket == mock_settings.influxdb_bucket

    def test_bucket_override(self, mock_settings) -> None:
        repo = InfluxDBRepository(mock_settings, measurement="orders", bucket="other-bucket")
        assert repo._bucket == "other-bucket"


class TestGet:
    async def test_get_found_returns_dict(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        from influxdb_client.client.flux_table import FluxRecord, FluxTable, TableList

        table = FluxTable()
        table.records.append(FluxRecord(table=0, values={"id": "1", "name": "widget", "_time": 1}))
        tables = TableList()
        tables.append(table)
        mock_client.query_api.return_value.query.return_value = tables

        result = await repo.get("1")
        assert result == {"id": "1", "name": "widget"}

    async def test_get_not_found_returns_none(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        from influxdb_client.client.flux_table import TableList

        mock_client.query_api.return_value.query.return_value = TableList()
        result = await repo.get("missing")
        assert result is None

    async def test_get_api_exception_raises_adapter_query_error(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        mock_client.query_api.return_value.query.side_effect = ApiException(status=400, reason="bad query")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_timeout_raises_adapter_timeout_error(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        mock_client.query_api.return_value.query.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_aiohttp_client_error_raises_adapter_connection_error(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        """
        A connection dropped mid-query must surface as AdapterConnectionError
        (retryable), not AdapterQueryError, matching the connection-vs-query
        distinction this ecosystem's other adapters make for their own
        drivers' connection-class exceptions.
        """
        mock_client.query_api.return_value.query.side_effect = aiohttp.ClientConnectorError(
            connection_key=MagicMock(), os_error=OSError("connection lost")
        )
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"
        assert exc_info.value.retryable is True

    async def test_get_api_exception_401_raises_adapter_configuration_error(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        mock_client.query_api.return_value.query.side_effect = ApiException(status=401, reason="unauthorized")
        with pytest.raises(AdapterConfigurationError):
            await repo.get("1")

    async def test_get_api_exception_5xx_raises_adapter_connection_error(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        """
        This adapter's documented decision: a 5xx from InfluxDB's HTTP
        layer is treated as transient/retryable connection-class, not a
        permanent query failure.
        """
        mock_client.query_api.return_value.query.side_effect = ApiException(status=503, reason="unavailable")
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get("1")
        assert exc_info.value.retryable is True


class TestList:
    async def test_list_returns_rows_and_count(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        from influxdb_client.client.flux_table import FluxRecord, FluxTable, TableList

        table = FluxTable()
        table.records.append(FluxRecord(table=0, values={"id": "1", "name": "a", "_time": 1}))
        table.records.append(FluxRecord(table=0, values={"id": "2", "name": "b", "_time": 2}))
        tables = TableList()
        tables.append(table)
        mock_client.query_api.return_value.query.return_value = tables

        entities, count = await repo.list(limit=10, offset=0)
        assert entities == [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]
        assert count == 2

    async def test_list_respects_limit_and_offset(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        from influxdb_client.client.flux_table import FluxRecord, FluxTable, TableList

        table = FluxTable()
        for i in range(5):
            table.records.append(FluxRecord(table=0, values={"id": str(i), "_time": i}))
        tables = TableList()
        tables.append(table)
        mock_client.query_api.return_value.query.return_value = tables

        entities, total = await repo.list(limit=2, offset=1)
        assert total == 5
        assert entities == [{"id": "1"}, {"id": "2"}]

    async def test_list_api_exception_raises_adapter_query_error(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        mock_client.query_api.return_value.query.side_effect = ApiException(status=400, reason="bad")
        with pytest.raises(AdapterQueryError):
            await repo.list(limit=10, offset=0)

    async def test_list_timeout_raises_adapter_timeout_error(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        mock_client.query_api.return_value.query.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.list(limit=10, offset=0)


class TestCreate:
    async def test_create_returns_entity_unchanged(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        entity = {"id": "99", "name": "thing"}
        result = await repo.create(entity)
        mock_client.write_api.return_value.write.assert_called_once()
        assert result == entity

    async def test_create_writes_point_with_correct_measurement_and_tag(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        await repo.create({"id": "1", "name": "x"})
        _, kwargs = mock_client.write_api.return_value.write.call_args
        point = kwargs["record"]
        assert point._name == "readings"
        assert point._tags == {"id": "1"}
        assert point._fields == {"name": "x"}

    async def test_create_missing_id_tag_raises_adapter_query_error(
        self, repo: InfluxDBRepository
    ) -> None:
        with pytest.raises(AdapterQueryError):
            await repo.create({"name": "no-id"})

    async def test_create_api_exception_raises_adapter_query_error(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        mock_client.write_api.return_value.write.side_effect = ApiException(status=400, reason="bad write")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.create({"id": "1", "name": "x"})
        assert exc_info.value.operation == "create"

    async def test_create_timeout_raises_adapter_timeout_error(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        mock_client.write_api.return_value.write.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.create({"id": "1", "name": "x"})


class TestUpdate:
    async def test_update_missing_entity_returns_none(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        from influxdb_client.client.flux_table import TableList

        mock_client.query_api.return_value.query.return_value = TableList()
        result = await repo.update({"id": "999", "name": "ghost"})
        assert result is None
        mock_client.write_api.return_value.write.assert_not_called()

    async def test_update_existing_overwrites_with_same_timestamp(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        from influxdb_client.client.flux_table import FluxRecord, FluxTable, TableList

        table = FluxTable()
        table.records.append(FluxRecord(table=0, values={"id": "1", "name": "original", "_time": 777}))
        tables = TableList()
        tables.append(table)
        mock_client.query_api.return_value.query.return_value = tables

        result = await repo.update({"id": "1", "name": "modified"})
        assert result == {"id": "1", "name": "modified"}

        _, kwargs = mock_client.write_api.return_value.write.call_args
        point = kwargs["record"]
        assert point._time == 777
        assert point._fields == {"name": "modified"}

    async def test_update_requires_id_tag(self, repo: InfluxDBRepository) -> None:
        with pytest.raises(AdapterQueryError):
            await repo.update({"name": "no-id"})

    async def test_update_timeout_raises_adapter_timeout_error(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        from influxdb_client.client.flux_table import FluxRecord, FluxTable, TableList

        table = FluxTable()
        table.records.append(FluxRecord(table=0, values={"id": "1", "_time": 1}))
        tables = TableList()
        tables.append(table)
        mock_client.query_api.return_value.query.return_value = tables
        mock_client.write_api.return_value.write.side_effect = asyncio.TimeoutError()

        with pytest.raises(AdapterTimeoutError):
            await repo.update({"id": "1", "name": "x"})


class TestDelete:
    async def test_delete_existing_returns_true(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        from influxdb_client.client.flux_table import FluxRecord, FluxTable, TableList

        table = FluxTable()
        table.records.append(FluxRecord(table=0, values={"id": "1", "_time": 1}))
        tables = TableList()
        tables.append(table)
        mock_client.query_api.return_value.query.return_value = tables

        result = await repo.delete("1")
        assert result is True
        mock_client.delete_api.return_value.delete.assert_called_once()

    async def test_delete_missing_returns_false(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        from influxdb_client.client.flux_table import TableList

        mock_client.query_api.return_value.query.return_value = TableList()
        result = await repo.delete("missing")
        assert result is False
        mock_client.delete_api.return_value.delete.assert_not_called()

    async def test_delete_api_exception_raises_adapter_query_error(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        from influxdb_client.client.flux_table import FluxRecord, FluxTable, TableList

        table = FluxTable()
        table.records.append(FluxRecord(table=0, values={"id": "1", "_time": 1}))
        tables = TableList()
        tables.append(table)
        mock_client.query_api.return_value.query.return_value = tables
        mock_client.delete_api.return_value.delete.side_effect = ApiException(status=400, reason="bad delete")

        with pytest.raises(AdapterQueryError):
            await repo.delete("1")

    async def test_delete_timeout_raises_adapter_timeout_error(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        from influxdb_client.client.flux_table import FluxRecord, FluxTable, TableList

        table = FluxTable()
        table.records.append(FluxRecord(table=0, values={"id": "1", "_time": 1}))
        tables = TableList()
        tables.append(table)
        mock_client.query_api.return_value.query.return_value = tables
        mock_client.delete_api.return_value.delete.side_effect = asyncio.TimeoutError()

        with pytest.raises(AdapterTimeoutError):
            await repo.delete("1")


class TestHealthAndLifecycle:
    async def test_health_returns_ready(self, repo: InfluxDBRepository, mock_client: MagicMock) -> None:
        health = await repo.health()
        assert health.status.name == "READY"

    async def test_health_returns_failed_on_ping_false(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        mock_client.ping = AsyncMock(return_value=False)
        health = await repo.health()
        assert health.status.name == "FAILED"

    async def test_health_never_raises(self, repo: InfluxDBRepository, mock_client: MagicMock) -> None:
        mock_client.ping = AsyncMock(side_effect=RuntimeError("boom"))
        health = await repo.health()
        assert health.status.name == "FAILED"

    async def test_close_removes_client_from_cache(
        self, repo: InfluxDBRepository, mock_client: MagicMock, mock_settings
    ) -> None:
        import openframe.adapters.db.influxdb.connection as conn_module

        await repo.close()
        assert conn_module._cache_key(mock_settings) not in conn_module._client_cache
        mock_client.close.assert_awaited_once()


class TestRawDriverAccess:
    async def test_client_returns_cached_client(
        self, repo: InfluxDBRepository, mock_client: MagicMock
    ) -> None:
        client = await repo.client()
        assert client is mock_client
