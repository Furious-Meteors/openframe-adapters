"""
tests/conftest.py — openframe-adapters-db-influxdb
=====================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.

All tests run with zero network calls. ``InfluxDBClientAsync`` is mocked at
the ``openframe.adapters.db.influxdb.connection`` import level so no real
InfluxDB server is needed.
"""
from __future__ import annotations

import re
from typing import Any

# Canonical OTel reset fixtures from openframe-core v3.0.
# Provides (autouse): reset_telemetry_state
# Provides (on-demand): span_exporter, metric_reader
from openframe.core.testing.fixtures import *  # noqa: F401, F403

import pytest
from unittest.mock import AsyncMock, MagicMock

from influxdb_client.client.flux_table import FluxRecord, FluxTable, TableList


@pytest.fixture
def mock_client() -> MagicMock:
    """A fully mocked InfluxDBClientAsync."""
    client = MagicMock()
    client.ping = AsyncMock(return_value=True)
    client.close = AsyncMock()

    write_api = MagicMock()
    write_api.write = AsyncMock(return_value=True)
    client.write_api = MagicMock(return_value=write_api)

    query_api = MagicMock()
    query_api.query = AsyncMock(return_value=TableList())
    client.query_api = MagicMock(return_value=query_api)

    delete_api = MagicMock()
    delete_api.delete = AsyncMock(return_value=True)
    client.delete_api = MagicMock(return_value=delete_api)

    return client


@pytest.fixture
def mock_settings() -> object:
    """An InfluxDBSettings instance with dummy connection details."""
    from openframe.adapters.db.influxdb import InfluxDBSettings

    return InfluxDBSettings(
        influxdb_url="http://localhost:8086",
        influxdb_token="test-token",
        influxdb_org="test-org",
        influxdb_bucket="test-bucket",
    )


@pytest.fixture
def repo(mock_settings: object, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch):
    """
    An InfluxDBRepository wired to a mocked client.

    The mock client is injected directly into ``_client_cache`` so
    ``get_influxdb_client()`` never attempts a real connection.
    """
    from openframe.adapters.db.influxdb import InfluxDBRepository
    import openframe.adapters.db.influxdb.connection as conn_module

    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client  # type: ignore[attr-defined]
    r = InfluxDBRepository(mock_settings, measurement="readings", id_tag="id")  # type: ignore[arg-type]
    yield r
    conn_module._client_cache.clear()


def _record_from_store(row: dict[str, Any]) -> FluxRecord:
    values = dict(row)
    values["_time"] = row["time"]
    values.pop("time", None)
    return FluxRecord(table=0, values=values)


@pytest.fixture
def stateful_mock_client(mock_client: MagicMock):
    """
    A mocked InfluxDBClientAsync backed by an in-memory point store.

    Simulates InfluxDB write/query/delete behaviour closely enough for the
    RepositoryContractTests behavioural assertions (create -> get, list
    pagination, overwrite-on-update, delete) to pass without a real
    InfluxDB server. Mirrors the pattern Postgres's stateful mock pool
    fixture uses, adapted to InfluxDB's point/Flux model instead of SQL.

    Write side: inspects the written ``Point``'s private ``_tags``/
    ``_fields``/``_time`` attributes (the only way to recover what was
    written — the driver has no public point-introspection API) and
    stores one dict per id-tag value, keyed by id.

    Query side: parses the Flux query text this package's own
    ``repository.py`` generates (not arbitrary Flux) to distinguish a
    single-id lookup (``_flux_get_latest``, recognisable by the
    ``r["id"] == "..."`` filter clause) from a list-all query
    (``_flux_list_all``, no id filter).
    """
    store: dict[str, dict[str, Any]] = {}
    _clock = {"n": 0}

    def _write(bucket: str, org: str, record, **kwargs) -> bool:
        tags = dict(record._tags)
        fields = dict(record._fields)
        row_id = tags.get("id")
        time_value = record._time
        if time_value is None:
            _clock["n"] += 1
            time_value = _clock["n"]
        row = {**tags, **fields, "time": time_value}
        store[row_id] = row
        return True

    id_filter_re = re.compile(r'r\["id"\] == "([^"]*)"')

    def _query(query: str, org: str = None, **kwargs) -> TableList:
        table = FluxTable()
        m = id_filter_re.search(query)
        if m:
            row_id = m.group(1)
            row = store.get(row_id)
            if row is not None:
                table.records.append(_record_from_store(row))
        else:
            for row in sorted(store.values(), key=lambda r: r["time"]):
                table.records.append(_record_from_store(row))
        tables = TableList()
        tables.append(table)
        return tables

    def _delete(start, stop, predicate: str, bucket: str, org: str = None) -> bool:
        m = re.search(r'id="([^"]*)"', predicate)
        if m:
            store.pop(m.group(1), None)
        return True

    mock_client.write_api.return_value.write = AsyncMock(side_effect=_write)
    mock_client.query_api.return_value.query = AsyncMock(side_effect=_query)
    mock_client.delete_api.return_value.delete = AsyncMock(side_effect=_delete)
    return mock_client
