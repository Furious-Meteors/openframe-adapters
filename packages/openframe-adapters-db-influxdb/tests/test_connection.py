"""
tests/test_connection.py
==========================
Unit tests for get_influxdb_client() and _client_cache.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from influxdb_client.rest import ApiException

from openframe.adapters.db.influxdb import InfluxDBSettings
from openframe.adapters.db.influxdb.connection import _cache_key, _client_cache, get_influxdb_client
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)


@pytest.fixture(autouse=True)
def clear_client_cache():
    """Ensure _client_cache is clean before and after every test."""
    _client_cache.clear()
    yield
    _client_cache.clear()


@pytest.fixture
def settings() -> InfluxDBSettings:
    return InfluxDBSettings(
        influxdb_url="http://localhost:8086",
        influxdb_token="tok",
        influxdb_org="org",
        influxdb_bucket="bucket",
    )


@pytest.fixture
def settings_alt() -> InfluxDBSettings:
    return InfluxDBSettings(
        influxdb_url="http://otherhost:8086",
        influxdb_token="tok",
        influxdb_org="org",
        influxdb_bucket="bucket",
    )


def _make_fake_client(ping_return=True) -> MagicMock:
    client = MagicMock()
    client.ping = AsyncMock(return_value=ping_return)
    client.close = AsyncMock()
    return client


class TestGetInfluxDBClient:
    async def test_returns_cached_client_on_second_call(self, settings: InfluxDBSettings) -> None:
        fake_client = _make_fake_client()
        with patch(
            "openframe.adapters.db.influxdb.connection.InfluxDBClientAsync",
            return_value=fake_client,
        ):
            client1 = await get_influxdb_client(settings)
            client2 = await get_influxdb_client(settings)

        assert client1 is client2
        assert client1 is fake_client
        fake_client.ping.assert_called_once()

    async def test_different_urls_produce_different_clients(
        self, settings: InfluxDBSettings, settings_alt: InfluxDBSettings
    ) -> None:
        client_a = _make_fake_client()
        client_b = _make_fake_client()
        with patch(
            "openframe.adapters.db.influxdb.connection.InfluxDBClientAsync",
            side_effect=[client_a, client_b],
        ):
            c1 = await get_influxdb_client(settings)
            c2 = await get_influxdb_client(settings_alt)

        assert c1 is not c2
        assert c1 is client_a
        assert c2 is client_b

    async def test_same_url_different_token_produces_different_clients(
        self, settings: InfluxDBSettings
    ) -> None:
        """
        Regression test: two Settings for the SAME influxdb_url but a
        DIFFERENT token must NOT share a client — the second caller must
        not silently inherit the first caller's credentials.
        """
        settings_other_token = InfluxDBSettings(
            influxdb_url=settings.influxdb_url,
            influxdb_token="different-token",
            influxdb_org=settings.influxdb_org,
            influxdb_bucket=settings.influxdb_bucket,
        )
        client_a = _make_fake_client()
        client_b = _make_fake_client()
        with patch(
            "openframe.adapters.db.influxdb.connection.InfluxDBClientAsync",
            side_effect=[client_a, client_b],
        ):
            c1 = await get_influxdb_client(settings)
            c2 = await get_influxdb_client(settings_other_token)

        assert c1 is not c2
        assert c1 is client_a
        assert c2 is client_b
        # A third call matching the second config reuses it.
        with patch(
            "openframe.adapters.db.influxdb.connection.InfluxDBClientAsync",
            side_effect=AssertionError("should not create a third client"),
        ):
            c3 = await get_influxdb_client(settings_other_token)
        assert c3 is client_b

    async def test_client_stored_in_cache_after_creation(self, settings: InfluxDBSettings) -> None:
        fake_client = _make_fake_client()
        with patch(
            "openframe.adapters.db.influxdb.connection.InfluxDBClientAsync",
            return_value=fake_client,
        ):
            await get_influxdb_client(settings)

        assert _client_cache[_cache_key(settings)] is fake_client

    async def test_timeout_raises_adapter_timeout_error(self, settings: InfluxDBSettings) -> None:
        fake_client = MagicMock()
        fake_client.ping = AsyncMock(side_effect=asyncio.TimeoutError())
        fake_client.close = AsyncMock()
        with patch(
            "openframe.adapters.db.influxdb.connection.InfluxDBClientAsync",
            return_value=fake_client,
        ):
            with pytest.raises(AdapterTimeoutError) as exc_info:
                await get_influxdb_client(settings)

        assert exc_info.value.operation == "connect"
        fake_client.close.assert_awaited_once()

    @pytest.mark.parametrize("status", [401, 403])
    async def test_auth_rejection_raises_adapter_configuration_error(
        self, settings: InfluxDBSettings, status: int
    ) -> None:
        fake_client = MagicMock()
        fake_client.ping = AsyncMock(side_effect=ApiException(status=status, reason="unauthorized"))
        fake_client.close = AsyncMock()
        with patch(
            "openframe.adapters.db.influxdb.connection.InfluxDBClientAsync",
            return_value=fake_client,
        ):
            with pytest.raises(AdapterConfigurationError) as exc_info:
                await get_influxdb_client(settings)

        assert exc_info.value.operation == "init"
        fake_client.close.assert_awaited_once()

    async def test_api_exception_non_auth_raises_adapter_connection_error(
        self, settings: InfluxDBSettings
    ) -> None:
        fake_client = MagicMock()
        fake_client.ping = AsyncMock(side_effect=ApiException(status=500, reason="boom"))
        fake_client.close = AsyncMock()
        with patch(
            "openframe.adapters.db.influxdb.connection.InfluxDBClientAsync",
            return_value=fake_client,
        ):
            with pytest.raises(AdapterConnectionError):
                await get_influxdb_client(settings)

    async def test_aiohttp_client_error_raises_adapter_connection_error(
        self, settings: InfluxDBSettings
    ) -> None:
        """
        aiohttp connection-level failures (DNS, refused) never become
        ApiException — see connection.py's module docstring. They must be
        caught explicitly or they'd escape as a raw driver-adjacent
        exception past the adapter boundary.
        """
        fake_client = MagicMock()
        fake_client.ping = AsyncMock(
            side_effect=aiohttp.ClientConnectorError(
                connection_key=MagicMock(), os_error=OSError("DNS failure")
            )
        )
        fake_client.close = AsyncMock()
        with patch(
            "openframe.adapters.db.influxdb.connection.InfluxDBClientAsync",
            return_value=fake_client,
        ):
            with pytest.raises(AdapterConnectionError) as exc_info:
                await get_influxdb_client(settings)

        assert exc_info.value.cause is not None
