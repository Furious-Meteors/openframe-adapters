"""
tests/test_connection.py — openframe-adapters-db-falkordb
==============================================================
Unit tests for get_falkordb_client() and the client cache.
"""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest
import redis.exceptions

from openframe.adapters.db.falkordb import FalkorDBSettings
from openframe.adapters.db.falkordb.connection import (
    _cache_key,
    _client_cache,
    get_falkordb_client,
)
from openframe.core.exceptions import AdapterConnectionError, AdapterTimeoutError


class TestGetFalkorDBClient:
    async def test_returns_cached_client_on_second_call(
        self, mock_settings: FalkorDBSettings, mock_falkordb_client
    ) -> None:
        """Second call with same connection shape returns the exact same client object."""
        import openframe.adapters.db.falkordb.connection as conn_module
        conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client

        client1 = await get_falkordb_client(mock_settings)
        client2 = await get_falkordb_client(mock_settings)
        assert client1 is client2
        conn_module._client_cache.clear()

    async def test_different_hosts_produce_different_clients(self) -> None:
        """Each unique (host, port, ...) tuple gets its own independent client."""
        import openframe.adapters.db.falkordb.connection as conn_module

        settings_a = FalkorDBSettings(falkordb_host="host-a")
        settings_b = FalkorDBSettings(falkordb_host="host-b")

        client_a = MagicMock()
        client_b = MagicMock()
        conn_module._client_cache[conn_module._cache_key(settings_a)] = client_a
        conn_module._client_cache[conn_module._cache_key(settings_b)] = client_b

        result_a = await get_falkordb_client(settings_a)
        result_b = await get_falkordb_client(settings_b)
        assert result_a is not result_b
        conn_module._client_cache.clear()

    async def test_same_host_different_socket_timeout_produces_different_clients(self) -> None:
        """
        Regression test: two Settings for the SAME host/port but different
        falkordb_socket_timeout must NOT share a client — the second
        caller must not silently inherit the first caller's socket
        configuration.
        """
        import openframe.adapters.db.falkordb.connection as conn_module

        settings_short = FalkorDBSettings(falkordb_host="host", falkordb_socket_timeout=1.0)
        settings_long = FalkorDBSettings(falkordb_host="host", falkordb_socket_timeout=30.0)

        client_short = MagicMock(name="client_short")
        client_long = MagicMock(name="client_long")
        conn_module._client_cache[conn_module._cache_key(settings_short)] = client_short
        conn_module._client_cache[conn_module._cache_key(settings_long)] = client_long

        result_short = await get_falkordb_client(settings_short)
        result_long = await get_falkordb_client(settings_long)
        assert result_short is not result_long
        assert result_short is client_short
        assert result_long is client_long
        conn_module._client_cache.clear()

    async def test_client_stored_in_cache_keyed_by_connection_shape(
        self, mock_settings: FalkorDBSettings, mock_falkordb_client
    ) -> None:
        """After get_falkordb_client(), the cache contains the client at the cache key."""
        import openframe.adapters.db.falkordb.connection as conn_module
        conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client

        await get_falkordb_client(mock_settings)
        assert conn_module._cache_key(mock_settings) in conn_module._client_cache
        conn_module._client_cache.clear()

    async def test_connection_error_raises_adapter_connection_error(
        self, mock_settings: FalkorDBSettings
    ) -> None:
        """redis.exceptions.ConnectionError → AdapterConnectionError."""
        import openframe.adapters.db.falkordb.connection as conn_module
        conn_module._client_cache.clear()

        with patch(
            "openframe.adapters.db.falkordb.connection.FalkorDB",
            side_effect=redis.exceptions.ConnectionError("refused"),
        ):
            with pytest.raises(AdapterConnectionError):
                await get_falkordb_client(mock_settings)

    async def test_authentication_error_raises_adapter_connection_error(
        self, mock_settings: FalkorDBSettings
    ) -> None:
        """redis.exceptions.AuthenticationError → AdapterConnectionError."""
        import openframe.adapters.db.falkordb.connection as conn_module
        conn_module._client_cache.clear()

        with patch(
            "openframe.adapters.db.falkordb.connection.FalkorDB",
            side_effect=redis.exceptions.AuthenticationError("bad password"),
        ):
            with pytest.raises(AdapterConnectionError):
                await get_falkordb_client(mock_settings)

    async def test_timeout_raises_adapter_timeout_error(
        self, mock_settings: FalkorDBSettings
    ) -> None:
        """asyncio.TimeoutError → AdapterTimeoutError."""
        import openframe.adapters.db.falkordb.connection as conn_module
        conn_module._client_cache.clear()

        with patch(
            "openframe.adapters.db.falkordb.connection.FalkorDB",
            side_effect=asyncio.TimeoutError(),
        ):
            with pytest.raises(AdapterTimeoutError):
                await get_falkordb_client(mock_settings)

    async def test_ping_called_on_client_connection(
        self, mock_settings: FalkorDBSettings
    ) -> None:
        """A successful connect calls .connection.ping() to verify connectivity."""
        import openframe.adapters.db.falkordb.connection as conn_module
        conn_module._client_cache.clear()

        fake_client = MagicMock()
        fake_client.connection.ping = MagicMock(
            side_effect=lambda: asyncio.sleep(0, result=True)
        )

        with patch(
            "openframe.adapters.db.falkordb.connection.FalkorDB",
            return_value=fake_client,
        ):
            await get_falkordb_client(mock_settings)
        fake_client.connection.ping.assert_called_once()
        conn_module._client_cache.clear()
