"""
tests/test_connection.py
==========================
Unit tests for get_chromadb_client() and _client_cache.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from openframe.adapters.db.chromadb import ChromaDBSettings
from openframe.adapters.db.chromadb.connection import (
    _cache_key,
    _client_cache,
    get_chromadb_client,
)
from openframe.core.exceptions import AdapterConnectionError, AdapterTimeoutError


@pytest.fixture(autouse=True)
def clear_client_cache():
    """Ensure _client_cache is clean before and after every test."""
    _client_cache.clear()
    yield
    _client_cache.clear()


@pytest.fixture
def settings() -> ChromaDBSettings:
    return ChromaDBSettings(chroma_collection="items")


@pytest.fixture
def settings_alt() -> ChromaDBSettings:
    return ChromaDBSettings(chroma_collection="items", chroma_database="other_db")


class TestGetChromaDBClient:
    async def test_returns_cached_client_on_second_call(
        self, settings: ChromaDBSettings
    ) -> None:
        fake_client = MagicMock()
        with patch(
            "openframe.adapters.db.chromadb.connection.chromadb.AsyncHttpClient",
            new=AsyncMock(return_value=fake_client),
        ):
            client1 = await get_chromadb_client(settings)
            client2 = await get_chromadb_client(settings)

        assert client1 is client2
        assert client1 is fake_client

    async def test_different_databases_produce_different_clients(
        self, settings: ChromaDBSettings, settings_alt: ChromaDBSettings
    ) -> None:
        """
        Regression-shaped test: two Settings pointing at the same host:port
        but a different tenant/database must NOT share a client — the
        second caller must not silently inherit the first caller's
        tenant/database scope.
        """
        client_a = MagicMock(name="client_a")
        client_b = MagicMock(name="client_b")
        create_mock = AsyncMock(side_effect=[client_a, client_b])
        with patch(
            "openframe.adapters.db.chromadb.connection.chromadb.AsyncHttpClient",
            new=create_mock,
        ):
            c1 = await get_chromadb_client(settings)
            c2 = await get_chromadb_client(settings_alt)

        assert c1 is not c2
        assert c1 is client_a
        assert c2 is client_b
        # A third call with settings matching the second config reuses it.
        with patch(
            "openframe.adapters.db.chromadb.connection.chromadb.AsyncHttpClient",
            new=AsyncMock(side_effect=AssertionError("should not create a third client")),
        ):
            c3 = await get_chromadb_client(settings_alt)
        assert c3 is client_b

    async def test_connect_error_raises_adapter_connection_error(
        self, settings: ChromaDBSettings
    ) -> None:
        with patch(
            "openframe.adapters.db.chromadb.connection.chromadb.AsyncHttpClient",
            new=AsyncMock(side_effect=httpx.ConnectError("all connection attempts failed")),
        ):
            with pytest.raises(AdapterConnectionError) as exc_info:
                await get_chromadb_client(settings)

        assert exc_info.value.cause is not None
        assert exc_info.value.operation == "connect"

    async def test_httpx_timeout_raises_adapter_timeout_error(
        self, settings: ChromaDBSettings
    ) -> None:
        """
        httpx.TimeoutException must be classified as AdapterTimeoutError,
        not AdapterConnectionError — it is itself an httpx.TransportError
        subclass, so the check order in get_chromadb_client() matters.
        """
        with patch(
            "openframe.adapters.db.chromadb.connection.chromadb.AsyncHttpClient",
            new=AsyncMock(side_effect=httpx.ConnectTimeout("timed out")),
        ):
            with pytest.raises(AdapterTimeoutError) as exc_info:
                await get_chromadb_client(settings)

        assert exc_info.value.operation == "connect"

    async def test_asyncio_timeout_raises_adapter_timeout_error(
        self, settings: ChromaDBSettings
    ) -> None:
        async def _slow(*args, **kwargs):
            await asyncio.sleep(10)

        settings_fast = ChromaDBSettings(chroma_collection="items", connection_timeout=0.01)
        with patch(
            "openframe.adapters.db.chromadb.connection.chromadb.AsyncHttpClient",
            new=_slow,
        ):
            with pytest.raises(AdapterTimeoutError) as exc_info:
                await get_chromadb_client(settings_fast)

        assert exc_info.value.operation == "connect"

    async def test_client_stored_in_cache_after_creation(
        self, settings: ChromaDBSettings
    ) -> None:
        fake_client = MagicMock()
        with patch(
            "openframe.adapters.db.chromadb.connection.chromadb.AsyncHttpClient",
            new=AsyncMock(return_value=fake_client),
        ):
            await get_chromadb_client(settings)

        assert _client_cache[_cache_key(settings)] is fake_client

    def test_cache_key_covers_every_connection_identity_field(
        self, settings: ChromaDBSettings
    ) -> None:
        key = _cache_key(settings)
        assert key == (
            settings.chroma_host,
            settings.chroma_port,
            settings.chroma_ssl,
            settings.chroma_tenant,
            settings.chroma_database,
        )
