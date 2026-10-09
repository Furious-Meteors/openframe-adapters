"""
tests/test_connection.py
==========================
Unit tests for get_milvus_client() and _client_cache.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pymilvus.exceptions import MilvusException

from openframe.adapters.db.milvus import MilvusSettings
from openframe.adapters.db.milvus.connection import (
    _cache_key,
    _client_cache,
    _looks_like_connection_failure,
    get_milvus_client,
)
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
def settings() -> MilvusSettings:
    return MilvusSettings(milvus_uri="http://localhost:19530")


@pytest.fixture
def settings_alt() -> MilvusSettings:
    return MilvusSettings(milvus_uri="http://localhost:19531")


class TestGetMilvusClient:
    async def test_returns_cached_client_on_second_call(
        self, settings: MilvusSettings
    ) -> None:
        fake_client = MagicMock()
        fake_client.list_collections = AsyncMock(return_value=[])
        with patch(
            "openframe.adapters.db.milvus.connection.AsyncMilvusClient",
            return_value=fake_client,
        ):
            client1 = await get_milvus_client(settings)
            client2 = await get_milvus_client(settings)

        assert client1 is client2
        assert client1 is fake_client

    async def test_different_uris_produce_different_clients(
        self, settings: MilvusSettings, settings_alt: MilvusSettings
    ) -> None:
        client_a = MagicMock(name="client_a")
        client_a.list_collections = AsyncMock(return_value=[])
        client_b = MagicMock(name="client_b")
        client_b.list_collections = AsyncMock(return_value=[])
        with patch(
            "openframe.adapters.db.milvus.connection.AsyncMilvusClient",
            side_effect=[client_a, client_b],
        ):
            c1 = await get_milvus_client(settings)
            c2 = await get_milvus_client(settings_alt)

        assert c1 is not c2
        assert c1 is client_a
        assert c2 is client_b

    async def test_same_uri_different_token_produces_different_clients(
        self, settings: MilvusSettings
    ) -> None:
        """
        Regression test: two Settings for the SAME milvus_uri but a
        different token must NOT share a client — the second caller must
        not silently inherit the first caller's credentials.
        """
        settings_other_token = MilvusSettings(
            milvus_uri=settings.milvus_uri, milvus_token="other-token"
        )
        client_a = MagicMock(name="client_a")
        client_a.list_collections = AsyncMock(return_value=[])
        client_b = MagicMock(name="client_b")
        client_b.list_collections = AsyncMock(return_value=[])
        with patch(
            "openframe.adapters.db.milvus.connection.AsyncMilvusClient",
            side_effect=[client_a, client_b],
        ):
            c1 = await get_milvus_client(settings)
            c2 = await get_milvus_client(settings_other_token)

        assert c1 is not c2
        assert c1 is client_a
        assert c2 is client_b
        # A third call matching the second config reuses it.
        with patch(
            "openframe.adapters.db.milvus.connection.AsyncMilvusClient",
            side_effect=AssertionError("should not create a third client"),
        ):
            c3 = await get_milvus_client(settings_other_token)
        assert c3 is client_b

    async def test_construction_error_raises_adapter_configuration_error(
        self, settings: MilvusSettings
    ) -> None:
        with patch(
            "openframe.adapters.db.milvus.connection.AsyncMilvusClient",
            side_effect=MilvusException(message="bad uri"),
        ):
            with pytest.raises(AdapterConfigurationError) as exc_info:
                await get_milvus_client(settings)
        assert exc_info.value.operation == "init"

    async def test_connectivity_probe_failure_raises_adapter_connection_error(
        self, settings: MilvusSettings
    ) -> None:
        fake_client = MagicMock()
        fake_client.list_collections = AsyncMock(
            side_effect=MilvusException(message="Fail connecting to server")
        )
        with patch(
            "openframe.adapters.db.milvus.connection.AsyncMilvusClient",
            return_value=fake_client,
        ):
            with pytest.raises(AdapterConnectionError) as exc_info:
                await get_milvus_client(settings)
        assert exc_info.value.operation == "connect"

    async def test_connectivity_probe_timeout_raises_adapter_timeout_error(
        self, settings: MilvusSettings
    ) -> None:
        async def _hang(*args, **kwargs):
            raise asyncio.TimeoutError()

        fake_client = MagicMock()
        fake_client.list_collections = AsyncMock(side_effect=asyncio.TimeoutError())
        with patch(
            "openframe.adapters.db.milvus.connection.AsyncMilvusClient",
            return_value=fake_client,
        ):
            with pytest.raises(AdapterTimeoutError) as exc_info:
                await get_milvus_client(settings)
        assert exc_info.value.operation == "connect"

    async def test_client_stored_in_cache_after_creation(
        self, settings: MilvusSettings
    ) -> None:
        fake_client = MagicMock()
        fake_client.list_collections = AsyncMock(return_value=[])
        with patch(
            "openframe.adapters.db.milvus.connection.AsyncMilvusClient",
            return_value=fake_client,
        ):
            await get_milvus_client(settings)

        assert _client_cache[_cache_key(settings)] is fake_client


class TestLooksLikeConnectionFailure:
    @pytest.mark.parametrize(
        "message",
        [
            "Fail connecting to server on 127.0.0.1:19",
            "failed to connect to all addresses",
            "server unavailable",
            "deadline exceeded",
            "connection reset by peer",
            "connection refused",
        ],
    )
    def test_detects_connection_phrases(self, message: str) -> None:
        exc = MilvusException(message=message)
        assert _looks_like_connection_failure(exc) is True

    def test_does_not_flag_unrelated_message(self) -> None:
        exc = MilvusException(message="field 'vector' dimension mismatch")
        assert _looks_like_connection_failure(exc) is False
