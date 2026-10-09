"""
tests/test_connection.py
==========================
Unit tests for get_qdrant_client() and _client_cache.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from openframe.adapters.db.qdrant import QdrantSettings
from openframe.adapters.db.qdrant.connection import _cache_key, _client_cache, get_qdrant_client
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
def settings() -> QdrantSettings:
    return QdrantSettings(qdrant_url="http://localhost:6333")


@pytest.fixture
def settings_alt() -> QdrantSettings:
    return QdrantSettings(qdrant_url="http://localhost:6333", qdrant_api_key="alt-key")


def _patched_client(get_collections_result=None, get_collections_side_effect=None):
    fake_client = MagicMock()
    fake_client.get_collections = AsyncMock(
        return_value=get_collections_result, side_effect=get_collections_side_effect
    )
    return fake_client


class TestGetQdrantClient:
    async def test_returns_cached_client_on_second_call(
        self, settings: QdrantSettings
    ) -> None:
        fake_client = _patched_client()
        with patch(
            "openframe.adapters.db.qdrant.connection.AsyncQdrantClient",
            return_value=fake_client,
        ):
            client1 = await get_qdrant_client(settings)
            client2 = await get_qdrant_client(settings)

        assert client1 is client2
        assert client1 is fake_client
        fake_client.get_collections.assert_awaited_once()

    async def test_different_api_keys_produce_different_clients(
        self, settings: QdrantSettings, settings_alt: QdrantSettings
    ) -> None:
        """
        Regression guard: two Settings for the SAME qdrant_url but
        different qdrant_api_key must NOT share a client — the second
        caller must not silently inherit the first caller's credentials.
        """
        client_a = _patched_client()
        client_b = _patched_client()
        with patch(
            "openframe.adapters.db.qdrant.connection.AsyncQdrantClient",
            side_effect=[client_a, client_b],
        ):
            c1 = await get_qdrant_client(settings)
            c2 = await get_qdrant_client(settings_alt)

        assert c1 is not c2
        assert c1 is client_a
        assert c2 is client_b

        # A third call matching the second config reuses it, no new client.
        with patch(
            "openframe.adapters.db.qdrant.connection.AsyncQdrantClient",
            side_effect=AssertionError("should not create a third client"),
        ):
            c3 = await get_qdrant_client(settings_alt)
        assert c3 is client_b

    async def test_connectivity_probe_failure_raises_adapter_connection_error(
        self, settings: QdrantSettings
    ) -> None:
        from qdrant_client.http.exceptions import ResponseHandlingException

        fake_client = _patched_client(
            get_collections_side_effect=ResponseHandlingException(
                ConnectionError("boom")
            )
        )
        with patch(
            "openframe.adapters.db.qdrant.connection.AsyncQdrantClient",
            return_value=fake_client,
        ):
            with pytest.raises(AdapterConnectionError) as exc_info:
                await get_qdrant_client(settings)

        assert exc_info.value.cause is not None

    async def test_unexpected_response_5xx_raises_adapter_connection_error(
        self, settings: QdrantSettings
    ) -> None:
        from qdrant_client.http.exceptions import UnexpectedResponse

        exc = UnexpectedResponse(
            status_code=500, reason_phrase="err", content=b"", headers={}
        )
        fake_client = _patched_client(get_collections_side_effect=exc)
        with patch(
            "openframe.adapters.db.qdrant.connection.AsyncQdrantClient",
            return_value=fake_client,
        ):
            with pytest.raises(AdapterConnectionError):
                await get_qdrant_client(settings)

    async def test_unexpected_response_4xx_raises_adapter_configuration_error(
        self, settings: QdrantSettings
    ) -> None:
        from qdrant_client.http.exceptions import UnexpectedResponse

        exc = UnexpectedResponse(
            status_code=403, reason_phrase="forbidden", content=b"", headers={}
        )
        fake_client = _patched_client(get_collections_side_effect=exc)
        with patch(
            "openframe.adapters.db.qdrant.connection.AsyncQdrantClient",
            return_value=fake_client,
        ):
            with pytest.raises(AdapterConfigurationError):
                await get_qdrant_client(settings)

    async def test_timeout_raises_adapter_timeout_error(
        self, settings: QdrantSettings
    ) -> None:
        fake_client = _patched_client(get_collections_side_effect=asyncio.TimeoutError())
        with patch(
            "openframe.adapters.db.qdrant.connection.AsyncQdrantClient",
            return_value=fake_client,
        ):
            with pytest.raises(AdapterTimeoutError) as exc_info:
                await get_qdrant_client(settings)

        assert exc_info.value.operation == "connect"

    async def test_invalid_url_raises_adapter_configuration_error(
        self, settings: QdrantSettings
    ) -> None:
        with patch(
            "openframe.adapters.db.qdrant.connection.AsyncQdrantClient",
            side_effect=ValueError("bad url"),
        ):
            with pytest.raises(AdapterConfigurationError) as exc_info:
                await get_qdrant_client(settings)

        assert exc_info.value.operation == "init"

    async def test_client_stored_in_cache_after_creation(
        self, settings: QdrantSettings
    ) -> None:
        fake_client = _patched_client()
        with patch(
            "openframe.adapters.db.qdrant.connection.AsyncQdrantClient",
            return_value=fake_client,
        ):
            await get_qdrant_client(settings)

        assert _client_cache[_cache_key(settings)] is fake_client
