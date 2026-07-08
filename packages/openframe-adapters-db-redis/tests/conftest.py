"""
tests/conftest.py — openframe-adapters-db-redis
==================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.

All tests run with zero network calls. The redis.asyncio.Redis client is
mocked at the ``openframe.adapters.db.redis.connection`` import level so
no real Redis server is needed.
"""
from __future__ import annotations

# Canonical OTel reset fixtures from openframe-core v3.0.
# Provides (autouse): reset_telemetry_state
# Provides (on-demand): span_exporter, metric_reader
from openframe.core.testing.fixtures import *  # noqa: F401, F403

import pytest
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def mock_redis():
    """A fully mocked redis.asyncio.Redis client. No network calls."""
    client = MagicMock()
    client.ping   = AsyncMock(return_value=True)
    client.get    = AsyncMock(return_value=None)
    client.set    = AsyncMock(return_value=True)
    client.delete = AsyncMock(return_value=1)
    client.mget   = AsyncMock(return_value=[])
    client.aclose = AsyncMock()

    # scan_iter must be an async generator — yields nothing by default.
    async def _scan_iter(match=None, count=None):
        return
        yield  # pragma: no cover — makes this an async generator

    client.scan_iter = _scan_iter
    return client


@pytest.fixture
def mock_settings():
    """A RedisSettings instance with dummy connection details."""
    from openframe.adapters.db.redis import RedisSettings
    return RedisSettings(redis_url="redis://localhost:6379/0")


@pytest.fixture
def repo(mock_settings, mock_redis):
    """A RedisRepository wired to a mocked Redis client."""
    from openframe.adapters.db.redis import RedisRepository
    import openframe.adapters.db.redis.connection as conn_module

    conn_module._client_cache[mock_settings.redis_url] = mock_redis
    r = RedisRepository(mock_settings)
    yield r
    conn_module._client_cache.clear()
