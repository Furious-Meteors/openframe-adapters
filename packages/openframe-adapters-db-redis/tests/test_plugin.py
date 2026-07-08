"""
tests/test_plugin.py — openframe-adapters-db-redis
=====================================================
Tests for RedisPlugin: protocol conformance, lifecycle, and health.
"""
from __future__ import annotations

import pytest

from unittest.mock import AsyncMock

from openframe.adapters.db.redis import RedisPlugin, RedisRepository, RedisSettings
from openframe.core.contracts import BasePort, PluginContext, PluginHealth, PluginStatus
from openframe.core.testing.contracts import PortContractTests


@pytest.fixture
def settings():
    """A RedisSettings instance with dummy connection details."""
    return RedisSettings(redis_url="redis://localhost:6379/0")


@pytest.fixture
def plugin(settings: RedisSettings) -> RedisPlugin:
    """An uninitialized RedisPlugin."""
    return RedisPlugin(settings)


@pytest.fixture
def plugin_context() -> PluginContext:
    """A minimal PluginContext for tests."""
    return PluginContext(
        config={},
        plugin_name="openframe-redis",
    )


# ── Protocol conformance ───────────────────────────────────────────────────

def test_redis_plugin_satisfies_base_port_protocol(plugin: RedisPlugin) -> None:
    """RedisPlugin must satisfy the BasePort runtime-checkable Protocol."""
    assert isinstance(plugin, BasePort)


def test_redis_plugin_name(plugin: RedisPlugin) -> None:
    assert plugin.name == "openframe-redis"


def test_redis_plugin_version(plugin: RedisPlugin) -> None:
    assert plugin.version == "2.0.0"


def test_redis_plugin_capability(plugin: RedisPlugin) -> None:
    assert plugin.capability == "cache"


# ── Lifecycle ──────────────────────────────────────────────────────────────

async def test_initialize_succeeds_when_health_is_ready(
    plugin: RedisPlugin,
    plugin_context: PluginContext,
    mock_redis: object,
    mock_settings: RedisSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Successful client creation + repo.health() READY → plugin status READY."""
    import openframe.adapters.db.redis.connection as conn_module
    conn_module._client_cache[mock_settings.redis_url] = mock_redis  # type: ignore[arg-type]
    monkeypatch.setattr(
        RedisRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.READY, message="")),
    )

    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.READY
    conn_module._client_cache.clear()


async def test_initialize_fails_when_health_is_not_ready(
    plugin: RedisPlugin,
    plugin_context: PluginContext,
    mock_redis: object,
    mock_settings: RedisSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """repo.health() reports non-READY → status FAILED, AdapterConnectionError raised."""
    import openframe.adapters.db.redis.connection as conn_module
    from openframe.core.exceptions import AdapterConnectionError

    conn_module._client_cache[mock_settings.redis_url] = mock_redis  # type: ignore[arg-type]
    monkeypatch.setattr(
        RedisRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.FAILED, message="refused")),
    )

    plugin._settings = mock_settings
    with pytest.raises(AdapterConnectionError):
        await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.FAILED
    conn_module._client_cache.clear()


async def test_shutdown_never_raises_when_repo_is_none(plugin: RedisPlugin) -> None:
    """shutdown() must not raise even if repo was never set."""
    plugin._repo = None
    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED


async def test_shutdown_sets_status_stopped(
    plugin: RedisPlugin,
    plugin_context: PluginContext,
    mock_redis: object,
    mock_settings: RedisSettings,
) -> None:
    """shutdown() always transitions to STOPPED."""
    import openframe.adapters.db.redis.connection as conn_module
    conn_module._client_cache[mock_settings.redis_url] = mock_redis  # type: ignore[arg-type]
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED
    conn_module._client_cache.clear()


async def test_get_repository_raises_before_initialize(plugin: RedisPlugin) -> None:
    """get_repository() must raise RuntimeError when plugin is not READY."""
    with pytest.raises(RuntimeError, match="not ready"):
        plugin.get_repository()


async def test_get_repository_returns_redis_repository_after_initialize(
    plugin: RedisPlugin,
    plugin_context: PluginContext,
    mock_redis: object,
    mock_settings: RedisSettings,
) -> None:
    """get_repository() returns a RedisRepository after successful init."""
    import openframe.adapters.db.redis.connection as conn_module
    conn_module._client_cache[mock_settings.redis_url] = mock_redis  # type: ignore[arg-type]
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    repo = plugin.get_repository()
    assert isinstance(repo, RedisRepository)
    conn_module._client_cache.clear()


# ── Health ─────────────────────────────────────────────────────────────────

async def test_health_returns_failed_when_not_initialized(plugin: RedisPlugin) -> None:
    """health() before initialize() → FAILED status."""
    health = await plugin.health()
    assert health.status == PluginStatus.FAILED


async def test_health_never_raises(plugin: RedisPlugin) -> None:
    """health() must never raise — even with a None repo."""
    plugin._repo = None
    result = await plugin.health()
    assert result is not None
    assert result.status == PluginStatus.FAILED


async def test_health_returns_ready_after_initialize(
    plugin: RedisPlugin,
    plugin_context: PluginContext,
    mock_redis: object,
    mock_settings: RedisSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """health() after successful init delegates to repo.health() → READY."""
    import openframe.adapters.db.redis.connection as conn_module
    conn_module._client_cache[mock_settings.redis_url] = mock_redis  # type: ignore[arg-type]
    monkeypatch.setattr(
        RedisRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.READY, message="")),
    )
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    health = await plugin.health()
    assert health.status == PluginStatus.READY
    conn_module._client_cache.clear()


async def test_health_returns_failed_when_repo_health_fails(
    plugin: RedisPlugin,
    plugin_context: PluginContext,
    mock_redis: object,
    mock_settings: RedisSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """health() delegates to repo.health() → FAILED status surfaces, no exception raised."""
    import openframe.adapters.db.redis.connection as conn_module

    conn_module._client_cache[mock_settings.redis_url] = mock_redis  # type: ignore[arg-type]
    monkeypatch.setattr(
        RedisRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.READY, message="")),
    )
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    # Now make repo.health() report a failure
    monkeypatch.setattr(
        RedisRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.FAILED, message="db gone")),
    )
    result = await plugin.health()
    assert result is not None
    assert result.status == PluginStatus.FAILED
    conn_module._client_cache.clear()


# ── Contract tests ─────────────────────────────────────────────────────────

class TestRedisPluginContracts(PortContractTests):
    """
    RedisPlugin passes the full openframe BasePort contract suite
    (identity + lifecycle: initialize -> health -> idempotent shutdown).
    """

    @pytest.fixture
    def port(self, mock_settings: RedisSettings, mock_redis: object) -> RedisPlugin:
        import openframe.adapters.db.redis.connection as conn_module

        conn_module._client_cache[mock_settings.redis_url] = mock_redis  # type: ignore[arg-type]

        plugin = RedisPlugin(mock_settings)
        yield plugin
        conn_module._client_cache.clear()
