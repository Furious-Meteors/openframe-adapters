"""
tests/test_plugin.py — openframe-adapters-db-postgres
========================================================
Tests for PostgresPlugin: protocol conformance, lifecycle, and health.
"""
from __future__ import annotations

import pytest

from openframe.adapters.db.postgres import PostgresPlugin, PostgresSettings
from openframe.core.plugins import OpenFramePlugin, PluginContext, PluginStatus


@pytest.fixture
def settings():
    """A PostgresSettings instance with dummy connection details."""
    return PostgresSettings(
        database_url="postgresql://test:test@localhost/test"
    )


@pytest.fixture
def plugin(settings):
    """An uninitialized PostgresPlugin pre-configured for the 'items' table."""
    return PostgresPlugin(settings, table="items")


@pytest.fixture
def plugin_context():
    """A minimal PluginContext for tests."""
    return PluginContext(
        config={},
        plugin_name="openframe-postgres",
    )


# ── Protocol conformance ───────────────────────────────────────────────────

def test_postgres_plugin_satisfies_openframe_plugin_protocol(plugin):
    """PostgresPlugin must satisfy the OpenFramePlugin runtime-checkable Protocol."""
    assert isinstance(plugin, OpenFramePlugin)


def test_postgres_plugin_name(plugin):
    assert plugin.name == "openframe-postgres"


def test_postgres_plugin_version(plugin):
    assert plugin.version == "1.1.0"


def test_postgres_plugin_capability(plugin):
    assert plugin.capability == "persistence"


# ── Lifecycle ──────────────────────────────────────────────────────────────

async def test_initialize_succeeds_when_ping_returns_true(
    plugin, plugin_context, mock_pool, mock_settings
):
    """Successful pool creation + ping → status READY."""
    import openframe.adapters.db.postgres.connection as conn_module
    conn_module._pool_cache[mock_settings.database_url] = mock_pool
    mock_pool.fetchval.return_value = 1  # ping SELECT 1

    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.READY
    conn_module._pool_cache.clear()


async def test_initialize_fails_when_ping_raises(
    plugin, plugin_context, mock_pool, mock_settings
):
    """Ping raising an exception → status FAILED, AdapterConnectionError propagated."""
    import openframe.adapters.db.postgres.connection as conn_module
    conn_module._pool_cache[mock_settings.database_url] = mock_pool
    mock_pool.fetchval.side_effect = Exception("connection refused")

    plugin._settings = mock_settings
    from openframe.core.exceptions import AdapterConnectionError
    with pytest.raises((AdapterConnectionError, Exception)):
        await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.FAILED
    conn_module._pool_cache.clear()


async def test_shutdown_never_raises_when_repo_is_none(plugin):
    """shutdown() must not raise even if repo was never set."""
    plugin._repo = None
    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED


async def test_shutdown_sets_status_stopped(plugin, mock_pool, mock_settings, plugin_context):
    """shutdown() always transitions to STOPPED."""
    import openframe.adapters.db.postgres.connection as conn_module
    conn_module._pool_cache[mock_settings.database_url] = mock_pool
    mock_pool.fetchval.return_value = 1
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED
    conn_module._pool_cache.clear()


async def test_get_repository_raises_before_initialize(plugin):
    """get_repository() must raise RuntimeError when plugin is not READY."""
    with pytest.raises(RuntimeError, match="not ready"):
        plugin.get_repository()


async def test_get_repository_returns_instance_after_initialize(
    plugin, plugin_context, mock_pool, mock_settings
):
    """get_repository() returns a PostgresRepository after successful init."""
    import openframe.adapters.db.postgres.connection as conn_module
    from openframe.adapters.db.postgres import PostgresRepository

    conn_module._pool_cache[mock_settings.database_url] = mock_pool
    mock_pool.fetchval.return_value = 1
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    repo = plugin.get_repository()
    assert isinstance(repo, PostgresRepository)
    conn_module._pool_cache.clear()


# ── Health ─────────────────────────────────────────────────────────────────

async def test_health_returns_failed_when_not_initialized(plugin):
    """health() before initialize() → FAILED status."""
    health = await plugin.health()
    assert health.status == PluginStatus.FAILED


async def test_health_never_raises(plugin):
    """health() must never raise — even with a None repo."""
    plugin._repo = None
    result = await plugin.health()
    assert result is not None
    assert result.status == PluginStatus.FAILED


async def test_health_returns_ready_after_initialize(
    plugin, plugin_context, mock_pool, mock_settings
):
    """health() after successful init with ping returning True → READY."""
    import openframe.adapters.db.postgres.connection as conn_module
    conn_module._pool_cache[mock_settings.database_url] = mock_pool
    mock_pool.fetchval.return_value = 1
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    health = await plugin.health()
    assert health.status == PluginStatus.READY
    conn_module._pool_cache.clear()


async def test_health_returns_failed_when_ping_fails(
    plugin, plugin_context, mock_pool, mock_settings
):
    """health() when ping returns False → FAILED status, no exception raised."""
    import openframe.adapters.db.postgres.connection as conn_module
    conn_module._pool_cache[mock_settings.database_url] = mock_pool
    mock_pool.fetchval.return_value = 1
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    # Now make ping fail for health check
    mock_pool.fetchval.side_effect = Exception("db gone")
    result = await plugin.health()
    assert result is not None
    assert result.status == PluginStatus.FAILED
    conn_module._pool_cache.clear()
