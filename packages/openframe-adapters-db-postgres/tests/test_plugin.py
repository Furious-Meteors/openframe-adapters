"""
tests/test_plugin.py — openframe-adapters-db-postgres
========================================================
Tests for PostgresPlugin: protocol conformance, lifecycle, and health.
"""
from __future__ import annotations

import pytest

from openframe.adapters.db.postgres import PostgresPlugin, PostgresRepository, PostgresSettings
from openframe.core.contracts import BasePort, PluginContext, PluginStatus
from openframe.core.testing.contracts import PortContractTests


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

def test_postgres_plugin_satisfies_base_port_protocol(plugin):
    """PostgresPlugin must satisfy the BasePort runtime-checkable Protocol."""
    assert isinstance(plugin, BasePort)


def test_postgres_plugin_name(plugin):
    assert plugin.name == "openframe-postgres"


def test_postgres_plugin_version(plugin):
    assert plugin.version == "1.3.0"


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


# ── repository_class parameter ─────────────────────────────────────────────

def test_plugin_defaults_to_base_repository_class(settings):
    """Backwards compatibility — no repository_class passed."""
    plugin = PostgresPlugin(settings, table="items")
    assert plugin._repository_class is PostgresRepository


def test_plugin_accepts_custom_repository_class(settings):
    class CustomRepo(PostgresRepository):
        pass

    plugin = PostgresPlugin(settings, table="items", repository_class=CustomRepo)
    assert plugin._repository_class is CustomRepo


def test_plugin_rejects_non_repository_class(settings):
    """repository_class must be a subclass of PostgresRepository — TypeError if not."""
    with pytest.raises(TypeError, match="subclass of PostgresRepository"):
        PostgresPlugin(settings, table="items", repository_class=object)  # type: ignore[arg-type]


async def test_initialize_constructs_custom_repository_class(
    settings, plugin_context, mock_pool, mock_settings
):
    """
    REGRESSION: plugin always constructed the base class, silently discarding
    domain subclass overrides of entity mapping methods.
    """
    import openframe.adapters.db.postgres.connection as conn_module

    class CustomRepo(PostgresRepository):
        marker = True

    conn_module._pool_cache[mock_settings.database_url] = mock_pool
    mock_pool.fetchval.return_value = 1

    plugin = PostgresPlugin(mock_settings, table="items", repository_class=CustomRepo)
    await plugin.initialize(plugin_context)

    repo = plugin.get_repository()
    assert isinstance(repo, CustomRepo)
    assert hasattr(repo, "marker")
    conn_module._pool_cache.clear()


async def test_get_repository_returns_subclass_not_base_class(
    settings, plugin_context, mock_pool, mock_settings
):
    """type(repo) must be the subclass, not just isinstance-compatible."""
    import openframe.adapters.db.postgres.connection as conn_module

    class CustomRepo(PostgresRepository):
        pass

    conn_module._pool_cache[mock_settings.database_url] = mock_pool
    mock_pool.fetchval.return_value = 1

    plugin = PostgresPlugin(mock_settings, table="items", repository_class=CustomRepo)
    await plugin.initialize(plugin_context)

    assert type(plugin.get_repository()) is CustomRepo
    conn_module._pool_cache.clear()


# ── Contract tests ─────────────────────────────────────────────────────────

class TestPostgresPluginContracts(PortContractTests):
    """
    PostgresPlugin passes the full openframe BasePort contract suite
    (identity + lifecycle: initialize -> health -> idempotent shutdown).
    """

    @pytest.fixture
    def port(self, mock_settings, mock_pool) -> PostgresPlugin:
        import openframe.adapters.db.postgres.connection as conn_module

        conn_module._pool_cache[mock_settings.database_url] = mock_pool
        mock_pool.fetchval.return_value = 1  # ping / SELECT 1

        plugin = PostgresPlugin(mock_settings, table="items")
        yield plugin
        conn_module._pool_cache.clear()
