"""
tests/test_plugin.py — openframe-adapters-db-chromadb
========================================================
Tests for ChromaDBPlugin: protocol conformance, lifecycle, and health.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from openframe.adapters.db.chromadb import ChromaDBPlugin, ChromaDBRepository, ChromaDBSettings
from openframe.core.ports import BasePort, PluginContext, PluginHealth, PluginStatus
from openframe.core.testing.contracts import PortContractTests


@pytest.fixture
def settings():
    """A ChromaDBSettings instance with dummy connection details."""
    return ChromaDBSettings(chroma_collection="items")


@pytest.fixture
def plugin(settings):
    """An uninitialized ChromaDBPlugin pre-configured for the 'items' collection."""
    return ChromaDBPlugin(settings, collection="items")


@pytest.fixture
def plugin_context():
    """A minimal PluginContext for tests."""
    return PluginContext(
        config={},
        plugin_name="openframe-chromadb",
    )


# ── Protocol conformance ───────────────────────────────────────────────────

def test_chromadb_plugin_satisfies_base_port_protocol(plugin):
    """ChromaDBPlugin must satisfy the BasePort runtime-checkable Protocol."""
    assert isinstance(plugin, BasePort)


def test_chromadb_plugin_name(plugin):
    assert plugin.name == "openframe-chromadb"


def test_chromadb_plugin_version(plugin):
    assert plugin.version == "0.1.0"


def test_chromadb_plugin_capability(plugin):
    assert plugin.capability == "search"


# ── Lifecycle ──────────────────────────────────────────────────────────────

async def test_initialize_succeeds_when_health_is_ready(
    plugin, plugin_context, mock_client, mock_settings, monkeypatch
):
    """Successful collection creation + repo.health() READY → plugin status READY."""
    import openframe.adapters.db.chromadb.connection as conn_module
    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client
    monkeypatch.setattr(
        ChromaDBRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.READY, message="")),
    )

    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.READY
    conn_module._client_cache.clear()


async def test_initialize_fails_when_health_is_not_ready(
    plugin, plugin_context, mock_client, mock_settings, monkeypatch
):
    """repo.health() reports non-READY → status FAILED, AdapterConnectionError raised."""
    import openframe.adapters.db.chromadb.connection as conn_module
    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client
    monkeypatch.setattr(
        ChromaDBRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.FAILED, message="connection refused")),
    )

    plugin._settings = mock_settings
    from openframe.core.exceptions import AdapterConnectionError
    with pytest.raises(AdapterConnectionError):
        await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.FAILED
    conn_module._client_cache.clear()


async def test_shutdown_never_raises_when_repo_is_none(plugin):
    """shutdown() must not raise even if repo was never set."""
    plugin._repo = None
    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED


async def test_shutdown_sets_status_stopped(plugin, mock_client, mock_settings, plugin_context):
    """shutdown() always transitions to STOPPED."""
    import openframe.adapters.db.chromadb.connection as conn_module
    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED
    conn_module._client_cache.clear()


async def test_get_repository_raises_before_initialize(plugin):
    """get_repository() must raise RuntimeError when plugin is not READY."""
    with pytest.raises(RuntimeError, match="not ready"):
        plugin.get_repository()


async def test_get_repository_returns_instance_after_initialize(
    plugin, plugin_context, mock_client, mock_settings
):
    """get_repository() returns a ChromaDBRepository after successful init."""
    import openframe.adapters.db.chromadb.connection as conn_module

    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    repo = plugin.get_repository()
    assert isinstance(repo, ChromaDBRepository)
    conn_module._client_cache.clear()


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
    plugin, plugin_context, mock_client, mock_settings, monkeypatch
):
    """health() after successful init delegates to repo.health() → READY."""
    import openframe.adapters.db.chromadb.connection as conn_module
    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client
    monkeypatch.setattr(
        ChromaDBRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.READY, message="")),
    )
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    health = await plugin.health()
    assert health.status == PluginStatus.READY
    conn_module._client_cache.clear()


async def test_health_returns_failed_when_repo_health_fails(
    plugin, plugin_context, mock_client, mock_settings, monkeypatch
):
    """health() delegates to repo.health() → FAILED status surfaces, no exception raised."""
    import openframe.adapters.db.chromadb.connection as conn_module
    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client
    monkeypatch.setattr(
        ChromaDBRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.READY, message="")),
    )
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    monkeypatch.setattr(
        ChromaDBRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.FAILED, message="chroma gone")),
    )
    result = await plugin.health()
    assert result is not None
    assert result.status == PluginStatus.FAILED
    conn_module._client_cache.clear()


# ── repository_class parameter ─────────────────────────────────────────────

def test_plugin_defaults_to_base_repository_class(settings):
    """Backwards compatibility — no repository_class passed."""
    plugin = ChromaDBPlugin(settings, collection="items")
    assert plugin._repository_class is ChromaDBRepository


def test_plugin_accepts_custom_repository_class(settings):
    class CustomRepo(ChromaDBRepository):
        pass

    plugin = ChromaDBPlugin(settings, collection="items", repository_class=CustomRepo)
    assert plugin._repository_class is CustomRepo


def test_plugin_rejects_non_repository_class(settings):
    """repository_class must be a subclass of ChromaDBRepository — TypeError if not."""
    with pytest.raises(TypeError, match="subclass of ChromaDBRepository"):
        ChromaDBPlugin(settings, collection="items", repository_class=object)  # type: ignore[arg-type]


async def test_initialize_constructs_custom_repository_class(
    settings, plugin_context, mock_client, mock_settings
):
    """
    REGRESSION-shaped test: plugin must construct the injected subclass,
    not silently discard domain subclass overrides of entity mapping
    methods (see postgres/kafka's [CHANGELOG] historical bug this pattern
    guards against).
    """
    import openframe.adapters.db.chromadb.connection as conn_module

    class CustomRepo(ChromaDBRepository):
        marker = True

    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client

    plugin = ChromaDBPlugin(mock_settings, collection="items", repository_class=CustomRepo)
    await plugin.initialize(plugin_context)

    repo = plugin.get_repository()
    assert isinstance(repo, CustomRepo)
    assert hasattr(repo, "marker")
    conn_module._client_cache.clear()


async def test_get_repository_returns_subclass_not_base_class(
    settings, plugin_context, mock_client, mock_settings
):
    """type(repo) must be the subclass, not just isinstance-compatible."""
    import openframe.adapters.db.chromadb.connection as conn_module

    class CustomRepo(ChromaDBRepository):
        pass

    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client

    plugin = ChromaDBPlugin(mock_settings, collection="items", repository_class=CustomRepo)
    await plugin.initialize(plugin_context)

    assert type(plugin.get_repository()) is CustomRepo
    conn_module._client_cache.clear()


# ── Contract tests ─────────────────────────────────────────────────────────

class TestChromaDBPluginContracts(PortContractTests):
    """
    ChromaDBPlugin passes the full openframe BasePort contract suite
    (identity + lifecycle: initialize -> health -> idempotent shutdown).
    """

    @pytest.fixture
    def port(self, mock_settings, mock_client) -> ChromaDBPlugin:
        import openframe.adapters.db.chromadb.connection as conn_module

        conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_client

        plugin = ChromaDBPlugin(mock_settings, collection="items")
        yield plugin
        conn_module._client_cache.clear()
