"""
tests/test_plugin.py — openframe-adapters-db-falkordb
==========================================================
Tests for FalkorDBPlugin: protocol conformance, lifecycle, and health.
"""
from __future__ import annotations

import pytest

from unittest.mock import AsyncMock

from openframe.adapters.db.falkordb import FalkorDBPlugin, FalkorDBRepository, FalkorDBSettings
from openframe.core.ports import BasePort, PluginContext, PluginHealth, PluginStatus
from openframe.core.testing.contracts import PortContractTests


@pytest.fixture
def settings():
    """A FalkorDBSettings instance with default connection details."""
    return FalkorDBSettings()


@pytest.fixture
def plugin(settings: FalkorDBSettings) -> FalkorDBPlugin:
    """An uninitialized FalkorDBPlugin."""
    return FalkorDBPlugin(settings)


@pytest.fixture
def plugin_context() -> PluginContext:
    """A minimal PluginContext for tests."""
    return PluginContext(
        config={},
        plugin_name="openframe-falkordb",
    )


# ── Protocol conformance ───────────────────────────────────────────────────

def test_falkordb_plugin_satisfies_base_port_protocol(plugin: FalkorDBPlugin) -> None:
    """FalkorDBPlugin must satisfy the BasePort runtime-checkable Protocol."""
    assert isinstance(plugin, BasePort)


def test_falkordb_plugin_name(plugin: FalkorDBPlugin) -> None:
    assert plugin.name == "openframe-falkordb"


def test_falkordb_plugin_version(plugin: FalkorDBPlugin) -> None:
    assert plugin.version == "0.1.0"


def test_falkordb_plugin_capability(plugin: FalkorDBPlugin) -> None:
    assert plugin.capability == "search"


# ── Lifecycle ──────────────────────────────────────────────────────────────

async def test_initialize_succeeds_when_health_is_ready(
    plugin: FalkorDBPlugin,
    plugin_context: PluginContext,
    mock_falkordb_client: object,
    mock_settings: FalkorDBSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Successful client creation + repo.health() READY → plugin status READY."""
    import openframe.adapters.db.falkordb.connection as conn_module
    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client  # type: ignore[arg-type]
    monkeypatch.setattr(
        FalkorDBRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.READY, message="")),
    )

    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.READY
    conn_module._client_cache.clear()


async def test_initialize_fails_when_health_is_not_ready(
    plugin: FalkorDBPlugin,
    plugin_context: PluginContext,
    mock_falkordb_client: object,
    mock_settings: FalkorDBSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """repo.health() reports non-READY → status FAILED, AdapterConnectionError raised."""
    import openframe.adapters.db.falkordb.connection as conn_module
    from openframe.core.exceptions import AdapterConnectionError

    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client  # type: ignore[arg-type]
    monkeypatch.setattr(
        FalkorDBRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.FAILED, message="refused")),
    )

    plugin._settings = mock_settings
    with pytest.raises(AdapterConnectionError):
        await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.FAILED
    conn_module._client_cache.clear()


async def test_shutdown_never_raises_when_repo_is_none(plugin: FalkorDBPlugin) -> None:
    """shutdown() must not raise even if repo was never set."""
    plugin._repo = None
    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED


async def test_shutdown_sets_status_stopped(
    plugin: FalkorDBPlugin,
    plugin_context: PluginContext,
    mock_falkordb_client: object,
    mock_settings: FalkorDBSettings,
) -> None:
    """shutdown() always transitions to STOPPED."""
    import openframe.adapters.db.falkordb.connection as conn_module
    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client  # type: ignore[arg-type]
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED
    conn_module._client_cache.clear()


async def test_get_repository_raises_before_initialize(plugin: FalkorDBPlugin) -> None:
    """get_repository() must raise RuntimeError when plugin is not READY."""
    with pytest.raises(RuntimeError, match="not ready"):
        plugin.get_repository()


async def test_get_repository_returns_falkordb_repository_after_initialize(
    plugin: FalkorDBPlugin,
    plugin_context: PluginContext,
    mock_falkordb_client: object,
    mock_settings: FalkorDBSettings,
) -> None:
    """get_repository() returns a FalkorDBRepository after successful init."""
    import openframe.adapters.db.falkordb.connection as conn_module
    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client  # type: ignore[arg-type]
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    repo = plugin.get_repository()
    assert isinstance(repo, FalkorDBRepository)
    conn_module._client_cache.clear()


# ── Health ─────────────────────────────────────────────────────────────────

async def test_health_returns_failed_when_not_initialized(plugin: FalkorDBPlugin) -> None:
    """health() before initialize() → FAILED status."""
    health = await plugin.health()
    assert health.status == PluginStatus.FAILED


async def test_health_never_raises(plugin: FalkorDBPlugin) -> None:
    """health() must never raise — even with a None repo."""
    plugin._repo = None
    result = await plugin.health()
    assert result is not None
    assert result.status == PluginStatus.FAILED


async def test_health_returns_ready_after_initialize(
    plugin: FalkorDBPlugin,
    plugin_context: PluginContext,
    mock_falkordb_client: object,
    mock_settings: FalkorDBSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """health() after successful init delegates to repo.health() → READY."""
    import openframe.adapters.db.falkordb.connection as conn_module
    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client  # type: ignore[arg-type]
    monkeypatch.setattr(
        FalkorDBRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.READY, message="")),
    )
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    health = await plugin.health()
    assert health.status == PluginStatus.READY
    conn_module._client_cache.clear()


async def test_health_returns_failed_when_repo_health_fails(
    plugin: FalkorDBPlugin,
    plugin_context: PluginContext,
    mock_falkordb_client: object,
    mock_settings: FalkorDBSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """health() delegates to repo.health() → FAILED status surfaces, no exception raised."""
    import openframe.adapters.db.falkordb.connection as conn_module

    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client  # type: ignore[arg-type]
    monkeypatch.setattr(
        FalkorDBRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.READY, message="")),
    )
    plugin._settings = mock_settings
    await plugin.initialize(plugin_context)

    # Now make repo.health() report a failure
    monkeypatch.setattr(
        FalkorDBRepository,
        "health",
        AsyncMock(return_value=PluginHealth(status=PluginStatus.FAILED, message="db gone")),
    )
    result = await plugin.health()
    assert result is not None
    assert result.status == PluginStatus.FAILED
    conn_module._client_cache.clear()


# ── Contract tests ─────────────────────────────────────────────────────────

class TestFalkorDBPluginContracts(PortContractTests):
    """
    FalkorDBPlugin passes the full openframe BasePort contract suite
    (identity + lifecycle: initialize -> health -> idempotent shutdown).
    """

    @pytest.fixture
    def port(self, mock_settings: FalkorDBSettings, mock_falkordb_client: object) -> FalkorDBPlugin:
        import openframe.adapters.db.falkordb.connection as conn_module

        conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client  # type: ignore[arg-type]

        plugin = FalkorDBPlugin(mock_settings)
        yield plugin
        conn_module._client_cache.clear()


# ── repository_class parameter ─────────────────────────────────────────────

def test_plugin_defaults_to_base_repository_class(settings: FalkorDBSettings) -> None:
    """No repository_class passed → _repository_class is FalkorDBRepository."""
    plugin = FalkorDBPlugin(settings)
    assert plugin._repository_class is FalkorDBRepository


def test_plugin_accepts_custom_repository_class(settings: FalkorDBSettings) -> None:
    """Custom repository_class subclass is stored without error."""
    class _TestRepo(FalkorDBRepository):
        pass

    plugin = FalkorDBPlugin(settings, repository_class=_TestRepo)
    assert plugin._repository_class is _TestRepo


def test_plugin_rejects_non_repository_class(settings: FalkorDBSettings) -> None:
    """Non-subclass raises TypeError with a clear message."""
    with pytest.raises(TypeError, match="must be a subclass of FalkorDBRepository"):
        FalkorDBPlugin(settings, repository_class=object)  # type: ignore[arg-type]


async def test_get_repository_returns_subclass_not_base_class(
    settings: FalkorDBSettings,
    plugin_context: PluginContext,
    mock_falkordb_client: object,
    mock_settings: FalkorDBSettings,
) -> None:
    """get_repository() returns the domain subclass, not base."""
    class _DomainRepo(FalkorDBRepository):
        pass

    import openframe.adapters.db.falkordb.connection as conn_module
    conn_module._client_cache[conn_module._cache_key(mock_settings)] = mock_falkordb_client  # type: ignore[arg-type]

    plugin = FalkorDBPlugin(mock_settings, repository_class=_DomainRepo)
    await plugin.initialize(plugin_context)

    repo = plugin.get_repository()
    assert isinstance(repo, _DomainRepo)
    assert type(repo) is _DomainRepo
    conn_module._client_cache.clear()
