"""
tests/test_plugin.py — openframe-adapters-queue-nats
=======================================================
Tests for NatsPlugin: protocol conformance, lifecycle, health, and
domain-subclass injection (producer_class=/consumer_class=).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import nats.errors
import pytest

from openframe.adapters.queue.nats import (
    NatsConsumer,
    NatsPlugin,
    NatsProducer,
    NatsSettings,
)
from openframe.core.ports import BasePort, PluginContext, PluginStatus
from openframe.core.testing.contracts import PortContractTests


@pytest.fixture
def settings():
    return NatsSettings(nats_servers="nats://localhost:4222")


@pytest.fixture
def plugin(settings: NatsSettings) -> NatsPlugin:
    return NatsPlugin(settings)


@pytest.fixture
def plugin_context() -> PluginContext:
    return PluginContext(config={}, plugin_name="openframe-nats")


def _patch_connect(mock_nats_client: MagicMock):
    return patch(
        "openframe.adapters.queue.nats.producer.nats.connect",
        AsyncMock(return_value=mock_nats_client),
    )


# ── Protocol conformance ───────────────────────────────────────────────────

def test_nats_plugin_satisfies_base_port_protocol(plugin: NatsPlugin) -> None:
    assert isinstance(plugin, BasePort)


def test_nats_plugin_name(plugin: NatsPlugin) -> None:
    assert plugin.name == "openframe-nats"


def test_nats_plugin_version(plugin: NatsPlugin) -> None:
    assert plugin.version == "0.1.0"


def test_nats_plugin_capability(plugin: NatsPlugin) -> None:
    assert plugin.capability == "queue"


# ── Lifecycle ──────────────────────────────────────────────────────────────

async def test_initialize_succeeds_and_sets_ready(
    plugin: NatsPlugin,
    plugin_context: PluginContext,
    mock_nats_client: MagicMock,
) -> None:
    """Successful producer start → status READY."""
    with _patch_connect(mock_nats_client):
        await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.READY


async def test_initialize_fails_when_producer_start_raises(
    plugin: NatsPlugin,
    plugin_context: PluginContext,
) -> None:
    """Producer start raising → status FAILED, exception propagated."""
    with patch(
        "openframe.adapters.queue.nats.producer.nats.connect",
        AsyncMock(side_effect=nats.errors.NoServersError()),
    ):
        from openframe.core.exceptions import AdapterConnectionError
        with pytest.raises(AdapterConnectionError):
            await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.FAILED


async def test_shutdown_sets_status_stopped(
    plugin: NatsPlugin,
    plugin_context: PluginContext,
    mock_nats_client: MagicMock,
) -> None:
    with _patch_connect(mock_nats_client):
        await plugin.initialize(plugin_context)

    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED


async def test_shutdown_never_raises_when_producer_is_none(
    plugin: NatsPlugin,
) -> None:
    """shutdown() must not raise even if producer was never set."""
    plugin._producer = None
    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED


async def test_get_producer_raises_before_initialize(plugin: NatsPlugin) -> None:
    with pytest.raises(RuntimeError, match="not ready"):
        plugin.get_producer()


async def test_get_producer_returns_nats_producer_after_initialize(
    plugin: NatsPlugin,
    plugin_context: PluginContext,
    mock_nats_client: MagicMock,
) -> None:
    with _patch_connect(mock_nats_client):
        await plugin.initialize(plugin_context)

    p = plugin.get_producer()
    assert isinstance(p, NatsProducer)


def test_make_consumer_returns_nats_consumer(plugin: NatsPlugin) -> None:
    c = plugin.make_consumer()
    assert isinstance(c, NatsConsumer)


# ── Health ─────────────────────────────────────────────────────────────────

async def test_health_returns_failed_when_not_initialized(
    plugin: NatsPlugin,
) -> None:
    health = await plugin.health()
    assert health.status == PluginStatus.FAILED


async def test_health_never_raises(plugin: NatsPlugin) -> None:
    """health() must never raise under any circumstances."""
    plugin._producer = None
    result = await plugin.health()
    assert result is not None
    assert result.status == PluginStatus.FAILED


async def test_health_returns_ready_after_initialize(
    plugin: NatsPlugin,
    plugin_context: PluginContext,
    mock_nats_client: MagicMock,
) -> None:
    with _patch_connect(mock_nats_client):
        await plugin.initialize(plugin_context)

    health = await plugin.health()
    assert health.status == PluginStatus.READY


# ── producer_class parameter ───────────────────────────────────────────────

def test_plugin_defaults_to_base_producer_class(settings: NatsSettings) -> None:
    """Backwards compatibility — no producer_class passed."""
    plugin = NatsPlugin(settings)
    assert plugin._producer_class is NatsProducer


def test_plugin_accepts_custom_producer_class(settings: NatsSettings) -> None:
    class CustomProducer(NatsProducer):
        pass

    plugin = NatsPlugin(settings, producer_class=CustomProducer)
    assert plugin._producer_class is CustomProducer


def test_plugin_rejects_non_producer_class(settings: NatsSettings) -> None:
    """producer_class must be a subclass of NatsProducer — TypeError if not."""
    with pytest.raises(TypeError, match="subclass of NatsProducer"):
        NatsPlugin(settings, producer_class=object)  # type: ignore[arg-type]


async def test_initialize_constructs_custom_producer_class(
    settings: NatsSettings,
    plugin_context: PluginContext,
    mock_nats_client: MagicMock,
) -> None:
    """
    REGRESSION: plugin must construct the injected producer_class, not
    silently discard domain subclass overrides of _serialise().
    """
    class CustomProducer(NatsProducer):
        marker = True

    plugin = NatsPlugin(settings, producer_class=CustomProducer)
    with _patch_connect(mock_nats_client):
        await plugin.initialize(plugin_context)

    producer = plugin.get_producer()
    assert isinstance(producer, CustomProducer)
    assert hasattr(producer, "marker")


async def test_get_producer_returns_subclass_not_base_class(
    settings: NatsSettings,
    plugin_context: PluginContext,
    mock_nats_client: MagicMock,
) -> None:
    """type(producer) must be the subclass, not just isinstance-compatible."""
    class CustomProducer(NatsProducer):
        pass

    plugin = NatsPlugin(settings, producer_class=CustomProducer)
    with _patch_connect(mock_nats_client):
        await plugin.initialize(plugin_context)

    assert type(plugin.get_producer()) is CustomProducer


# ── Contract tests ─────────────────────────────────────────────────────────

class TestNatsPluginContracts(PortContractTests):
    """
    NatsPlugin passes the full openframe BasePort contract suite
    (identity + lifecycle: initialize -> health -> idempotent shutdown).
    """

    @pytest.fixture
    def port(self, settings: NatsSettings, mock_nats_client: MagicMock) -> NatsPlugin:
        with _patch_connect(mock_nats_client):
            plugin = NatsPlugin(settings)
            yield plugin


# ── consumer_class parameter ───────────────────────────────────────────────

def test_plugin_defaults_to_base_consumer_class(settings: NatsSettings) -> None:
    """No consumer_class passed → _consumer_class is NatsConsumer."""
    plugin = NatsPlugin(settings)
    assert plugin._consumer_class is NatsConsumer


def test_plugin_accepts_custom_consumer_class(settings: NatsSettings) -> None:
    """Custom consumer_class subclass is stored without error."""
    class _TestConsumer(NatsConsumer):
        pass

    plugin = NatsPlugin(settings, consumer_class=_TestConsumer)
    assert plugin._consumer_class is _TestConsumer


def test_plugin_rejects_non_consumer_class(settings: NatsSettings) -> None:
    """Non-subclass raises TypeError with a clear message."""
    with pytest.raises(TypeError, match="must be a subclass of NatsConsumer"):
        NatsPlugin(settings, consumer_class=object)  # type: ignore[arg-type]


def test_make_consumer_returns_subclass_not_base_class(settings: NatsSettings) -> None:
    """
    make_consumer() returns the configured subclass.

    Does NOT require initialize() — make_consumer() constructs on demand
    with no broker connection.
    """
    class _DomainConsumer(NatsConsumer):
        pass

    plugin = NatsPlugin(settings, consumer_class=_DomainConsumer)
    consumer = plugin.make_consumer()

    assert isinstance(consumer, _DomainConsumer)
    assert type(consumer) is _DomainConsumer
