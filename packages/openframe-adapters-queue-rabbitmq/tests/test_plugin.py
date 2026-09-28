"""
tests/test_plugin.py — openframe-adapters-queue-rabbitmq
=============================================================
Tests for RabbitmqPlugin: protocol conformance, lifecycle, health, and
producer_class/consumer_class injection.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import aio_pika.exceptions
import pytest

from openframe.adapters.queue.rabbitmq import (
    RabbitmqConsumer,
    RabbitmqPlugin,
    RabbitmqProducer,
    RabbitmqSettings,
)
from openframe.core.exceptions import AdapterConnectionError
from openframe.core.ports import BasePort, PluginContext, PluginStatus
from openframe.core.testing.contracts import PortContractTests


@pytest.fixture
def settings():
    return RabbitmqSettings(rabbitmq_url="amqp://guest:guest@localhost:5672/")


@pytest.fixture
def plugin(settings: RabbitmqSettings) -> RabbitmqPlugin:
    return RabbitmqPlugin(settings)


@pytest.fixture
def plugin_context() -> PluginContext:
    return PluginContext(config={}, plugin_name="openframe-rabbitmq")


def _patch_connect(mock_connection):
    return patch(
        "openframe.adapters.queue.rabbitmq.producer.aio_pika.connect_robust",
        AsyncMock(return_value=mock_connection),
    )


# ── Protocol conformance ───────────────────────────────────────────────────

def test_rabbitmq_plugin_satisfies_base_port_protocol(plugin: RabbitmqPlugin) -> None:
    assert isinstance(plugin, BasePort)


def test_rabbitmq_plugin_name(plugin: RabbitmqPlugin) -> None:
    assert plugin.name == "openframe-rabbitmq"


def test_rabbitmq_plugin_version(plugin: RabbitmqPlugin) -> None:
    assert plugin.version == "0.1.0"


def test_rabbitmq_plugin_capability(plugin: RabbitmqPlugin) -> None:
    assert plugin.capability == "queue"


def test_rabbitmq_plugin_version_matches_pyproject() -> None:
    """version class attribute must match this package's own pyproject.toml version."""
    import tomllib
    from pathlib import Path

    pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text())
    assert RabbitmqPlugin.version == data["project"]["version"]


# ── Lifecycle ──────────────────────────────────────────────────────────────

async def test_initialize_succeeds_and_sets_ready(
    plugin: RabbitmqPlugin,
    plugin_context: PluginContext,
    mock_connection: MagicMock,
) -> None:
    """Successful producer start → status READY."""
    with _patch_connect(mock_connection):
        await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.READY


async def test_initialize_fails_when_producer_start_raises(
    plugin: RabbitmqPlugin,
    plugin_context: PluginContext,
) -> None:
    """Producer start raising → status FAILED, exception propagated."""
    with patch(
        "openframe.adapters.queue.rabbitmq.producer.aio_pika.connect_robust",
        AsyncMock(side_effect=aio_pika.exceptions.AMQPConnectionError("unreachable")),
    ):
        with pytest.raises(AdapterConnectionError):
            await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.FAILED


async def test_shutdown_sets_status_stopped(
    plugin: RabbitmqPlugin,
    plugin_context: PluginContext,
    mock_connection: MagicMock,
) -> None:
    with _patch_connect(mock_connection):
        await plugin.initialize(plugin_context)

    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED


async def test_shutdown_never_raises_when_producer_is_none(
    plugin: RabbitmqPlugin,
) -> None:
    """shutdown() must not raise even if producer was never set."""
    plugin._producer = None
    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED


async def test_get_producer_raises_before_initialize(plugin: RabbitmqPlugin) -> None:
    with pytest.raises(RuntimeError, match="not ready"):
        plugin.get_producer()


async def test_get_producer_returns_rabbitmq_producer_after_initialize(
    plugin: RabbitmqPlugin,
    plugin_context: PluginContext,
    mock_connection: MagicMock,
) -> None:
    with _patch_connect(mock_connection):
        await plugin.initialize(plugin_context)

    p = plugin.get_producer()
    assert isinstance(p, RabbitmqProducer)


def test_make_consumer_returns_rabbitmq_consumer(plugin: RabbitmqPlugin) -> None:
    c = plugin.make_consumer()
    assert isinstance(c, RabbitmqConsumer)


# ── Health ─────────────────────────────────────────────────────────────────

async def test_health_returns_failed_when_not_initialized(
    plugin: RabbitmqPlugin,
) -> None:
    health = await plugin.health()
    assert health.status == PluginStatus.FAILED


async def test_health_never_raises(plugin: RabbitmqPlugin) -> None:
    """health() must never raise under any circumstances."""
    plugin._producer = None
    result = await plugin.health()
    assert result is not None
    assert result.status == PluginStatus.FAILED


async def test_health_returns_ready_after_initialize(
    plugin: RabbitmqPlugin,
    plugin_context: PluginContext,
    mock_connection: MagicMock,
) -> None:
    with _patch_connect(mock_connection):
        await plugin.initialize(plugin_context)

    health = await plugin.health()
    assert health.status == PluginStatus.READY


# ── producer_class parameter ───────────────────────────────────────────────

def test_plugin_defaults_to_base_producer_class(settings: RabbitmqSettings) -> None:
    """Backwards compatibility — no producer_class passed."""
    plugin = RabbitmqPlugin(settings)
    assert plugin._producer_class is RabbitmqProducer


def test_plugin_accepts_custom_producer_class(settings: RabbitmqSettings) -> None:
    class CustomProducer(RabbitmqProducer):
        pass

    plugin = RabbitmqPlugin(settings, producer_class=CustomProducer)
    assert plugin._producer_class is CustomProducer


def test_plugin_rejects_non_producer_class(settings: RabbitmqSettings) -> None:
    """producer_class must be a subclass of RabbitmqProducer — TypeError if not."""
    with pytest.raises(TypeError, match="subclass of RabbitmqProducer"):
        RabbitmqPlugin(settings, producer_class=object)  # type: ignore[arg-type]


async def test_initialize_constructs_custom_producer_class(
    settings: RabbitmqSettings,
    plugin_context: PluginContext,
    mock_connection: MagicMock,
) -> None:
    """
    REGRESSION: plugin must not always construct the base class, silently
    discarding domain subclass overrides of _serialise().
    """
    class CustomProducer(RabbitmqProducer):
        marker = True

    plugin = RabbitmqPlugin(settings, producer_class=CustomProducer)
    with _patch_connect(mock_connection):
        await plugin.initialize(plugin_context)

    producer = plugin.get_producer()
    assert isinstance(producer, CustomProducer)
    assert hasattr(producer, "marker")


async def test_get_producer_returns_subclass_not_base_class(
    settings: RabbitmqSettings,
    plugin_context: PluginContext,
    mock_connection: MagicMock,
) -> None:
    """type(producer) must be the subclass, not just isinstance-compatible."""
    class CustomProducer(RabbitmqProducer):
        pass

    plugin = RabbitmqPlugin(settings, producer_class=CustomProducer)
    with _patch_connect(mock_connection):
        await plugin.initialize(plugin_context)

    assert type(plugin.get_producer()) is CustomProducer


# ── Contract tests ─────────────────────────────────────────────────────────

class TestRabbitmqPluginContracts(PortContractTests):
    """
    RabbitmqPlugin passes the full openframe BasePort contract suite
    (identity + lifecycle: initialize -> health -> idempotent shutdown).
    """

    @pytest.fixture
    def port(self, settings: RabbitmqSettings, mock_connection: MagicMock) -> RabbitmqPlugin:
        with _patch_connect(mock_connection):
            plugin = RabbitmqPlugin(settings)
            yield plugin


# ── consumer_class parameter ───────────────────────────────────────────────

def test_plugin_defaults_to_base_consumer_class(settings: RabbitmqSettings) -> None:
    """No consumer_class passed → _consumer_class is RabbitmqConsumer."""
    plugin = RabbitmqPlugin(settings)
    assert plugin._consumer_class is RabbitmqConsumer


def test_plugin_accepts_custom_consumer_class(settings: RabbitmqSettings) -> None:
    """Custom consumer_class subclass is stored without error."""
    class _TestConsumer(RabbitmqConsumer):
        pass

    plugin = RabbitmqPlugin(settings, consumer_class=_TestConsumer)
    assert plugin._consumer_class is _TestConsumer


def test_plugin_rejects_non_consumer_class(settings: RabbitmqSettings) -> None:
    """Non-subclass raises TypeError with a clear message."""
    with pytest.raises(TypeError, match="must be a subclass of RabbitmqConsumer"):
        RabbitmqPlugin(settings, consumer_class=object)  # type: ignore[arg-type]


def test_make_consumer_returns_subclass_not_base_class(settings: RabbitmqSettings) -> None:
    """
    make_consumer() returns the configured subclass.

    Does NOT require initialize() — make_consumer() constructs on demand
    with no broker connection.
    """
    class _DomainConsumer(RabbitmqConsumer):
        pass

    plugin = RabbitmqPlugin(settings, consumer_class=_DomainConsumer)
    consumer = plugin.make_consumer()

    assert isinstance(consumer, _DomainConsumer)
    assert type(consumer) is _DomainConsumer
