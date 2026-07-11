"""
tests/test_plugin.py — openframe-adapters-queue-kafka
=======================================================
Tests for KafkaPlugin: protocol conformance, lifecycle, and health.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from openframe.adapters.queue.kafka import (
    KafkaConsumer,
    KafkaPlugin,
    KafkaProducer,
    KafkaSettings,
)
from openframe.core.ports import BasePort, PluginContext, PluginStatus
from openframe.core.testing.contracts import PortContractTests


@pytest.fixture
def settings():
    return KafkaSettings(kafka_bootstrap_servers="localhost:9092")


@pytest.fixture
def plugin(settings: KafkaSettings) -> KafkaPlugin:
    return KafkaPlugin(settings)


@pytest.fixture
def plugin_context() -> PluginContext:
    return PluginContext(config={}, plugin_name="openframe-kafka")


# ── Protocol conformance ───────────────────────────────────────────────────

def test_kafka_plugin_satisfies_base_port_protocol(plugin: KafkaPlugin) -> None:
    assert isinstance(plugin, BasePort)


def test_kafka_plugin_name(plugin: KafkaPlugin) -> None:
    assert plugin.name == "openframe-kafka"


def test_kafka_plugin_version(plugin: KafkaPlugin) -> None:
    assert plugin.version == "1.4.3"


def test_kafka_plugin_capability(plugin: KafkaPlugin) -> None:
    assert plugin.capability == "queue"


# ── Lifecycle ──────────────────────────────────────────────────────────────

async def test_initialize_succeeds_and_sets_ready(
    plugin: KafkaPlugin,
    plugin_context: PluginContext,
    mock_producer_client: MagicMock,
) -> None:
    """Successful producer start → status READY."""
    with patch(
        "openframe.adapters.queue.kafka.producer.AIOKafkaProducer",
        return_value=mock_producer_client,
    ):
        await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.READY


async def test_initialize_fails_when_producer_start_raises(
    plugin: KafkaPlugin,
    plugin_context: PluginContext,
) -> None:
    """Producer start raising → status FAILED, exception propagated."""
    with patch(
        "openframe.adapters.queue.kafka.producer.AIOKafkaProducer"
    ) as MockProducer:
        import aiokafka.errors
        MockProducer.return_value.start = AsyncMock(
            side_effect=aiokafka.errors.KafkaConnectionError()
        )
        from openframe.core.exceptions import AdapterConnectionError
        with pytest.raises(AdapterConnectionError):
            await plugin.initialize(plugin_context)

    assert plugin._status == PluginStatus.FAILED


async def test_shutdown_sets_status_stopped(
    plugin: KafkaPlugin,
    plugin_context: PluginContext,
    mock_producer_client: MagicMock,
) -> None:
    with patch(
        "openframe.adapters.queue.kafka.producer.AIOKafkaProducer",
        return_value=mock_producer_client,
    ):
        await plugin.initialize(plugin_context)

    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED


async def test_shutdown_never_raises_when_producer_is_none(
    plugin: KafkaPlugin,
) -> None:
    """shutdown() must not raise even if producer was never set."""
    plugin._producer = None
    await plugin.shutdown()
    assert plugin._status == PluginStatus.STOPPED


async def test_get_producer_raises_before_initialize(plugin: KafkaPlugin) -> None:
    with pytest.raises(RuntimeError, match="not ready"):
        plugin.get_producer()


async def test_get_producer_returns_kafka_producer_after_initialize(
    plugin: KafkaPlugin,
    plugin_context: PluginContext,
    mock_producer_client: MagicMock,
) -> None:
    with patch(
        "openframe.adapters.queue.kafka.producer.AIOKafkaProducer",
        return_value=mock_producer_client,
    ):
        await plugin.initialize(plugin_context)

    p = plugin.get_producer()
    assert isinstance(p, KafkaProducer)


def test_make_consumer_returns_kafka_consumer(plugin: KafkaPlugin) -> None:
    c = plugin.make_consumer()
    assert isinstance(c, KafkaConsumer)


# ── Health ─────────────────────────────────────────────────────────────────

async def test_health_returns_failed_when_not_initialized(
    plugin: KafkaPlugin,
) -> None:
    health = await plugin.health()
    assert health.status == PluginStatus.FAILED


async def test_health_never_raises(plugin: KafkaPlugin) -> None:
    """health() must never raise under any circumstances."""
    plugin._producer = None
    result = await plugin.health()
    assert result is not None
    assert result.status == PluginStatus.FAILED


async def test_health_returns_ready_after_initialize(
    plugin: KafkaPlugin,
    plugin_context: PluginContext,
    mock_producer_client: MagicMock,
) -> None:
    with patch(
        "openframe.adapters.queue.kafka.producer.AIOKafkaProducer",
        return_value=mock_producer_client,
    ):
        await plugin.initialize(plugin_context)

    health = await plugin.health()
    assert health.status == PluginStatus.READY


# ── producer_class parameter ───────────────────────────────────────────────

def test_plugin_defaults_to_base_producer_class(settings: KafkaSettings) -> None:
    """Backwards compatibility — no producer_class passed."""
    plugin = KafkaPlugin(settings)
    assert plugin._producer_class is KafkaProducer


def test_plugin_accepts_custom_producer_class(settings: KafkaSettings) -> None:
    class CustomProducer(KafkaProducer):
        pass

    plugin = KafkaPlugin(settings, producer_class=CustomProducer)
    assert plugin._producer_class is CustomProducer


def test_plugin_rejects_non_producer_class(settings: KafkaSettings) -> None:
    """producer_class must be a subclass of KafkaProducer — TypeError if not."""
    with pytest.raises(TypeError, match="subclass of KafkaProducer"):
        KafkaPlugin(settings, producer_class=object)  # type: ignore[arg-type]


async def test_initialize_constructs_custom_producer_class(
    settings: KafkaSettings,
    plugin_context: PluginContext,
    mock_producer_client: MagicMock,
) -> None:
    """
    REGRESSION: plugin always constructed the base class, silently discarding
    domain subclass overrides of _serialise().
    """
    class CustomProducer(KafkaProducer):
        marker = True

    plugin = KafkaPlugin(settings, producer_class=CustomProducer)
    with patch(
        "openframe.adapters.queue.kafka.producer.AIOKafkaProducer",
        return_value=mock_producer_client,
    ):
        await plugin.initialize(plugin_context)

    producer = plugin.get_producer()
    assert isinstance(producer, CustomProducer)
    assert hasattr(producer, "marker")


async def test_get_producer_returns_subclass_not_base_class(
    settings: KafkaSettings,
    plugin_context: PluginContext,
    mock_producer_client: MagicMock,
) -> None:
    """type(producer) must be the subclass, not just isinstance-compatible."""
    class CustomProducer(KafkaProducer):
        pass

    plugin = KafkaPlugin(settings, producer_class=CustomProducer)
    with patch(
        "openframe.adapters.queue.kafka.producer.AIOKafkaProducer",
        return_value=mock_producer_client,
    ):
        await plugin.initialize(plugin_context)

    assert type(plugin.get_producer()) is CustomProducer


# ── Contract tests ─────────────────────────────────────────────────────────

class TestKafkaPluginContracts(PortContractTests):
    """
    KafkaPlugin passes the full openframe BasePort contract suite
    (identity + lifecycle: initialize -> health -> idempotent shutdown).
    """

    @pytest.fixture
    def port(self, settings: KafkaSettings, mock_producer_client: MagicMock) -> KafkaPlugin:
        with patch(
            "openframe.adapters.queue.kafka.producer.AIOKafkaProducer",
            return_value=mock_producer_client,
        ):
            plugin = KafkaPlugin(settings)
            yield plugin


# ── consumer_class parameter ───────────────────────────────────────────────

def test_plugin_defaults_to_base_consumer_class(settings: KafkaSettings) -> None:
    """No consumer_class passed → _consumer_class is KafkaConsumer."""
    plugin = KafkaPlugin(settings)
    assert plugin._consumer_class is KafkaConsumer


def test_plugin_accepts_custom_consumer_class(settings: KafkaSettings) -> None:
    """Custom consumer_class subclass is stored without error."""
    class _TestConsumer(KafkaConsumer):
        pass

    plugin = KafkaPlugin(settings, consumer_class=_TestConsumer)
    assert plugin._consumer_class is _TestConsumer


def test_plugin_rejects_non_consumer_class(settings: KafkaSettings) -> None:
    """Non-subclass raises TypeError with a clear message."""
    with pytest.raises(TypeError, match="must be a subclass of KafkaConsumer"):
        KafkaPlugin(settings, consumer_class=object)  # type: ignore[arg-type]


def test_make_consumer_returns_subclass_not_base_class(settings: KafkaSettings) -> None:
    """
    make_consumer() returns the configured subclass.

    Does NOT require initialize() — make_consumer() constructs on demand
    with no broker connection.
    """
    class _DomainConsumer(KafkaConsumer):
        pass

    plugin = KafkaPlugin(settings, consumer_class=_DomainConsumer)
    consumer = plugin.make_consumer()

    assert isinstance(consumer, _DomainConsumer)
    assert type(consumer) is _DomainConsumer
