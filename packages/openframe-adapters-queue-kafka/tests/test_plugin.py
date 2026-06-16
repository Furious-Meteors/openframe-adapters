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
from openframe.core.plugins import OpenFramePlugin, PluginContext, PluginStatus


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

def test_kafka_plugin_satisfies_openframe_plugin_protocol(plugin: KafkaPlugin) -> None:
    assert isinstance(plugin, OpenFramePlugin)


def test_kafka_plugin_name(plugin: KafkaPlugin) -> None:
    assert plugin.name == "openframe-kafka"


def test_kafka_plugin_version(plugin: KafkaPlugin) -> None:
    assert plugin.version == "1.1.0"


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
