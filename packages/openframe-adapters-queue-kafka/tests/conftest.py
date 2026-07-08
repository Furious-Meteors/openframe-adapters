"""
tests/conftest.py — openframe-adapters-queue-kafka
=====================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.
"""
from __future__ import annotations

# Canonical OTel reset fixtures from openframe-core v3.0.
# Provides (autouse): reset_telemetry_state
# Provides (on-demand): span_exporter, metric_reader
from openframe.core.testing.fixtures import *  # noqa: F401, F403

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.fixture
def mock_producer_client():
    """A mocked AIOKafkaProducer. No network calls."""
    p = MagicMock()
    p.start         = AsyncMock()
    p.stop          = AsyncMock()
    p.send_and_wait = AsyncMock()
    p.flush         = AsyncMock()
    return p


@pytest.fixture
def mock_consumer_client():
    """A mocked AIOKafkaConsumer. No network calls."""
    c = MagicMock()
    c.start  = AsyncMock()
    c.stop   = AsyncMock()
    c.commit = AsyncMock()
    # __aiter__ makes it usable as `async for msg in consumer`
    c.__aiter__ = MagicMock(return_value=c)
    c.__anext__ = AsyncMock(side_effect=StopAsyncIteration)
    return c


@pytest.fixture
def mock_settings():
    """A KafkaSettings instance with dummy broker address."""
    from openframe.adapters.queue.kafka import KafkaSettings
    return KafkaSettings(kafka_bootstrap_servers="localhost:9092")


@pytest.fixture
def producer(mock_settings, mock_producer_client):
    """KafkaProducer with a mocked AIOKafkaProducer already started."""
    from openframe.adapters.queue.kafka import KafkaProducer
    with patch(
        "openframe.adapters.queue.kafka.producer.AIOKafkaProducer",
        return_value=mock_producer_client,
    ):
        p = KafkaProducer(mock_settings)
        p._producer = mock_producer_client
        yield p


@pytest.fixture
def consumer(mock_settings):
    """A KafkaConsumer with settings (no live broker)."""
    from openframe.adapters.queue.kafka import KafkaConsumer
    return KafkaConsumer(mock_settings)
