"""
tests/conftest.py — openframe-adapters-queue-rabbitmq
=========================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.

No real network calls anywhere in this suite — every aio_pika object
(connection, channel, queue, message) is mocked.
"""
from __future__ import annotations

# Canonical OTel reset fixtures from openframe-core v3.0.
# Provides (autouse): reset_telemetry_state
# Provides (on-demand): span_exporter, metric_reader
from openframe.core.testing.fixtures import *  # noqa: F401, F403

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


def _make_mock_queue_iterator(messages: list):
    """
    Build a mock object usable as ``async with q.iterator() as it: async for m in it``.

    Mirrors the Kafka suite's ``__aiter__``/``__anext__`` mocking style —
    yields each message in ``messages`` in order, then raises
    ``StopAsyncIteration``.
    """
    it = MagicMock()
    it.__aenter__ = AsyncMock(return_value=it)
    it.__aexit__ = AsyncMock(return_value=False)
    it.__aiter__ = MagicMock(return_value=it)
    it.__anext__ = AsyncMock(side_effect=[*messages, StopAsyncIteration])
    return it


@pytest.fixture
def mock_channel():
    """A mocked aio_pika channel. No network calls."""
    ch = MagicMock()
    ch.close = AsyncMock()
    ch.set_qos = AsyncMock()
    exchange = MagicMock()
    exchange.publish = AsyncMock()
    ch.default_exchange = exchange
    return ch


@pytest.fixture
def mock_connection(mock_channel):
    """A mocked aio_pika robust connection. No network calls."""
    conn = MagicMock()
    conn.channel = AsyncMock(return_value=mock_channel)
    conn.close = AsyncMock()
    conn.is_closed = False
    return conn


@pytest.fixture
def mock_queue():
    """A mocked aio_pika queue with an empty iterator by default."""
    q = MagicMock()
    q.iterator = MagicMock(return_value=_make_mock_queue_iterator([]))
    return q


@pytest.fixture
def mock_settings():
    """A RabbitmqSettings instance with a dummy broker URL."""
    from openframe.adapters.queue.rabbitmq import RabbitmqSettings
    return RabbitmqSettings(rabbitmq_url="amqp://guest:guest@localhost:5672/")


@pytest.fixture
def producer(mock_settings, mock_connection, mock_channel):
    """RabbitmqProducer with a mocked connection/channel already started."""
    from openframe.adapters.queue.rabbitmq import RabbitmqProducer
    with patch(
        "openframe.adapters.queue.rabbitmq.producer.aio_pika.connect_robust",
        AsyncMock(return_value=mock_connection),
    ):
        p = RabbitmqProducer(mock_settings)
        p._connection = mock_connection
        p._channel = mock_channel
        yield p


@pytest.fixture
def consumer(mock_settings):
    """A RabbitmqConsumer with settings (no live broker)."""
    from openframe.adapters.queue.rabbitmq import RabbitmqConsumer
    return RabbitmqConsumer(mock_settings)
