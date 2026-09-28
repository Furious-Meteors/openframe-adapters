"""
tests/conftest.py — openframe-adapters-queue-nats
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
def mock_pub_ack():
    """A fake nats.js.api.PubAck-shaped object returned by js.publish()."""
    ack = MagicMock()
    ack.stream = "OPENFRAME"
    ack.seq = 1
    return ack


@pytest.fixture
def mock_jetstream_context(mock_pub_ack):
    """A mocked JetStreamContext. No network calls."""
    js = MagicMock()
    js.publish = AsyncMock(return_value=mock_pub_ack)
    js.add_stream = AsyncMock()
    js.subscribe = AsyncMock()
    return js


@pytest.fixture
def mock_nats_client(mock_jetstream_context):
    """A mocked NATS client (as returned by nats.connect()). No network calls."""
    nc = MagicMock()
    nc.close = AsyncMock()
    nc.jetstream = MagicMock(return_value=mock_jetstream_context)
    return nc


@pytest.fixture
def mock_subscription():
    """
    A mocked JetStream push Subscription.

    ``.messages`` is set up as an async-iterable (mirroring Kafka's
    ``mock_consumer_client.__aiter__``/``__anext__`` pattern) so
    ``async for msg in sub.messages:`` terminates naturally via
    ``StopAsyncIteration`` — no real broker, no background task needed.
    """
    sub = MagicMock()
    sub.unsubscribe = AsyncMock()
    messages = MagicMock()
    messages.__aiter__ = MagicMock(return_value=messages)
    messages.__anext__ = AsyncMock(side_effect=StopAsyncIteration)
    sub.messages = messages
    return sub


@pytest.fixture
def mock_settings():
    """A NatsSettings instance with a dummy server address."""
    from openframe.adapters.queue.nats import NatsSettings
    return NatsSettings(nats_servers="nats://localhost:4222")


@pytest.fixture
def producer(mock_settings, mock_nats_client, mock_jetstream_context):
    """NatsProducer with a mocked NATS client/JetStream context already started."""
    from openframe.adapters.queue.nats import NatsProducer
    with patch(
        "openframe.adapters.queue.nats.producer.nats.connect",
        AsyncMock(return_value=mock_nats_client),
    ):
        p = NatsProducer(mock_settings)
        p._nc = mock_nats_client
        p._js = mock_jetstream_context
        yield p


@pytest.fixture
def consumer(mock_settings):
    """A NatsConsumer with settings (no live broker)."""
    from openframe.adapters.queue.nats import NatsConsumer
    return NatsConsumer(mock_settings)
