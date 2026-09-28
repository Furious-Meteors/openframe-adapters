"""
tests/test_consumer.py — openframe-adapters-queue-nats
=========================================================
Contract tests (ConsumerContractTests) + adapter-specific tests for
NatsConsumer.
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import nats.errors
import pytest

from openframe.adapters.queue.nats import NatsConsumer, NatsSettings
from openframe.core.exceptions import AdapterConfigurationError, AdapterConnectionError
from openframe.core.ports import BaseConsumer
from openframe.core.testing import ConsumerContractTests


def _make_raw_msg(payload: dict, headers: dict | None = None) -> MagicMock:
    msg = MagicMock()
    msg.data = json.dumps(payload).encode("utf-8")
    msg.headers = headers
    msg.ack = AsyncMock()
    msg.nak = AsyncMock()
    return msg


def _patch_connect(mock_nats_client: MagicMock):
    return patch(
        "openframe.adapters.queue.nats.consumer.nats.connect",
        AsyncMock(return_value=mock_nats_client),
    )


# ── Contract tests ─────────────────────────────────────────────────────────

class TestNatsConsumerContracts(ConsumerContractTests):
    """
    NatsConsumer passes the full openframe consumer contract suite.

    All ConsumerContractTests run against a NatsConsumer wired to a mocked
    NATS client/JetStream subscription that delivers one message then stops.
    No real NATS server required.
    """

    @pytest.fixture
    def consumer(
        self,
        mock_settings: NatsSettings,
        mock_nats_client: MagicMock,
        mock_jetstream_context: MagicMock,
        mock_subscription: MagicMock,
    ):
        raw_msg = _make_raw_msg({"content": "test message", "type": "event"})
        mock_subscription.messages.__anext__ = AsyncMock(
            side_effect=[raw_msg, StopAsyncIteration]
        )
        mock_jetstream_context.subscribe = AsyncMock(return_value=mock_subscription)
        with _patch_connect(mock_nats_client):
            yield NatsConsumer(mock_settings)

    @pytest.fixture
    def port(self, consumer):
        return consumer

    @pytest.fixture
    def make_message(self):
        def _make(content: str = "test") -> dict:
            return {"content": content, "type": "event"}
        return _make


# ── Adapter-specific tests ─────────────────────────────────────────────────

class TestProtocolConformance:
    def test_isinstance_base_consumer(self, consumer: NatsConsumer) -> None:
        assert isinstance(consumer, BaseConsumer)


class TestSubscribe:
    async def test_subscribe_connects_and_creates_durable_subscription(
        self,
        consumer: NatsConsumer,
        mock_nats_client: MagicMock,
        mock_jetstream_context: MagicMock,
        mock_subscription: MagicMock,
    ) -> None:
        mock_jetstream_context.subscribe = AsyncMock(return_value=mock_subscription)
        with _patch_connect(mock_nats_client):
            await consumer.subscribe(AsyncMock())
        mock_jetstream_context.subscribe.assert_called_once()
        _, kwargs = mock_jetstream_context.subscribe.call_args
        assert kwargs["manual_ack"] is True
        assert kwargs["config"].durable_name == consumer._settings.nats_durable_name

    async def test_subscribe_calls_handler_with_deserialised_message(
        self,
        consumer: NatsConsumer,
        mock_nats_client: MagicMock,
        mock_jetstream_context: MagicMock,
        mock_subscription: MagicMock,
    ) -> None:
        raw_msg = _make_raw_msg({"event": "item.created"})
        mock_subscription.messages.__anext__ = AsyncMock(
            side_effect=[raw_msg, StopAsyncIteration]
        )
        mock_jetstream_context.subscribe = AsyncMock(return_value=mock_subscription)

        handler = AsyncMock()
        with _patch_connect(mock_nats_client):
            await consumer.subscribe(handler)

        handler.assert_called_once_with({"event": "item.created"})

    async def test_subscribe_acks_after_successful_handler(
        self,
        consumer: NatsConsumer,
        mock_nats_client: MagicMock,
        mock_jetstream_context: MagicMock,
        mock_subscription: MagicMock,
    ) -> None:
        raw_msg = _make_raw_msg({"event": "ok"})
        mock_subscription.messages.__anext__ = AsyncMock(
            side_effect=[raw_msg, StopAsyncIteration]
        )
        mock_jetstream_context.subscribe = AsyncMock(return_value=mock_subscription)

        with _patch_connect(mock_nats_client):
            await consumer.subscribe(AsyncMock())

        raw_msg.ack.assert_called_once()
        raw_msg.nak.assert_not_called()

    async def test_subscribe_naks_after_handler_failure(
        self,
        consumer: NatsConsumer,
        mock_nats_client: MagicMock,
        mock_jetstream_context: MagicMock,
        mock_subscription: MagicMock,
    ) -> None:
        raw_msg = _make_raw_msg({"event": "bad"})
        mock_subscription.messages.__anext__ = AsyncMock(
            side_effect=[raw_msg, StopAsyncIteration]
        )
        mock_jetstream_context.subscribe = AsyncMock(return_value=mock_subscription)

        failing_handler = AsyncMock(side_effect=ValueError("bad message"))
        with _patch_connect(mock_nats_client):
            await consumer.subscribe(failing_handler)

        raw_msg.nak.assert_called_once()
        raw_msg.ack.assert_not_called()

    async def test_subscribe_raises_adapter_connection_error_on_no_servers(
        self, consumer: NatsConsumer
    ) -> None:
        with patch(
            "openframe.adapters.queue.nats.consumer.nats.connect",
            AsyncMock(side_effect=nats.errors.NoServersError()),
        ):
            with pytest.raises(AdapterConnectionError):
                await consumer.subscribe(AsyncMock())

    async def test_subscribe_raises_adapter_configuration_error_on_other_nats_error(
        self, consumer: NatsConsumer
    ) -> None:
        with patch(
            "openframe.adapters.queue.nats.consumer.nats.connect",
            AsyncMock(side_effect=nats.errors.AuthorizationError()),
        ):
            with pytest.raises(AdapterConfigurationError):
                await consumer.subscribe(AsyncMock())


class TestNack:
    async def test_nack_calls_nak_on_message(self, consumer: NatsConsumer) -> None:
        """nack() must call msg.nak() — redelivery signal — never raise."""
        msg = MagicMock()
        msg.nak = AsyncMock()
        await consumer.nack(msg)
        msg.nak.assert_called_once()

    async def test_nack_never_raises_when_nak_fails(self, consumer: NatsConsumer) -> None:
        msg = MagicMock()
        msg.nak = AsyncMock(side_effect=RuntimeError("boom"))
        await consumer.nack(msg)  # must not raise


class TestAck:
    async def test_ack_calls_ack_on_message(self, consumer: NatsConsumer) -> None:
        msg = MagicMock()
        msg.ack = AsyncMock()
        await consumer.ack(msg)
        msg.ack.assert_called_once()

    async def test_ack_never_raises_when_ack_fails(self, consumer: NatsConsumer) -> None:
        msg = MagicMock()
        msg.ack = AsyncMock(side_effect=RuntimeError("boom"))
        await consumer.ack(msg)  # must not raise


class TestClose:
    async def test_close_sets_running_false(self, consumer: NatsConsumer) -> None:
        consumer._running = True
        await consumer.close()
        assert consumer._running is False

    async def test_close_never_raises_when_not_subscribed(
        self, consumer: NatsConsumer
    ) -> None:
        consumer._sub = None
        consumer._nc = None
        await consumer.close()  # must not raise
