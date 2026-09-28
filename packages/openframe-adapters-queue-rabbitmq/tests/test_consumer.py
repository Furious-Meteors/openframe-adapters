"""
tests/test_consumer.py — openframe-adapters-queue-rabbitmq
===============================================================
Contract tests (ConsumerContractTests) + adapter-specific tests for
RabbitmqConsumer. No real network calls — aio_pika is fully mocked.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import aio_pika.exceptions
import pytest

from openframe.adapters.queue.rabbitmq import RabbitmqConsumer, RabbitmqSettings
from openframe.core.exceptions import AdapterConnectionError, AdapterQueryError
from openframe.core.ports import BaseConsumer
from openframe.core.testing import ConsumerContractTests

from conftest import _make_mock_queue_iterator


def _raw_message(payload: dict, headers: dict | None = None) -> MagicMock:
    msg = MagicMock()
    msg.body = json.dumps(payload).encode("utf-8")
    msg.headers = headers or {}
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    return msg


def _patch_connect(mock_connection):
    return patch(
        "openframe.adapters.queue.rabbitmq.consumer.aio_pika.connect_robust",
        AsyncMock(return_value=mock_connection),
    )


# ── Contract tests ─────────────────────────────────────────────────────────

class TestRabbitmqConsumerContracts(ConsumerContractTests):
    """
    RabbitmqConsumer passes the full openframe consumer contract suite.

    All ConsumerContractTests run against a RabbitmqConsumer wired to a
    mocked aio_pika connection/channel/queue that delivers one message then
    stops. No real broker required.
    """

    @pytest.fixture
    def consumer(
        self,
        mock_settings: RabbitmqSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
        mock_queue: MagicMock,
    ):
        raw_msg = _raw_message({"content": "test message", "type": "event"})
        mock_queue.iterator = MagicMock(return_value=_make_mock_queue_iterator([raw_msg]))
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        with _patch_connect(mock_connection):
            yield RabbitmqConsumer(mock_settings)

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
    def test_isinstance_base_consumer(self, consumer: RabbitmqConsumer) -> None:
        assert isinstance(consumer, BaseConsumer)


class TestSubscribe:
    async def test_subscribe_opens_connection_and_channel(
        self,
        consumer: RabbitmqConsumer,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
        mock_queue: MagicMock,
    ) -> None:
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        with _patch_connect(mock_connection):
            await consumer.subscribe(AsyncMock())
        mock_connection.channel.assert_called_once()
        mock_channel.declare_queue.assert_called_once_with(
            consumer._settings.rabbitmq_queue, durable=consumer._settings.rabbitmq_durable
        )

    async def test_subscribe_sets_qos_prefetch(
        self,
        consumer: RabbitmqConsumer,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
        mock_queue: MagicMock,
    ) -> None:
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        with _patch_connect(mock_connection):
            await consumer.subscribe(AsyncMock())
        mock_channel.set_qos.assert_called_once_with(
            prefetch_count=consumer._settings.rabbitmq_prefetch_count
        )

    async def test_subscribe_calls_handler_with_deserialised_message(
        self,
        consumer: RabbitmqConsumer,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
        mock_queue: MagicMock,
    ) -> None:
        raw_msg = _raw_message({"event": "item.created"})
        mock_queue.iterator = MagicMock(return_value=_make_mock_queue_iterator([raw_msg]))
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        handler = AsyncMock()
        with _patch_connect(mock_connection):
            await consumer.subscribe(handler)

        handler.assert_called_once_with({"event": "item.created"})

    async def test_subscribe_acks_after_successful_handler(
        self,
        consumer: RabbitmqConsumer,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
        mock_queue: MagicMock,
    ) -> None:
        raw_msg = _raw_message({"event": "ok"})
        mock_queue.iterator = MagicMock(return_value=_make_mock_queue_iterator([raw_msg]))
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        with _patch_connect(mock_connection):
            await consumer.subscribe(AsyncMock())

        raw_msg.ack.assert_called_once()
        raw_msg.nack.assert_not_called()

    async def test_subscribe_nacks_with_requeue_after_handler_failure(
        self,
        consumer: RabbitmqConsumer,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
        mock_queue: MagicMock,
    ) -> None:
        raw_msg = _raw_message({"event": "bad"})
        mock_queue.iterator = MagicMock(return_value=_make_mock_queue_iterator([raw_msg]))
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        failing_handler = AsyncMock(side_effect=ValueError("bad message"))
        with _patch_connect(mock_connection):
            await consumer.subscribe(failing_handler)

        raw_msg.ack.assert_not_called()
        raw_msg.nack.assert_called_once_with(requeue=True)

    async def test_subscribe_raises_adapter_connection_error_on_broker_failure(
        self, consumer: RabbitmqConsumer
    ) -> None:
        with patch(
            "openframe.adapters.queue.rabbitmq.consumer.aio_pika.connect_robust",
            AsyncMock(side_effect=aio_pika.exceptions.AMQPConnectionError("unreachable")),
        ):
            with pytest.raises(AdapterConnectionError):
                await consumer.subscribe(AsyncMock())

    async def test_subscribe_raises_adapter_query_error_on_channel_failure(
        self, consumer: RabbitmqConsumer, mock_connection: MagicMock
    ) -> None:
        mock_connection.channel = AsyncMock(
            side_effect=aio_pika.exceptions.ChannelClosed(None, 406, "PRECONDITION_FAILED")
        )
        with _patch_connect(mock_connection):
            with pytest.raises(AdapterQueryError):
                await consumer.subscribe(AsyncMock())


class TestAckNack:
    async def test_ack_calls_ack_on_current_raw_message(
        self, consumer: RabbitmqConsumer
    ) -> None:
        raw = MagicMock()
        raw.ack = AsyncMock()
        consumer._current_raw_message = raw
        await consumer.ack({"event": "test"})
        raw.ack.assert_called_once()

    async def test_nack_calls_nack_with_requeue_on_current_raw_message(
        self, consumer: RabbitmqConsumer
    ) -> None:
        raw = MagicMock()
        raw.nack = AsyncMock()
        consumer._current_raw_message = raw
        await consumer.nack({"event": "test"})
        raw.nack.assert_called_once_with(requeue=True)

    async def test_ack_never_raises_when_no_current_message(
        self, consumer: RabbitmqConsumer
    ) -> None:
        consumer._current_raw_message = None
        await consumer.ack({"event": "test"})  # must not raise

    async def test_ack_never_raises_when_underlying_ack_fails(
        self, consumer: RabbitmqConsumer
    ) -> None:
        raw = MagicMock()
        raw.ack = AsyncMock(side_effect=RuntimeError("boom"))
        consumer._current_raw_message = raw
        await consumer.ack({"event": "test"})  # must not raise


class TestClose:
    async def test_close_sets_running_false(self, consumer: RabbitmqConsumer) -> None:
        consumer._running = True
        await consumer.close()
        assert consumer._running is False

    async def test_close_never_raises_when_connection_is_none(
        self, consumer: RabbitmqConsumer
    ) -> None:
        consumer._connection = None
        consumer._channel = None
        await consumer.close()  # must not raise

    async def test_close_closes_channel_and_connection(
        self, consumer: RabbitmqConsumer, mock_connection: MagicMock, mock_channel: MagicMock
    ) -> None:
        consumer._connection = mock_connection
        consumer._channel = mock_channel
        await consumer.close()
        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()
