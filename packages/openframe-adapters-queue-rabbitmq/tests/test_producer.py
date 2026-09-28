"""
tests/test_producer.py — openframe-adapters-queue-rabbitmq
===============================================================
Contract tests (ProducerContractTests) + adapter-specific tests for
RabbitmqProducer. No real network calls — aio_pika is fully mocked.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import aio_pika.exceptions
import pytest

from openframe.adapters.queue.rabbitmq import RabbitmqProducer, RabbitmqSettings
from openframe.core.exceptions import (
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseProducer
from openframe.core.testing import ProducerContractTests


# ── Contract tests ─────────────────────────────────────────────────────────

class TestRabbitmqProducerContracts(ProducerContractTests):
    """
    RabbitmqProducer passes the full openframe producer contract suite.

    All ProducerContractTests run against a RabbitmqProducer wired to a
    mocked aio_pika connection/channel. No real broker required.
    """

    @pytest.fixture
    def producer(self, mock_settings: RabbitmqSettings, mock_connection: MagicMock, mock_channel: MagicMock):
        with patch(
            "openframe.adapters.queue.rabbitmq.producer.aio_pika.connect_robust",
            AsyncMock(return_value=mock_connection),
        ):
            p = RabbitmqProducer(mock_settings)
            p._connection = mock_connection
            p._channel = mock_channel
            yield p

    @pytest.fixture
    def port(self, producer):
        return producer

    @pytest.fixture
    def make_message(self):
        def _make(content: str = "test message") -> dict:
            return {"content": content, "type": "event"}
        return _make


# ── Adapter-specific tests ─────────────────────────────────────────────────

class TestProtocolConformance:
    def test_isinstance_base_producer(self, producer: RabbitmqProducer) -> None:
        assert isinstance(producer, BaseProducer)


class TestPublish:
    async def test_publish_calls_default_exchange_publish(
        self, producer: RabbitmqProducer, mock_channel: MagicMock
    ) -> None:
        await producer.publish({"event": "test"})
        mock_channel.default_exchange.publish.assert_called_once()

    async def test_publish_serialises_to_json_bytes(
        self, producer: RabbitmqProducer, mock_channel: MagicMock
    ) -> None:
        message = {"event": "item.created", "id": "abc"}
        await producer.publish(message)
        call_args = mock_channel.default_exchange.publish.call_args
        amqp_message = call_args.args[0]
        assert call_args.kwargs["routing_key"] == producer._settings.rabbitmq_queue
        assert json.loads(amqp_message.body.decode("utf-8")) == message

    async def test_publish_raises_runtime_error_if_not_started(
        self, mock_settings: RabbitmqSettings
    ) -> None:
        p = RabbitmqProducer(mock_settings)
        with pytest.raises(RuntimeError, match="not started"):
            await p.publish({"event": "test"})

    async def test_publish_channel_error_raises_adapter_query_error(
        self, producer: RabbitmqProducer, mock_channel: MagicMock
    ) -> None:
        mock_channel.default_exchange.publish.side_effect = aio_pika.exceptions.MessageProcessError(
            "publish failed", "undeliverable"
        )
        with pytest.raises(AdapterQueryError) as exc_info:
            await producer.publish({"event": "test"})
        assert exc_info.value.operation == "publish"

    async def test_publish_connection_error_raises_adapter_connection_error(
        self, producer: RabbitmqProducer, mock_channel: MagicMock
    ) -> None:
        mock_channel.default_exchange.publish.side_effect = aio_pika.exceptions.ConnectionClosed(
            None
        )
        with pytest.raises(AdapterConnectionError) as exc_info:
            await producer.publish({"event": "test"})
        assert exc_info.value.operation == "publish"
        assert exc_info.value.retryable is True

    async def test_publish_timeout_raises_adapter_timeout_error(
        self, producer: RabbitmqProducer, mock_channel: MagicMock
    ) -> None:
        mock_channel.default_exchange.publish.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await producer.publish({"event": "test"})
        assert exc_info.value.operation == "publish"


class TestPublishBatch:
    async def test_publish_batch_sends_each_message(
        self, producer: RabbitmqProducer, mock_channel: MagicMock
    ) -> None:
        messages = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        await producer.publish_batch(messages)
        assert mock_channel.default_exchange.publish.call_count == 3

    async def test_publish_batch_raises_runtime_error_if_not_started(
        self, mock_settings: RabbitmqSettings
    ) -> None:
        p = RabbitmqProducer(mock_settings)
        with pytest.raises(RuntimeError, match="not started"):
            await p.publish_batch([{"event": "test"}])

    async def test_publish_batch_query_error_raises_adapter_query_error(
        self, producer: RabbitmqProducer, mock_channel: MagicMock
    ) -> None:
        mock_channel.default_exchange.publish.side_effect = aio_pika.exceptions.ChannelClosed(
            None, 406, "PRECONDITION_FAILED"
        )
        with pytest.raises(AdapterQueryError):
            await producer.publish_batch([{"id": "1"}])


class TestStart:
    async def test_start_raises_adapter_connection_error_on_broker_failure(
        self, mock_settings: RabbitmqSettings
    ) -> None:
        with patch(
            "openframe.adapters.queue.rabbitmq.producer.aio_pika.connect_robust",
            AsyncMock(side_effect=aio_pika.exceptions.AMQPConnectionError("unreachable")),
        ):
            p = RabbitmqProducer(mock_settings)
            with pytest.raises(AdapterConnectionError):
                await p.start()

    async def test_start_raises_adapter_connection_error_on_raw_os_error(
        self, mock_settings: RabbitmqSettings
    ) -> None:
        """
        Regression guard: a raw OSError/ConnectionError from the socket layer
        (before aio_pika wraps it into an AMQP-specific exception type) must
        still be classified as a retryable AdapterConnectionError, not leak
        through untranslated.
        """
        with patch(
            "openframe.adapters.queue.rabbitmq.producer.aio_pika.connect_robust",
            AsyncMock(side_effect=OSError("Connect call failed")),
        ):
            p = RabbitmqProducer(mock_settings)
            with pytest.raises(AdapterConnectionError):
                await p.start()

    async def test_start_raises_adapter_timeout_error(
        self, mock_settings: RabbitmqSettings
    ) -> None:
        with patch(
            "openframe.adapters.queue.rabbitmq.producer.aio_pika.connect_robust",
            AsyncMock(side_effect=asyncio.TimeoutError()),
        ):
            p = RabbitmqProducer(mock_settings)
            with pytest.raises(AdapterTimeoutError):
                await p.start()

    async def test_start_opens_channel(
        self, mock_settings: RabbitmqSettings, mock_connection: MagicMock, mock_channel: MagicMock
    ) -> None:
        with patch(
            "openframe.adapters.queue.rabbitmq.producer.aio_pika.connect_robust",
            AsyncMock(return_value=mock_connection),
        ):
            p = RabbitmqProducer(mock_settings)
            await p.start()
        mock_connection.channel.assert_called_once()
        assert p._channel is mock_channel


class TestClose:
    async def test_close_calls_close_on_channel_and_connection(
        self, producer: RabbitmqProducer, mock_connection: MagicMock, mock_channel: MagicMock
    ) -> None:
        await producer.close()
        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()

    async def test_close_is_idempotent(
        self, producer: RabbitmqProducer
    ) -> None:
        await producer.close()
        await producer.close()  # second call — must not raise


class TestHealth:
    async def test_health_never_raises_on_broken_connection_object(
        self, producer: RabbitmqProducer, mock_connection: MagicMock
    ) -> None:
        """health() must never raise, even if `.is_closed` itself explodes."""
        type(mock_connection).is_closed = property(lambda self: (_ for _ in ()).throw(RuntimeError("boom")))
        result = await producer.health()
        assert result.status.name == "FAILED"
