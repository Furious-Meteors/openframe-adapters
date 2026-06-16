"""
tests/test_producer.py — openframe-adapters-queue-kafka
=========================================================
Contract tests (ProducerContractTests) + adapter-specific tests for
KafkaProducer.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import aiokafka.errors
import pytest

from openframe.adapters.queue.kafka import KafkaProducer, KafkaSettings
from openframe.core.exceptions import (
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseProducer
from openframe.core.testing import ProducerContractTests


# ── Contract tests ─────────────────────────────────────────────────────────

class TestKafkaProducerContracts(ProducerContractTests):
    """
    KafkaProducer passes the full openframe producer contract suite.

    All 4 ProducerContractTests run against a KafkaProducer wired to a
    mocked AIOKafkaProducer. No real Kafka broker required.
    """

    @pytest.fixture
    def producer(self, mock_settings: KafkaSettings, mock_producer_client: MagicMock):
        with patch(
            "openframe.adapters.queue.kafka.producer.AIOKafkaProducer",
            return_value=mock_producer_client,
        ):
            p = KafkaProducer(mock_settings)
            p._producer = mock_producer_client
            yield p

    @pytest.fixture
    def make_message(self):
        def _make(content: str = "test message") -> dict:
            return {"content": content, "type": "event"}
        return _make


# ── Adapter-specific tests ─────────────────────────────────────────────────

class TestProtocolConformance:
    def test_isinstance_base_producer(self, producer: KafkaProducer) -> None:
        assert isinstance(producer, BaseProducer)


class TestPublish:
    async def test_publish_calls_send_and_wait(
        self, producer: KafkaProducer, mock_producer_client: MagicMock
    ) -> None:
        await producer.publish({"event": "test"})
        mock_producer_client.send_and_wait.assert_called_once()

    async def test_publish_serialises_to_json_bytes(
        self, producer: KafkaProducer, mock_producer_client: MagicMock
    ) -> None:
        message = {"event": "item.created", "id": "abc"}
        await producer.publish(message)
        call_args = mock_producer_client.send_and_wait.call_args
        topic, value = call_args.args
        assert topic == producer._settings.kafka_topic
        assert json.loads(value.decode("utf-8")) == message

    async def test_publish_raises_runtime_error_if_not_started(
        self, mock_settings: KafkaSettings
    ) -> None:
        p = KafkaProducer(mock_settings)
        with pytest.raises(RuntimeError, match="not started"):
            await p.publish({"event": "test"})

    async def test_publish_kafka_error_raises_adapter_query_error(
        self, producer: KafkaProducer, mock_producer_client: MagicMock
    ) -> None:
        mock_producer_client.send_and_wait.side_effect = aiokafka.errors.KafkaError()
        with pytest.raises(AdapterQueryError) as exc_info:
            await producer.publish({"event": "test"})
        assert exc_info.value.operation == "publish"

    async def test_publish_timeout_raises_adapter_timeout_error(
        self, producer: KafkaProducer, mock_producer_client: MagicMock
    ) -> None:
        mock_producer_client.send_and_wait.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await producer.publish({"event": "test"})
        assert exc_info.value.operation == "publish"


class TestPublishBatch:
    async def test_publish_batch_sends_each_message(
        self, producer: KafkaProducer, mock_producer_client: MagicMock
    ) -> None:
        messages = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        await producer.publish_batch(messages)
        assert mock_producer_client.send_and_wait.call_count == 3

    async def test_publish_batch_flushes_after_sends(
        self, producer: KafkaProducer, mock_producer_client: MagicMock
    ) -> None:
        await producer.publish_batch([{"id": "1"}])
        mock_producer_client.flush.assert_called_once()

    async def test_publish_batch_raises_runtime_error_if_not_started(
        self, mock_settings: KafkaSettings
    ) -> None:
        p = KafkaProducer(mock_settings)
        with pytest.raises(RuntimeError, match="not started"):
            await p.publish_batch([{"event": "test"}])


class TestStart:
    async def test_start_raises_adapter_connection_error_on_broker_failure(
        self, mock_settings: KafkaSettings
    ) -> None:
        with patch(
            "openframe.adapters.queue.kafka.producer.AIOKafkaProducer"
        ) as MockProducer:
            MockProducer.return_value.start = AsyncMock(
                side_effect=aiokafka.errors.KafkaConnectionError()
            )
            p = KafkaProducer(mock_settings)
            with pytest.raises(AdapterConnectionError):
                await p.start()


class TestClose:
    async def test_close_calls_stop_on_producer(
        self, producer: KafkaProducer, mock_producer_client: MagicMock
    ) -> None:
        await producer.close()
        mock_producer_client.stop.assert_called_once()

    async def test_close_is_idempotent(
        self, producer: KafkaProducer, mock_producer_client: MagicMock
    ) -> None:
        await producer.close()
        await producer.close()  # second call — must not raise
