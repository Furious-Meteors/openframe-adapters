"""
tests/test_consumer.py — openframe-adapters-queue-kafka
=========================================================
Contract tests (ConsumerContractTests) + adapter-specific tests for
KafkaConsumer.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import aiokafka.errors
import pytest

from openframe.adapters.queue.kafka import KafkaConsumer, KafkaSettings
from openframe.core.exceptions import AdapterConnectionError
from openframe.core.ports import BaseConsumer
from openframe.core.testing import ConsumerContractTests


# ── Contract tests ─────────────────────────────────────────────────────────

class TestKafkaConsumerContracts(ConsumerContractTests):
    """
    KafkaConsumer passes the full openframe consumer contract suite.

    All 5 ConsumerContractTests run against a KafkaConsumer wired to a
    mocked AIOKafkaConsumer that delivers one message then stops.
    No real Kafka broker required.
    """

    @pytest.fixture
    def consumer(self, mock_settings: KafkaSettings, mock_consumer_client: MagicMock):
        import json
        # Deliver one message then stop — satisfies all three stateful
        # contract tests (deliver, ack-on-success, nack-on-failure).
        raw_msg = MagicMock()
        raw_msg.value = json.dumps(
            {"content": "test message", "type": "event"}
        ).encode("utf-8")
        mock_consumer_client.__anext__ = AsyncMock(
            side_effect=[raw_msg, StopAsyncIteration]
        )
        with patch(
            "openframe.adapters.queue.kafka.consumer.AIOKafkaConsumer",
            return_value=mock_consumer_client,
        ):
            yield KafkaConsumer(mock_settings)

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
    def test_isinstance_base_consumer(self, consumer: KafkaConsumer) -> None:
        assert isinstance(consumer, BaseConsumer)


class TestSubscribe:
    async def test_subscribe_starts_consumer_client(
        self,
        consumer: KafkaConsumer,
        mock_consumer_client: MagicMock,
    ) -> None:
        """subscribe() must start the AIOKafkaConsumer."""
        with patch(
            "openframe.adapters.queue.kafka.consumer.AIOKafkaConsumer",
            return_value=mock_consumer_client,
        ):
            await consumer.subscribe(AsyncMock())
        mock_consumer_client.start.assert_called_once()

    async def test_subscribe_calls_handler_with_deserialised_message(
        self,
        consumer: KafkaConsumer,
        mock_consumer_client: MagicMock,
    ) -> None:
        """Handler receives the deserialised dict, not raw bytes."""
        import json

        raw_msg = MagicMock()
        raw_msg.value = json.dumps({"event": "item.created"}).encode("utf-8")

        mock_consumer_client.__anext__ = AsyncMock(
            side_effect=[raw_msg, StopAsyncIteration]
        )

        handler = AsyncMock()
        with patch(
            "openframe.adapters.queue.kafka.consumer.AIOKafkaConsumer",
            return_value=mock_consumer_client,
        ):
            await consumer.subscribe(handler)

        handler.assert_called_once_with({"event": "item.created"})

    async def test_subscribe_commits_after_successful_handler(
        self,
        consumer: KafkaConsumer,
        mock_consumer_client: MagicMock,
    ) -> None:
        """Offset must be committed after a handler returns without raising."""
        import json

        raw_msg = MagicMock()
        raw_msg.value = json.dumps({"event": "ok"}).encode("utf-8")
        mock_consumer_client.__anext__ = AsyncMock(
            side_effect=[raw_msg, StopAsyncIteration]
        )

        with patch(
            "openframe.adapters.queue.kafka.consumer.AIOKafkaConsumer",
            return_value=mock_consumer_client,
        ):
            await consumer.subscribe(AsyncMock())

        mock_consumer_client.commit.assert_called_once()

    async def test_subscribe_does_not_commit_after_handler_failure(
        self,
        consumer: KafkaConsumer,
        mock_consumer_client: MagicMock,
    ) -> None:
        """If handler raises, offset must NOT be committed."""
        import json

        raw_msg = MagicMock()
        raw_msg.value = json.dumps({"event": "bad"}).encode("utf-8")
        mock_consumer_client.__anext__ = AsyncMock(
            side_effect=[raw_msg, StopAsyncIteration]
        )

        failing_handler = AsyncMock(side_effect=ValueError("bad message"))
        with patch(
            "openframe.adapters.queue.kafka.consumer.AIOKafkaConsumer",
            return_value=mock_consumer_client,
        ):
            await consumer.subscribe(failing_handler)

        mock_consumer_client.commit.assert_not_called()

    async def test_subscribe_raises_adapter_connection_error_on_broker_failure(
        self,
        consumer: KafkaConsumer,
    ) -> None:
        with patch(
            "openframe.adapters.queue.kafka.consumer.AIOKafkaConsumer"
        ) as MockConsumer:
            MockConsumer.return_value.start = AsyncMock(
                side_effect=aiokafka.errors.KafkaConnectionError()
            )
            with pytest.raises(AdapterConnectionError):
                await consumer.subscribe(AsyncMock())


class TestNack:
    async def test_nack_does_not_commit(
        self, consumer: KafkaConsumer, mock_consumer_client: MagicMock
    ) -> None:
        """nack() must never call commit — just log and continue."""
        consumer._consumer = mock_consumer_client
        await consumer.nack({"event": "test"})
        mock_consumer_client.commit.assert_not_called()


class TestClose:
    async def test_close_sets_running_false(self, consumer: KafkaConsumer) -> None:
        consumer._running = True
        await consumer.close()
        assert consumer._running is False

    async def test_close_never_raises_when_consumer_is_none(
        self, consumer: KafkaConsumer
    ) -> None:
        consumer._consumer = None
        await consumer.close()  # must not raise
