"""
tests/test_producer.py — openframe-adapters-queue-nats
=========================================================
Contract tests (ProducerContractTests) + adapter-specific tests for
NatsProducer, including the connection-vs-query _wrap_nats() classification.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import nats.errors
import nats.js.errors
import pytest

from openframe.adapters.queue.nats import NatsProducer, NatsSettings
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseProducer
from openframe.core.testing import ProducerContractTests


# ── Contract tests ─────────────────────────────────────────────────────────

class TestNatsProducerContracts(ProducerContractTests):
    """
    NatsProducer passes the full openframe producer contract suite.

    All ProducerContractTests run against a NatsProducer wired to a mocked
    NATS client/JetStream context. No real NATS server required.
    """

    @pytest.fixture
    def producer(
        self, mock_settings: NatsSettings, mock_nats_client: MagicMock, mock_jetstream_context: MagicMock
    ):
        with patch(
            "openframe.adapters.queue.nats.producer.nats.connect",
            AsyncMock(return_value=mock_nats_client),
        ):
            p = NatsProducer(mock_settings)
            p._nc = mock_nats_client
            p._js = mock_jetstream_context
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
    def test_isinstance_base_producer(self, producer: NatsProducer) -> None:
        assert isinstance(producer, BaseProducer)


class TestPublish:
    async def test_publish_calls_jetstream_publish(
        self, producer: NatsProducer, mock_jetstream_context: MagicMock
    ) -> None:
        await producer.publish({"event": "test"})
        mock_jetstream_context.publish.assert_called_once()

    async def test_publish_serialises_to_json_bytes(
        self, producer: NatsProducer, mock_jetstream_context: MagicMock
    ) -> None:
        message = {"event": "item.created", "id": "abc"}
        await producer.publish(message)
        call_args = mock_jetstream_context.publish.call_args
        subject, value = call_args.args
        assert subject == producer._settings.nats_subject
        assert json.loads(value.decode("utf-8")) == message

    async def test_publish_raises_runtime_error_if_not_started(
        self, mock_settings: NatsSettings
    ) -> None:
        p = NatsProducer(mock_settings)
        with pytest.raises(RuntimeError, match="not started"):
            await p.publish({"event": "test"})

    async def test_publish_timeout_raises_adapter_timeout_error(
        self, producer: NatsProducer, mock_jetstream_context: MagicMock
    ) -> None:
        mock_jetstream_context.publish.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await producer.publish({"event": "test"})
        assert exc_info.value.operation == "publish"

    async def test_publish_driver_timeout_error_raises_adapter_timeout_error(
        self, producer: NatsProducer, mock_jetstream_context: MagicMock
    ) -> None:
        """nats.errors.TimeoutError IS-A builtins.TimeoutError (== asyncio.TimeoutError)."""
        mock_jetstream_context.publish.side_effect = nats.errors.TimeoutError()
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await producer.publish({"event": "test"})
        assert exc_info.value.operation == "publish"

    @pytest.mark.parametrize(
        "exc_factory",
        [
            lambda: nats.errors.NoServersError(),
            lambda: nats.errors.ConnectionClosedError(),
            lambda: nats.errors.ConnectionDrainingError(),
            lambda: nats.errors.ConnectionReconnectingError(),
            lambda: nats.errors.StaleConnectionError(),
            lambda: nats.errors.OutboundBufferLimitError(),
        ],
        ids=[
            "NoServersError",
            "ConnectionClosedError",
            "ConnectionDrainingError",
            "ConnectionReconnectingError",
            "StaleConnectionError",
            "OutboundBufferLimitError",
        ],
    )
    async def test_publish_connection_class_error_raises_adapter_connection_error(
        self, producer: NatsProducer, mock_jetstream_context: MagicMock, exc_factory
    ) -> None:
        """
        Regression-shape test: a connection-class nats-py exception mid-publish
        must surface as AdapterConnectionError (retryable), not AdapterQueryError
        — matching Postgres/Mongo/Redis's own connection-vs-query distinction.
        """
        mock_jetstream_context.publish.side_effect = exc_factory()
        with pytest.raises(AdapterConnectionError) as exc_info:
            await producer.publish({"event": "test"})
        assert exc_info.value.operation == "publish"
        assert exc_info.value.retryable is True

    @pytest.mark.parametrize(
        "exc_factory",
        [
            lambda: nats.errors.MaxPayloadError(),
            lambda: nats.errors.BadSubjectError(),
            lambda: nats.errors.NoRespondersError(),
            lambda: nats.js.errors.BadRequestError(description="bad"),
            lambda: nats.js.errors.ServerError(description="server error"),
        ],
        ids=[
            "MaxPayloadError",
            "BadSubjectError",
            "NoRespondersError",
            "js.BadRequestError",
            "js.ServerError",
        ],
    )
    async def test_publish_query_class_error_raises_adapter_query_error(
        self, producer: NatsProducer, mock_jetstream_context: MagicMock, exc_factory
    ) -> None:
        """
        Query/publish-class (in-band) failures must surface as AdapterQueryError
        (not retryable), never as AdapterConnectionError.
        """
        mock_jetstream_context.publish.side_effect = exc_factory()
        with pytest.raises(AdapterQueryError) as exc_info:
            await producer.publish({"event": "test"})
        assert exc_info.value.operation == "publish"


class TestPublishBatch:
    async def test_publish_batch_sends_each_message(
        self, producer: NatsProducer, mock_jetstream_context: MagicMock
    ) -> None:
        messages = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        await producer.publish_batch(messages)
        assert mock_jetstream_context.publish.call_count == 3

    async def test_publish_batch_raises_runtime_error_if_not_started(
        self, mock_settings: NatsSettings
    ) -> None:
        p = NatsProducer(mock_settings)
        with pytest.raises(RuntimeError, match="not started"):
            await p.publish_batch([{"event": "test"}])

    async def test_publish_batch_query_error_raises_adapter_query_error(
        self, producer: NatsProducer, mock_jetstream_context: MagicMock
    ) -> None:
        mock_jetstream_context.publish.side_effect = nats.errors.MaxPayloadError()
        with pytest.raises(AdapterQueryError) as exc_info:
            await producer.publish_batch([{"event": "test"}])
        assert exc_info.value.operation == "publish_batch"


class TestStart:
    async def test_start_connects_and_ensures_stream(
        self, mock_settings: NatsSettings, mock_nats_client: MagicMock, mock_jetstream_context: MagicMock
    ) -> None:
        with patch(
            "openframe.adapters.queue.nats.producer.nats.connect",
            AsyncMock(return_value=mock_nats_client),
        ):
            p = NatsProducer(mock_settings)
            await p.start()
        mock_jetstream_context.add_stream.assert_called_once()
        assert p._js is mock_jetstream_context

    async def test_start_raises_adapter_connection_error_on_no_servers(
        self, mock_settings: NatsSettings
    ) -> None:
        with patch(
            "openframe.adapters.queue.nats.producer.nats.connect",
            AsyncMock(side_effect=nats.errors.NoServersError()),
        ):
            p = NatsProducer(mock_settings)
            with pytest.raises(AdapterConnectionError):
                await p.start()

    async def test_start_raises_adapter_configuration_error_on_other_nats_error(
        self, mock_settings: NatsSettings
    ) -> None:
        with patch(
            "openframe.adapters.queue.nats.producer.nats.connect",
            AsyncMock(side_effect=nats.errors.AuthorizationError()),
        ):
            p = NatsProducer(mock_settings)
            with pytest.raises(AdapterConfigurationError):
                await p.start()

    async def test_start_raises_adapter_timeout_error(
        self, mock_settings: NatsSettings
    ) -> None:
        with patch(
            "openframe.adapters.queue.nats.producer.nats.connect",
            AsyncMock(side_effect=asyncio.TimeoutError()),
        ):
            p = NatsProducer(mock_settings)
            with pytest.raises(AdapterTimeoutError):
                await p.start()

    async def test_start_ignores_bad_request_on_stream_already_exists(
        self, mock_settings: NatsSettings, mock_nats_client: MagicMock, mock_jetstream_context: MagicMock
    ) -> None:
        mock_jetstream_context.add_stream.side_effect = nats.js.errors.BadRequestError(description="stream exists")
        with patch(
            "openframe.adapters.queue.nats.producer.nats.connect",
            AsyncMock(return_value=mock_nats_client),
        ):
            p = NatsProducer(mock_settings)
            await p.start()  # must not raise
        assert p._js is mock_jetstream_context


class TestClose:
    async def test_close_calls_close_on_client(
        self, producer: NatsProducer, mock_nats_client: MagicMock
    ) -> None:
        await producer.close()
        mock_nats_client.close.assert_called_once()

    async def test_close_is_idempotent(
        self, producer: NatsProducer, mock_nats_client: MagicMock
    ) -> None:
        await producer.close()
        await producer.close()  # second call — must not raise
