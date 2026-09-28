"""
tests/test_consumer_propagation.py — openframe-adapters-queue-nats
======================================================================
Proves NatsConsumer.subscribe() extracts an incoming traceparent header
and makes it the active context for the handler — so any span the handler
creates (the realistic case: TracingProxy wrapping a downstream repository
call) becomes a CHILD of the producer's trace, not a disconnected root.
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from openframe.adapters.queue.nats import NatsConsumer, NatsSettings

_propagator = TraceContextTextMapPropagator()


def _inject_headers_like_producer() -> dict[str, str]:
    """
    Builds headers the same way NatsProducer._inject_trace_headers() does —
    written inline here so this test doesn't depend on importing a private
    function from another module. Unlike Kafka's list[tuple[str, bytes]]
    header shape, nats-py headers are a plain dict[str, str].
    """
    carrier: dict[str, str] = {}
    _propagator.inject(carrier)
    return carrier


def _make_settings() -> NatsSettings:
    return NatsSettings(
        nats_servers="nats://localhost:4222",
        nats_subject="artifact.created",
        nats_durable_name="test-durable",
    )


def _mock_nats_client(mock_subscription: MagicMock) -> MagicMock:
    js = MagicMock()
    js.add_stream = AsyncMock()
    js.subscribe = AsyncMock(return_value=mock_subscription)
    nc = MagicMock()
    nc.close = AsyncMock()
    nc.jetstream = MagicMock(return_value=js)
    return nc


async def test_consumer_extracts_real_producer_injected_headers() -> None:
    """
    Use the REAL producer's header-injection shape to build the headers,
    not a hand-built traceparent string — proves the two sides of this
    boundary actually agree on the wire format.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = trace.get_tracer("research-pipeline", tracer_provider=provider)

    # Producer side: a real span, real injection — exactly what
    # NatsProducer.publish() does today.
    with tracer.start_as_current_span("items_postgres.create_item"):
        headers = _inject_headers_like_producer()
    upstream_span = exporter.get_finished_spans()[0]

    # Consumer side: the message arrives with those exact headers.
    raw_msg = MagicMock()
    raw_msg.data = json.dumps({"event": "artifact.created"}).encode("utf-8")
    raw_msg.headers = headers
    raw_msg.ack = AsyncMock()
    raw_msg.nak = AsyncMock()

    messages = MagicMock()
    messages.__aiter__ = MagicMock(return_value=messages)
    messages.__anext__ = AsyncMock(side_effect=[raw_msg, StopAsyncIteration])
    mock_subscription = MagicMock()
    mock_subscription.messages = messages
    mock_subscription.unsubscribe = AsyncMock()

    mock_nc = _mock_nats_client(mock_subscription)

    downstream_exporter = InMemorySpanExporter()
    downstream_provider = TracerProvider()
    downstream_provider.add_span_processor(SimpleSpanProcessor(downstream_exporter))
    downstream_tracer = trace.get_tracer(
        "research-pipeline-consumer", tracer_provider=downstream_provider
    )

    async def handler(message: dict) -> None:
        # This is the realistic case: the handler's own work is traced
        # (e.g. via TracingProxy wrapping a repository call). It must
        # land under the extracted parent context automatically.
        with downstream_tracer.start_as_current_span("repository.artifact.create"):
            pass

    consumer = NatsConsumer(_make_settings())
    with patch(
        "openframe.adapters.queue.nats.consumer.nats.connect",
        AsyncMock(return_value=mock_nc),
    ):
        await consumer.subscribe(handler)

    handler_span = downstream_exporter.get_finished_spans()[0]
    assert handler_span.context.trace_id == upstream_span.context.trace_id, (
        "Handler's span did not continue the producer's trace — "
        "extraction did not actually attach the parent context."
    )
    assert format(handler_span.parent.span_id, "016x") == format(
        upstream_span.context.span_id, "016x"
    )


async def test_consumer_with_no_headers_behaves_unchanged() -> None:
    """No traceparent present (e.g. a non-instrumented producer) — handler
    still runs normally, with no special parent context. Pure regression
    check: this must not break messages from producers that never inject
    trace headers."""
    raw_msg = MagicMock()
    raw_msg.data = json.dumps({"event": "legacy"}).encode("utf-8")
    raw_msg.headers = None  # no headers at all
    raw_msg.ack = AsyncMock()
    raw_msg.nak = AsyncMock()

    messages = MagicMock()
    messages.__aiter__ = MagicMock(return_value=messages)
    messages.__anext__ = AsyncMock(side_effect=[raw_msg, StopAsyncIteration])
    mock_subscription = MagicMock()
    mock_subscription.messages = messages
    mock_subscription.unsubscribe = AsyncMock()

    mock_nc = _mock_nats_client(mock_subscription)

    handler = AsyncMock()
    consumer = NatsConsumer(_make_settings())
    with patch(
        "openframe.adapters.queue.nats.consumer.nats.connect",
        AsyncMock(return_value=mock_nc),
    ):
        await consumer.subscribe(handler)

    handler.assert_called_once_with({"event": "legacy"})
    raw_msg.ack.assert_called_once()
