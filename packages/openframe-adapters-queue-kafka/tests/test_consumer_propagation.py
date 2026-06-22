"""
tests/test_consumer_propagation.py — openframe-adapters-queue-kafka
======================================================================
Proves KafkaConsumer.subscribe() extracts an incoming traceparent header
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

from openframe.adapters.queue.kafka import KafkaConsumer, KafkaSettings

_propagator = TraceContextTextMapPropagator()


def _inject_headers_like_producer() -> list[tuple[str, bytes]]:
    """
    Builds headers the same way KafkaProducer._inject_trace_headers() does —
    written inline here so this test doesn't depend on importing a private
    function from another module (fragile regardless of sync state between
    repos/branches). If your local KafkaProducer doesn't yet have its own
    _inject_trace_headers(), this is exactly the snippet to add there too.
    """
    carrier: dict[str, str] = {}
    _propagator.inject(carrier)
    return [(k, v.encode("utf-8")) for k, v in carrier.items()]


def _make_settings() -> KafkaSettings:
    return KafkaSettings(
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="artifact.created",
        kafka_group_id="test-group",
    )


async def test_consumer_extracts_real_producer_injected_headers(
) -> None:
    """
    Use the REAL producer's _inject_trace_headers() to build the headers,
    not a hand-built traceparent string — proves the two sides of this
    boundary actually agree on the wire format.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = trace.get_tracer("research-pipeline", tracer_provider=provider)

    # Producer side: a real span, real injection — exactly what
    # KafkaProducer.publish() does today (see _inject_headers_like_producer
    # docstring above if your local producer.py doesn't have this yet).
    with tracer.start_as_current_span("items_postgres.create_item"):
        headers = _inject_headers_like_producer()
    upstream_span = exporter.get_finished_spans()[0]

    # Consumer side: the message arrives with those exact headers.
    raw_msg = MagicMock()
    raw_msg.value = json.dumps({"event": "artifact.created"}).encode("utf-8")
    raw_msg.headers = headers

    mock_consumer_client = MagicMock()
    mock_consumer_client.start = AsyncMock()
    mock_consumer_client.stop = AsyncMock()
    mock_consumer_client.commit = AsyncMock()
    mock_consumer_client.__aiter__ = lambda self: self
    mock_consumer_client.__anext__ = AsyncMock(side_effect=[raw_msg, StopAsyncIteration])

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

    consumer = KafkaConsumer(_make_settings())
    with patch(
        "openframe.adapters.queue.kafka.consumer.AIOKafkaConsumer",
        return_value=mock_consumer_client,
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
    check: this must not break messages from producers that never call
    _inject_trace_headers()."""
    raw_msg = MagicMock()
    raw_msg.value = json.dumps({"event": "legacy"}).encode("utf-8")
    raw_msg.headers = []  # no headers at all

    mock_consumer_client = MagicMock()
    mock_consumer_client.start = AsyncMock()
    mock_consumer_client.stop = AsyncMock()
    mock_consumer_client.commit = AsyncMock()
    mock_consumer_client.__aiter__ = lambda self: self
    mock_consumer_client.__anext__ = AsyncMock(side_effect=[raw_msg, StopAsyncIteration])

    handler = AsyncMock()
    consumer = KafkaConsumer(_make_settings())
    with patch(
        "openframe.adapters.queue.kafka.consumer.AIOKafkaConsumer",
        return_value=mock_consumer_client,
    ):
        await consumer.subscribe(handler)

    handler.assert_called_once_with({"event": "legacy"})
    mock_consumer_client.commit.assert_called_once()