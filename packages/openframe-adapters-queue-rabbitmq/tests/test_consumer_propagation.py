"""
tests/test_consumer_propagation.py — openframe-adapters-queue-rabbitmq
===========================================================================
Proves RabbitmqConsumer.subscribe() extracts an incoming traceparent header
and makes it the active context for the handler — so any span the handler
creates (the realistic case: TracingProxy wrapping a downstream repository
call) becomes a CHILD of the producer's trace, not a disconnected root.

Unlike Kafka's ``list[tuple[str, bytes]]`` header shape, AMQP message
headers are a plain field table (``dict[str, FieldValue]``), so this test
also exercises the "no byte-decoding needed for our own producer's string
values" path in the consumer, alongside the defensive bytes-decode branch
for interop with non-Python producers that might send bytes.
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from openframe.adapters.queue.rabbitmq import RabbitmqConsumer, RabbitmqSettings

from conftest import _make_mock_queue_iterator

_propagator = TraceContextTextMapPropagator()


def _inject_headers_like_producer() -> dict[str, str]:
    """
    Builds headers the same way RabbitmqProducer._inject_trace_headers()
    does — written inline here so this test doesn't depend on importing a
    private function from another module (fragile regardless of sync state
    between repos/branches). If your local RabbitmqProducer doesn't yet
    have its own _inject_trace_headers(), this is exactly the snippet to
    add there too.
    """
    carrier: dict[str, str] = {}
    _propagator.inject(carrier)
    return carrier


def _make_settings() -> RabbitmqSettings:
    return RabbitmqSettings(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        rabbitmq_queue="artifact.created",
    )


def _mock_connection_stack(raw_msg):
    """Wire up a mocked connection/channel/queue delivering ``raw_msg`` once."""
    exporter_channel = MagicMock()
    exporter_channel.set_qos = AsyncMock()

    mock_queue = MagicMock()
    mock_queue.iterator = MagicMock(return_value=_make_mock_queue_iterator([raw_msg]))

    exporter_channel.declare_queue = AsyncMock(return_value=mock_queue)
    exporter_channel.close = AsyncMock()

    mock_connection = MagicMock()
    mock_connection.channel = AsyncMock(return_value=exporter_channel)
    mock_connection.close = AsyncMock()
    return mock_connection


async def test_consumer_extracts_real_producer_injected_headers() -> None:
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
    # RabbitmqProducer.publish() does today (see _inject_headers_like_producer
    # docstring above if your local producer.py doesn't have this yet).
    with tracer.start_as_current_span("items_postgres.create_item"):
        headers = _inject_headers_like_producer()
    upstream_span = exporter.get_finished_spans()[0]

    # Consumer side: the message arrives with those exact headers.
    raw_msg = MagicMock()
    raw_msg.body = json.dumps({"event": "artifact.created"}).encode("utf-8")
    raw_msg.headers = headers
    raw_msg.ack = AsyncMock()
    raw_msg.nack = AsyncMock()

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

    consumer = RabbitmqConsumer(_make_settings())
    mock_connection = _mock_connection_stack(raw_msg)
    from unittest.mock import patch

    with patch(
        "openframe.adapters.queue.rabbitmq.consumer.aio_pika.connect_robust",
        AsyncMock(return_value=mock_connection),
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
    raw_msg.ack.assert_called_once()


async def test_consumer_with_no_headers_behaves_unchanged() -> None:
    """No traceparent present (e.g. a non-instrumented producer) — handler
    still runs normally, with no special parent context. Pure regression
    check: this must not break messages from producers that never call
    _inject_trace_headers()."""
    raw_msg = MagicMock()
    raw_msg.body = json.dumps({"event": "legacy"}).encode("utf-8")
    raw_msg.headers = {}  # no headers at all
    raw_msg.ack = AsyncMock()
    raw_msg.nack = AsyncMock()

    handler = AsyncMock()
    consumer = RabbitmqConsumer(_make_settings())
    mock_connection = _mock_connection_stack(raw_msg)
    from unittest.mock import patch

    with patch(
        "openframe.adapters.queue.rabbitmq.consumer.aio_pika.connect_robust",
        AsyncMock(return_value=mock_connection),
    ):
        await consumer.subscribe(handler)

    handler.assert_called_once_with({"event": "legacy"})
    raw_msg.ack.assert_called_once()


async def test_consumer_decodes_byte_valued_headers() -> None:
    """
    Interop guard: a non-Python producer (or a broker plugin) may send
    header values as raw AMQP long-strings that arrive as ``bytes`` rather
    than ``str``. The consumer must defensively decode these before handing
    them to the W3C propagator, which expects ``str`` values only.
    """
    carrier = _inject_headers_like_producer()
    byte_headers = {k: v.encode("utf-8") for k, v in carrier.items()}

    raw_msg = MagicMock()
    raw_msg.body = json.dumps({"event": "bytes-headers"}).encode("utf-8")
    raw_msg.headers = byte_headers
    raw_msg.ack = AsyncMock()
    raw_msg.nack = AsyncMock()

    handler = AsyncMock()
    consumer = RabbitmqConsumer(_make_settings())
    mock_connection = _mock_connection_stack(raw_msg)
    from unittest.mock import patch

    with patch(
        "openframe.adapters.queue.rabbitmq.consumer.aio_pika.connect_robust",
        AsyncMock(return_value=mock_connection),
    ):
        await consumer.subscribe(handler)  # must not raise on bytes header values

    handler.assert_called_once_with({"event": "bytes-headers"})
