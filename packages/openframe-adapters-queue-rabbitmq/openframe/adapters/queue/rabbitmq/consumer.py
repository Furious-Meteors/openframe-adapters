"""
openframe/adapters/queue/rabbitmq/consumer.py
================================================
RabbitMQ message consumer implementing ``BaseConsumer[T]`` from
``openframe-core`` via structural subtyping.

Each ``subscribe()`` call opens a new robust connection/channel, declares
the configured queue, and pulls messages via ``queue.iterator()`` — an
async-iterator wrapper over aio_pika's push-based ``queue.consume()`` API
that mirrors ``KafkaConsumer``'s ``async for msg in consumer`` shape
closely enough to share the same test double style. Iteration continues
until ``close()`` is called.

Acknowledgement is native AMQP: each ``IncomingMessage`` supports
``await message.ack()`` / ``await message.nack(requeue=True)`` directly —
no manual offset/seek-back bookkeeping is needed (unlike Kafka's manual
commit dance). The adapter still calls through its own ``ack()``/``nack()``
methods to satisfy ``BaseConsumer``'s protocol shape; internally they
operate on the most recently received raw ``IncomingMessage`` (mirroring
``KafkaConsumer.ack()``/``.nack()``, which likewise ignore the ``message``
argument and act on the adapter's own internal cursor/reference rather
than requiring the caller to pass the raw driver object back in).

Error handling:
    Connection/channel/queue-declare failures are translated via
    ``_wrap_rabbitmq()`` — see that function's docstring for the exact
    connection-vs-query classification.
    Handler exceptions are caught, logged, and the message is nacked with
    ``requeue=True`` (redelivered) — the loop continues to the next message.

Cross-service trace correlation:
    Each message's headers are checked for a W3C ``traceparent`` (injected by
    the producing side — see ``RabbitmqProducer._inject_trace_headers``). If
    present, the ``handler`` invocation runs inside that extracted parent
    context, so any spans the handler creates (e.g. via ``TracingProxy``
    wrapping a downstream repository) continue the producer's trace instead
    of starting a disconnected one. If no traceparent is present, the
    handler runs with no special context — unchanged behaviour.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable, Generic, TypeVar

import aio_pika
import aio_pika.exceptions

from opentelemetry import context as otel_context

from openframe.core.exceptions import AdapterConnectionError, AdapterError, AdapterQueryError
from openframe.core.ports import BaseConsumer, Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.tracing.propagation import extract as _extract_propagation

from .config import RabbitmqSettings

__all__ = ["RabbitmqConsumer"]

T = TypeVar("T")
_logger = logging.getLogger(__name__)

# See producer.py's identically-named constant for why both the AMQP-specific
# connection error and the raw builtin socket errors are listed explicitly.
_CONNECTION_ERRORS: tuple[type[BaseException], ...] = (
    aio_pika.exceptions.AMQPConnectionError,
    ConnectionError,
    OSError,
)


class RabbitmqConsumer(Generic[T]):
    """
    RabbitMQ message consumer.

    Implements ``BaseConsumer[T]`` structurally — no inheritance from Protocol.
    Deserialises JSON messages. Uses AMQP's native per-message ack/nack —
    no manual offset commit bookkeeping needed.

    Each ``subscribe()`` call opens a new connection/channel, declares the
    configured queue, and iterates messages until ``close()`` is called.
    Handler failures trigger ``nack(requeue=True)`` — the message is
    redelivered by the broker.

    Usage::

        consumer = RabbitmqConsumer(settings)

        async def handle(event: dict) -> None:
            print(f"Received: {event}")

        await consumer.subscribe(handle)   # runs until close() called

    Structural conformance::

        assert isinstance(consumer, BaseConsumer)
    """

    name:       str = "openframe-rabbitmq-consumer"
    version:    str = "0.1.0"
    capability: Capability = Capability.QUEUE

    def __init__(self, settings: RabbitmqSettings) -> None:
        self._settings = settings
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._queue: aio_pika.abc.AbstractQueue | None = None
        self._running: bool = False
        self._initialized: bool = False
        self._current_raw_message: aio_pika.abc.AbstractIncomingMessage | None = None

    # ------------------------------------------------------------------
    # Deserialisation (override in typed subclasses)
    # ------------------------------------------------------------------

    def _deserialise(self, raw: bytes) -> T:
        """
        Deserialise bytes from RabbitMQ to the message type ``T``.

        Base implementation: ``json.loads(raw.decode("utf-8"))``.
        Subclasses override for custom deserialisation (protobuf, Avro, etc.).
        """
        return json.loads(raw.decode("utf-8"))  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Error translation
    # ------------------------------------------------------------------

    def _wrap_rabbitmq(self, exc: Exception, operation: str) -> AdapterError:
        """
        Map a driver exception to the appropriate ``AdapterError`` subclass.

        Identical classification to ``RabbitmqProducer._wrap_rabbitmq()`` —
        connection-class failures (lost/unreachable broker, retryable) vs.
        query-class failures (channel/queue-declare error, not retryable).
        Caller must ``raise ... from exc`` at the call site.
        """
        if isinstance(exc, _CONNECTION_ERRORS):
            return AdapterConnectionError(
                f"{operation} failed — connection to RabbitMQ was lost: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        return AdapterQueryError(
            f"{operation} on queue {self._settings.rabbitmq_queue!r} failed: {exc}",
            adapter=self._settings.adapter_name,
            operation=operation,
            cause=exc,
        )

    # ------------------------------------------------------------------
    # BaseConsumer[T] interface
    # ------------------------------------------------------------------

    async def subscribe(
        self,
        handler: Callable[[T], Awaitable[None]],
    ) -> None:
        """
        Start consuming messages and invoke ``handler`` for each one.

        Runs until ``close()`` is called. Acknowledges via native AMQP
        ack/nack — the message is acked only after ``handler`` returns
        successfully. If ``handler`` raises, the message is nacked with
        ``requeue=True`` and the loop continues.

        Args:
            handler: Async callable that processes each deserialised message.

        Raises:
            AdapterConnectionError: Cannot reach the RabbitMQ broker.
            AdapterQueryError:      Channel/queue-declare error.
        """
        try:
            async with asyncio.timeout(self._settings.connection_timeout):
                self._connection = await aio_pika.connect_robust(
                    self._settings.rabbitmq_url,
                    reconnect_interval=self._settings.rabbitmq_reconnect_interval,
                )
                self._channel = await self._connection.channel()
                await self._channel.set_qos(
                    prefetch_count=self._settings.rabbitmq_prefetch_count
                )
                self._queue = await self._channel.declare_queue(
                    self._settings.rabbitmq_queue,
                    durable=self._settings.rabbitmq_durable,
                )
        except (aio_pika.exceptions.AMQPError, ConnectionError, OSError) as exc:
            self._connection = None
            self._channel = None
            self._queue = None
            raise self._wrap_rabbitmq(exc, "subscribe") from exc

        self._running = True
        try:
            queue_iter = self._queue.iterator()
            async with queue_iter:
                async for message in queue_iter:
                    if not self._running:
                        break
                    self._current_raw_message = message
                    payload: T | None = None
                    try:
                        payload = self._deserialise(message.body)
                        carrier = {
                            k: (v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v)
                            for k, v in (message.headers or {}).items()
                        }
                        parent_ctx = _extract_propagation(carrier)
                        token = otel_context.attach(parent_ctx)
                        try:
                            await handler(payload)
                        finally:
                            otel_context.detach(token)
                        await self.ack(payload)
                    except Exception as exc:  # noqa: BLE001
                        _logger.error(
                            "RabbitmqConsumer handler failed for queue %r: %s",
                            self._settings.rabbitmq_queue,
                            exc,
                        )
                        await self.nack(payload)
        finally:
            await self._close_connection()
            self._running = False

    async def ack(self, message: T) -> None:
        """
        Acknowledge a message, telling the broker it was processed.

        Operates on the most recently received raw ``IncomingMessage``
        (see module docstring). Called automatically after successful
        handler invocation. Never raises.

        Args:
            message: The message that was successfully processed
                     (unused directly — see module docstring).
        """
        if self._current_raw_message is not None:
            try:
                await self._current_raw_message.ack()
            except Exception as exc:  # noqa: BLE001
                _logger.error("RabbitmqConsumer ack error (ignored): %s", exc)

    async def nack(self, message: T) -> None:
        """
        Negatively acknowledge a message — it will be requeued for redelivery.

        Operates on the most recently received raw ``IncomingMessage``
        (see module docstring). Never raises.

        Args:
            message: The message that failed processing
                     (unused directly — see module docstring).
        """
        if self._current_raw_message is not None:
            try:
                await self._current_raw_message.nack(requeue=True)
            except Exception as exc:  # noqa: BLE001
                _logger.error("RabbitmqConsumer nack error (ignored): %s", exc)

    async def close(self) -> None:
        """
        Stop the consumer.

        Sets ``_running = False`` to break the iteration loop and closes
        the channel/connection. Never raises.
        """
        self._running = False
        await self._close_connection()

    async def _close_connection(self) -> None:
        """Close channel then connection, in order. Never raises."""
        if self._channel is not None:
            try:
                await self._channel.close()
            except Exception as exc:  # noqa: BLE001
                _logger.error("RabbitmqConsumer channel close error (ignored): %s", exc)
            finally:
                self._channel = None
        if self._connection is not None:
            try:
                await self._connection.close()
            except Exception as exc:  # noqa: BLE001
                _logger.error("RabbitmqConsumer connection close error (ignored): %s", exc)
            finally:
                self._connection = None
        self._queue = None

    # ------------------------------------------------------------------
    # BasePort (Identity + Lifecycle) interface
    # ------------------------------------------------------------------

    async def initialize(self, context: PluginContext) -> None:
        """
        BasePort lifecycle entry point.

        Each ``subscribe()`` call opens and closes its own connection
        (consumers are short-lived per session), so there is no persistent
        connection to establish here — this simply marks the consumer as
        ready for use.
        """
        self._initialized = True

    async def shutdown(self) -> None:
        """BasePort lifecycle entry point — alias for close(). Never raises."""
        self._initialized = False
        await self.close()

    async def health(self) -> PluginHealth:
        """
        BasePort lifecycle entry point — returns a PluginHealth snapshot.

        Never raises.
        """
        try:
            if self._initialized:
                return PluginHealth(status=PluginStatus.READY, message="")
            return PluginHealth(status=PluginStatus.FAILED, message="not initialized")
        except Exception as exc:  # noqa: BLE001
            return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
