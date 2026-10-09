"""
openframe/adapters/queue/rabbitmq/producer.py
================================================
RabbitMQ message producer implementing ``BaseProducer[T]`` from
``openframe-core`` via structural subtyping.

Messages are serialised to JSON bytes and published to the default
(nameless, direct) exchange with the configured queue as the routing key —
the simplest AMQP pattern that delivers straight to a named queue with no
exchange/binding setup required. The underlying ``aio_pika`` connection
must be explicitly started via ``await producer.start()`` before calling
``publish()`` or ``publish_batch()``.

Connection strategy:
    ``aio_pika.connect_robust()`` is used — a fully async-native,
    auto-reconnecting connection. No executor-wrapping needed (unlike
    threaded drivers): every call in this module is a plain ``await``.

Error handling:
    Every driver exception is translated via ``_wrap_rabbitmq()``, which
    distinguishes connection-class failures (broken/lost connection,
    retryable) from publish-class failures (non-retryable) — see that
    function's docstring for the exact classification.

Timeout strategy:
    ``asyncio.timeout(settings.operation_timeout)`` wraps every publish;
    ``asyncio.timeout(settings.connection_timeout)`` wraps connection setup.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Generic, TypeVar

import aio_pika
import aio_pika.exceptions

from openframe.core.ports import Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import (
    AdapterConnectionError,
    AdapterError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseProducer
from openframe.core.tracing.propagation import inject as _inject_propagation

from .config import RabbitmqSettings

__all__ = ["RabbitmqProducer"]

T = TypeVar("T")
_logger = logging.getLogger(__name__)

# Connection-class aio_pika/aiormq exceptions — a broken/lost/unreachable
# connection. ``aio_pika.exceptions.AMQPConnectionError`` is itself a
# subclass of the builtin ``ConnectionError`` (see aiormq.exceptions), but
# ``ConnectionError``/``OSError`` are listed explicitly too: aio_pika's own
# ``connect_robust()`` can raise a raw socket-level ``OSError`` (e.g.
# "Connect call failed") before it has been wrapped into an AMQP-specific
# exception type — verified against the installed aio-pika/aiormq source
# (``aiormq.exceptions.CONNECTION_EXCEPTIONS`` includes ``OSError`` for
# exactly this reason). Do not assume ``AMQPConnectionError`` alone covers
# every connection-class failure.
_CONNECTION_ERRORS: tuple[type[BaseException], ...] = (
    aio_pika.exceptions.AMQPConnectionError,
    ConnectionError,
    OSError,
)


def _inject_trace_headers() -> dict[str, str]:
    """
    Build AMQP message headers carrying the active W3C traceparent.

    Mirrors ``RabbitmqConsumer``'s extraction on the other side of this
    boundary. Unlike Kafka's ``list[tuple[str, bytes]]`` header shape,
    ``aio_pika.Message(headers=...)`` accepts a plain ``dict[str, str]``
    AMQP field table directly — no byte-encoding needed. A no-op (empty
    dict) when there is no active span or the SDK has not been initialised.
    """
    carrier: dict[str, str] = {}
    _inject_propagation(carrier)
    return carrier


class RabbitmqProducer(Generic[T]):
    """
    RabbitMQ message producer.

    Implements ``BaseProducer[T]`` structurally — no inheritance from Protocol.
    Serialises messages to JSON bytes before publishing to RabbitMQ's default
    exchange, routed to the configured queue by routing key.

    The underlying ``aio_pika`` connection/channel must be started before use.
    Call ``await producer.start()`` or use ``RabbitmqPlugin`` for managed
    lifecycle.

    Usage::

        producer = RabbitmqProducer(settings)
        await producer.start()
        await producer.publish({"event": "item.created", "id": "abc"})
        await producer.close()

    Structural conformance::

        assert isinstance(producer, BaseProducer)
    """

    name:       str = "openframe-rabbitmq-producer"
    version:    str = "0.1.0"
    capability: Capability = Capability.QUEUE

    def __init__(self, settings: RabbitmqSettings) -> None:
        self._settings = settings
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None

    # ------------------------------------------------------------------
    # Serialisation (override in typed subclasses)
    # ------------------------------------------------------------------

    def _serialise(self, message: T) -> bytes:
        """
        Serialise a message to bytes for RabbitMQ.

        Base implementation: ``json.dumps(message).encode("utf-8")``.
        Subclasses override for custom serialisation (protobuf, Avro, etc.).
        """
        return json.dumps(message).encode("utf-8")

    # ------------------------------------------------------------------
    # Error translation
    # ------------------------------------------------------------------

    def _wrap_rabbitmq(self, exc: Exception, operation: str) -> AdapterError:
        """
        Map a driver exception to the appropriate ``AdapterError`` subclass.

        Distinguishes a lost/broken/unreachable connection
        (``AdapterConnectionError`` — retryable) from an in-band publish
        failure such as a channel-level protocol error or an undeliverable
        message (``AdapterQueryError`` — not retryable by default), the
        same distinction ``PostgresRepository._wrap_asyncpg()`` and
        ``MongoRepository._wrap_pymongo()`` make for their own drivers.

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
            f"{operation} to queue {self._settings.rabbitmq_queue!r} failed: {exc}",
            adapter=self._settings.adapter_name,
            operation=operation,
            cause=exc,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        Open the underlying robust connection and channel.

        Must be called before ``publish()`` or ``publish_batch()``.

        Raises:
            AdapterConnectionError: Broker is unreachable, or lost mid-setup.
            AdapterTimeoutError:    Start exceeded ``connection_timeout``.
        """
        try:
            async with asyncio.timeout(self._settings.connection_timeout):
                self._connection = await aio_pika.connect_robust(
                    self._settings.rabbitmq_url,
                    reconnect_interval=self._settings.rabbitmq_reconnect_interval,
                )
                self._channel = await self._connection.channel()
        except asyncio.TimeoutError as exc:
            self._connection = None
            self._channel = None
            raise AdapterTimeoutError(
                f"RabbitMQ producer start timed out after {self._settings.connection_timeout}s",
                adapter=self._settings.adapter_name,
                operation="start",
                cause=exc,
            ) from exc
        except (aio_pika.exceptions.AMQPError, ConnectionError, OSError) as exc:
            self._connection = None
            self._channel = None
            raise self._wrap_rabbitmq(exc, "start") from exc

    async def close(self) -> None:
        """
        Close the channel and connection, releasing resources.

        Idempotent — safe to call multiple times. Never raises.
        """
        if self._channel is not None:
            try:
                await self._channel.close()
            except Exception as exc:  # noqa: BLE001
                _logger.error("RabbitmqProducer channel close error (ignored): %s", exc)
            finally:
                self._channel = None
        if self._connection is not None:
            try:
                await self._connection.close()
            except Exception as exc:  # noqa: BLE001
                _logger.error("RabbitmqProducer connection close error (ignored): %s", exc)
            finally:
                self._connection = None

    # ------------------------------------------------------------------
    # BasePort (Identity + Lifecycle) interface
    # ------------------------------------------------------------------

    async def initialize(self, context: PluginContext) -> None:
        """BasePort lifecycle entry point — alias for start()."""
        await self.start()

    async def shutdown(self) -> None:
        """BasePort lifecycle entry point — alias for close(). Never raises."""
        await self.close()

    async def health(self) -> PluginHealth:
        """
        BasePort lifecycle entry point — returns a PluginHealth snapshot.

        READY when the underlying connection is open, FAILED otherwise.
        Never raises.
        """
        try:
            if self._connection is not None and not self._connection.is_closed:
                return PluginHealth(status=PluginStatus.READY, message="")
            return PluginHealth(status=PluginStatus.FAILED, message="producer not started")
        except Exception as exc:  # noqa: BLE001
            return PluginHealth(status=PluginStatus.FAILED, message=str(exc))

    # ------------------------------------------------------------------
    # BaseProducer[T] interface
    # ------------------------------------------------------------------

    async def publish(self, message: T) -> None:
        """
        Publish a single message to the configured queue.

        Serialises the message to JSON bytes and publishes it to the
        default exchange, routed by the queue name.

        Args:
            message: The message to publish.

        Raises:
            RuntimeError:           Producer not started — call ``start()`` first.
            AdapterConnectionError: Connection was lost mid-publish.
            AdapterQueryError:      Publish failed (channel/broker error).
            AdapterTimeoutError:    Publish exceeded ``operation_timeout``.
        """
        if self._channel is None:
            raise RuntimeError(
                "RabbitmqProducer not started. Call await producer.start() first."
            )
        body = self._serialise(message)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await self._channel.default_exchange.publish(
                    aio_pika.Message(body=body, headers=_inject_trace_headers()),
                    routing_key=self._settings.rabbitmq_queue,
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"publish exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="publish",
                cause=exc,
            ) from exc
        except (aio_pika.exceptions.AMQPError, ConnectionError, OSError) as exc:
            raise self._wrap_rabbitmq(exc, "publish") from exc

    async def publish_batch(self, messages: list[T]) -> None:
        """
        Publish multiple messages to the configured queue.

        Publishes each message individually. Raises on the first failure —
        messages already published are not rolled back.

        Args:
            messages: List of messages to publish.

        Raises:
            RuntimeError:           Producer not started.
            AdapterConnectionError: Connection was lost mid-publish.
            AdapterQueryError:      Batch publish failed.
            AdapterTimeoutError:    Exceeded ``operation_timeout``.
        """
        if self._channel is None:
            raise RuntimeError(
                "RabbitmqProducer not started. Call await producer.start() first."
            )
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                for message in messages:
                    body = self._serialise(message)
                    await self._channel.default_exchange.publish(
                        aio_pika.Message(body=body, headers=_inject_trace_headers()),
                        routing_key=self._settings.rabbitmq_queue,
                    )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"publish_batch exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="publish_batch",
                cause=exc,
            ) from exc
        except (aio_pika.exceptions.AMQPError, ConnectionError, OSError) as exc:
            raise self._wrap_rabbitmq(exc, "publish_batch") from exc
