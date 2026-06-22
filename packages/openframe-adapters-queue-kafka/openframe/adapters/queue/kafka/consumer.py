"""
openframe/adapters/queue/kafka/consumer.py
============================================
Kafka message consumer implementing ``BaseConsumer[T]`` from
``openframe-core`` via structural subtyping.

Each ``subscribe()`` call creates, starts, and runs a new
``AIOKafkaConsumer`` until ``close()`` is called. Manual offset commit
is used — the offset is committed only after the handler returns
successfully. If the handler raises, ``nack()`` is called (no commit,
message will be redelivered on the next poll).

Error handling:
    ``KafkaConnectionError`` on startup → ``AdapterConnectionError``.
    ``KafkaError`` on startup → ``AdapterConfigurationError``.
    Handler exceptions are caught, logged, and the loop continues.

Cross-service trace correlation:
    Each message's headers are checked for a W3C ``traceparent`` (injected by
    the producing side — see ``KafkaProducer._inject_trace_headers``). If
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
from typing import Any, Awaitable, Callable, Generic, TypeVar

import aiokafka
import aiokafka.errors
from aiokafka import AIOKafkaConsumer

from opentelemetry import context as otel_context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
)

from .config import KafkaSettings

__all__ = ["KafkaConsumer"]

T = TypeVar("T")
_logger = logging.getLogger(__name__)
_propagator = TraceContextTextMapPropagator()


def _extract_trace_context(headers: list[tuple[str, bytes]]) -> otel_context.Context:
    """
    Extract a W3C traceparent (if present) from a Kafka message's headers.

    Mirrors ``KafkaProducer._inject_trace_headers`` on the other side of
    this boundary. ``aiokafka`` message headers are ``list[tuple[str, bytes]]``
    rather than a dict, so they're decoded into the str->str carrier shape
    the propagator expects. If no traceparent header is present, ``extract``
    returns the current (empty) context unchanged — safe no-op, identical
    to today's behaviour.
    """
    carrier = {k: v.decode("utf-8") for k, v in (headers or [])}
    return _propagator.extract(carrier)


class KafkaConsumer(Generic[T]):
    """
    Kafka message consumer.

    Implements ``BaseConsumer[T]`` structurally — no inheritance from Protocol.
    Deserialises JSON messages. Manual offset commit after handler success.

    Each ``subscribe()`` call creates a new ``AIOKafkaConsumer``, starts it,
    polls until ``close()`` is called, then stops it. Handler failures trigger
    ``nack()`` (no commit — message is redelivered).

    Usage::

        consumer = KafkaConsumer(settings)

        async def handle(event: dict) -> None:
            print(f"Received: {event}")

        await consumer.subscribe(handle)   # runs until close() called

    Structural conformance::

        assert isinstance(consumer, BaseConsumer)
    """

    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings
        self._consumer: AIOKafkaConsumer | None = None
        self._running: bool = False

    # ------------------------------------------------------------------
    # Deserialisation (override in typed subclasses)
    # ------------------------------------------------------------------

    def _deserialise(self, raw: bytes) -> T:
        """
        Deserialise bytes from Kafka to the message type ``T``.

        Base implementation: ``json.loads(raw.decode("utf-8"))``.
        Subclasses override for custom deserialisation (protobuf, Avro, etc.).
        """
        return json.loads(raw.decode("utf-8"))  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # BaseConsumer[T] interface
    # ------------------------------------------------------------------

    async def subscribe(
        self,
        handler: Callable[[T], Awaitable[None]],
    ) -> None:
        """
        Start consuming messages and invoke ``handler`` for each one.

        Runs until ``close()`` is called. Uses manual commit — commits
        the offset only after ``handler`` returns successfully. If
        ``handler`` raises, calls ``nack()`` and continues to the next
        message.

        Args:
            handler: Async callable that processes each deserialised message.

        Raises:
            AdapterConnectionError:    Cannot reach the Kafka broker.
            AdapterConfigurationError: Invalid topic, group, or credentials.
        """
        kwargs: dict[str, Any] = {
            "bootstrap_servers": self._settings.kafka_bootstrap_servers,
            "group_id": self._settings.kafka_group_id,
            "auto_offset_reset": self._settings.kafka_auto_offset_reset,
            "enable_auto_commit": False,
            "max_poll_records": self._settings.kafka_max_poll_records,
            "session_timeout_ms": self._settings.kafka_session_timeout_ms,
            "request_timeout_ms": self._settings.kafka_request_timeout_ms,
            "security_protocol": self._settings.kafka_security_protocol,
        }
        if self._settings.kafka_sasl_mechanism:
            kwargs["sasl_mechanism"] = self._settings.kafka_sasl_mechanism
            kwargs["sasl_plain_username"] = self._settings.kafka_sasl_username
            kwargs["sasl_plain_password"] = self._settings.kafka_sasl_password

        try:
            self._consumer = AIOKafkaConsumer(
                self._settings.kafka_topic, **kwargs
            )
            await self._consumer.start()
            self._running = True
        except aiokafka.errors.KafkaConnectionError as exc:
            self._consumer = None
            raise AdapterConnectionError(
                f"Kafka consumer cannot connect to {self._settings.kafka_bootstrap_servers!r}: {exc}",
                adapter=self._settings.adapter_name,
                operation="subscribe",
                cause=exc,
            ) from exc
        except aiokafka.errors.KafkaError as exc:
            self._consumer = None
            raise AdapterConfigurationError(
                f"Kafka consumer configuration error: {exc}",
                adapter=self._settings.adapter_name,
                operation="subscribe",
            ) from exc

        try:
            async for msg in self._consumer:
                if not self._running:
                    break
                try:
                    message = self._deserialise(msg.value)
                    parent_ctx = _extract_trace_context(msg.headers)
                    token = otel_context.attach(parent_ctx)
                    try:
                        await handler(message)
                    finally:
                        otel_context.detach(token)
                    await self.ack(message)
                except Exception as exc:  # noqa: BLE001
                    _logger.error(
                        "KafkaConsumer handler failed for topic %r: %s",
                        self._settings.kafka_topic,
                        exc,
                    )
                    await self.nack(message)
        finally:
            if self._consumer is not None:
                try:
                    await self._consumer.stop()
                except Exception as exc:  # noqa: BLE001
                    _logger.error("KafkaConsumer stop error (ignored): %s", exc)
            self._consumer = None
            self._running = False

    async def ack(self, message: T) -> None:
        """
        Acknowledge a message by committing the current offset.

        Called automatically after successful handler invocation.
        Can also be called manually for custom acknowledgement flows.
        Never raises.

        Args:
            message: The message that was successfully processed.
        """
        if self._consumer is not None:
            try:
                await self._consumer.commit()
            except Exception as exc:  # noqa: BLE001
                _logger.error("KafkaConsumer ack (commit) error (ignored): %s", exc)

    async def nack(self, message: T) -> None:
        """
        Negatively acknowledge a message.

        Does NOT commit the offset — the message will be redelivered
        on the next poll. Logs the failure. Never raises.

        Args:
            message: The message that failed processing.
        """
        _logger.warning(
            "KafkaConsumer nack — message will be redelivered on topic %r",
            self._settings.kafka_topic,
        )

    async def close(self) -> None:
        """
        Stop the consumer.

        Sets ``_running = False`` to break the polling loop. The consumer
        is stopped in the ``finally`` block of ``subscribe()``. Never raises.
        """
        self._running = False
        if self._consumer is not None:
            try:
                await self._consumer.stop()
            except Exception as exc:  # noqa: BLE001
                _logger.error("KafkaConsumer close error (ignored): %s", exc)
            self._consumer = None