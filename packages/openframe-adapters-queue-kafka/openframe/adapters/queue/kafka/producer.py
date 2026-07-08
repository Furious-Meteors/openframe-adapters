"""
openframe/adapters/queue/kafka/producer.py
============================================
Kafka message producer implementing ``BaseProducer[T]`` from
``openframe-core`` via structural subtyping.

Messages are serialised to JSON bytes before being sent to Kafka.
The underlying ``AIOKafkaProducer`` must be explicitly started via
``await producer.start()`` before calling ``publish()`` or
``publish_batch()``.

Error handling:
    Every ``aiokafka.errors.*`` is caught and re-raised as the appropriate
    ``AdapterError`` subclass with cause chaining.

Timeout strategy:
    ``asyncio.timeout(settings.operation_timeout)`` wraps every send.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Generic, TypeVar

import aiokafka
import aiokafka.errors
from aiokafka import AIOKafkaProducer

from openframe.core.ports import Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseProducer
from openframe.core.tracing.propagation import inject as _inject_propagation

from .config import KafkaSettings

__all__ = ["KafkaProducer"]

T = TypeVar("T")
_logger = logging.getLogger(__name__)


def _inject_trace_headers() -> list[tuple[str, bytes]]:
    """
    Build Kafka message headers carrying the active W3C traceparent.

    Mirrors ``KafkaConsumer``'s extraction on the other side of this
    boundary. ``aiokafka`` message headers are ``list[tuple[str, bytes]]``
    rather than a dict, so the string carrier populated by ``inject()`` is
    encoded into that shape. A no-op (empty list) when there is no active
    span or the SDK has not been initialised.
    """
    carrier: dict[str, str] = {}
    _inject_propagation(carrier)
    return [(k, v.encode("utf-8")) for k, v in carrier.items()]


class KafkaProducer(Generic[T]):
    """
    Kafka message producer.

    Implements ``BaseProducer[T]`` structurally — no inheritance from Protocol.
    Serialises messages to JSON bytes before publishing to Kafka.

    The underlying ``AIOKafkaProducer`` must be started before use.
    Call ``await producer.start()`` or use ``KafkaPlugin`` for managed
    lifecycle.

    Usage::

        producer = KafkaProducer(settings)
        await producer.start()
        await producer.publish({"event": "item.created", "id": "abc"})
        await producer.close()

    Structural conformance::

        assert isinstance(producer, BaseProducer)
    """

    name:       str = "openframe-kafka-producer"
    version:    str = "1.3.0"
    capability: Capability = Capability.QUEUE

    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None

    # ------------------------------------------------------------------
    # Serialisation (override in typed subclasses)
    # ------------------------------------------------------------------

    def _serialise(self, message: T) -> bytes:
        """
        Serialise a message to bytes for Kafka.

        Base implementation: ``json.dumps(message).encode("utf-8")``.
        Subclasses override for custom serialisation (protobuf, Avro, etc.).
        """
        return json.dumps(message).encode("utf-8")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        Start the underlying ``AIOKafkaProducer``.

        Must be called before ``publish()`` or ``publish_batch()``.

        Raises:
            AdapterConnectionError:    Broker is unreachable.
            AdapterConfigurationError: Invalid bootstrap servers or settings.
            AdapterTimeoutError:       Start exceeded ``connection_timeout``.
        """
        kwargs: dict[str, Any] = {
            "bootstrap_servers": self._settings.kafka_bootstrap_servers,
            "request_timeout_ms": self._settings.kafka_request_timeout_ms,
            "security_protocol": self._settings.kafka_security_protocol,
        }
        if self._settings.kafka_sasl_mechanism:
            kwargs["sasl_mechanism"] = self._settings.kafka_sasl_mechanism
            kwargs["sasl_plain_username"] = self._settings.kafka_sasl_username
            kwargs["sasl_plain_password"] = self._settings.kafka_sasl_password

        try:
            async with asyncio.timeout(self._settings.connection_timeout):
                self._producer = AIOKafkaProducer(**kwargs)
                await self._producer.start()
        except asyncio.TimeoutError as exc:
            self._producer = None
            raise AdapterTimeoutError(
                f"Kafka producer start timed out after {self._settings.connection_timeout}s",
                adapter=self._settings.adapter_name,
                operation="start",
                cause=exc,
            ) from exc
        except aiokafka.errors.KafkaConnectionError as exc:
            self._producer = None
            raise AdapterConnectionError(
                f"Kafka producer cannot connect to {self._settings.kafka_bootstrap_servers!r}: {exc}",
                adapter=self._settings.adapter_name,
                operation="start",
                cause=exc,
            ) from exc
        except aiokafka.errors.KafkaError as exc:
            self._producer = None
            raise AdapterConfigurationError(
                f"Kafka producer configuration error: {exc}",
                adapter=self._settings.adapter_name,
                operation="start",
            ) from exc

    async def close(self) -> None:
        """
        Stop the producer and release resources.

        Idempotent — safe to call multiple times. Never raises.
        """
        if self._producer is not None:
            try:
                await self._producer.stop()
            except Exception as exc:  # noqa: BLE001
                _logger.error("KafkaProducer close error (ignored): %s", exc)
            finally:
                self._producer = None

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

        READY when the underlying producer has been started, FAILED
        otherwise. Never raises.
        """
        if self._producer is not None:
            return PluginHealth(status=PluginStatus.READY, message="")
        return PluginHealth(status=PluginStatus.FAILED, message="producer not started")

    # ------------------------------------------------------------------
    # BaseProducer[T] interface
    # ------------------------------------------------------------------

    async def publish(self, message: T) -> None:
        """
        Publish a single message to the configured topic.

        Serialises the message to JSON bytes and waits for broker
        acknowledgement via ``send_and_wait``.

        Args:
            message: The message to publish.

        Raises:
            RuntimeError:        Producer not started — call ``start()`` first.
            AdapterQueryError:   Publish failed (broker error).
            AdapterTimeoutError: Publish exceeded ``operation_timeout``.
        """
        if self._producer is None:
            raise RuntimeError(
                "KafkaProducer not started. Call await producer.start() first."
            )
        value = self._serialise(message)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await self._producer.send_and_wait(
                    self._settings.kafka_topic, value,
                    headers=_inject_trace_headers(),
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"publish exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="publish",
                cause=exc,
            ) from exc
        except aiokafka.errors.KafkaError as exc:
            raise AdapterQueryError(
                f"publish to topic {self._settings.kafka_topic!r} failed: {exc}",
                adapter=self._settings.adapter_name,
                operation="publish",
                cause=exc,
            ) from exc

    async def publish_batch(self, messages: list[T]) -> None:
        """
        Publish multiple messages to the configured topic.

        Sends each message individually, then flushes to ensure all
        are delivered. Raises on the first failure — messages already
        sent are not rolled back.

        Args:
            messages: List of messages to publish.

        Raises:
            RuntimeError:        Producer not started.
            AdapterQueryError:   Batch publish failed.
            AdapterTimeoutError: Exceeded ``operation_timeout``.
        """
        if self._producer is None:
            raise RuntimeError(
                "KafkaProducer not started. Call await producer.start() first."
            )
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                for message in messages:
                    value = self._serialise(message)
                    await self._producer.send_and_wait(
                        self._settings.kafka_topic, value,
                        headers=_inject_trace_headers(),
                    )
                await self._producer.flush()
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"publish_batch exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="publish_batch",
                cause=exc,
            ) from exc
        except aiokafka.errors.KafkaError as exc:
            raise AdapterQueryError(
                f"publish_batch to topic {self._settings.kafka_topic!r} failed: {exc}",
                adapter=self._settings.adapter_name,
                operation="publish_batch",
                cause=exc,
            ) from exc
