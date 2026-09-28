"""
openframe/adapters/queue/nats/producer.py
============================================
NATS JetStream message producer implementing ``BaseProducer[T]`` from
``openframe-core`` via structural subtyping.

Messages are serialised to JSON bytes before being published to a
JetStream stream. The underlying NATS client must be explicitly started
via ``await producer.start()`` before calling ``publish()`` or
``publish_batch()``.

Why JetStream: core NATS pub/sub has no persistence and no publish
acknowledgement — ``js.publish()`` (JetStream) round-trips to the server
and returns a ``PubAck`` (or raises), which is what lets ``publish()``
actually distinguish "delivered" from "failed" the way ``BaseProducer``
requires. See ``config.py`` for the full rationale.

Error handling:
    Every ``nats.errors.Error``/``nats.js.errors.Error`` raised by a
    publish call is translated to the appropriate ``AdapterError``
    subclass via ``_wrap_nats()`` (see ``errors.py``) — never re-raised
    as a raw driver exception.

Timeout strategy:
    ``asyncio.timeout(settings.operation_timeout)`` wraps every publish.
    ``asyncio.timeout(settings.connection_timeout)`` wraps ``start()``.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Generic, TypeVar

import nats
import nats.errors
import nats.js.errors
from nats.aio.client import Client as NatsClient
from nats.js import JetStreamContext

from openframe.core.ports import Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseProducer
from openframe.core.tracing.propagation import inject as _inject_propagation

from .config import NatsSettings
from .errors import wrap_nats

__all__ = ["NatsProducer"]

T = TypeVar("T")
_logger = logging.getLogger(__name__)


def _inject_trace_headers() -> dict[str, str]:
    """
    Build NATS message headers carrying the active W3C traceparent.

    Mirrors ``NatsConsumer``'s extraction on the other side of this
    boundary. Unlike Kafka's ``list[tuple[str, bytes]]`` header shape,
    ``nats-py`` headers are a plain ``dict[str, str]``, so the carrier
    populated by ``inject()`` is used as-is. A no-op (empty dict) when
    there is no active span or the SDK has not been initialised.
    """
    carrier: dict[str, str] = {}
    _inject_propagation(carrier)
    return carrier


class NatsProducer(Generic[T]):
    """
    NATS JetStream message producer.

    Implements ``BaseProducer[T]`` structurally — no inheritance from Protocol.
    Serialises messages to JSON bytes before publishing to a JetStream stream.

    The underlying NATS client must be started before use. Call
    ``await producer.start()`` or use ``NatsPlugin`` for managed lifecycle.

    Usage::

        producer = NatsProducer(settings)
        await producer.start()
        await producer.publish({"event": "item.created", "id": "abc"})
        await producer.close()

    Structural conformance::

        assert isinstance(producer, BaseProducer)
    """

    name:       str = "openframe-nats-producer"
    version:    str = "0.1.0"
    capability: Capability = Capability.QUEUE

    def __init__(self, settings: NatsSettings) -> None:
        self._settings = settings
        self._nc: NatsClient | None = None
        self._js: JetStreamContext | None = None

    # ------------------------------------------------------------------
    # Serialisation (override in typed subclasses)
    # ------------------------------------------------------------------

    def _serialise(self, message: T) -> bytes:
        """
        Serialise a message to bytes for NATS.

        Base implementation: ``json.dumps(message).encode("utf-8")``.
        Subclasses override for custom serialisation (protobuf, Avro, etc.).
        """
        return json.dumps(message).encode("utf-8")

    # ------------------------------------------------------------------
    # Error translation
    # ------------------------------------------------------------------

    def _wrap_nats(self, exc: Exception, operation: str) -> AdapterConnectionError | AdapterQueryError:
        """
        Translate a ``nats.errors.Error``/``nats.js.errors.Error`` into the
        appropriate ``AdapterError`` subclass. See ``errors.wrap_nats()``
        for the full connection-vs-query classification and its rationale.
        """
        return wrap_nats(
            exc,
            operation,
            adapter=self._settings.adapter_name,
            resource=self._settings.nats_subject,
        )

    async def _ensure_stream(self) -> None:
        """
        Idempotently ensure the configured JetStream stream exists.

        ``add_stream()`` is safe to call against an already-existing stream
        with an unchanged config (no-op update). A ``BadRequestError`` here
        means a stream with this name already exists with an incompatible
        config — logged and ignored rather than failing ``start()``, since
        that's an operational/ops-owned concern (e.g. a shared stream
        provisioned out-of-band), not a connectivity failure.
        """
        assert self._js is not None
        try:
            await self._js.add_stream(
                name=self._settings.nats_stream,
                subjects=[self._settings.nats_subject],
            )
        except nats.js.errors.BadRequestError as exc:
            _logger.debug(
                "NatsProducer: stream %r already exists (or config unchanged): %s",
                self._settings.nats_stream, exc,
            )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        Connect to NATS and obtain a JetStream context.

        Must be called before ``publish()`` or ``publish_batch()``.

        Raises:
            AdapterConnectionError:    No server reachable.
            AdapterConfigurationError: Invalid servers, auth rejected, or
                                       JetStream/stream setup failed.
            AdapterTimeoutError:       Start exceeded ``connection_timeout``.
        """
        kwargs: dict[str, Any] = {
            "servers": self._settings.server_list,
            "max_reconnect_attempts": self._settings.nats_max_reconnect_attempts,
        }
        if self._settings.nats_user:
            kwargs["user"] = self._settings.nats_user
            kwargs["password"] = self._settings.nats_password
        if self._settings.nats_token:
            kwargs["token"] = self._settings.nats_token

        try:
            async with asyncio.timeout(self._settings.connection_timeout):
                self._nc = await nats.connect(**kwargs)
                self._js = self._nc.jetstream()
                await self._ensure_stream()
        except asyncio.TimeoutError as exc:
            self._nc = None
            self._js = None
            raise AdapterTimeoutError(
                f"NATS producer start timed out after {self._settings.connection_timeout}s",
                adapter=self._settings.adapter_name,
                operation="start",
                cause=exc,
            ) from exc
        except nats.errors.NoServersError as exc:
            self._nc = None
            self._js = None
            raise AdapterConnectionError(
                f"NATS producer cannot connect to {self._settings.nats_servers!r}: {exc}",
                adapter=self._settings.adapter_name,
                operation="start",
                cause=exc,
            ) from exc
        except nats.errors.Error as exc:
            self._nc = None
            self._js = None
            raise AdapterConfigurationError(
                f"NATS producer configuration error: {exc}",
                adapter=self._settings.adapter_name,
                operation="start",
            ) from exc

    async def close(self) -> None:
        """
        Close the NATS connection and release resources.

        Idempotent — safe to call multiple times. Never raises.
        """
        if self._nc is not None:
            try:
                await self._nc.close()
            except Exception as exc:  # noqa: BLE001
                _logger.error("NatsProducer close error (ignored): %s", exc)
            finally:
                self._nc = None
                self._js = None

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

        READY when the underlying JetStream context has been obtained,
        FAILED otherwise. Never raises.
        """
        if self._js is not None:
            return PluginHealth(status=PluginStatus.READY, message="")
        return PluginHealth(status=PluginStatus.FAILED, message="producer not started")

    # ------------------------------------------------------------------
    # BaseProducer[T] interface
    # ------------------------------------------------------------------

    async def publish(self, message: T) -> None:
        """
        Publish a single message to the configured subject.

        Serialises the message to JSON bytes and publishes it via the
        JetStream context, which round-trips to the server for a publish
        acknowledgement (``PubAck``) before returning.

        Args:
            message: The message to publish.

        Raises:
            RuntimeError:        Producer not started — call ``start()`` first.
            AdapterConnectionError: Connection to NATS was lost/unavailable.
            AdapterQueryError:   Publish failed (e.g. rejected by JetStream).
            AdapterTimeoutError: Publish exceeded ``operation_timeout``.
        """
        if self._js is None:
            raise RuntimeError(
                "NatsProducer not started. Call await producer.start() first."
            )
        value = self._serialise(message)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await self._js.publish(
                    self._settings.nats_subject, value,
                    headers=_inject_trace_headers(),
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"publish exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="publish",
                cause=exc,
            ) from exc
        except nats.errors.Error as exc:
            raise self._wrap_nats(exc, "publish") from exc

    async def publish_batch(self, messages: list[T]) -> None:
        """
        Publish multiple messages to the configured subject.

        Publishes each message individually (JetStream has no native
        multi-message batch publish API — each publish already round-trips
        for its own acknowledgement). Raises on the first failure —
        messages already published are not rolled back.

        Args:
            messages: List of messages to publish.

        Raises:
            RuntimeError:        Producer not started.
            AdapterConnectionError: Connection to NATS was lost/unavailable.
            AdapterQueryError:   Batch publish failed.
            AdapterTimeoutError: Exceeded ``operation_timeout``.
        """
        if self._js is None:
            raise RuntimeError(
                "NatsProducer not started. Call await producer.start() first."
            )
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                for message in messages:
                    value = self._serialise(message)
                    await self._js.publish(
                        self._settings.nats_subject, value,
                        headers=_inject_trace_headers(),
                    )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"publish_batch exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="publish_batch",
                cause=exc,
            ) from exc
        except nats.errors.Error as exc:
            raise self._wrap_nats(exc, "publish_batch") from exc
