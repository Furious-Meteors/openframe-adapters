"""
openframe/adapters/queue/nats/consumer.py
============================================
NATS JetStream message consumer implementing ``BaseConsumer[T]`` from
``openframe-core`` via structural subtyping.

Each ``subscribe()`` call connects, creates a durable JetStream (push)
subscription with ``manual_ack=True``, and iterates its message stream
until ``close()`` is called. Each message is individually acknowledged
(``msg.ack()``) after the handler returns successfully, or negatively
acknowledged (``msg.nak()``) if the handler raises — JetStream redelivers
a ``nak()``'d (or un-acked, once ``ack_wait`` elapses) message according
to the durable consumer's ``max_deliver``/``ack_wait`` config.

Why per-message ack/nak objects, not the deserialised payload:
    Kafka's ``ack()``/``nack()`` operate on a single global "commit the
    current offset" call — which message triggered it is irrelevant, so
    Kafka's ``ack(message: T)``/``nack(message: T)`` take (and ignore) the
    deserialised payload. JetStream has no equivalent global cursor: each
    message carries its own ack handle (a reply subject), and only the
    exact ``nats.aio.msg.Msg`` that was delivered can be acked/nak'd. This
    adapter's ``ack()``/``nack()`` therefore take the raw ``Msg`` object
    (not the deserialised ``T``) — the only way to target the right
    message. This still satisfies ``BaseConsumer`` structurally (the
    protocol only requires an async method accepting one positional
    argument; it does not constrain what that argument's type must be).

Error handling:
    Connection-establishment failures (``nats.errors.NoServersError`` etc.)
    on ``subscribe()`` -> ``AdapterConnectionError``. Other driver errors
    at connect time -> ``AdapterConfigurationError``. Handler exceptions
    are caught, logged, and the loop continues (via ``nack()``).

Cross-service trace correlation:
    Each message's headers are checked for a W3C ``traceparent`` (injected
    by the producing side — see ``NatsProducer._inject_trace_headers``).
    If present, the ``handler`` invocation runs inside that extracted
    parent context, so any spans the handler creates continue the
    producer's trace instead of starting a disconnected one. If no
    traceparent is present, the handler runs with no special context —
    unchanged behaviour.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Generic, TypeVar

import nats
import nats.errors
import nats.js.errors
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js import JetStreamContext
from nats.js import api as nats_js_api

from opentelemetry import context as otel_context

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
)
from openframe.core.ports import BaseConsumer, Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.tracing.propagation import extract as _extract_propagation

from .config import NatsSettings
from .errors import wrap_nats

__all__ = ["NatsConsumer"]

T = TypeVar("T")
_logger = logging.getLogger(__name__)


class NatsConsumer(Generic[T]):
    """
    NATS JetStream message consumer.

    Implements ``BaseConsumer[T]`` structurally — no inheritance from Protocol.
    Deserialises JSON messages. Per-message ack/nak via JetStream's durable
    consumer redelivery (``manual_ack=True``).

    Each ``subscribe()`` call connects, creates a durable JetStream
    subscription, iterates messages until ``close()`` is called, then tears
    the connection down. Handler failures trigger ``nack()`` (``msg.nak()``
    — the message is redelivered per the durable consumer's config).

    Usage::

        consumer = NatsConsumer(settings)

        async def handle(event: dict) -> None:
            print(f"Received: {event}")

        await consumer.subscribe(handle)   # runs until close() called

    Structural conformance::

        assert isinstance(consumer, BaseConsumer)
    """

    name:       str = "openframe-nats-consumer"
    version:    str = "0.1.0"
    capability: Capability = Capability.QUEUE

    def __init__(self, settings: NatsSettings) -> None:
        self._settings = settings
        self._nc: NatsClient | None = None
        self._js: JetStreamContext | None = None
        self._sub: Subscription | None = None
        self._running: bool = False
        self._initialized: bool = False

    # ------------------------------------------------------------------
    # Deserialisation (override in typed subclasses)
    # ------------------------------------------------------------------

    def _deserialise(self, raw: bytes) -> T:
        """
        Deserialise bytes from NATS to the message type ``T``.

        Base implementation: ``json.loads(raw.decode("utf-8"))``.
        Subclasses override for custom deserialisation (protobuf, Avro, etc.).
        """
        return json.loads(raw.decode("utf-8"))  # type: ignore[return-value]

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
        """Idempotently ensure the configured JetStream stream exists (see NatsProducer)."""
        assert self._js is not None
        try:
            await self._js.add_stream(
                name=self._settings.nats_stream,
                subjects=[self._settings.nats_subject],
            )
        except nats.js.errors.BadRequestError as exc:
            _logger.debug(
                "NatsConsumer: stream %r already exists (or config unchanged): %s",
                self._settings.nats_stream, exc,
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

        Runs until ``close()`` is called (or the subscription's message
        stream ends). Uses a durable JetStream consumer with
        ``manual_ack=True`` — acks the message only after ``handler``
        returns successfully. If ``handler`` raises, calls ``nack()`` and
        continues to the next message.

        Args:
            handler: Async callable that processes each deserialised message.

        Raises:
            AdapterConnectionError:    Cannot reach any NATS server.
            AdapterConfigurationError: Invalid subject, stream, or credentials.
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
            self._nc = await nats.connect(**kwargs)
            self._js = self._nc.jetstream()
            await self._ensure_stream()

            deliver_policy = (
                nats_js_api.DeliverPolicy.NEW
                if self._settings.nats_deliver_policy == "new"
                else nats_js_api.DeliverPolicy.ALL
            )
            consumer_config = nats_js_api.ConsumerConfig(
                durable_name=self._settings.nats_durable_name,
                ack_wait=self._settings.nats_ack_wait_seconds,
                max_deliver=self._settings.nats_max_deliver,
                deliver_policy=deliver_policy,
            )
            self._sub = await self._js.subscribe(
                self._settings.nats_subject,
                stream=self._settings.nats_stream,
                config=consumer_config,
                manual_ack=True,
            )
            self._running = True
        except nats.errors.NoServersError as exc:
            self._nc = None
            self._js = None
            self._sub = None
            raise AdapterConnectionError(
                f"NATS consumer cannot connect to {self._settings.nats_servers!r}: {exc}",
                adapter=self._settings.adapter_name,
                operation="subscribe",
                cause=exc,
            ) from exc
        except nats.errors.Error as exc:
            self._nc = None
            self._js = None
            self._sub = None
            raise AdapterConfigurationError(
                f"NATS consumer configuration error: {exc}",
                adapter=self._settings.adapter_name,
                operation="subscribe",
            ) from exc

        try:
            async for msg in self._sub.messages:
                if not self._running:
                    break
                try:
                    message = self._deserialise(msg.data)
                    carrier: dict[str, str] = dict(msg.headers or {})
                    parent_ctx = _extract_propagation(carrier)
                    token = otel_context.attach(parent_ctx)
                    try:
                        await handler(message)
                    finally:
                        otel_context.detach(token)
                    await self.ack(msg)
                except Exception as exc:  # noqa: BLE001
                    _logger.error(
                        "NatsConsumer handler failed for subject %r: %s",
                        self._settings.nats_subject,
                        exc,
                    )
                    await self.nack(msg)
        finally:
            if self._sub is not None:
                try:
                    await self._sub.unsubscribe()
                except Exception as exc:  # noqa: BLE001
                    _logger.error("NatsConsumer unsubscribe error (ignored): %s", exc)
            if self._nc is not None:
                try:
                    await self._nc.close()
                except Exception as exc:  # noqa: BLE001
                    _logger.error("NatsConsumer close error (ignored): %s", exc)
            self._sub = None
            self._nc = None
            self._js = None
            self._running = False

    async def ack(self, message: Msg) -> None:
        """
        Acknowledge a message.

        Called automatically after successful handler invocation. Takes the
        raw ``nats.aio.msg.Msg`` (not the deserialised payload — see module
        docstring). Can also be called manually for custom acknowledgement
        flows. Never raises.

        Args:
            message: The raw NATS message that was successfully processed.
        """
        try:
            await message.ack()
        except Exception as exc:  # noqa: BLE001
            _logger.error("NatsConsumer ack error (ignored): %s", exc)

    async def nack(self, message: Msg) -> None:
        """
        Negatively acknowledge a message.

        Signals JetStream to redeliver the message (subject to the durable
        consumer's ``max_deliver``/``ack_wait`` config). Takes the raw
        ``nats.aio.msg.Msg`` (not the deserialised payload — see module
        docstring). Never raises.

        Args:
            message: The raw NATS message that failed processing.
        """
        _logger.warning(
            "NatsConsumer nack — message will be redelivered on subject %r",
            self._settings.nats_subject,
        )
        try:
            await message.nak()
        except Exception as exc:  # noqa: BLE001
            _logger.error("NatsConsumer nak error (ignored): %s", exc)

    async def close(self) -> None:
        """
        Stop the consumer.

        Sets ``_running = False`` to break the message loop. The
        subscription and connection are torn down in the ``finally`` block
        of ``subscribe()``. Never raises.
        """
        self._running = False
        if self._sub is not None:
            try:
                await self._sub.unsubscribe()
            except Exception as exc:  # noqa: BLE001
                _logger.error("NatsConsumer close (unsubscribe) error (ignored): %s", exc)
        if self._nc is not None:
            try:
                await self._nc.close()
            except Exception as exc:  # noqa: BLE001
                _logger.error("NatsConsumer close error (ignored): %s", exc)
            self._nc = None

    # ------------------------------------------------------------------
    # BasePort (Identity + Lifecycle) interface
    # ------------------------------------------------------------------

    async def initialize(self, context: PluginContext) -> None:
        """
        BasePort lifecycle entry point.

        Each ``subscribe()`` call constructs and connects its own client
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
        if self._initialized:
            return PluginHealth(status=PluginStatus.READY, message="")
        return PluginHealth(status=PluginStatus.FAILED, message="not initialized")
