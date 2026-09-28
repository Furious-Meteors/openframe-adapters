"""
openframe/adapters/queue/nats/plugin.py
==========================================
OpenFrame plugin wrapper for NatsProducer and NatsConsumer.

Stability: beta
Capability: "queue"

Manages the ``NatsProducer`` lifecycle. Consumers are created on-demand
via ``make_consumer()`` since each consume session is short-lived.

Recommended usage — ApplicationBootstrap.compose() (requires openframe-core>=3.3)::

    from openframe.core.runtime import ApplicationBootstrap
    from openframe.core.ports import Capability
    from openframe.adapters.queue.nats import NatsPlugin, NatsSettings

    async with ApplicationBootstrap.compose(NatsPlugin(NatsSettings())) as app:
        plugin = app.get(Capability.QUEUE)
        producer = plugin.get_producer()
        await producer.publish({"event": "item.created"})

        consumer = plugin.make_consumer()
        await consumer.subscribe(handler)

Reach for a subclassed ``ApplicationBootstrap`` with ``configure()`` instead
of ``compose()`` once this plugin needs its own ``config=``/``init_timeout=``,
or registration order that depends on a runtime condition. For what neither
tier covers (e.g. ``get_all()`` for an intentional multi-port-per-capability
setup — separate producer-only/consumer-only plugin instances), use
``app.registry`` — the underlying ``PluginRegistry``.

No lifecycle needed (tests/scripts) — construct the producer directly::

    producer = NatsProducer(NatsSettings())
    await producer.start()
"""
# Capability: "queue"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/how-it-works/#choosing-a-wiring-pattern
from __future__ import annotations

import logging

from openframe.core.ports import BasePort, Capability, PluginContext, PluginHealth, PluginStatus

from openframe.adapters.queue.nats.config import NatsSettings
from openframe.adapters.queue.nats.consumer import NatsConsumer
from openframe.adapters.queue.nats.producer import NatsProducer

__all__ = ["NatsPlugin"]

_logger = logging.getLogger(__name__)


class NatsPlugin(BasePort):
    """
    NATS JetStream adapter plugin for the OpenFrame plugin registry.

    Capability: "queue"

    Stability: beta

    Lifecycle:
        initialize() — starts the ``NatsProducer`` and verifies connectivity.
                       Raises on failure.
        shutdown()   — closes the producer. Never raises.
        health()     — checks whether the producer is READY. Never raises.

    The plugin exposes ``get_producer()`` and ``make_consumer()`` after
    initialization for use in the composition root or ApplicationBootstrap.

    By default constructs a plain NatsProducer. To use a domain-specific
    subclass (e.g. one that overrides _serialise), pass it via producer_class::

        registry.register(NatsPlugin(
            NatsSettings(),
            producer_class=ArtifactEventProducer,
        ))

    ``make_consumer()`` similarly honours ``consumer_class`` — pass a
    domain-specific subclass (e.g. one that overrides _deserialise) to
    have it constructed for you instead of building it manually::

        # With both custom producer and consumer subclasses:
        plugin = NatsPlugin(
            NatsSettings(),
            producer_class=ArtifactEventProducer,
            consumer_class=OrderEventConsumer,
        )
    """

    name:       str = "openframe-nats"
    version:    str = "0.1.0"
    capability: Capability = Capability.QUEUE

    def __init__(
        self,
        settings: NatsSettings,
        producer_class: type[NatsProducer] = NatsProducer,
        consumer_class: type[NatsConsumer] = NatsConsumer,
    ) -> None:
        """
        Args:
            settings:       NatsSettings instance.
            producer_class: The NatsProducer subclass to construct.
                            Defaults to the base NatsProducer. Pass a
                            domain-specific subclass here to get proper
                            _serialise() overrides through get_producer().
            consumer_class: The NatsConsumer subclass to construct via
                            make_consumer(). Defaults to the base
                            NatsConsumer. Pass a domain-specific subclass
                            here to get proper _deserialise() overrides
                            without constructing the consumer manually
                            outside the plugin.

        Raises:
            TypeError: producer_class is not a subclass of NatsProducer.
            TypeError: consumer_class is not a subclass of NatsConsumer.
        """
        if not (isinstance(producer_class, type) and issubclass(producer_class, NatsProducer)):
            raise TypeError(
                f"producer_class must be a subclass of NatsProducer, "
                f"got {producer_class!r}"
            )
        if not (isinstance(consumer_class, type) and issubclass(consumer_class, NatsConsumer)):
            raise TypeError(
                f"consumer_class must be a subclass of NatsConsumer, "
                f"got {consumer_class!r}"
            )
        self._settings = settings
        self._producer_class = producer_class
        self._consumer_class = consumer_class
        self._producer: NatsProducer | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Start the NatsProducer and verify broker connectivity.

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError:    No NATS server reachable.
            AdapterConfigurationError: Invalid servers or settings.
            AdapterTimeoutError:       Start exceeded ``connection_timeout``.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            self._producer = self._producer_class(self._settings)
            await self._producer.start()
            self._status = PluginStatus.READY
            _logger.info(
                "NatsPlugin initialized — %s (producer_class=%s, consumer_class=%s)",
                self._settings.nats_servers,
                self._producer_class.__name__,
                self._consumer_class.__name__,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Close the NatsProducer.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._producer is not None:
                await self._producer.close()
                _logger.info("NatsPlugin shutdown complete.")
        except Exception as exc:
            _logger.error("NatsPlugin shutdown error (ignored): %s", exc)
        finally:
            self._status = PluginStatus.STOPPED

    async def health(self) -> PluginHealth:
        """
        Return current health snapshot.

        Never raises — returns FAILED status on any exception.
        """
        try:
            if self._producer is None or self._status != PluginStatus.READY:
                return PluginHealth(
                    status=PluginStatus.FAILED,
                    message=f"Plugin status: {self._status.name}",
                )
            return PluginHealth(status=PluginStatus.READY, message="")
        except Exception as exc:
            return PluginHealth(
                status=PluginStatus.FAILED,
                message=str(exc),
            )

    def get_producer(self) -> NatsProducer:
        """
        Return the started producer.

        Only valid after ``registry.initialize_all()`` has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._producer is None or self._status != PluginStatus.READY:
            raise RuntimeError(
                f"NatsPlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        return self._producer

    def make_consumer(self) -> NatsConsumer:
        """
        Create a new consumer with this plugin's settings.

        Uses the ``consumer_class`` passed at construction time, so
        domain subclasses that override ``_deserialise()`` are returned
        correctly without constructing the consumer manually outside
        the plugin.

        Consumers are short-lived (one per ``subscribe()`` session) so
        they are created on demand rather than cached.

        Returns:
            A fresh instance of the configured ``consumer_class``,
            ready to call ``subscribe()`` on.
        """
        return self._consumer_class(self._settings)
