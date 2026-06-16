"""
openframe/adapters/queue/kafka/plugin.py
==========================================
OpenFrame plugin wrapper for KafkaProducer and KafkaConsumer.

Stability: beta
Capability: "queue"

Manages the ``KafkaProducer`` lifecycle. Consumers are created on-demand
via ``make_consumer()`` since each consume session is short-lived.

Usage via PluginRegistry (optional)::

    from openframe.core.plugins import PluginRegistry
    from openframe.adapters.queue.kafka import KafkaPlugin, KafkaSettings

    registry = PluginRegistry()
    registry.register(KafkaPlugin(KafkaSettings()))
    await registry.initialize_all()

    plugin = registry.get("queue")
    producer = plugin.get_producer()
    await producer.publish({"event": "item.created"})

    consumer = plugin.make_consumer()
    await consumer.subscribe(handler)

Usage via deps.py (unchanged, no plugin needed)::

    producer = KafkaProducer(KafkaSettings())
    await producer.start()
"""
# Capability: "queue"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/composition-root/
from __future__ import annotations

import logging

from openframe.core.plugins import PluginContext, PluginHealth, PluginStatus

from openframe.adapters.queue.kafka.config import KafkaSettings
from openframe.adapters.queue.kafka.consumer import KafkaConsumer
from openframe.adapters.queue.kafka.producer import KafkaProducer

__all__ = ["KafkaPlugin"]

_logger = logging.getLogger(__name__)


class KafkaPlugin:
    """
    Kafka adapter plugin for the OpenFrame plugin registry.

    Capability: "queue"

    Lifecycle:
        initialize() — starts the ``KafkaProducer`` and verifies connectivity.
                       Raises on failure.
        shutdown()   — closes the producer. Never raises.
        health()     — checks whether the producer is READY. Never raises.

    The plugin exposes ``get_producer()`` and ``make_consumer()`` after
    initialization for use in the composition root or ApplicationBootstrap.
    """

    name:       str = "openframe-kafka"
    version:    str = "1.1.0"
    capability: str = "queue"

    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings
        self._producer: KafkaProducer | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Start the KafkaProducer and verify broker connectivity.

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError:    Broker is unreachable.
            AdapterConfigurationError: Invalid bootstrap servers or settings.
            AdapterTimeoutError:       Start exceeded ``connection_timeout``.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            self._producer = KafkaProducer(self._settings)
            await self._producer.start()
            self._status = PluginStatus.READY
            _logger.info(
                "KafkaPlugin initialized — %s",
                self._settings.kafka_bootstrap_servers,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Close the KafkaProducer.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._producer is not None:
                await self._producer.close()
                _logger.info("KafkaPlugin shutdown complete.")
        except Exception as exc:
            _logger.error("KafkaPlugin shutdown error (ignored): %s", exc)
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

    def get_producer(self) -> KafkaProducer:
        """
        Return the started producer.

        Only valid after ``registry.initialize_all()`` has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._producer is None or self._status != PluginStatus.READY:
            raise RuntimeError(
                f"KafkaPlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        return self._producer

    def make_consumer(self) -> KafkaConsumer:
        """
        Create a new ``KafkaConsumer`` with this plugin's settings.

        Consumers are short-lived (one per ``subscribe()`` session) so
        they are created on demand rather than cached.

        Returns:
            A fresh ``KafkaConsumer`` ready to call ``subscribe()`` on.
        """
        return KafkaConsumer(self._settings)
