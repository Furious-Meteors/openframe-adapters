"""
openframe.adapters.queue.nats
================================
NATS JetStream queue adapter for the OpenFrame Microservice Suite.

Public API::

    NatsSettings  — Pydantic Settings subclass for connection config.
    NatsProducer  — Generic async message producer (BaseProducer[T]).
    NatsConsumer  — Generic async message consumer (BaseConsumer[T]).
    NatsPlugin    — BasePort implementation for PluginRegistry.

Quick start::

    from openframe.adapters.queue.nats import (
        NatsSettings,
        NatsProducer,
        NatsConsumer,
    )

    settings = NatsSettings(nats_servers="nats://localhost:4222")

    # Produce
    producer = NatsProducer(settings)
    await producer.start()
    await producer.publish({"event": "item.created", "id": "abc"})
    await producer.close()

    # Consume
    consumer = NatsConsumer(settings)

    async def handle(event: dict) -> None:
        print(f"Received: {event}")

    await consumer.subscribe(handle)
"""
from __future__ import annotations

from .config import NatsSettings
from .consumer import NatsConsumer
from .plugin import NatsPlugin
from .producer import NatsProducer

__all__ = [
    "NatsSettings",
    "NatsProducer",
    "NatsConsumer",
    "NatsPlugin",
]
