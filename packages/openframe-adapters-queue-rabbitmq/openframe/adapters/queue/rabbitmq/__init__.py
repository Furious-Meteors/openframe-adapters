"""
openframe.adapters.queue.rabbitmq
====================================
RabbitMQ (AMQP) queue adapter for the OpenFrame Microservice Suite.

Public API::

    RabbitmqSettings — Pydantic Settings subclass for connection config.
    RabbitmqProducer — Generic async message producer (BaseProducer[T]).
    RabbitmqConsumer — Generic async message consumer (BaseConsumer[T]).
    RabbitmqPlugin   — BasePort implementation for PluginRegistry.

Quick start::

    from openframe.adapters.queue.rabbitmq import (
        RabbitmqSettings,
        RabbitmqProducer,
        RabbitmqConsumer,
    )

    settings = RabbitmqSettings(rabbitmq_url="amqp://guest:guest@localhost:5672/")

    # Produce
    producer = RabbitmqProducer(settings)
    await producer.start()
    await producer.publish({"event": "item.created", "id": "abc"})
    await producer.close()

    # Consume
    consumer = RabbitmqConsumer(settings)

    async def handle(event: dict) -> None:
        print(f"Received: {event}")

    await consumer.subscribe(handle)
"""
from __future__ import annotations

from .config import RabbitmqSettings
from .consumer import RabbitmqConsumer
from .plugin import RabbitmqPlugin
from .producer import RabbitmqProducer

__all__ = [
    "RabbitmqSettings",
    "RabbitmqProducer",
    "RabbitmqConsumer",
    "RabbitmqPlugin",
]
