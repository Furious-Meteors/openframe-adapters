"""
openframe.adapters.queue.kafka
================================
Apache Kafka queue adapter for the OpenFrame Microservice Suite.

Public API::

    KafkaSettings  — Pydantic Settings subclass for connection config.
    KafkaProducer  — Generic async message producer (BaseProducer[T]).
    KafkaConsumer  — Generic async message consumer (BaseConsumer[T]).
    KafkaPlugin    — BasePort implementation for PluginRegistry.

Quick start::

    from openframe.adapters.queue.kafka import (
        KafkaSettings,
        KafkaProducer,
        KafkaConsumer,
    )

    settings = KafkaSettings(kafka_bootstrap_servers="localhost:9092")

    # Produce
    producer = KafkaProducer(settings)
    await producer.start()
    await producer.publish({"event": "item.created", "id": "abc"})
    await producer.close()

    # Consume
    consumer = KafkaConsumer(settings)

    async def handle(event: dict) -> None:
        print(f"Received: {event}")

    await consumer.subscribe(handle)
"""
from __future__ import annotations

from .config import KafkaSettings
from .consumer import KafkaConsumer
from .plugin import KafkaPlugin
from .producer import KafkaProducer

__all__ = [
    "KafkaSettings",
    "KafkaProducer",
    "KafkaConsumer",
    "KafkaPlugin",
]
