"""
openframe/adapters/queue/rabbitmq/config.py
==============================================
Settings for the RabbitMQ queue adapter, sourced from environment variables
via ``openframe-core``'s ``BaseAdapterSettings`` (pydantic-settings).

Required env vars:
    RABBITMQ_URL: AMQP connection URL.
                  Format: amqp://user:password@host:port/vhost

Optional env vars (all have defaults):
    RABBITMQ_QUEUE:               str   = "openframe"  — default queue name
    RABBITMQ_DURABLE:             bool  = True          — durable queue declare
    RABBITMQ_PREFETCH_COUNT:      int   = 10            — consumer QoS prefetch
    RABBITMQ_RECONNECT_INTERVAL:  float = 5.0           — robust-reconnect backoff (s)
    ADAPTER_NAME:                 str   = "rabbitmq"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["RabbitmqSettings"]


class RabbitmqSettings(BaseAdapterSettings):
    """
    Pydantic-settings subclass for the RabbitMQ queue adapter.

    All fields are read from environment variables with the exact names
    shown above. ``BaseAdapterSettings`` provides ``operation_timeout``
    (default 10.0 s) and ``connection_timeout`` (default 30.0 s).
    """

    rabbitmq_url: str
    rabbitmq_queue: str = "openframe"
    rabbitmq_durable: bool = True
    rabbitmq_prefetch_count: int = 10
    rabbitmq_reconnect_interval: float = 5.0
    adapter_name: str = "rabbitmq"
