"""
openframe/adapters/queue/nats/config.py
==========================================
Settings for the NATS JetStream queue adapter, sourced from environment
variables via ``openframe-core``'s ``BaseAdapterSettings`` (pydantic-settings).

Required env vars:
    NATS_SERVERS: Comma-separated server list.
                  Format: nats://host:port,nats://host:port

Optional env vars (all have defaults):
    NATS_SUBJECT:                str   = "openframe"           — default subject
    NATS_STREAM:                 str   = "OPENFRAME"            — JetStream stream name
    NATS_DURABLE_NAME:           str   = "openframe-consumer"   — durable consumer name
    NATS_DELIVER_POLICY:         str   = "all"                  — "all" or "new"
    NATS_ACK_WAIT_SECONDS:       float = 30.0                   — redelivery wait
    NATS_MAX_DELIVER:            int   = 5                      — max redelivery attempts
    NATS_MAX_RECONNECT_ATTEMPTS: int   = 60
    NATS_USER:                   str   = ""
    NATS_PASSWORD:                str   = ""
    NATS_TOKEN:                  str   = ""
    ADAPTER_NAME:                str   = "nats"

Why JetStream (not core NATS):
    Core NATS pub/sub is fire-and-forget (at-most-once, no ack/nack, no
    redelivery, no persistence) — there is nothing meaningful for
    ``BaseConsumer.ack()``/``nack()`` to do on top of it. JetStream adds a
    persistent stream plus a durable, acknowledgeable consumer, which is
    what gives ``ack()``/``nack()`` real at-least-once semantics: a handler
    failure (``nack()`` -> ``msg.nak()``) triggers redelivery, matching the
    contract ``KafkaConsumer`` provides via manual offset commit. This
    adapter targets JetStream exclusively for that reason.
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["NatsSettings"]


class NatsSettings(BaseAdapterSettings):
    """
    Pydantic-settings subclass for the NATS JetStream queue adapter.

    All fields are read from environment variables with the exact names
    shown above. ``BaseAdapterSettings`` provides ``operation_timeout``
    (default 10.0 s) and ``connection_timeout`` (default 30.0 s).
    """

    nats_servers: str
    nats_subject: str = "openframe"
    nats_stream: str = "OPENFRAME"
    nats_durable_name: str = "openframe-consumer"
    nats_deliver_policy: str = "all"
    nats_ack_wait_seconds: float = 30.0
    nats_max_deliver: int = 5
    nats_max_reconnect_attempts: int = 60
    nats_user: str = ""
    nats_password: str = ""
    nats_token: str = ""
    adapter_name: str = "nats"

    @property
    def server_list(self) -> list[str]:
        """Comma-separated ``nats_servers`` split into a list for ``nats.connect()``."""
        return [s.strip() for s in self.nats_servers.split(",") if s.strip()]
