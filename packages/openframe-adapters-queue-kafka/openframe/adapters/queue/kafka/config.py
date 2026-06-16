"""
openframe/adapters/queue/kafka/config.py
==========================================
Settings for the Kafka queue adapter, sourced from environment variables
via ``openframe-core``'s ``BaseAdapterSettings`` (pydantic-settings).

Required env vars:
    KAFKA_BOOTSTRAP_SERVERS: Comma-separated broker list.
                             Format: host:port,host:port

Optional env vars (all have defaults):
    KAFKA_TOPIC:              str  = "openframe"       — default topic
    KAFKA_GROUP_ID:           str  = "openframe-group" — consumer group
    KAFKA_AUTO_OFFSET_RESET:  str  = "earliest"
    KAFKA_MAX_POLL_RECORDS:   int  = 10
    KAFKA_SESSION_TIMEOUT_MS: int  = 30000
    KAFKA_REQUEST_TIMEOUT_MS: int  = 30000
    KAFKA_SECURITY_PROTOCOL:  str  = "PLAINTEXT"
    KAFKA_SASL_MECHANISM:     str  = ""
    KAFKA_SASL_USERNAME:      str  = ""
    KAFKA_SASL_PASSWORD:      str  = ""
    ADAPTER_NAME:             str  = "kafka"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["KafkaSettings"]


class KafkaSettings(BaseAdapterSettings):
    """
    Pydantic-settings subclass for the Kafka queue adapter.

    All fields are read from environment variables with the exact names
    shown above. ``BaseAdapterSettings`` provides ``operation_timeout``
    (default 30.0 s) and ``connection_timeout`` (default 30.0 s).
    """

    kafka_bootstrap_servers: str
    kafka_topic: str = "openframe"
    kafka_group_id: str = "openframe-group"
    kafka_auto_offset_reset: str = "earliest"
    kafka_max_poll_records: int = 10
    kafka_session_timeout_ms: int = 30_000
    kafka_request_timeout_ms: int = 30_000
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str = ""
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    adapter_name: str = "kafka"
