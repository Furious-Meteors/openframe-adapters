"""
openframe/adapters/db/redis/config.py
======================================
Settings for the Redis adapter, sourced from environment variables via
``openframe-core``'s ``BaseAdapterSettings`` (pydantic-settings).

Required env vars:
    REDIS_URL: Redis connection URL.
               Format: redis://[:password@]host[:port][/db]
               TLS:    rediss://host:port

Optional env vars (all have defaults):
    REDIS_MAX_CONNECTIONS:        int   = 10
    REDIS_SOCKET_TIMEOUT:         float = 5.0
    REDIS_SOCKET_CONNECT_TIMEOUT: float = 5.0
    REDIS_KEY_PREFIX:             str   = "openframe"
    REDIS_DEFAULT_TTL:            int   = 0
    ADAPTER_NAME:                 str   = "redis"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["RedisSettings"]


class RedisSettings(BaseAdapterSettings):
    """
    Pydantic-settings subclass for the Redis adapter.

    All fields are read from environment variables with the exact names
    shown above. ``BaseAdapterSettings`` provides ``operation_timeout``
    (default 30.0 s) and ``connection_timeout`` (default 30.0 s).
    """

    redis_url: str
    redis_max_connections: int = 10
    redis_socket_timeout: float = 5.0
    redis_socket_connect_timeout: float = 5.0
    redis_key_prefix: str = "openframe"
    redis_default_ttl: int = 0
    adapter_name: str = "redis"
