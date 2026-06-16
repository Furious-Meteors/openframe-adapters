"""
openframe.adapters.db.redis
=============================
Redis key-value adapter for the OpenFrame Microservice Suite.

Public API::

    RedisSettings      — Pydantic Settings subclass for connection config.
    RedisRepository    — Generic async repository (BaseRepository + HealthCheck).
    get_redis_client   — Async factory that creates / returns the cached client.
    RedisPlugin        — OpenFramePlugin implementation for PluginRegistry.

Quick start::

    from openframe.adapters.db.redis import (
        RedisSettings,
        RedisRepository,
        get_redis_client,
    )

    settings = RedisSettings(redis_url="redis://localhost:6379/0")

    # Raw dict mode
    repo = RedisRepository(settings)
    item = await repo.get("abc-123")           # dict | None

    # Typed mode — subclass and override mapping methods
    class SessionRepository(RedisRepository[Session]):
        def _dict_to_entity(self, data: dict) -> Session:
            return Session(**data)
        def _entity_to_dict(self, entity: Session) -> dict:
            return entity.model_dump()
"""
from __future__ import annotations

from .config import RedisSettings
from .connection import get_redis_client
from .plugin import RedisPlugin
from .repository import RedisRepository

__all__ = [
    "RedisSettings",
    "RedisRepository",
    "get_redis_client",
    "RedisPlugin",
]
