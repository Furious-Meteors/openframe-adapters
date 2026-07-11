"""
openframe/adapters/db/redis/plugin.py
======================================
OpenFrame plugin wrapper for RedisRepository.

Stability: beta
Capability: "cache"

Redis is the cache layer in the ``rest-min`` bundle.
Postgres is ``"persistence"``. Both can coexist in the same ``PluginRegistry``
because they expose different capabilities::

    registry.get(Capability.PERSISTENCE)  # → PostgresPlugin
    registry.get(Capability.CACHE)        # → RedisPlugin

Usage via PluginRegistry (optional)::

    from openframe.core.plugins import PluginRegistry
    from openframe.core.ports import Capability
    from openframe.adapters.db.redis import RedisPlugin, RedisSettings

    registry = PluginRegistry()
    registry.register(RedisPlugin(RedisSettings()))
    await registry.initialize_all()

    plugin = registry.get(Capability.CACHE)
    repo = plugin.get_repository()

Usage via deps.py (unchanged, no plugin needed)::

    repo = RedisRepository(RedisSettings())
"""
# Capability: "cache"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/composition-root/
from __future__ import annotations

import logging

from openframe.core.ports import BasePort, Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import AdapterConnectionError

from openframe.adapters.db.redis.config import RedisSettings
from openframe.adapters.db.redis.connection import _client_cache, get_redis_client
from openframe.adapters.db.redis.repository import RedisRepository

__all__ = ["RedisPlugin"]

_logger = logging.getLogger(__name__)


class RedisPlugin(BasePort):
    """
    Redis adapter plugin for the OpenFrame plugin registry.

    Capability: "cache"

    Lifecycle:
        initialize() — creates the Redis client and verifies connectivity
                       via the repository's health(). Raises
                       ``AdapterConnectionError`` if Redis is unreachable.
        shutdown()   — closes the Redis client. Never raises.
        health()     — delegates to the repository's health() and returns
                       its PluginHealth. Never raises.

    The plugin exposes get_repository() after initialization for use
    in the composition root or ApplicationBootstrap.

    By default constructs a plain RedisRepository. To use a domain-specific
    subclass, pass it via repository_class::

        # With a domain-specific subclass:
        registry.register(RedisPlugin(
            RedisSettings(),
            repository_class=SessionRedisRepository,
        ))
    """

    name:       str = "openframe-redis"
    version:    str = "2.0.3"
    capability: Capability = Capability.CACHE

    def __init__(
        self,
        settings: RedisSettings,
        repository_class: type[RedisRepository] = RedisRepository,
    ) -> None:
        """
        Args:
            settings:         RedisSettings instance.
            repository_class: The RedisRepository subclass to construct.
                              Defaults to the base RedisRepository. Pass a
                              domain-specific subclass here to get proper
                              _serialise()/_deserialise() overrides through
                              get_repository().

        Raises:
            TypeError: repository_class is not a subclass of RedisRepository.
        """
        if not (
            isinstance(repository_class, type)
            and issubclass(repository_class, RedisRepository)
        ):
            raise TypeError(
                f"repository_class must be a subclass of RedisRepository, "
                f"got {repository_class!r}"
            )
        self._settings = settings
        self._repository_class = repository_class
        self._repo: RedisRepository | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Initialize the Redis client and verify connectivity.

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError: Redis is unreachable or credentials
                                    are invalid.
            AdapterConfigurationError: REDIS_URL is malformed.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            await get_redis_client(self._settings)
            self._repo = self._repository_class(self._settings)
            health = await self._repo.health()
            if health.status != PluginStatus.READY:
                raise AdapterConnectionError(
                    health.message or "Redis health check failed after client creation",
                    adapter="redis",
                    operation="initialize",
                ) from None
            self._status = PluginStatus.READY
            _logger.info(
                "RedisPlugin initialized — %s (repository_class=%s)",
                self._settings.redis_url.split("@")[-1],
                self._repository_class.__name__,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Close the Redis client.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._repo is not None:
                await self._repo.close()
                _logger.info("RedisPlugin shutdown complete.")
        except Exception as exc:
            _logger.error("RedisPlugin shutdown error (ignored): %s", exc)
        finally:
            self._status = PluginStatus.STOPPED

    async def health(self) -> PluginHealth:
        """
        Return current health snapshot.

        Delegates to the repository's own ``health()`` — no translation
        needed, it already returns a ``PluginHealth``.

        Never raises — returns FAILED status on any exception.
        """
        try:
            if self._repo is None or self._status != PluginStatus.READY:
                return PluginHealth(
                    status=PluginStatus.FAILED,
                    message=f"Plugin status: {self._status.name}",
                )
            return await self._repo.health()
        except Exception as exc:
            return PluginHealth(
                status=PluginStatus.FAILED,
                message=str(exc),
            )

    def get_repository(self) -> RedisRepository:
        """
        Return the initialized repository.

        Only valid after registry.initialize_all() has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._repo is None or self._status != PluginStatus.READY:
            raise RuntimeError(
                f"RedisPlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        return self._repo
