"""
openframe/adapters/db/mongo/plugin.py
=======================================
OpenFrame plugin wrapper for MongoRepository.

Stability: beta
Capability: "persistence"

Usage via PluginRegistry (optional)::

    from openframe.core.plugins import PluginRegistry
    from openframe.adapters.db.mongo import MongoPlugin, MongoSettings

    registry = PluginRegistry()
    registry.register(MongoPlugin(MongoSettings(), collection="documents"))
    await registry.initialize_all()

    plugin = registry.get("persistence")
    repo = plugin.get_repository()

Usage via deps.py (unchanged, no plugin needed)::

    repo = MongoRepository(MongoSettings(), collection="documents")
"""
# Capability: "persistence"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/composition-root/
from __future__ import annotations

import logging

from openframe.core.ports import BasePort, Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import AdapterConnectionError

from openframe.adapters.db.mongo.config import MongoSettings
from openframe.adapters.db.mongo.connection import _client_cache, get_mongo_client
from openframe.adapters.db.mongo.repository import MongoRepository

__all__ = ["MongoPlugin"]

_logger = logging.getLogger(__name__)


class MongoPlugin(BasePort):
    """
    MongoDB adapter plugin for the OpenFrame plugin registry.

    Capability: "persistence"

    Stability: beta

    Lifecycle:
        initialize() — creates the Motor client (lazy) and verifies
                       connectivity via the repository's health(). Raises
                       AdapterConnectionError if MongoDB is unreachable.
        shutdown()   — closes the client connection. Never raises.
        health()     — delegates to the repository's health() and returns
                       its PluginHealth. Never raises.

    The plugin exposes get_repository() after initialization for use
    in the composition root or ApplicationBootstrap.

    By default constructs a plain MongoRepository. To use a domain-specific
    subclass (e.g. one that overrides _doc_to_entity/_entity_to_doc), pass
    it via repository_class::

        registry.register(MongoPlugin(
            MongoSettings(),
            collection="artifacts",
            repository_class=ArtifactMongoRepository,
        ))
        # registry.get("persistence").get_repository() now returns
        # an ArtifactMongoRepository instance, not a plain MongoRepository.
    """

    name:       str = "openframe-mongo"
    version:    str = "2.0.1"
    capability: Capability = Capability.PERSISTENCE

    def __init__(
        self,
        settings: MongoSettings,
        collection: str = "documents",
        repository_class: type[MongoRepository] = MongoRepository,
    ) -> None:
        """
        Args:
            settings:         MongoSettings instance.
            collection:       MongoDB collection name.
            repository_class: The MongoRepository subclass to construct.
                              Defaults to the base MongoRepository. Pass a
                              domain-specific subclass here to get proper
                              _doc_to_entity()/_entity_to_doc() mapping
                              through get_repository().

        Raises:
            TypeError: repository_class is not a subclass of MongoRepository.
        """
        if not (isinstance(repository_class, type) and issubclass(repository_class, MongoRepository)):
            raise TypeError(
                f"repository_class must be a subclass of MongoRepository, "
                f"got {repository_class!r}"
            )
        self._settings = settings
        self._collection = collection
        self._repository_class = repository_class
        self._repo: MongoRepository | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Initialize the MongoDB client and verify connectivity.

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError: MongoDB is unreachable or credentials
                                    are invalid.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            get_mongo_client(self._settings)
            self._repo = self._repository_class(self._settings, collection=self._collection)
            health = await self._repo.health()
            if health.status != PluginStatus.READY:
                raise AdapterConnectionError(
                    health.message or "MongoDB health check failed after client creation",
                    adapter="mongo",
                    operation="initialize",
                ) from None
            self._status = PluginStatus.READY
            _logger.info(
                "MongoPlugin initialized — %s/%s (repository_class=%s)",
                self._settings.mongo_url.split("@")[-1],
                self._settings.mongo_database,
                self._repository_class.__name__,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Close the MongoDB client connection.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._repo is not None:
                await self._repo.close()
                _logger.info("MongoPlugin shutdown complete.")
        except Exception as exc:
            _logger.error("MongoPlugin shutdown error (ignored): %s", exc)
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

    def get_repository(self) -> MongoRepository:
        """
        Return the initialized repository.

        Only valid after registry.initialize_all() has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._repo is None or self._status != PluginStatus.READY:
            raise RuntimeError(
                f"MongoPlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        return self._repo
