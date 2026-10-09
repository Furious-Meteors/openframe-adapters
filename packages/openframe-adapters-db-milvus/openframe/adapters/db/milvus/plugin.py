"""
openframe/adapters/db/milvus/plugin.py
=========================================
OpenFrame plugin wrapper for MilvusRepository.

Stability: beta

Recommended usage — ApplicationBootstrap.compose() (openframe-core>=3.3)::

    from openframe.core.runtime import ApplicationBootstrap
    from openframe.core.ports import Capability
    from openframe.adapters.db.milvus import MilvusPlugin, MilvusSettings

    plugin = MilvusPlugin(MilvusSettings(), collection_name="items")

    async with ApplicationBootstrap.compose(plugin) as app:
        repo = app.get(Capability.SEARCH)
        item = await repo.get("abc-123")
    # plugin.shutdown() ran automatically on exit

Use a subclassed ApplicationBootstrap (configure()) instead when you need
per-port config=/init_timeout= or conditional registration order, and
app.registry (PluginRegistry escape hatch) only for what neither tier
covers.

For tests, scripts, or anywhere plugin lifecycle management isn't needed,
construct MilvusRepository directly (no plugin required)::

    repo = MilvusRepository(MilvusSettings(), collection_name="items")
    traced = TracingProxy(repo, prefix="repository.item")
"""
# Capability: "search"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/how-it-works/#choosing-a-wiring-pattern
from __future__ import annotations

import logging

from openframe.core.exceptions import AdapterConnectionError
from openframe.core.ports import BasePort, Capability, PluginContext, PluginHealth, PluginStatus

from openframe.adapters.db.milvus.config import MilvusSettings
from openframe.adapters.db.milvus.connection import get_milvus_client
from openframe.adapters.db.milvus.repository import MilvusRepository

__all__ = ["MilvusPlugin"]

_logger = logging.getLogger(__name__)


class MilvusPlugin(BasePort):
    """
    Milvus adapter plugin for the OpenFrame plugin registry.

    Capability: "search"

    Stability: beta

    Lifecycle:
        initialize() — creates the AsyncMilvusClient and verifies
                       connectivity via the repository's health(). Raises
                       AdapterConnectionError if Milvus is unreachable.
        shutdown()   — closes the client. Never raises.
        health()     — delegates to the repository's health() and returns
                       its PluginHealth. Never raises.

    The plugin exposes get_repository() after initialization for use
    in the composition root or ApplicationBootstrap.

    By default constructs a plain MilvusRepository. To use a domain-specific
    subclass, pass it via repository_class::

        registry.register(MilvusPlugin(
            MilvusSettings(),
            collection_name="items",
            repository_class=ItemMilvusRepository,
        ))
    """

    name:       str = "openframe-milvus"
    version:    str = "0.1.0"
    capability: Capability = Capability.SEARCH

    def __init__(
        self,
        settings: MilvusSettings,
        collection_name: str = "",
        id_field: str = "id",
        vector_field: str = "vector",
        repository_class: type[MilvusRepository] = MilvusRepository,
    ) -> None:
        """
        Args:
            settings:         MilvusSettings instance.
            collection_name:  Collection name. If omitted the plugin acts
                              as a connection manager only (no
                              get_repository()).
            id_field:         Primary key field name. Defaults to "id".
            vector_field:     Vector field name. Defaults to "vector".
            repository_class: The MilvusRepository subclass to construct.
                              Defaults to the base MilvusRepository. Pass a
                              domain-specific subclass here to get proper
                              entity mapping through get_repository().

        Raises:
            TypeError: repository_class is not a subclass of MilvusRepository.
        """
        if not (isinstance(repository_class, type) and issubclass(repository_class, MilvusRepository)):
            raise TypeError(
                f"repository_class must be a subclass of MilvusRepository, "
                f"got {repository_class!r}"
            )
        self._settings = settings
        self._collection_name = collection_name
        self._id_field = id_field
        self._vector_field = vector_field
        self._repository_class = repository_class
        self._repo: MilvusRepository | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Initialize the Milvus client and verify connectivity.

        If a ``collection_name`` was passed to the constructor, a
        :class:`MilvusRepository` is created and connectivity is verified
        via ``repo.health()``. When no collection is provided the plugin
        is used purely as a connection manager: connectivity is verified
        with a direct ``list_collections()`` call.

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError: Milvus is unreachable or credentials
                                    are invalid.
            AdapterConfigurationError: milvus_uri is malformed.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            client = await get_milvus_client(self._settings)
            if self._collection_name:
                self._repo = self._repository_class(
                    self._settings,
                    collection_name=self._collection_name,
                    id_field=self._id_field,
                    vector_field=self._vector_field,
                )
                health = await self._repo.health()
                if health.status != PluginStatus.READY:
                    raise AdapterConnectionError(
                        health.message or "Milvus health check failed after client creation",
                        adapter="milvus",
                        operation="initialize",
                    ) from None
            else:
                try:
                    await client.list_collections()
                except Exception as exc:
                    raise AdapterConnectionError(
                        f"Milvus connectivity check failed: {exc}",
                        adapter="milvus",
                        operation="initialize",
                    ) from exc
            self._status = PluginStatus.READY
            _logger.info(
                "MilvusPlugin initialized — %s (repository_class=%s)",
                self._settings.milvus_uri,
                self._repository_class.__name__,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Close the Milvus client.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._repo is not None:
                await self._repo.close()
                _logger.info("MilvusPlugin shutdown complete.")
        except Exception as exc:
            _logger.error("MilvusPlugin shutdown error (ignored): %s", exc)
        finally:
            self._status = PluginStatus.STOPPED

    async def health(self) -> PluginHealth:
        """
        Return current health snapshot.

        Delegates to the repository's own ``health()`` when one exists —
        no translation needed, it already returns a ``PluginHealth``. When
        the plugin was constructed without a collection (connection-manager-
        only mode, no repository), the client is checked directly.

        Never raises — returns FAILED status on any exception.
        """
        try:
            if self._status != PluginStatus.READY:
                return PluginHealth(
                    status=PluginStatus.FAILED,
                    message=f"Plugin status: {self._status.name}",
                )
            if self._repo is not None:
                return await self._repo.health()
            try:
                client = await get_milvus_client(self._settings)
                await client.list_collections()
                return PluginHealth(status=PluginStatus.READY, message="")
            except Exception as exc:
                return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
        except Exception as exc:
            return PluginHealth(
                status=PluginStatus.FAILED,
                message=str(exc),
            )

    def get_repository(self) -> MilvusRepository:
        """
        Return the initialized repository.

        Only valid after registry.initialize_all() has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._status != PluginStatus.READY:
            raise RuntimeError(
                f"MilvusPlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        if self._repo is None:
            raise RuntimeError(
                "MilvusPlugin was initialized without a collection name. "
                "Pass collection_name=... to MilvusPlugin() to enable get_repository()."
            )
        return self._repo
