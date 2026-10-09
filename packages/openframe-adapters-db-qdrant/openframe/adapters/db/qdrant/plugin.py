"""
openframe/adapters/db/qdrant/plugin.py
=========================================
OpenFrame plugin wrapper for QdrantVectorStore.

Stability: beta

Recommended usage — ApplicationBootstrap.compose() (openframe-core>=3.3)::

    from openframe.core.runtime import ApplicationBootstrap
    from openframe.core.ports import Capability
    from openframe.adapters.db.qdrant import QdrantPlugin, QdrantSettings

    plugin = QdrantPlugin(QdrantSettings(), collection="documents")

    async with ApplicationBootstrap.compose(plugin) as app:
        store = app.get(Capability.SEARCH)
        results = await store.search(query_vector=[0.1, 0.2, 0.3], k=5)
    # plugin.shutdown() ran automatically on exit

Use a subclassed ApplicationBootstrap (configure()) instead when you need
per-port config=/init_timeout= or conditional registration order, and
app.registry (PluginRegistry escape hatch) only for what neither tier
covers.

For tests, scripts, or anywhere plugin lifecycle management isn't needed,
construct QdrantVectorStore directly (no plugin required)::

    store = QdrantVectorStore(QdrantSettings(), collection="documents")
    traced = TracingProxy(store, prefix="vectorstore.documents")
"""
# Capability: "search"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/how-it-works/#choosing-a-wiring-pattern
from __future__ import annotations

import logging

from openframe.core.ports import BasePort, Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import AdapterConnectionError

from openframe.adapters.db.qdrant.config import QdrantSettings
from openframe.adapters.db.qdrant.connection import get_qdrant_client
from openframe.adapters.db.qdrant.repository import QdrantVectorStore

__all__ = ["QdrantPlugin"]

_logger = logging.getLogger(__name__)


class QdrantPlugin(BasePort):
    """
    Qdrant adapter plugin for the OpenFrame plugin registry.

    Capability: "search"

    Stability: beta

    Lifecycle:
        initialize() — creates the AsyncQdrantClient and verifies
                       connectivity via the store's health(). Raises
                       AdapterConnectionError if Qdrant is unreachable.
        shutdown()   — closes the client. Never raises.
        health()     — delegates to the store's health() and returns
                       its PluginHealth. Never raises.

    The plugin exposes get_repository() after initialization for use
    in the composition root or ApplicationBootstrap.

    By default constructs a plain QdrantVectorStore. To use a
    domain-specific subclass, pass it via repository_class::

        registry.register(QdrantPlugin(
            QdrantSettings(),
            collection="documents",
            repository_class=DocumentQdrantStore,
        ))
    """

    name:       str = "openframe-qdrant"
    version:    str = "0.1.0"
    capability: Capability = Capability.SEARCH

    def __init__(
        self,
        settings: QdrantSettings,
        collection: str = "",
        repository_class: type[QdrantVectorStore] = QdrantVectorStore,
    ) -> None:
        """
        Args:
            settings:         QdrantSettings instance.
            collection:       Collection name. If omitted the plugin acts
                              as a connection manager only (no
                              get_repository()).
            repository_class: The QdrantVectorStore subclass to construct.
                              Defaults to the base QdrantVectorStore. Pass
                              a domain-specific subclass here to get
                              proper entity mapping through
                              get_repository().

        Raises:
            TypeError: repository_class is not a subclass of QdrantVectorStore.
        """
        if not (isinstance(repository_class, type) and issubclass(repository_class, QdrantVectorStore)):
            raise TypeError(
                f"repository_class must be a subclass of QdrantVectorStore, "
                f"got {repository_class!r}"
            )
        self._settings = settings
        self._collection = collection
        self._repository_class = repository_class
        self._repo: QdrantVectorStore | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Initialize the Qdrant client and verify connectivity.

        If a ``collection`` was passed to the constructor, a
        :class:`QdrantVectorStore` is created and connectivity is
        verified via ``store.health()``. When no collection is provided
        the plugin is used purely as a connection manager: connectivity
        is verified with a direct ``get_collections()`` against the
        client.

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError: Qdrant is unreachable or credentials
                                    are invalid.
            AdapterConfigurationError: qdrant_url is malformed.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            client = await get_qdrant_client(self._settings)
            if self._collection:
                self._repo = self._repository_class(
                    self._settings,
                    collection=self._collection,
                )
                health = await self._repo.health()
                if health.status != PluginStatus.READY:
                    raise AdapterConnectionError(
                        health.message or "Qdrant health check failed after client creation",
                        adapter="qdrant",
                        operation="initialize",
                    ) from None
            else:
                try:
                    await client.get_collections()
                except Exception as exc:
                    raise AdapterConnectionError(
                        f"Qdrant connectivity check failed: {exc}",
                        adapter="qdrant",
                        operation="initialize",
                    ) from exc
            self._status = PluginStatus.READY
            _logger.info(
                "QdrantPlugin initialized — %s (repository_class=%s)",
                self._settings.qdrant_url,
                self._repository_class.__name__,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Close the Qdrant client.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._repo is not None:
                await self._repo.close()
                _logger.info("QdrantPlugin shutdown complete.")
        except Exception as exc:
            _logger.error("QdrantPlugin shutdown error (ignored): %s", exc)
        finally:
            self._status = PluginStatus.STOPPED

    async def health(self) -> PluginHealth:
        """
        Return current health snapshot.

        Delegates to the store's own ``health()`` when one exists — no
        translation needed, it already returns a ``PluginHealth``. When
        the plugin was constructed without a collection
        (connection-manager-only mode, no store), the client is checked
        directly.

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
                client = await get_qdrant_client(self._settings)
                await client.get_collections()
                return PluginHealth(status=PluginStatus.READY, message="")
            except Exception as exc:
                return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
        except Exception as exc:
            return PluginHealth(
                status=PluginStatus.FAILED,
                message=str(exc),
            )

    def get_repository(self) -> QdrantVectorStore:
        """
        Return the initialized store.

        Only valid after registry.initialize_all() has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._status != PluginStatus.READY:
            raise RuntimeError(
                f"QdrantPlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        if self._repo is None:
            raise RuntimeError(
                "QdrantPlugin was initialized without a collection name. "
                "Pass collection=... to QdrantPlugin() to enable get_repository()."
            )
        return self._repo
