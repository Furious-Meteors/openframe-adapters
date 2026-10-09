"""
openframe/adapters/db/chromadb/plugin.py
===========================================
OpenFrame plugin wrapper for ChromaDBRepository.

Stability: beta

Recommended usage — ApplicationBootstrap.compose() (openframe-core>=3.3)::

    from openframe.core.runtime import ApplicationBootstrap
    from openframe.core.ports import Capability
    from openframe.adapters.db.chromadb import ChromaDBPlugin, ChromaDBSettings

    plugin = ChromaDBPlugin(ChromaDBSettings(), collection="items")

    async with ApplicationBootstrap.compose(plugin) as app:
        repo = app.get(Capability.SEARCH)
        results = await repo.search(query_vector=[0.1, 0.2, 0.3], k=5)
    # plugin.shutdown() ran automatically on exit

Use a subclassed ApplicationBootstrap (configure()) instead when you need
per-port config=/init_timeout= or conditional registration order, and
app.registry (PluginRegistry escape hatch) only for what neither tier
covers.

For tests, scripts, or anywhere plugin lifecycle management isn't needed,
construct ChromaDBRepository directly (no plugin required)::

    repo = ChromaDBRepository(ChromaDBSettings(), collection="items")
    traced = TracingProxy(repo, prefix="repository.item")
"""
# Capability: "search"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/how-it-works/#choosing-a-wiring-pattern
from __future__ import annotations

import logging

from openframe.core.exceptions import AdapterConnectionError
from openframe.core.ports import BasePort, Capability, PluginContext, PluginHealth, PluginStatus

from openframe.adapters.db.chromadb.config import ChromaDBSettings
from openframe.adapters.db.chromadb.repository import ChromaDBRepository

__all__ = ["ChromaDBPlugin"]

_logger = logging.getLogger(__name__)


class ChromaDBPlugin(BasePort):
    """
    ChromaDB adapter plugin for the OpenFrame plugin registry.

    Capability: "search"

    Stability: beta

    Lifecycle:
        initialize() — creates the repository (and, transitively, the
                       cached ``AsyncClientAPI``/collection handle) and
                       verifies connectivity via the repository's
                       health(). Raises AdapterConnectionError if Chroma
                       is unreachable.
        shutdown()   — releases the repository's collection handle
                       reference. Never raises.
        health()     — delegates to the repository's health() and returns
                       its PluginHealth. Never raises.

    The plugin exposes get_repository() after initialization for use
    in the composition root or ApplicationBootstrap.

    By default constructs a plain ChromaDBRepository. To use a
    domain-specific subclass, pass it via repository_class::

        registry.register(ChromaDBPlugin(
            ChromaDBSettings(),
            collection="items",
            repository_class=ItemChromaDBRepository,
        ))
    """

    name:       str = "openframe-chromadb"
    version:    str = "0.1.0"
    capability: Capability = Capability.SEARCH

    def __init__(
        self,
        settings: ChromaDBSettings,
        collection: str = "",
        repository_class: type[ChromaDBRepository] = ChromaDBRepository,
    ) -> None:
        """
        Args:
            settings:         ChromaDBSettings instance.
            collection:       Collection name. If omitted, falls back to
                              ``repository_class``'s ``_collection`` class
                              attribute or ``settings.chroma_collection``.
            repository_class: The ChromaDBRepository subclass to construct.
                              Defaults to the base ChromaDBRepository. Pass
                              a domain-specific subclass here to get proper
                              entity mapping through get_repository().

        Raises:
            TypeError: repository_class is not a subclass of ChromaDBRepository.
        """
        if not (
            isinstance(repository_class, type)
            and issubclass(repository_class, ChromaDBRepository)
        ):
            raise TypeError(
                f"repository_class must be a subclass of ChromaDBRepository, "
                f"got {repository_class!r}"
            )
        self._settings = settings
        self._collection = collection
        self._repository_class = repository_class
        self._repo: ChromaDBRepository | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Initialize the ChromaDB repository and verify connectivity.

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError:    Chroma is unreachable.
            AdapterConfigurationError: No collection name is available.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            self._repo = self._repository_class(
                self._settings,
                collection=self._collection or None,
            )
            health = await self._repo.health()
            if health.status != PluginStatus.READY:
                raise AdapterConnectionError(
                    health.message or "Chroma health check failed after collection creation",
                    adapter=self._settings.adapter_name,
                    operation="initialize",
                ) from None
            self._status = PluginStatus.READY
            _logger.info(
                "ChromaDBPlugin initialized — %s:%s (repository_class=%s)",
                self._settings.chroma_host,
                self._settings.chroma_port,
                self._repository_class.__name__,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Release the repository's collection handle reference.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._repo is not None:
                await self._repo.close()
                _logger.info("ChromaDBPlugin shutdown complete.")
        except Exception as exc:
            _logger.error("ChromaDBPlugin shutdown error (ignored): %s", exc)
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
            if self._status != PluginStatus.READY:
                return PluginHealth(
                    status=PluginStatus.FAILED,
                    message=f"Plugin status: {self._status.name}",
                )
            if self._repo is not None:
                return await self._repo.health()
            return PluginHealth(status=PluginStatus.FAILED, message="Repository not initialized")
        except Exception as exc:
            return PluginHealth(
                status=PluginStatus.FAILED,
                message=str(exc),
            )

    def get_repository(self) -> ChromaDBRepository:
        """
        Return the initialized repository.

        Only valid after registry.initialize_all() has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._status != PluginStatus.READY:
            raise RuntimeError(
                f"ChromaDBPlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        if self._repo is None:
            raise RuntimeError("ChromaDBPlugin failed to construct its repository.")
        return self._repo
