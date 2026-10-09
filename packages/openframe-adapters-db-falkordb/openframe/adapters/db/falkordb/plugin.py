"""
openframe/adapters/db/falkordb/plugin.py
===========================================
OpenFrame plugin wrapper for FalkorDBRepository.

Stability: experimental (new ``BaseGraphStore`` port, openframe-core>=3.6)
Capability: "search"

``Capability.SEARCH`` is reused (not a new taxonomy member) — the same
precedent the vector-store adapters (Qdrant/Milvus/Chroma) set: graph
stores, like vector stores, are reference adapters for the ``SEARCH``
capability rather than ``PERSISTENCE``, even though both extend
``BaseRepository`` and offer full CRUD.

Recommended usage — ApplicationBootstrap.compose() (requires openframe-core>=3.3)::

    from openframe.core.runtime import ApplicationBootstrap
    from openframe.core.ports import Capability
    from openframe.adapters.db.falkordb import FalkorDBPlugin, FalkorDBSettings

    async with ApplicationBootstrap.compose(FalkorDBPlugin(FalkorDBSettings())) as app:
        repo = app.get(Capability.SEARCH).get_repository()
        await repo.create({"id": "1", "name": "Alice"})
        friends = await repo.traverse(
            "MATCH (a:Entity)-[:KNOWS]->(b:Entity) WHERE a.id = $id RETURN b",
            {"id": "1"},
        )

Reach for a subclassed ``ApplicationBootstrap`` with ``configure()`` instead
of ``compose()`` once this plugin needs its own ``config=``/``init_timeout=``,
or registration order that depends on a runtime condition. For what neither
tier covers (e.g. ``get_all()`` for an intentional multi-port-per-capability
setup), use ``app.registry`` — the underlying ``PluginRegistry``.

No lifecycle needed (tests/scripts) — construct the repository directly::

    repo = FalkorDBRepository(FalkorDBSettings())
"""
# Capability: "search"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/how-it-works/#choosing-a-wiring-pattern
from __future__ import annotations

import logging

from openframe.core.ports import BasePort, Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import AdapterConnectionError

from openframe.adapters.db.falkordb.config import FalkorDBSettings
from openframe.adapters.db.falkordb.connection import _client_cache, get_falkordb_client
from openframe.adapters.db.falkordb.repository import FalkorDBRepository

__all__ = ["FalkorDBPlugin"]

_logger = logging.getLogger(__name__)


class FalkorDBPlugin(BasePort):
    """
    FalkorDB adapter plugin for the OpenFrame plugin registry.

    Capability: "search"

    Lifecycle:
        initialize() — creates the FalkorDB client, verifies connectivity,
                       and ensures the repository's ``(label, id)`` unique
                       constraint exists, via the repository's own
                       ``initialize()``. Raises ``AdapterConnectionError``
                       if FalkorDB is unreachable.
        shutdown()   — closes the FalkorDB client. Never raises.
        health()     — delegates to the repository's health() and returns
                       its PluginHealth. Never raises.

    The plugin exposes get_repository() after initialization for use
    in the composition root or ApplicationBootstrap.

    By default constructs a plain FalkorDBRepository. To use a domain-specific
    subclass, pass it via repository_class::

        # With a domain-specific subclass:
        registry.register(FalkorDBPlugin(
            FalkorDBSettings(),
            repository_class=PersonGraphRepository,
        ))
    """

    name:       str = "openframe-falkordb"
    version:    str = "0.1.0"
    capability: Capability = Capability.SEARCH

    def __init__(
        self,
        settings: FalkorDBSettings,
        repository_class: type[FalkorDBRepository] = FalkorDBRepository,
    ) -> None:
        """
        Args:
            settings:         FalkorDBSettings instance.
            repository_class: The FalkorDBRepository subclass to construct.
                              Defaults to the base FalkorDBRepository. Pass a
                              domain-specific subclass here to get proper
                              _entity_to_properties()/_node_to_entity()
                              overrides through get_repository().

        Raises:
            TypeError: repository_class is not a subclass of FalkorDBRepository.
        """
        if not (
            isinstance(repository_class, type)
            and issubclass(repository_class, FalkorDBRepository)
        ):
            raise TypeError(
                f"repository_class must be a subclass of FalkorDBRepository, "
                f"got {repository_class!r}"
            )
        self._settings = settings
        self._repository_class = repository_class
        self._repo: FalkorDBRepository | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Initialize the FalkorDB client and verify connectivity.

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError: FalkorDB is unreachable or credentials
                                    are invalid.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            await get_falkordb_client(self._settings)
            self._repo = self._repository_class(self._settings)
            health = await self._repo.health()
            if health.status != PluginStatus.READY:
                raise AdapterConnectionError(
                    health.message or "FalkorDB health check failed after client creation",
                    adapter="falkordb",
                    operation="initialize",
                ) from None
            # Ensure the (label, id) unique constraint exists before this
            # plugin is marked READY — see FalkorDBRepository._ensure_ready().
            await self._repo._ensure_ready()
            self._status = PluginStatus.READY
            _logger.info(
                "FalkorDBPlugin initialized — %s:%s (repository_class=%s)",
                self._settings.falkordb_host,
                self._settings.falkordb_port,
                self._repository_class.__name__,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Close the FalkorDB client.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._repo is not None:
                await self._repo.close()
                _logger.info("FalkorDBPlugin shutdown complete.")
        except Exception as exc:
            _logger.error("FalkorDBPlugin shutdown error (ignored): %s", exc)
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

    def get_repository(self) -> FalkorDBRepository:
        """
        Return the initialized repository.

        Only valid after registry.initialize_all() has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._repo is None or self._status != PluginStatus.READY:
            raise RuntimeError(
                f"FalkorDBPlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        return self._repo
