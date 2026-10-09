"""
openframe/adapters/db/cassandra/plugin.py
=============================================
OpenFrame plugin wrapper for CassandraRepository.

Stability: beta

Recommended usage — ApplicationBootstrap.compose() (openframe-core>=3.3)::

    from openframe.core.runtime import ApplicationBootstrap
    from openframe.core.ports import Capability
    from openframe.adapters.db.cassandra import CassandraPlugin, CassandraSettings

    plugin = CassandraPlugin(
        CassandraSettings(cassandra_contact_points=["10.0.0.1"]),
        table="items",
        id_column="id",
    )

    async with ApplicationBootstrap.compose(plugin) as app:
        repo = app.get(Capability.PERSISTENCE)
        item = await repo.get("abc-123")
    # plugin.shutdown() ran automatically on exit

Use a subclassed ApplicationBootstrap (configure()) instead when you need
per-port config=/init_timeout= or conditional registration order, and
app.registry (PluginRegistry escape hatch) only for what neither tier
covers.

For tests, scripts, or anywhere plugin lifecycle management isn't needed,
construct CassandraRepository directly (no plugin required)::

    repo = CassandraRepository(CassandraSettings(cassandra_contact_points=["10.0.0.1"]))
    traced = TracingProxy(repo, prefix="repository.item")
"""
# Capability: "persistence"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/how-it-works/#choosing-a-wiring-pattern
from __future__ import annotations

import logging

from openframe.core.ports import BasePort, Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import AdapterConnectionError

from openframe.adapters.db.cassandra.config import CassandraSettings
from openframe.adapters.db.cassandra.connection import get_cassandra_session
from openframe.adapters.db.cassandra.repository import CassandraRepository

__all__ = ["CassandraPlugin"]

_logger = logging.getLogger(__name__)


class CassandraPlugin(BasePort):
    """
    Cassandra adapter plugin for the OpenFrame plugin registry.

    Capability: "persistence"

    Stability: beta

    Lifecycle:
        initialize() — creates the cassandra-driver Session and verifies
                       connectivity via the repository's health(). Raises
                       AdapterConnectionError if the cluster is unreachable.
        shutdown()   — shuts down the cluster/session. Never raises.
        health()     — delegates to the repository's health() and returns
                       its PluginHealth. Never raises.

    The plugin exposes get_repository() after initialization for use in the
    composition root or ApplicationBootstrap.

    By default constructs a plain CassandraRepository. To use a
    domain-specific subclass, pass it via repository_class::

        registry.register(CassandraPlugin(
            CassandraSettings(cassandra_contact_points=["10.0.0.1"]),
            table="items",
            id_column="id",
            repository_class=ItemCassandraRepository,
        ))
    """

    name:       str = "openframe-cassandra"
    version:    str = "0.1.0"
    capability: Capability = Capability.PERSISTENCE

    def __init__(
        self,
        settings: CassandraSettings,
        table: str = "",
        id_column: str = "id",
        repository_class: type[CassandraRepository] = CassandraRepository,
    ) -> None:
        """
        Args:
            settings:         CassandraSettings instance.
            table:            Table name. If omitted the plugin acts as a
                              connection manager only (no get_repository()).
            id_column:        Primary key column name. Defaults to "id".
            repository_class: The CassandraRepository subclass to construct.
                              Defaults to the base CassandraRepository. Pass
                              a domain-specific subclass here to get proper
                              entity mapping through get_repository().

        Raises:
            TypeError: repository_class is not a subclass of CassandraRepository.
        """
        if not (
            isinstance(repository_class, type)
            and issubclass(repository_class, CassandraRepository)
        ):
            raise TypeError(
                f"repository_class must be a subclass of CassandraRepository, "
                f"got {repository_class!r}"
            )
        self._settings = settings
        self._table = table
        self._id_column = id_column
        self._repository_class = repository_class
        self._repo: CassandraRepository | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Initialize the Cassandra session and verify connectivity.

        If a ``table`` was passed to the constructor, a
        :class:`CassandraRepository` is created and connectivity is verified
        via ``repo.health()``. When no table is provided the plugin is used
        purely as a connection manager: connectivity is verified with a
        direct query against the session.

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError: Cassandra is unreachable or credentials
                                    are invalid.
            AdapterConfigurationError: contact points list is malformed.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            session = await get_cassandra_session(self._settings)
            if self._table:
                self._repo = self._repository_class(
                    self._settings,
                    table=self._table,
                    id_column=self._id_column,
                )
                health = await self._repo.health()
                if health.status != PluginStatus.READY:
                    raise AdapterConnectionError(
                        health.message or "Cassandra health check failed after session creation",
                        adapter="cassandra",
                        operation="initialize",
                    ) from None
            else:
                # Verify session connectivity without requiring a table.
                try:
                    import asyncio

                    from openframe.adapters.db.cassandra.connection import (
                        _response_future_to_asyncio,
                    )

                    loop = asyncio.get_running_loop()
                    response_future = session.execute_async(
                        "SELECT now() FROM system.local"
                    )
                    await asyncio.wait_for(
                        _response_future_to_asyncio(response_future, loop), timeout=5.0
                    )
                except Exception as exc:
                    raise AdapterConnectionError(
                        f"Cassandra connectivity check failed: {exc}",
                        adapter="cassandra",
                        operation="initialize",
                    ) from exc
            self._status = PluginStatus.READY
            _logger.info(
                "CassandraPlugin initialized — contact_points=%s (repository_class=%s)",
                self._settings.cassandra_contact_points,
                self._repository_class.__name__,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Shut down the Cassandra cluster/session.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._repo is not None:
                await self._repo.close()
                _logger.info("CassandraPlugin shutdown complete.")
        except Exception as exc:
            _logger.error("CassandraPlugin shutdown error (ignored): %s", exc)
        finally:
            self._status = PluginStatus.STOPPED

    async def health(self) -> PluginHealth:
        """
        Return current health snapshot.

        Delegates to the repository's own ``health()`` when one exists — no
        translation needed, it already returns a ``PluginHealth``. When the
        plugin was constructed without a table (connection-manager-only
        mode, no repository), the session is checked directly.

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
            # No repository — check the session directly.
            try:
                import asyncio

                from openframe.adapters.db.cassandra.connection import (
                    _response_future_to_asyncio,
                )

                session = await get_cassandra_session(self._settings)
                loop = asyncio.get_running_loop()
                response_future = session.execute_async("SELECT now() FROM system.local")
                await asyncio.wait_for(
                    _response_future_to_asyncio(response_future, loop), timeout=5.0
                )
                return PluginHealth(status=PluginStatus.READY, message="")
            except Exception as exc:
                return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
        except Exception as exc:
            return PluginHealth(
                status=PluginStatus.FAILED,
                message=str(exc),
            )

    def get_repository(self) -> CassandraRepository:
        """
        Return the initialized repository.

        Only valid after registry.initialize_all() has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._status != PluginStatus.READY:
            raise RuntimeError(
                f"CassandraPlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        if self._repo is None:
            raise RuntimeError(
                "CassandraPlugin was initialized without a table name. "
                "Pass table=... to CassandraPlugin() to enable get_repository()."
            )
        return self._repo
