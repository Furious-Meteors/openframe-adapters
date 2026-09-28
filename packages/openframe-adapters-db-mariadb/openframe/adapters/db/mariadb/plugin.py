"""
openframe/adapters/db/mariadb/plugin.py
==========================================
OpenFrame plugin wrapper for MariadbRepository.

Stability: beta

Recommended usage — ApplicationBootstrap.compose() (openframe-core>=3.3)::

    from openframe.core.runtime import ApplicationBootstrap
    from openframe.core.ports import Capability
    from openframe.adapters.db.mariadb import MariadbPlugin, MariadbSettings

    plugin = MariadbPlugin(MariadbSettings(), table="items", id_column="id")

    async with ApplicationBootstrap.compose(plugin) as app:
        repo = app.get(Capability.PERSISTENCE)
        item = await repo.get("abc-123")
    # plugin.shutdown() ran automatically on exit

Use a subclassed ApplicationBootstrap (configure()) instead when you need
per-port config=/init_timeout= or conditional registration order, and
app.registry (PluginRegistry escape hatch) only for what neither tier
covers.

For tests, scripts, or anywhere plugin lifecycle management isn't needed,
construct MariadbRepository directly (no plugin required)::

    repo = MariadbRepository(MariadbSettings())
    traced = TracingProxy(repo, prefix="repository.item")
"""
# Capability: "persistence"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/how-it-works/#choosing-a-wiring-pattern
from __future__ import annotations

import logging

from openframe.core.ports import BasePort, Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import AdapterConnectionError

from openframe.adapters.db.mariadb.config import MariadbSettings
from openframe.adapters.db.mariadb.connection import _pool_cache, get_mariadb_pool
from openframe.adapters.db.mariadb.repository import MariadbRepository

__all__ = ["MariadbPlugin"]

_logger = logging.getLogger(__name__)


class MariadbPlugin(BasePort):
    """
    MariaDB adapter plugin for the OpenFrame plugin registry.

    Capability: "persistence"

    Stability: beta

    Lifecycle:
        initialize() — creates the aiomysql connection pool and verifies
                       connectivity via the repository's health(). Raises
                       AdapterConnectionError if the database is unreachable.
        shutdown()   — closes the connection pool. Never raises.
        health()     — delegates to the repository's health() and returns
                       its PluginHealth. Never raises.

    The plugin exposes get_repository() after initialization for use
    in the composition root or ApplicationBootstrap.

    By default constructs a plain MariadbRepository. To use a domain-specific
    subclass, pass it via repository_class::

        registry.register(MariadbPlugin(
            MariadbSettings(),
            table="items",
            id_column="id",
            repository_class=ItemMariadbRepository,
        ))
    """

    name:       str = "openframe-mariadb"
    version:    str = "0.1.0"
    capability: Capability = Capability.PERSISTENCE

    def __init__(
        self,
        settings: MariadbSettings,
        table: str = "",
        id_column: str = "id",
        repository_class: type[MariadbRepository] = MariadbRepository,
    ) -> None:
        """
        Args:
            settings:         MariadbSettings instance.
            table:            Table name. If omitted the plugin acts as a
                              connection manager only (no get_repository()).
            id_column:        Primary key column name. Defaults to "id".
            repository_class: The MariadbRepository subclass to construct.
                              Defaults to the base MariadbRepository. Pass a
                              domain-specific subclass here to get proper
                              entity mapping through get_repository().

        Raises:
            TypeError: repository_class is not a subclass of MariadbRepository.
        """
        if not (isinstance(repository_class, type) and issubclass(repository_class, MariadbRepository)):
            raise TypeError(
                f"repository_class must be a subclass of MariadbRepository, "
                f"got {repository_class!r}"
            )
        self._settings = settings
        self._table = table
        self._id_column = id_column
        self._repository_class = repository_class
        self._repo: MariadbRepository | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Initialize the MariaDB connection pool and verify connectivity.

        If a ``table`` was passed to the constructor, a
        :class:`MariadbRepository` is created and connectivity is verified
        via ``repo.health()``. When no table is provided the plugin is used
        purely as a connection manager: connectivity is verified with a
        direct ``SELECT 1`` against the pool.

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError: MariaDB is unreachable or credentials
                                    are invalid.
            AdapterConfigurationError: DATABASE_URL is malformed.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            pool = await get_mariadb_pool(self._settings)
            if self._table:
                self._repo = self._repository_class(
                    self._settings,
                    table=self._table,
                    id_column=self._id_column,
                )
                health = await self._repo.health()
                if health.status != PluginStatus.READY:
                    raise AdapterConnectionError(
                        health.message or "MariaDB health check failed after pool creation",
                        adapter="mariadb",
                        operation="initialize",
                    ) from None
            else:
                # Verify pool connectivity without requiring a table.
                try:
                    async with pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute("SELECT 1")
                            await cur.fetchone()
                except Exception as exc:
                    raise AdapterConnectionError(
                        f"MariaDB connectivity check failed: {exc}",
                        adapter="mariadb",
                        operation="initialize",
                    ) from exc
            self._status = PluginStatus.READY
            _logger.info(
                "MariadbPlugin initialized — %s (repository_class=%s)",
                self._settings.database_url.split("@")[-1],
                self._repository_class.__name__,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Close the MariaDB connection pool.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._repo is not None:
                await self._repo.close()
                _logger.info("MariadbPlugin shutdown complete.")
        except Exception as exc:
            _logger.error("MariadbPlugin shutdown error (ignored): %s", exc)
        finally:
            self._status = PluginStatus.STOPPED

    async def health(self) -> PluginHealth:
        """
        Return current health snapshot.

        Delegates to the repository's own ``health()`` when one exists — no
        translation needed, it already returns a ``PluginHealth``. When the
        plugin was constructed without a table (connection-manager-only
        mode, no repository), the pool is checked directly.

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
            # No repository — check the pool directly.
            try:
                pool = await get_mariadb_pool(self._settings)
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT 1")
                        await cur.fetchone()
                return PluginHealth(status=PluginStatus.READY, message="")
            except Exception as exc:
                return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
        except Exception as exc:
            return PluginHealth(
                status=PluginStatus.FAILED,
                message=str(exc),
            )

    def get_repository(self) -> MariadbRepository:
        """
        Return the initialized repository.

        Only valid after registry.initialize_all() has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._status != PluginStatus.READY:
            raise RuntimeError(
                f"MariadbPlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        if self._repo is None:
            raise RuntimeError(
                "MariadbPlugin was initialized without a table name. "
                "Pass table=... to MariadbPlugin() to enable get_repository()."
            )
        return self._repo
