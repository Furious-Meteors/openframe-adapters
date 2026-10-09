"""
openframe/adapters/db/oracle/plugin.py
========================================
OpenFrame plugin wrapper for OracleRepository.

Stability: beta

Recommended usage — ApplicationBootstrap.compose() (openframe-core>=3.3)::

    from openframe.core.runtime import ApplicationBootstrap
    from openframe.core.ports import Capability
    from openframe.adapters.db.oracle import OraclePlugin, OracleSettings

    plugin = OraclePlugin(OracleSettings(), table="items", id_column="id")

    async with ApplicationBootstrap.compose(plugin) as app:
        repo = app.get(Capability.PERSISTENCE)
        item = await repo.get("abc-123")
    # plugin.shutdown() ran automatically on exit

Use a subclassed ApplicationBootstrap (configure()) instead when you need
per-port config=/init_timeout= or conditional registration order, and
app.registry (PluginRegistry escape hatch) only for what neither tier
covers.

For tests, scripts, or anywhere plugin lifecycle management isn't needed,
construct OracleRepository directly (no plugin required)::

    repo = OracleRepository(OracleSettings())
    traced = TracingProxy(repo, prefix="repository.item")
"""
# Capability: "persistence"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/how-it-works/#choosing-a-wiring-pattern
from __future__ import annotations

import logging

from openframe.core.ports import BasePort, Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import AdapterConnectionError

from openframe.adapters.db.oracle.config import OracleSettings
from openframe.adapters.db.oracle.connection import _pool_cache, get_oracle_pool
from openframe.adapters.db.oracle.repository import OracleRepository

__all__ = ["OraclePlugin"]

_logger = logging.getLogger(__name__)


class OraclePlugin(BasePort):
    """
    Oracle adapter plugin for the OpenFrame plugin registry.

    Capability: "persistence"

    Stability: beta

    Lifecycle:
        initialize() — creates the oracledb async connection pool and
                       verifies connectivity via the repository's health().
                       Raises AdapterConnectionError if the database is
                       unreachable.
        shutdown()   — closes the connection pool. Never raises.
        health()     — delegates to the repository's health() and returns
                       its PluginHealth. Never raises.

    The plugin exposes get_repository() after initialization for use
    in the composition root or ApplicationBootstrap.

    By default constructs a plain OracleRepository. To use a domain-specific
    subclass, pass it via repository_class::

        registry.register(OraclePlugin(
            OracleSettings(),
            table="items",
            id_column="id",
            repository_class=ItemOracleRepository,
        ))
    """

    name:       str = "openframe-oracle"
    version:    str = "0.1.0"
    capability: Capability = Capability.PERSISTENCE

    def __init__(
        self,
        settings: OracleSettings,
        table: str = "",
        id_column: str = "id",
        repository_class: type[OracleRepository] = OracleRepository,
    ) -> None:
        """
        Args:
            settings:         OracleSettings instance.
            table:            Table name. If omitted the plugin acts as a
                              connection manager only (no get_repository()).
            id_column:        Primary key column name. Defaults to "id".
            repository_class: The OracleRepository subclass to construct.
                              Defaults to the base OracleRepository. Pass a
                              domain-specific subclass here to get proper
                              entity mapping through get_repository().

        Raises:
            TypeError: repository_class is not a subclass of OracleRepository.
        """
        if not (isinstance(repository_class, type) and issubclass(repository_class, OracleRepository)):
            raise TypeError(
                f"repository_class must be a subclass of OracleRepository, "
                f"got {repository_class!r}"
            )
        self._settings = settings
        self._table = table
        self._id_column = id_column
        self._repository_class = repository_class
        self._repo: OracleRepository | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Initialize the Oracle connection pool and verify connectivity.

        If a ``table`` was passed to the constructor, an
        :class:`OracleRepository` is created and connectivity is verified
        via ``repo.health()``. When no table is provided the plugin is used
        purely as a connection manager: connectivity is verified with a
        direct ``SELECT 1 FROM DUAL`` against the pool.

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError: Oracle is unreachable or credentials
                                    are invalid.
            AdapterConfigurationError: ORACLE_DSN is malformed.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            pool = await get_oracle_pool(self._settings)
            if self._table:
                self._repo = self._repository_class(
                    self._settings,
                    table=self._table,
                    id_column=self._id_column,
                )
                health = await self._repo.health()
                if health.status != PluginStatus.READY:
                    raise AdapterConnectionError(
                        health.message or "Oracle health check failed after pool creation",
                        adapter="oracle",
                        operation="initialize",
                    ) from None
            else:
                # Verify pool connectivity without requiring a table.
                try:
                    conn = await pool.acquire()
                    try:
                        async with conn.cursor() as cur:
                            await cur.execute("SELECT 1 FROM DUAL")
                            await cur.fetchone()
                    finally:
                        await pool.release(conn)
                except Exception as exc:
                    raise AdapterConnectionError(
                        f"Oracle connectivity check failed: {exc}",
                        adapter="oracle",
                        operation="initialize",
                    ) from exc
            self._status = PluginStatus.READY
            _logger.info(
                "OraclePlugin initialized — (repository_class=%s)",
                self._repository_class.__name__,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Close the Oracle connection pool.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._repo is not None:
                await self._repo.close()
                _logger.info("OraclePlugin shutdown complete.")
        except Exception as exc:
            _logger.error("OraclePlugin shutdown error (ignored): %s", exc)
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
                pool = await get_oracle_pool(self._settings)
                conn = await pool.acquire()
                try:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT 1 FROM DUAL")
                        await cur.fetchone()
                finally:
                    await pool.release(conn)
                return PluginHealth(status=PluginStatus.READY, message="")
            except Exception as exc:
                return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
        except Exception as exc:
            return PluginHealth(
                status=PluginStatus.FAILED,
                message=str(exc),
            )

    def get_repository(self) -> OracleRepository:
        """
        Return the initialized repository.

        Only valid after registry.initialize_all() has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._status != PluginStatus.READY:
            raise RuntimeError(
                f"OraclePlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        if self._repo is None:
            raise RuntimeError(
                "OraclePlugin was initialized without a table name. "
                "Pass table=... to OraclePlugin() to enable get_repository()."
            )
        return self._repo
