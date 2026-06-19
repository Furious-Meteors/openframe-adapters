"""
openframe/adapters/db/postgres/plugin.py
==========================================
OpenFrame plugin wrapper for PostgresRepository.

Stability: beta

Usage via PluginRegistry (optional)::

    from openframe.core.plugins import PluginRegistry
    from openframe.adapters.db.postgres import PostgresPlugin, PostgresSettings

    registry = PluginRegistry()
    registry.register(PostgresPlugin(PostgresSettings()))
    await registry.initialize_all()

    plugin = registry.get("persistence")
    repo = plugin.get_repository()

Usage via deps.py (unchanged, no plugin needed)::

    repo = PostgresRepository(PostgresSettings())
    traced = TracingProxy(repo, prefix="repository.item")
"""
# Capability: "persistence"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/composition-root/
from __future__ import annotations

import logging

from openframe.core.exceptions import AdapterConnectionError
from openframe.core.plugins import PluginContext, PluginHealth, PluginStatus

from openframe.adapters.db.postgres.config import PostgresSettings
from openframe.adapters.db.postgres.connection import _pool_cache, get_postgres_pool
from openframe.adapters.db.postgres.repository import PostgresRepository

__all__ = ["PostgresPlugin"]

_logger = logging.getLogger(__name__)


class PostgresPlugin:
    """
    PostgreSQL adapter plugin for the OpenFrame plugin registry.

    Capability: "persistence"

    Stability: beta

    Lifecycle:
        initialize() — creates the asyncpg connection pool and verifies
                       connectivity via ping(). Raises AdapterConnectionError
                       if the database is unreachable.
        shutdown()   — closes the connection pool. Never raises.
        health()     — calls ping() and returns PluginHealth. Never raises.

    The plugin exposes get_repository() after initialization for use
    in the composition root or ApplicationBootstrap.

    By default constructs a plain PostgresRepository. To use a domain-specific
    subclass, pass it via repository_class::

        registry.register(PostgresPlugin(
            PostgresSettings(),
            table="items",
            id_column="id",
            repository_class=ItemPostgresRepository,
        ))
    """

    name:       str = "openframe-postgres"
    version:    str = "1.2.0"
    capability: str = "persistence"

    def __init__(
        self,
        settings: PostgresSettings,
        table: str = "",
        id_column: str = "id",
        repository_class: type[PostgresRepository] = PostgresRepository,
    ) -> None:
        """
        Args:
            settings:         PostgresSettings instance.
            table:            Table name. If omitted the plugin acts as a
                              connection manager only (no get_repository()).
            id_column:        Primary key column name. Defaults to "id".
            repository_class: The PostgresRepository subclass to construct.
                              Defaults to the base PostgresRepository. Pass a
                              domain-specific subclass here to get proper
                              entity mapping through get_repository().

        Raises:
            TypeError: repository_class is not a subclass of PostgresRepository.
        """
        if not (isinstance(repository_class, type) and issubclass(repository_class, PostgresRepository)):
            raise TypeError(
                f"repository_class must be a subclass of PostgresRepository, "
                f"got {repository_class!r}"
            )
        self._settings = settings
        self._table = table
        self._id_column = id_column
        self._repository_class = repository_class
        self._repo: PostgresRepository | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Initialize the PostgreSQL connection pool and verify connectivity.

        If a ``table`` was passed to the constructor, a
        :class:`PostgresRepository` is created and connectivity is verified
        via ``repo.ping()``.  When no table is provided the plugin is used
        purely as a connection manager: connectivity is verified with a
        direct ``SELECT 1`` against the pool.

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError: PostgreSQL is unreachable or credentials
                                    are invalid.
            AdapterConfigurationError: DATABASE_URL is malformed.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            pool = await get_postgres_pool(self._settings)
            if self._table:
                self._repo = self._repository_class(
                    self._settings,
                    table=self._table,
                    id_column=self._id_column,
                )
                if not await self._repo.ping():
                    raise AdapterConnectionError(
                        "PostgreSQL ping failed after pool creation",
                        adapter="postgres",
                        operation="initialize",
                    )
            else:
                # Verify pool connectivity without requiring a table.
                try:
                    await pool.fetchval("SELECT 1")
                except Exception as exc:
                    raise AdapterConnectionError(
                        f"PostgreSQL connectivity check failed: {exc}",
                        adapter="postgres",
                        operation="initialize",
                    ) from exc
            self._status = PluginStatus.READY
            _logger.info(
                "PostgresPlugin initialized — %s (repository_class=%s)",
                self._settings.database_url.split("@")[-1],
                self._repository_class.__name__,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Close the PostgreSQL connection pool.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._repo is not None:
                await self._repo.close()
                _logger.info("PostgresPlugin shutdown complete.")
        except Exception as exc:
            _logger.error("PostgresPlugin shutdown error (ignored): %s", exc)
        finally:
            self._status = PluginStatus.STOPPED

    async def health(self) -> PluginHealth:
        """
        Return current health snapshot.

        Never raises — returns FAILED status on any exception.
        """
        try:
            if self._status != PluginStatus.READY:
                return PluginHealth(
                    status=PluginStatus.FAILED,
                    message=f"Plugin status: {self._status.name}",
                )
            if self._repo is not None:
                healthy = await self._repo.ping()
            else:
                # No repository — ping the pool directly.
                try:
                    pool = await get_postgres_pool(self._settings)
                    await pool.fetchval("SELECT 1")
                    healthy = True
                except Exception:
                    healthy = False
            return PluginHealth(
                status=PluginStatus.READY if healthy else PluginStatus.FAILED,
                message="" if healthy else "ping() returned False",
            )
        except Exception as exc:
            return PluginHealth(
                status=PluginStatus.FAILED,
                message=str(exc),
            )

    def get_repository(self) -> PostgresRepository:
        """
        Return the initialized repository.

        Only valid after registry.initialize_all() has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._status != PluginStatus.READY:
            raise RuntimeError(
                f"PostgresPlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        if self._repo is None:
            raise RuntimeError(
                "PostgresPlugin was initialized without a table name. "
                "Pass table=... to PostgresPlugin() to enable get_repository()."
            )
        return self._repo
