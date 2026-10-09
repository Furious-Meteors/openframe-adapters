"""
openframe/adapters/db/dynamodb/plugin.py
===========================================
OpenFrame plugin wrapper for DynamoDBRepository.

Stability: beta

Recommended usage — ApplicationBootstrap.compose() (openframe-core>=3.3)::

    from openframe.core.runtime import ApplicationBootstrap
    from openframe.core.ports import Capability
    from openframe.adapters.db.dynamodb import DynamoDBPlugin, DynamoDBSettings

    plugin = DynamoDBPlugin(DynamoDBSettings(), id_column="id")

    async with ApplicationBootstrap.compose(plugin) as app:
        repo = app.get(Capability.PERSISTENCE)
        item = await repo.get("abc-123")
    # plugin.shutdown() ran automatically on exit

Use a subclassed ApplicationBootstrap (configure()) instead when you need
per-port config=/init_timeout= or conditional registration order, and
app.registry (PluginRegistry escape hatch) only for what neither tier
covers.

For tests, scripts, or anywhere plugin lifecycle management isn't needed,
construct DynamoDBRepository directly (no plugin required)::

    repo = DynamoDBRepository(DynamoDBSettings())
    traced = TracingProxy(repo, prefix="repository.item")
"""
# Capability: "persistence"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/how-it-works/#choosing-a-wiring-pattern
from __future__ import annotations

import logging

from openframe.core.ports import BasePort, Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import AdapterConnectionError

from openframe.adapters.db.dynamodb.config import DynamoDBSettings
from openframe.adapters.db.dynamodb.connection import get_dynamodb_table
from openframe.adapters.db.dynamodb.repository import DynamoDBRepository

__all__ = ["DynamoDBPlugin"]

_logger = logging.getLogger(__name__)


class DynamoDBPlugin(BasePort):
    """
    DynamoDB adapter plugin for the OpenFrame plugin registry.

    Capability: "persistence"

    Stability: beta

    Lifecycle:
        initialize() — creates the aioboto3 DynamoDB resource/Table and
                       verifies connectivity via the repository's health().
                       Raises AdapterConnectionError if DynamoDB is
                       unreachable or the configured table doesn't exist.
        shutdown()   — closes the cached resource. Never raises.
        health()     — delegates to the repository's health() and returns
                       its PluginHealth. Never raises.

    The plugin exposes get_repository() after initialization for use
    in the composition root or ApplicationBootstrap.

    By default constructs a plain DynamoDBRepository. To use a
    domain-specific subclass, pass it via repository_class::

        registry.register(DynamoDBPlugin(
            DynamoDBSettings(),
            id_column="id",
            repository_class=ItemDynamoDBRepository,
        ))
    """

    name:       str = "openframe-dynamodb"
    version:    str = "0.1.0"
    capability: Capability = Capability.PERSISTENCE

    def __init__(
        self,
        settings: DynamoDBSettings,
        id_column: str = "id",
        repository_class: type[DynamoDBRepository] = DynamoDBRepository,
    ) -> None:
        """
        Args:
            settings:         DynamoDBSettings instance.
            id_column:        Partition key attribute name. Defaults to "id".
            repository_class: The DynamoDBRepository subclass to construct.
                              Defaults to the base DynamoDBRepository. Pass a
                              domain-specific subclass here to get proper
                              entity mapping through get_repository().

        Raises:
            TypeError: repository_class is not a subclass of DynamoDBRepository.
        """
        if not (
            isinstance(repository_class, type)
            and issubclass(repository_class, DynamoDBRepository)
        ):
            raise TypeError(
                f"repository_class must be a subclass of DynamoDBRepository, "
                f"got {repository_class!r}"
            )
        self._settings = settings
        self._id_column = id_column
        self._repository_class = repository_class
        self._repo: DynamoDBRepository | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Initialize the DynamoDB resource and verify connectivity.

        Creates a :class:`DynamoDBRepository` and verifies connectivity via
        ``repo.health()`` (a ``describe_table`` call).

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError: DynamoDB is unreachable or the
                                    configured table doesn't exist.
            AdapterConfigurationError: Region/credentials are malformed.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            await get_dynamodb_table(self._settings)
            self._repo = self._repository_class(
                self._settings,
                id_column=self._id_column,
            )
            health = await self._repo.health()
            if health.status != PluginStatus.READY:
                raise AdapterConnectionError(
                    health.message or "DynamoDB health check failed after resource creation",
                    adapter="dynamodb",
                    operation="initialize",
                ) from None
            self._status = PluginStatus.READY
            _logger.info(
                "DynamoDBPlugin initialized — region=%s table=%s (repository_class=%s)",
                self._settings.aws_region,
                self._settings.dynamodb_table_name,
                self._repository_class.__name__,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Close the cached DynamoDB resource.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._repo is not None:
                await self._repo.close()
                _logger.info("DynamoDBPlugin shutdown complete.")
        except Exception as exc:
            _logger.error("DynamoDBPlugin shutdown error (ignored): %s", exc)
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

    def get_repository(self) -> DynamoDBRepository:
        """
        Return the initialized repository.

        Only valid after registry.initialize_all() has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._status != PluginStatus.READY:
            raise RuntimeError(
                f"DynamoDBPlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        if self._repo is None:
            raise RuntimeError(
                "DynamoDBPlugin has no repository instance despite READY status. "
                "This should not happen — please report this as a bug."
            )
        return self._repo
