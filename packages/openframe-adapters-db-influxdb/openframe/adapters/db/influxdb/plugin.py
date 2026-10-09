"""
openframe/adapters/db/influxdb/plugin.py
===========================================
OpenFrame plugin wrapper for InfluxDBRepository.

Stability: beta

Capability choice: this plugin registers under ``Capability.PERSISTENCE``
rather than a dedicated time-series capability. That enum is closed by
design — adding a member to it is an ADR-level decision, not something a
single new adapter package should do unilaterally, and nothing about
InfluxDB creates registry-lookup ambiguity that would justify one (nothing
else in this ecosystem currently registers more than one persistence-shaped
port per application in a way that ``Capability.PERSISTENCE`` can't
already disambiguate via distinct plugin instances/table-equivalents). See
``repository.py``'s module docstring for the much larger, real mismatch
this adapter has to document — the CRUD-shape one, not the capability-enum
one.

Recommended usage — ApplicationBootstrap.compose() (openframe-core>=3.3)::

    from openframe.core.runtime import ApplicationBootstrap
    from openframe.core.ports import Capability
    from openframe.adapters.db.influxdb import InfluxDBPlugin, InfluxDBSettings

    plugin = InfluxDBPlugin(InfluxDBSettings(), measurement="readings", id_tag="sensor_id")

    async with ApplicationBootstrap.compose(plugin) as app:
        repo = app.get(Capability.PERSISTENCE)
        reading = await repo.get("sensor-1")
    # plugin.shutdown() ran automatically on exit

Use a subclassed ApplicationBootstrap (configure()) instead when you need
per-port config=/init_timeout= or conditional registration order, and
app.registry (PluginRegistry escape hatch) only for what neither tier
covers.

For tests, scripts, or anywhere plugin lifecycle management isn't needed,
construct InfluxDBRepository directly (no plugin required)::

    repo = InfluxDBRepository(InfluxDBSettings())
    traced = TracingProxy(repo, prefix="repository.reading")
"""
# Capability: "persistence"
# See capability taxonomy:
# https://furious-meteors.github.io/openframe-core/developer-guide/how-it-works/#choosing-a-wiring-pattern
from __future__ import annotations

import logging

from openframe.core.ports import BasePort, Capability, PluginContext, PluginHealth, PluginStatus
from openframe.core.exceptions import AdapterConnectionError

from openframe.adapters.db.influxdb.config import InfluxDBSettings
from openframe.adapters.db.influxdb.connection import _client_cache, get_influxdb_client
from openframe.adapters.db.influxdb.repository import InfluxDBRepository

__all__ = ["InfluxDBPlugin"]

_logger = logging.getLogger(__name__)


class InfluxDBPlugin(BasePort):
    """
    InfluxDB 2.x adapter plugin for the OpenFrame plugin registry.

    Capability: "persistence" (see module docstring for why no dedicated
    time-series capability was added)

    Stability: beta

    Lifecycle:
        initialize() — creates the InfluxDBClientAsync and verifies
                       connectivity via the repository's health(). Raises
                       AdapterConnectionError if InfluxDB is unreachable.
        shutdown()   — closes the client. Never raises.
        health()     — delegates to the repository's health() and returns
                       its PluginHealth. Never raises.

    The plugin exposes get_repository() after initialization for use
    in the composition root or ApplicationBootstrap.

    By default constructs a plain InfluxDBRepository. To use a domain-specific
    subclass, pass it via repository_class::

        registry.register(InfluxDBPlugin(
            InfluxDBSettings(),
            measurement="readings",
            id_tag="sensor_id",
            repository_class=ReadingInfluxDBRepository,
        ))
    """

    name:       str = "openframe-influxdb"
    version:    str = "0.1.0"
    capability: Capability = Capability.PERSISTENCE

    def __init__(
        self,
        settings: InfluxDBSettings,
        measurement: str = "",
        id_tag: str = "id",
        bucket: str | None = None,
        repository_class: type[InfluxDBRepository] = InfluxDBRepository,
    ) -> None:
        """
        Args:
            settings:         InfluxDBSettings instance.
            measurement:      Measurement name. If omitted the plugin acts
                              as a connection manager only (no
                              get_repository()).
            id_tag:           Tag key treated as the entity id. Defaults
                              to "id".
            bucket:           Bucket override. Defaults to
                              settings.influxdb_bucket.
            repository_class: The InfluxDBRepository subclass to construct.
                              Defaults to the base InfluxDBRepository. Pass
                              a domain-specific subclass here to get proper
                              entity mapping through get_repository().

        Raises:
            TypeError: repository_class is not a subclass of InfluxDBRepository.
        """
        if not (isinstance(repository_class, type) and issubclass(repository_class, InfluxDBRepository)):
            raise TypeError(
                f"repository_class must be a subclass of InfluxDBRepository, "
                f"got {repository_class!r}"
            )
        self._settings = settings
        self._measurement = measurement
        self._id_tag = id_tag
        self._bucket = bucket
        self._repository_class = repository_class
        self._repo: InfluxDBRepository | None = None
        self._status = PluginStatus.REGISTERED

    async def initialize(self, context: PluginContext) -> None:
        """
        Initialize the InfluxDB client and verify connectivity.

        If a ``measurement`` was passed to the constructor, an
        :class:`InfluxDBRepository` is created and connectivity is verified
        via ``repo.health()``. When no measurement is provided the plugin
        is used purely as a connection manager: connectivity is verified
        with a direct ``ping()`` against the client.

        Args:
            context: Plugin context (config, plugin_name). Unused here —
                     settings are provided at construction time.

        Raises:
            AdapterConnectionError: InfluxDB is unreachable or credentials
                                    are invalid.
            AdapterConfigurationError: Token/org rejected.
        """
        self._status = PluginStatus.INITIALIZED
        try:
            client = await get_influxdb_client(self._settings)
            if self._measurement:
                self._repo = self._repository_class(
                    self._settings,
                    measurement=self._measurement,
                    id_tag=self._id_tag,
                    bucket=self._bucket,
                )
                health = await self._repo.health()
                if health.status != PluginStatus.READY:
                    raise AdapterConnectionError(
                        health.message or "InfluxDB health check failed after client creation",
                        adapter="influxdb",
                        operation="initialize",
                    ) from None
            else:
                try:
                    ok = await client.ping()
                    if not ok:
                        raise AdapterConnectionError(
                            "InfluxDB ping() returned False",
                            adapter="influxdb",
                            operation="initialize",
                        )
                except AdapterConnectionError:
                    raise
                except Exception as exc:
                    raise AdapterConnectionError(
                        f"InfluxDB connectivity check failed: {exc}",
                        adapter="influxdb",
                        operation="initialize",
                    ) from exc
            self._status = PluginStatus.READY
            _logger.info(
                "InfluxDBPlugin initialized — %s (repository_class=%s)",
                self._settings.influxdb_url,
                self._repository_class.__name__,
            )
        except Exception:
            self._status = PluginStatus.FAILED
            raise

    async def shutdown(self) -> None:
        """
        Close the InfluxDB client.

        Never raises — logs errors and continues.
        """
        self._status = PluginStatus.STOPPING
        try:
            if self._repo is not None:
                await self._repo.close()
                _logger.info("InfluxDBPlugin shutdown complete.")
        except Exception as exc:
            _logger.error("InfluxDBPlugin shutdown error (ignored): %s", exc)
        finally:
            self._status = PluginStatus.STOPPED

    async def health(self) -> PluginHealth:
        """
        Return current health snapshot.

        Delegates to the repository's own ``health()`` when one exists —
        no translation needed, it already returns a ``PluginHealth``. When
        the plugin was constructed without a measurement (connection-
        manager-only mode, no repository), the client is checked directly.

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
                client = await get_influxdb_client(self._settings)
                ok = await client.ping()
                if ok:
                    return PluginHealth(status=PluginStatus.READY, message="")
                return PluginHealth(status=PluginStatus.FAILED, message="InfluxDB ping() returned False")
            except Exception as exc:
                return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
        except Exception as exc:
            return PluginHealth(
                status=PluginStatus.FAILED,
                message=str(exc),
            )

    def get_repository(self) -> InfluxDBRepository:
        """
        Return the initialized repository.

        Only valid after registry.initialize_all() has been called.

        Raises:
            RuntimeError: Plugin not yet initialized.
        """
        if self._status != PluginStatus.READY:
            raise RuntimeError(
                f"InfluxDBPlugin is not ready (status: {self._status.name}). "
                "Call await registry.initialize_all() first."
            )
        if self._repo is None:
            raise RuntimeError(
                "InfluxDBPlugin was initialized without a measurement name. "
                "Pass measurement=... to InfluxDBPlugin() to enable get_repository()."
            )
        return self._repo
