"""
openframe/adapters/db/influxdb/config.py
===========================================
InfluxDB 2.x adapter settings.

Reads all connection configuration from environment variables via Pydantic
Settings. Every field is validated at instantiation — missing required
fields raise ``pydantic_core.ValidationError`` immediately so misconfigured
deployments fail fast on startup, never at first query.

Field names were verified against ``influxdb_client.InfluxDBClient``'s (and
its async counterpart ``InfluxDBClientAsync``'s) actual constructor
signature — ``url``, ``token``, ``org`` — plus the ``bucket`` that every
write/query call needs but that the client constructor itself does not take
(it is supplied per-call to ``write_api().write()`` /
``query_api().query()``). All four are InfluxDB 2.x's real required
connection parameters; there is no fifth "database name" concept the way
there is in InfluxDB 1.x (that version used ``database`` + an InfluxQL-only
API and is not what ``influxdb-client`` talks to).

Required env vars:
    INFLUXDB_URL:    Server URL, e.g. http://localhost:8086
    INFLUXDB_TOKEN:  API token (InfluxDB 2.x has no user/password auth on
                      the write/query path — only token auth).
    INFLUXDB_ORG:    Organization name or ID.
    INFLUXDB_BUCKET:  Default bucket name used by the repository when no
                      ``bucket=`` override is passed to its constructor.

Optional env vars (all have defaults):
    INFLUXDB_CLIENT_TIMEOUT_MS: int = 10000  (per-request timeout passed to
                                 InfluxDBClientAsync(timeout=...), in ms —
                                 the client's own unit)
    INFLUXDB_LOOKBACK:          str = "-30d" (Flux ``range(start: ...)``
                                 window used by get()/list() — see
                                 repository.py's module docstring for why a
                                 time-series query needs a range at all)
    INFLUXDB_ADAPTER_NAME:      str = "influxdb"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["InfluxDBSettings"]


class InfluxDBSettings(BaseAdapterSettings):
    """
    Settings for the InfluxDB 2.x adapter.

    All fields read from environment variables. Missing required fields
    raise ``pydantic_core.ValidationError`` at instantiation time.

    Inherits from ``BaseAdapterSettings``:
        adapter_name:       str   = "influxdb"  (overrides base default)
        connection_timeout: float = 30.0
        operation_timeout:  float = 10.0
        max_retries:        int   = 3

    Attributes:
        influxdb_url:    Server URL (required), e.g. "http://localhost:8086".
        influxdb_token:  API token (required).
        influxdb_org:    Organization name or ID (required).
        influxdb_bucket: Default bucket name (required).
        client_timeout_ms: Per-request timeout in milliseconds passed
                           straight through to
                           ``InfluxDBClientAsync(timeout=...)``. Default
                           10000 (the driver's own default).
        lookback:        Flux ``range(start: ...)`` window used by
                         ``get()``/``list()`` since Flux queries require an
                         explicit time range. Default "-30d". See
                         repository.py for why this exists.
    """

    influxdb_url: str
    influxdb_token: str
    influxdb_org: str
    influxdb_bucket: str
    client_timeout_ms: int = 10_000
    lookback: str = "-30d"
    adapter_name: str = "influxdb"
