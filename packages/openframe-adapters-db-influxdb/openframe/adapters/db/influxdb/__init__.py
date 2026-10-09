"""
openframe.adapters.db.influxdb
=================================
InfluxDB 2.x time-series database adapter for the OpenFrame Microservice
Suite.

Public API:

    InfluxDBSettings    — Pydantic Settings subclass for connection config.
    InfluxDBRepository  — Generic async repository (BaseRepository).
    get_influxdb_client — Async factory that creates / returns the cached client.
    InfluxDBPlugin       — BasePort-satisfying plugin wrapper.

Read ``repository.py``'s module docstring before relying on this adapter
for anything beyond simple cases — InfluxDB is a time-series database with
no primary key and immutable points, and ``BaseRepository``'s row-based
CRUD shape does not map onto it cleanly. That docstring documents exactly
what each method actually does and why, honestly, rather than papering
over the mismatch.

Quick start::

    from openframe.adapters.db.influxdb import (
        InfluxDBSettings,
        InfluxDBRepository,
        get_influxdb_client,
    )

    settings = InfluxDBSettings(
        influxdb_url="http://localhost:8086",
        influxdb_token="...",
        influxdb_org="my-org",
        influxdb_bucket="my-bucket",
    )

    repo = InfluxDBRepository(settings, measurement="readings", id_tag="sensor_id")
    reading = await repo.get("sensor-1")           # dict | None
"""
from __future__ import annotations

from .config import InfluxDBSettings
from .connection import get_influxdb_client
from .plugin import InfluxDBPlugin
from .repository import InfluxDBRepository

__all__ = [
    "InfluxDBSettings",
    "InfluxDBRepository",
    "get_influxdb_client",
    "InfluxDBPlugin",
]
