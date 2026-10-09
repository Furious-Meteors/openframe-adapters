"""
openframe/adapters/db/oracle/config.py
=======================================
Oracle adapter settings.

Reads all connection configuration from environment variables via Pydantic
Settings. Every field is validated at instantiation — missing required
fields raise ``pydantic_core.ValidationError`` immediately so misconfigured
deployments fail fast on startup.

Required env vars:
    ORACLE_DSN: Full python-oracledb connection string.
                Format (EasyConnect / "user/password@connect_string", the
                exact shape ``oracledb.connect_async()``'s own ``dsn``
                parameter documents accepting):

                    user/password@host:port/service_name

                e.g. ``app/secret@db.example.com:1521/orclpdb``. A bare
                connect descriptor without embedded credentials also works
                if you prefer to keep credentials elsewhere.

Optional env vars (all have defaults):
    POOL_MIN:        int = 1
    POOL_MAX:        int = 10
    POOL_INCREMENT:  int = 1
    POOL_TIMEOUT:    int = 60
    ADAPTER_NAME:    str = "oracle"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["OracleSettings"]


class OracleSettings(BaseAdapterSettings):
    """
    Settings for the Oracle adapter.

    All fields read from environment variables. Missing required fields
    raise ``pydantic_core.ValidationError`` at instantiation time.

    Inherits from ``BaseAdapterSettings``:
        adapter_name:       str   = "oracle"  (overrides base default)
        connection_timeout: float = 30.0
        operation_timeout:  float = 10.0
        max_retries:        int   = 3

    Attributes:
        oracle_dsn:      python-oracledb connection string (required). Format
                         matches what ``oracledb.connect_async()``'s ``dsn``
                         parameter documents:
                         ``user/password@host:port/service_name``.
        pool_min:        Minimum number of connections the pool opens.
                         Default 1.
        pool_max:        Maximum number of connections the pool may open.
                         Default 10.
        pool_increment:  Number of connections opened whenever the pool
                         needs to grow. Default 1.
        pool_timeout:    Seconds an idle pooled connection may sit before
                         being closed by the pool. Default 60.
    """

    oracle_dsn: str
    pool_min: int = 1
    pool_max: int = 10
    pool_increment: int = 1
    pool_timeout: int = 60
    adapter_name: str = "oracle"
