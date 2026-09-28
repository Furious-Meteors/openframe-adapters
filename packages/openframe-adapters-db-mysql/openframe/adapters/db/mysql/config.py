"""
openframe/adapters/db/mysql/config.py
=======================================
MySQL adapter settings.

Reads all connection configuration from environment variables via Pydantic
Settings. Every field is validated at instantiation — missing required fields
raise ``pydantic_core.ValidationError`` immediately so misconfigured
deployments fail fast on startup.

Required env vars:
    DATABASE_URL: Full aiomysql/PyMySQL DSN.
                  Format: mysql://user:pass@host:port/dbname

Optional env vars (all have defaults):
    POOL_SIZE:             int   = 10
    POOL_RECYCLE:          float = 300.0
    POOL_CONNECT_TIMEOUT:  float = 10.0
    MYSQL_ADAPTER_NAME:    str   = "mysql"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["MySQLSettings"]


class MySQLSettings(BaseAdapterSettings):
    """
    Settings for the MySQL adapter.

    All fields read from environment variables. Missing required fields raise
    ``pydantic_core.ValidationError`` at instantiation time.

    Inherits from ``BaseAdapterSettings``:
        adapter_name:       str   = "mysql"  (overrides base default)
        connection_timeout: float = 30.0
        operation_timeout:  float = 10.0
        max_retries:        int   = 3

    Attributes:
        database_url:         Full DSN, ``mysql://user:pass@host:port/dbname``
                              (required).
        pool_size:            Pool minsize and maxsize. Default 10.
        pool_recycle:         Seconds before a pooled connection is recycled
                              (aiomysql ``pool_recycle``). Default 300.0.
        pool_connect_timeout: Per-connection TCP connect timeout in seconds
                              (PyMySQL ``connect_timeout``). Default 10.0.
    """

    database_url: str
    pool_size: int = 10
    pool_recycle: float = 300.0
    pool_connect_timeout: float = 10.0
    adapter_name: str = "mysql"
