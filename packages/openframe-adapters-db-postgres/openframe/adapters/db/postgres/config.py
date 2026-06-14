"""
openframe/adapters/db/postgres/config.py
=========================================
PostgreSQL adapter settings.

Reads all connection configuration from environment variables via Pydantic
Settings. Every field is validated at instantiation — missing required fields
raise ``pydantic_core.ValidationError`` immediately so misconfigured
deployments fail fast on startup.

Required env vars:
    DATABASE_URL: Full asyncpg DSN.
                  Format: postgresql://user:pass@host:port/dbname
                  SSL:    postgresql://user:pass@host/dbname?ssl=require

Optional env vars (all have defaults):
    POOL_SIZE:                       int   = 10
    POOL_MAX_INACTIVE_CONN_LIFETIME: float = 300.0
    POOL_COMMAND_TIMEOUT:            float = 60.0
    POOL_MAX_QUERIES:                int   = 50000
    POSTGRES_ADAPTER_NAME:           str   = "postgres"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["PostgresSettings"]


class PostgresSettings(BaseAdapterSettings):
    """
    Settings for the PostgreSQL adapter.

    All fields read from environment variables. Missing required fields raise
    ``pydantic_core.ValidationError`` at instantiation time.

    Inherits from ``BaseAdapterSettings``:
        adapter_name:       str   = "postgres"  (overrides base default)
        connection_timeout: float = 30.0
        operation_timeout:  float = 10.0
        max_retries:        int   = 3

    Attributes:
        database_url:                     Full asyncpg DSN (required).
        pool_size:                        Pool min_size and max_size. Default 10.
        pool_max_inactive_conn_lifetime:  Seconds before idle connection is
                                          closed. Default 300.0.
        pool_command_timeout:             Per-statement timeout in the pool.
                                          Default 60.0.
        pool_max_queries:                 Queries per connection before recycle.
                                          Default 50 000.
    """

    database_url: str
    pool_size: int = 10
    pool_max_inactive_conn_lifetime: float = 300.0
    pool_command_timeout: float = 60.0
    pool_max_queries: int = 50_000
    adapter_name: str = "postgres"
