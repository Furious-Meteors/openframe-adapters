"""
openframe/adapters/db/cockroachdb/config.py
=============================================
CockroachDB adapter settings.

Reads all connection configuration from environment variables via Pydantic
Settings. Every field is validated at instantiation — missing required fields
raise ``pydantic_core.ValidationError`` immediately so misconfigured
deployments fail fast on startup.

CockroachDB speaks the PostgreSQL wire protocol, so this adapter uses the
same ``asyncpg`` driver as ``openframe-adapters-db-postgres``. The DSN
scheme is still ``postgresql://`` — asyncpg has no notion of "CockroachDB"
as a distinct backend, it only ever knows it is talking wire-protocol
Postgres. Point the URL at a CockroachDB node/load balancer the same way
you would a Postgres primary.

Required env vars:
    COCKROACHDB_URL: Full asyncpg DSN pointed at a CockroachDB cluster.
                      Format: postgresql://user:pass@host:port/dbname
                      SSL:    postgresql://user:pass@host:26257/dbname?sslmode=verify-full

Optional env vars (all have defaults):
    POOL_SIZE:                       int   = 10
    POOL_MAX_INACTIVE_CONN_LIFETIME: float = 300.0
    POOL_COMMAND_TIMEOUT:            float = 60.0
    POOL_MAX_QUERIES:                int   = 50000
    COCKROACHDB_ADAPTER_NAME:        str   = "cockroachdb"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["CockroachdbSettings"]


class CockroachdbSettings(BaseAdapterSettings):
    """
    Settings for the CockroachDB adapter.

    All fields read from environment variables. Missing required fields raise
    ``pydantic_core.ValidationError`` at instantiation time.

    Inherits from ``BaseAdapterSettings``:
        adapter_name:       str   = "cockroachdb"  (overrides base default)
        connection_timeout: float = 30.0
        operation_timeout:  float = 10.0
        max_retries:        int   = 3

    Attributes:
        cockroachdb_url:                  Full asyncpg DSN pointed at a
                                          CockroachDB cluster (required). The
                                          URL scheme is ``postgresql://`` —
                                          asyncpg only speaks the wire
                                          protocol, not a vendor name.
        pool_size:                        Pool min_size and max_size. Default 10.
        pool_max_inactive_conn_lifetime:  Seconds before idle connection is
                                          closed. Default 300.0.
        pool_command_timeout:             Per-statement timeout in the pool.
                                          Default 60.0.
        pool_max_queries:                 Queries per connection before recycle.
                                          Default 50 000.
    """

    cockroachdb_url: str
    pool_size: int = 10
    pool_max_inactive_conn_lifetime: float = 300.0
    pool_command_timeout: float = 60.0
    pool_max_queries: int = 50_000
    adapter_name: str = "cockroachdb"
