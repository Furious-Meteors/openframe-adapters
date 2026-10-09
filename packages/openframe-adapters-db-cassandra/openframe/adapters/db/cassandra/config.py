"""
openframe/adapters/db/cassandra/config.py
============================================
Cassandra adapter settings.

Reads all connection configuration from environment variables via Pydantic
Settings. Every field is validated at instantiation — missing required
fields raise ``pydantic_core.ValidationError`` immediately so misconfigured
deployments fail fast on startup.

Cassandra has no single connection-string DSN the way Postgres/Oracle do —
a client connects to a list of contact points and lets the driver discover
the rest of the cluster topology. ``cassandra_contact_points`` is therefore
the required field here, playing the same "required connection detail" role
``DATABASE_URL``/``ORACLE_DSN`` play in the other adapters.

Required env vars:
    CASSANDRA_CONTACT_POINTS: JSON array of hostnames/IPs, e.g.
                              '["10.0.0.1", "10.0.0.2"]'
                              (pydantic-settings parses a JSON-encoded list
                              from a single env var automatically).

Optional env vars (all have defaults):
    CASSANDRA_PORT:                int        = 9042
    CASSANDRA_KEYSPACE:            str | None = None
    CASSANDRA_USERNAME:            str | None = None
    CASSANDRA_PASSWORD:            str | None = None
    CASSANDRA_LOCAL_DC:            str | None = None
    CASSANDRA_PROTOCOL_VERSION:    int | None = None
    CASSANDRA_CORE_CONNECTIONS_PER_HOST: int  = 2
    CASSANDRA_ADAPTER_NAME:        str        = "cassandra"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["CassandraSettings"]


class CassandraSettings(BaseAdapterSettings):
    """
    Settings for the Cassandra adapter.

    All fields read from environment variables. Missing required fields
    raise ``pydantic_core.ValidationError`` at instantiation time.

    Inherits from ``BaseAdapterSettings``:
        adapter_name:       str   = "cassandra"  (overrides base default)
        connection_timeout: float = 30.0
        operation_timeout:  float = 10.0
        max_retries:        int   = 3

    Attributes:
        cassandra_contact_points: Hostnames/IPs of seed nodes the driver
                                  uses to discover the rest of the cluster
                                  (required).
        cassandra_port:           Native protocol port. Default 9042.
        cassandra_keyspace:       Keyspace to use. Optional — when unset,
                                  CRUD statements must be fully-qualified
                                  (``keyspace.table``).
        cassandra_username:       Username for ``PlainTextAuthProvider``.
                                  Optional — if set, ``cassandra_password``
                                  must also be set.
        cassandra_password:       Password for ``PlainTextAuthProvider``.
        cassandra_local_dc:       Local datacenter name, passed to
                                  ``DCAwareRoundRobinPolicy`` /
                                  ``ExecutionProfile`` when set. Optional.
        cassandra_protocol_version: Explicit native protocol version to
                                  negotiate. Optional — ``None`` lets the
                                  driver negotiate automatically.
        cassandra_core_connections_per_host: Connections the driver keeps
                                  open per host. Part of the pool-shape
                                  cache key (see ``connection.py``).
                                  Default 2.
    """

    cassandra_contact_points: list[str]
    cassandra_port: int = 9042
    cassandra_keyspace: str | None = None
    cassandra_username: str | None = None
    cassandra_password: str | None = None
    cassandra_local_dc: str | None = None
    cassandra_protocol_version: int | None = None
    cassandra_core_connections_per_host: int = 2
    adapter_name: str = "cassandra"
