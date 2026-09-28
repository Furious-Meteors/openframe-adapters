"""
openframe/adapters/db/mariadb/config.py
==========================================
MariaDB adapter settings.

Reads all connection configuration from environment variables via Pydantic
Settings. Every field is validated at instantiation — missing required fields
raise ``pydantic_core.ValidationError`` immediately so misconfigured
deployments fail fast on startup.

Required env vars:
    DATABASE_URL: Full aiomysql/PyMySQL-compatible DSN.
                  Format: mysql://user:pass@host:port/dbname

                  Field-naming note: this adapter deliberately reuses the
                  ``database_url`` field name (and ``DATABASE_URL`` env var)
                  from ``openframe-adapters-db-mysql`` rather than
                  introducing a MariaDB-specific ``mariadb_url``. MariaDB is
                  wire-protocol-compatible with MySQL and the DSN shape is
                  identical (``mysql://user:pass@host:port/dbname`` — there
                  is no separate ``mariadb://`` scheme in aiomysql/PyMySQL),
                  so keeping the same field name means a service can migrate
                  between the two adapter packages (e.g. moving a workload
                  from a MySQL server to a MariaDB server, or vice versa) by
                  changing only the import and dependency, with zero
                  environment/config changes required. The trade-off is that
                  ``DATABASE_URL`` doesn't visually distinguish which backend
                  a given deployment targets — acceptable here since exactly
                  one of the two adapter packages is installed per service.

Optional env vars (all have defaults):
    POOL_SIZE:             int   = 10
    POOL_RECYCLE:          float = 300.0
    POOL_CONNECT_TIMEOUT:  float = 10.0
    MARIADB_ADAPTER_NAME:  str   = "mariadb"

Auth-plugin note:
    Recent MySQL server versions default to the ``caching_sha2_password``
    authentication plugin, while MariaDB servers have historically defaulted
    to ``mysql_native_password`` (or MariaDB-specific plugins such as
    ``ed25519``/``unix_socket``). This adapter does not need to special-case
    this — ``aiomysql``/``PyMySQL`` negotiate the plugin the server offers —
    but if you hit an auth-negotiation failure connecting to a MariaDB
    server, check the server's ``default_authentication_plugin``/user auth
    plugin before assuming the DSN is wrong. See the package README's
    Configuration section for details.
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["MariadbSettings"]


class MariadbSettings(BaseAdapterSettings):
    """
    Settings for the MariaDB adapter.

    All fields read from environment variables. Missing required fields raise
    ``pydantic_core.ValidationError`` at instantiation time.

    Inherits from ``BaseAdapterSettings``:
        adapter_name:       str   = "mariadb"  (overrides base default)
        connection_timeout: float = 30.0
        operation_timeout:  float = 10.0
        max_retries:        int   = 3

    Attributes:
        database_url:         Full DSN, ``mysql://user:pass@host:port/dbname``
                              (required). See this module's docstring for why
                              this field intentionally shares its name with
                              ``openframe-adapters-db-mysql``'s ``database_url``.
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
    adapter_name: str = "mariadb"
