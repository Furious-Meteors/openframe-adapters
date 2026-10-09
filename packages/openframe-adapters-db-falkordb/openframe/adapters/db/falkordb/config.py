"""
openframe/adapters/db/falkordb/config.py
==========================================
Settings for the FalkorDB adapter, sourced from environment variables via
``openframe-core``'s ``BaseAdapterSettings`` (pydantic-settings).

FalkorDB is a Redis-based graph database — its Python client
(``falkordb.asyncio.FalkorDB``) wraps ``redis.asyncio.Redis`` and accepts
the same connection kwargs (``host``/``port``/``password``/``ssl``/...).
This settings class mirrors ``RedisSettings``'s host/port shape (not
Postgres's single-URL shape) for exactly that reason.

Required env vars:
    None — every field has a default suitable for a local FalkorDB
    instance started with ``docker run -p 6379:6379 falkordb/falkordb``.

Optional env vars (all have defaults):
    FALKORDB_HOST:                  str   = "localhost"
    FALKORDB_PORT:                  int   = 6379
    FALKORDB_PASSWORD:              str | None = None
    FALKORDB_SSL:                   bool  = False
    FALKORDB_SOCKET_TIMEOUT:        float = 5.0
    FALKORDB_SOCKET_CONNECT_TIMEOUT: float = 5.0
    FALKORDB_GRAPH_NAME:            str   = "openframe"
    FALKORDB_NODE_LABEL:            str   = "Entity"
    ADAPTER_NAME:                   str   = "falkordb"

Addressing scheme (see module docstring of ``repository.py`` for the full
explanation): nodes are addressed by an application-level ``id`` property,
never FalkorDB's internal, unstable ``id()`` function. ``node_label`` names
the single Cypher label this repository instance's nodes share and is
validated here, at config-load time, because Cypher has no parameterized
label syntax — the label gets interpolated directly into every query
string this adapter builds, so an unvalidated value would be a Cypher
injection vector.
"""
from __future__ import annotations

import re

from pydantic import field_validator

from openframe.core.config import BaseAdapterSettings

__all__ = ["FalkorDBSettings"]

# Conservative: ASCII letters, digits, underscore; must not start with a
# digit. This matches Cypher's own identifier grammar closely enough to be
# safe to interpolate directly into a query string, and is far stricter
# than Cypher technically allows (e.g. backtick-quoted labels) — safety
# over flexibility, since labels come from config, not end users.
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class FalkorDBSettings(BaseAdapterSettings):
    """
    Pydantic-settings subclass for the FalkorDB adapter.

    All fields are read from environment variables with the exact names
    shown above. ``BaseAdapterSettings`` provides ``operation_timeout``
    (default 30.0 s) and ``connection_timeout`` (default 30.0 s).

    Attributes:
        falkordb_host:        FalkorDB/Redis host. Default "localhost".
        falkordb_port:        FalkorDB/Redis port. Default 6379.
        falkordb_password:    Optional auth password. Default None.
        falkordb_ssl:         Use TLS. Default False.
        falkordb_socket_timeout:         Socket read/write timeout (s).
        falkordb_socket_connect_timeout: Socket connect timeout (s).
        falkordb_graph_name:   Name of the graph (FalkorDB's unit of
                               isolation, analogous to a Redis key/a
                               Postgres database). Default "openframe".
        falkordb_node_label:   The single Cypher label this repository's
                               nodes share. Validated to be a safe Cypher
                               identifier (``^[A-Za-z_][A-Za-z0-9_]*$``)
                               since it is interpolated into query strings
                               — Cypher has no parameter syntax for labels.
        adapter_name:          Identity name. Default "falkordb".

    Raises:
        pydantic_core.ValidationError: If ``falkordb_node_label`` is not a
            safe identifier, or any required field is missing.
    """

    falkordb_host: str = "localhost"
    falkordb_port: int = 6379
    falkordb_password: str | None = None
    falkordb_ssl: bool = False
    falkordb_socket_timeout: float = 5.0
    falkordb_socket_connect_timeout: float = 5.0
    falkordb_graph_name: str = "openframe"
    falkordb_node_label: str = "Entity"
    adapter_name: str = "falkordb"

    @field_validator("falkordb_node_label")
    @classmethod
    def _validate_node_label(cls, value: str) -> str:
        """
        Reject anything that is not a safe, injection-free Cypher label.

        This is the ONLY validation point — ``repository.py`` interpolates
        ``falkordb_node_label`` directly into every Cypher string it
        builds, trusting that this validator already ran. Validating here
        (at config-load time) rather than per-query means a malicious or
        malformed label fails fast at startup, not on the first query.
        """
        if not _SAFE_IDENTIFIER_RE.match(value):
            raise ValueError(
                f"falkordb_node_label={value!r} is not a safe Cypher "
                "identifier. Must match ^[A-Za-z_][A-Za-z0-9_]*$ "
                "(letters, digits, underscore; cannot start with a digit)."
            )
        return value

    @field_validator("falkordb_graph_name")
    @classmethod
    def _validate_graph_name(cls, value: str) -> str:
        """Graph names are passed as ``select_graph(graph_id)`` — a regular
        driver argument, not interpolated into Cypher — but we still
        reject empty strings since an empty graph name is always a
        configuration mistake."""
        if not value:
            raise ValueError("falkordb_graph_name must not be empty")
        return value
