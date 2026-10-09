"""
openframe/adapters/db/chromadb/config.py
==========================================
ChromaDB adapter settings.

Reads all connection configuration from environment variables via Pydantic
Settings. Every field is validated at instantiation — missing required
fields raise ``pydantic_core.ValidationError`` immediately so misconfigured
deployments fail fast on startup.

This adapter targets Chroma's client-server (HTTP) mode exclusively — see
``connection.py``'s module docstring for why, and for the async-client
research finding that shaped this decision. There is deliberately no
embedded/persistent-local-mode settings field (``persist_directory`` etc.):
that mode has no server to connect to and is out of scope for this adapter.

Required env vars:
    None — every field below has a default suitable for a local Chroma
    server started with ``chroma run``.

Optional env vars (all have defaults):
    CHROMA_HOST:              str   = "localhost"
    CHROMA_PORT:              int   = 8000
    CHROMA_SSL:               bool  = False
    CHROMA_TENANT:            str   = "default_tenant"
    CHROMA_DATABASE:          str   = "default_database"
    CHROMA_COLLECTION:        str   = ""   (required to actually use a repository)
    CHROMADB_ADAPTER_NAME:    str   = "chromadb"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["ChromaDBSettings"]


class ChromaDBSettings(BaseAdapterSettings):
    """
    Settings for the ChromaDB adapter.

    All fields read from environment variables. None are strictly required
    — every field has a default that matches a locally-run Chroma server
    (``chroma run --path ./chroma-data``, which listens on
    ``localhost:8000`` by default) — but ``chroma_collection`` must be set
    (either here or via the ``collection`` constructor argument on
    :class:`~openframe.adapters.db.chromadb.repository.ChromaDBRepository`)
    before any repository operation will succeed.

    Inherits from ``BaseAdapterSettings``:
        adapter_name:       str   = "chromadb"  (overrides base default)
        connection_timeout: float = 30.0
        operation_timeout:  float = 10.0
        max_retries:        int   = 3

    Attributes:
        chroma_host:       Hostname of the Chroma HTTP server. Default "localhost".
        chroma_port:       HTTP port of the Chroma server. Default 8000.
        chroma_ssl:        Whether to use HTTPS when talking to the server.
                           Default False.
        chroma_tenant:     Tenant name. Default "default_tenant".
        chroma_database:   Database name within the tenant. Default
                           "default_database".
        chroma_collection: Default collection name used when no ``collection``
                           argument is passed to the repository constructor.
                           Default "" (empty — must be set before use).
    """

    chroma_host: str = "localhost"
    chroma_port: int = 8000
    chroma_ssl: bool = False
    chroma_tenant: str = "default_tenant"
    chroma_database: str = "default_database"
    chroma_collection: str = ""
    adapter_name: str = "chromadb"
