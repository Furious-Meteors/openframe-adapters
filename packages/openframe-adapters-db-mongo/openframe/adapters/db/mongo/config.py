"""
openframe/adapters/db/mongo/config.py
=======================================
MongoDB adapter settings.

Reads all connection configuration from environment variables via Pydantic
Settings. Every field is validated at instantiation — missing required fields
raise ``pydantic_core.ValidationError`` immediately so misconfigured
deployments fail fast on startup.

Required env vars:
    MONGO_URL:      Full motor/pymongo connection string.
                    Standard: mongodb://user:pass@host:port
                    Atlas:    mongodb+srv://user:pass@cluster.mongodb.net
                    Atlas SRV URIs require dnspython, which motor resolves
                    automatically — no extra dependency needed.
    MONGO_DATABASE: Database name to operate on.

Optional env vars (all have defaults):
    MONGO_MIN_POOL_SIZE:               int  = 5
    MONGO_MAX_POOL_SIZE:               int  = 20
    MONGO_SERVER_SELECTION_TIMEOUT_MS: int  = 5000
    MONGO_TLS:                         bool = False
    MONGO_TLS_ALLOW_INVALID_CERTS:     bool = False
    ADAPTER_NAME:                      str  = "mongo"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["MongoSettings"]


class MongoSettings(BaseAdapterSettings):
    """
    Settings for the MongoDB adapter.

    All fields read from environment variables. Missing required fields raise
    ``pydantic_core.ValidationError`` at instantiation time.

    Inherits from ``BaseAdapterSettings``:
        adapter_name:       str   = "mongo"   (overrides base default)
        connection_timeout: float = 30.0
        operation_timeout:  float = 10.0
        max_retries:        int   = 3

    Attributes:
        mongo_url:                        Motor/pymongo connection string
                                          (required). Standard or Atlas SRV.
        mongo_database:                   Database name (required).
        mongo_min_pool_size:              Minimum connections in the pool.
                                          Default 5.
        mongo_max_pool_size:              Maximum connections in the pool.
                                          Default 20.
        mongo_server_selection_timeout_ms: Milliseconds to wait for a suitable
                                          server. Default 5000.
        mongo_tls:                        Enable TLS/SSL. Default False.
        mongo_tls_allow_invalid_certs:    Skip certificate validation — for
                                          development only. Default False.
    """

    mongo_url: str
    mongo_database: str
    mongo_min_pool_size: int = 5
    mongo_max_pool_size: int = 20
    mongo_server_selection_timeout_ms: int = 5000
    mongo_tls: bool = False
    mongo_tls_allow_invalid_certs: bool = False
    adapter_name: str = "mongo"
