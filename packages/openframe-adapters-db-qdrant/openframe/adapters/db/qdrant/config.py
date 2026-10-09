"""
openframe/adapters/db/qdrant/config.py
========================================
Qdrant adapter settings.

Reads all connection configuration from environment variables via Pydantic
Settings. Every field is validated at instantiation — missing required
fields raise ``pydantic_core.ValidationError`` immediately so misconfigured
deployments fail fast on startup.

Required env vars:
    QDRANT_URL: Base URL of the Qdrant server.
                Format: http://host:6333  or  https://host:6333

Optional env vars (all have defaults):
    QDRANT_API_KEY:      str | None = None   (Qdrant Cloud / secured instances)
    QDRANT_PREFER_GRPC:  bool       = False  (REST/httpx transport by default —
                                              see connection.py's module
                                              docstring for why)
    QDRANT_HTTPS:        bool | None = None  (force TLS; None = infer from URL)
    QDRANT_ADAPTER_NAME: str        = "qdrant"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["QdrantSettings"]


class QdrantSettings(BaseAdapterSettings):
    """
    Settings for the Qdrant vector-store adapter.

    All fields read from environment variables. Missing required fields
    raise ``pydantic_core.ValidationError`` at instantiation time.

    Inherits from ``BaseAdapterSettings``:
        adapter_name:       str   = "qdrant"  (overrides base default)
        connection_timeout:  float = 30.0
        operation_timeout:   float = 10.0
        max_retries:         int   = 3

    Attributes:
        qdrant_url:         Base URL of the Qdrant server (required).
        qdrant_api_key:     API key for Qdrant Cloud / secured deployments.
                            Default None (no auth).
        qdrant_prefer_grpc: Use the gRPC transport instead of REST. Default
                            False — see ``connection.py``'s module docstring
                            for the verified default-transport behaviour of
                            ``AsyncQdrantClient``.
        qdrant_https:       Force TLS on the connection. Default None, which
                            lets the client infer scheme from ``qdrant_url``.
    """

    qdrant_url: str
    qdrant_api_key: str | None = None
    qdrant_prefer_grpc: bool = False
    qdrant_https: bool | None = None
    adapter_name: str = "qdrant"
