"""
openframe/adapters/db/milvus/config.py
=========================================
Milvus adapter settings.

Reads all connection configuration from environment variables via Pydantic
Settings. Every field is validated at instantiation — missing required fields
raise ``pydantic_core.ValidationError`` immediately so misconfigured
deployments fail fast on startup.

Required env vars:
    MILVUS_URI: Full Milvus server URI.
                Format: http://host:19530
                Milvus Lite (local file):  ./milvus_demo.db
                Zilliz Cloud:               https://in01-xxx.zillizcloud.com:19531

Optional env vars (all have defaults):
    MILVUS_TOKEN:              str   = ""        (API key / "user:password")
    MILVUS_DB_NAME:            str   = "default"
    MILVUS_METRIC_TYPE:        str   = "COSINE"
    MILVUS_VECTOR_DIM:         int   = 128
    MILVUS_ADAPTER_NAME:       str   = "milvus"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["MilvusSettings"]


class MilvusSettings(BaseAdapterSettings):
    """
    Settings for the Milvus adapter.

    All fields read from environment variables. Missing required fields raise
    ``pydantic_core.ValidationError`` at instantiation time.

    Inherits from ``BaseAdapterSettings``:
        adapter_name:       str   = "milvus"  (overrides base default)
        connection_timeout: float = 30.0
        operation_timeout:  float = 10.0
        max_retries:        int   = 3

    Attributes:
        milvus_uri:    Full Milvus server URI (required). Accepts a remote
                       gRPC/HTTP endpoint (``http://host:19530``), a Zilliz
                       Cloud endpoint, or a local Milvus Lite file path
                       (``./milvus_demo.db``) — ``AsyncMilvusClient`` treats
                       all three the same way.
        milvus_token:  API key or ``"user:password"`` credential string.
                       Empty string means no auth (local Milvus Lite / an
                       unauthenticated standalone server).
        milvus_db_name: Milvus database name within the instance. Default
                       ``"default"`` — most deployments never create another.
        metric_type:   Similarity metric used by :meth:`search` and at
                       collection-creation time. One of ``COSINE``, ``L2``,
                       ``IP``. Default ``"COSINE"``.
        vector_dim:    Dimensionality of the embedding vectors stored in
                       the collection. Used when auto-creating a collection.
                       Default ``128``.
    """

    milvus_uri: str
    milvus_token: str = ""
    milvus_db_name: str = "default"
    metric_type: str = "COSINE"
    vector_dim: int = 128
    adapter_name: str = "milvus"
