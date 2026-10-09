"""
openframe/adapters/db/dynamodb/config.py
===========================================
DynamoDB adapter settings.

Reads all connection configuration from environment variables via Pydantic
Settings. Every field is validated at instantiation — missing required fields
raise ``pydantic_core.ValidationError`` immediately so misconfigured
deployments fail fast on startup.

Unlike Postgres/MySQL, DynamoDB has no single connection-string DSN. A client
is identified by an AWS region plus a table name (and, for local development,
an optional ``endpoint_url`` pointing at a local DynamoDB process instead of
the real AWS service). Credentials are optional here — when omitted,
``aioboto3``/``botocore`` fall back to the standard AWS credential chain
(environment variables, shared config file, instance/task role, etc.), which
is the normal way production deployments authenticate.

Required env vars:
    AWS_REGION:          AWS region, e.g. "us-east-1".
    DYNAMODB_TABLE_NAME: DynamoDB table name.

Optional env vars (all have defaults):
    DYNAMODB_ENDPOINT_URL:     str | None = None
                               Override endpoint, e.g. "http://localhost:8000"
                               for a local DynamoDB container. Leave unset to
                               hit real AWS.
    AWS_ACCESS_KEY_ID:         str | None = None
    AWS_SECRET_ACCESS_KEY:     str | None = None
    AWS_SESSION_TOKEN:         str | None = None
                               All three are optional. When unset, aioboto3
                               falls back to its normal credential resolution
                               chain. For local DynamoDB they can be any
                               non-empty placeholder string, since local
                               DynamoDB does not check them.
    DYNAMODB_ADAPTER_NAME:     str   = "dynamodb"
"""
from __future__ import annotations

from openframe.core.config import BaseAdapterSettings

__all__ = ["DynamoDBSettings"]


class DynamoDBSettings(BaseAdapterSettings):
    """
    Settings for the DynamoDB adapter.

    All fields read from environment variables. Missing required fields raise
    ``pydantic_core.ValidationError`` at instantiation time.

    Inherits from ``BaseAdapterSettings``:
        adapter_name:       str   = "dynamodb"  (overrides base default)
        connection_timeout: float = 30.0
        operation_timeout:  float = 10.0
        max_retries:        int   = 3

    Attributes:
        aws_region:            AWS region, e.g. ``"us-east-1"`` (required).
        dynamodb_table_name:   DynamoDB table name (required).
        endpoint_url:          Optional override endpoint, e.g.
                               ``"http://localhost:8000"`` for local DynamoDB.
                               ``None`` (default) hits real AWS.
        aws_access_key_id:     Optional explicit access key. ``None`` (default)
                               defers to aioboto3's normal credential chain.
        aws_secret_access_key: Optional explicit secret key.
        aws_session_token:     Optional session token (for temporary creds).
    """

    aws_region: str
    dynamodb_table_name: str
    endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None
    adapter_name: str = "dynamodb"
