"""
openframe.adapters.db.dynamodb
==================================
DynamoDB database adapter for the OpenFrame Microservice Suite.

Public API:

    DynamoDBSettings    — Pydantic Settings subclass for connection config.
    DynamoDBRepository  — Generic async repository (BaseRepository).
    get_dynamodb_table  — Async factory that creates / returns the cached
                          DynamoDB Table resource.

Quick start::

    from openframe.adapters.db.dynamodb import (
        DynamoDBSettings,
        DynamoDBRepository,
        get_dynamodb_table,
    )

    settings = DynamoDBSettings(aws_region="us-east-1", dynamodb_table_name="items")

    # Raw dict mode
    repo = DynamoDBRepository(settings, id_column="id")
    item = await repo.get("abc-123")           # dict | None

    # Typed mode — subclass and override mapping methods
    class ItemRepository(DynamoDBRepository[Item]):
        _id_column = "id"

        def _row_to_entity(self, row):
            return Item(**row)

        def _entity_to_row(self, entity):
            return entity.model_dump()
"""
from __future__ import annotations

from .config import DynamoDBSettings
from .connection import get_dynamodb_table
from .plugin import DynamoDBPlugin
from .repository import DynamoDBRepository

__all__ = [
    "DynamoDBSettings",
    "DynamoDBRepository",
    "get_dynamodb_table",
    "DynamoDBPlugin",
]
