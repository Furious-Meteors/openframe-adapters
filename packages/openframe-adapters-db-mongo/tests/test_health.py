"""
tests/test_health.py
======================
Unit tests for MongoRepository.ping() and is_ready().

Both methods must:
  - return True when the backend responds normally
  - return False (never raise) on any exception including timeout
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pymongo.errors
import pytest

from openframe.adapters.db.mongo import MongoRepository


class TestPing:
    async def test_ping_returns_true_when_command_ok(
        self, repo: MongoRepository, mock_client: MagicMock
    ) -> None:
        mock_client.admin.command = AsyncMock(return_value={"ok": 1})
        result = await repo.ping()
        assert result is True

    async def test_ping_returns_false_on_connection_failure(
        self, repo: MongoRepository, mock_client: MagicMock
    ) -> None:
        mock_client.admin.command = AsyncMock(
            side_effect=pymongo.errors.ConnectionFailure("down")
        )
        result = await repo.ping()
        assert result is False

    async def test_ping_returns_false_on_timeout(
        self, repo: MongoRepository, mock_client: MagicMock
    ) -> None:
        async def slow(*args, **kwargs):
            await asyncio.sleep(10)

        mock_client.admin.command = slow
        result = await repo.ping()
        assert result is False

    async def test_ping_returns_false_on_any_exception(
        self, repo: MongoRepository, mock_client: MagicMock
    ) -> None:
        mock_client.admin.command = AsyncMock(side_effect=RuntimeError("unexpected"))
        result = await repo.ping()
        assert result is False

    async def test_ping_never_raises(
        self, repo: MongoRepository, mock_client: MagicMock
    ) -> None:
        mock_client.admin.command = AsyncMock(side_effect=Exception("any error"))
        result = await repo.ping()
        assert result is False


class TestIsReady:
    async def test_is_ready_returns_true_when_list_collection_names_responds(
        self, repo: MongoRepository, mock_client: MagicMock
    ) -> None:
        db = MagicMock()
        db.list_collection_names = AsyncMock(return_value=["artifacts", "papers"])
        mock_client.__getitem__ = MagicMock(return_value=db)
        result = await repo.is_ready()
        assert result is True

    async def test_is_ready_returns_false_on_exception(
        self, repo: MongoRepository, mock_client: MagicMock
    ) -> None:
        db = MagicMock()
        db.list_collection_names = AsyncMock(
            side_effect=pymongo.errors.OperationFailure("auth error")
        )
        mock_client.__getitem__ = MagicMock(return_value=db)
        result = await repo.is_ready()
        assert result is False

    async def test_is_ready_returns_false_on_timeout(
        self, repo: MongoRepository, mock_client: MagicMock
    ) -> None:
        async def slow():
            await asyncio.sleep(20)

        db = MagicMock()
        db.list_collection_names = slow
        mock_client.__getitem__ = MagicMock(return_value=db)
        result = await repo.is_ready()
        assert result is False

    async def test_is_ready_never_raises(
        self, repo: MongoRepository, mock_client: MagicMock
    ) -> None:
        db = MagicMock()
        db.list_collection_names = AsyncMock(side_effect=Exception("any error"))
        mock_client.__getitem__ = MagicMock(return_value=db)
        result = await repo.is_ready()
        assert result is False
