"""
tests/test_health.py
======================
Unit tests for PostgresRepository.ping() and is_ready().

Both methods must:
  - return True when the pool responds normally
  - return False (never raise) on any exception, including timeout
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from openframe.adapters.db.postgres import PostgresRepository


class TestPing:
    async def test_ping_returns_true_when_pool_responds(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.fetchval = AsyncMock(return_value=1)
        result = await repo.ping()
        assert result is True

    async def test_ping_returns_false_on_exception(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.fetchval = AsyncMock(side_effect=RuntimeError("dead"))
        result = await repo.ping()
        assert result is False

    async def test_ping_returns_false_on_timeout(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        async def slow(*_args, **_kwargs):
            await asyncio.sleep(10)

        mock_pool.fetchval = slow
        result = await repo.ping()
        assert result is False

    async def test_ping_never_raises(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.fetchval = AsyncMock(side_effect=Exception("any error"))
        # Must not propagate
        result = await repo.ping()
        assert result is False


class TestIsReady:
    async def test_is_ready_returns_true_when_pool_responds(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.fetchval = AsyncMock(return_value=3)
        result = await repo.is_ready()
        assert result is True

    async def test_is_ready_returns_false_on_exception(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.fetchval = AsyncMock(side_effect=ConnectionError("refused"))
        result = await repo.is_ready()
        assert result is False

    async def test_is_ready_returns_false_on_timeout(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        async def slow(*_args, **_kwargs):
            await asyncio.sleep(20)

        mock_pool.fetchval = slow
        result = await repo.is_ready()
        assert result is False

    async def test_is_ready_never_raises(
        self, repo: PostgresRepository, mock_pool: MagicMock
    ) -> None:
        mock_pool.fetchval = AsyncMock(side_effect=Exception("any error"))
        result = await repo.is_ready()
        assert result is False
