"""
tests/test_health.py — openframe-adapters-db-redis
=====================================================
Unit tests for RedisRepository ping() and is_ready().
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
import redis.exceptions

from openframe.adapters.db.redis import RedisRepository


class TestPing:
    async def test_ping_returns_true_when_client_responds(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.ping.return_value = True
        result = await repo.ping()
        assert result is True

    async def test_ping_returns_false_on_connection_error(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.ping.side_effect = redis.exceptions.ConnectionError("down")
        result = await repo.ping()
        assert result is False

    async def test_ping_returns_false_on_timeout(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.ping.side_effect = asyncio.TimeoutError()
        result = await repo.ping()
        assert result is False

    async def test_ping_never_raises_on_any_exception(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.ping.side_effect = RuntimeError("unexpected")
        result = await repo.ping()
        assert result is False


class TestIsReady:
    async def test_is_ready_returns_true_when_info_has_redis_version(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.info.return_value = {"redis_version": "7.0.0", "uptime_in_seconds": 100}
        result = await repo.is_ready()
        assert result is True

    async def test_is_ready_returns_false_when_redis_version_missing(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.info.return_value = {"uptime_in_seconds": 100}
        result = await repo.is_ready()
        assert result is False

    async def test_is_ready_returns_false_on_exception(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.info.side_effect = redis.exceptions.ConnectionError("gone")
        result = await repo.is_ready()
        assert result is False

    async def test_is_ready_never_raises(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.info.side_effect = RuntimeError("unexpected")
        result = await repo.is_ready()
        assert result is False
