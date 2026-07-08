"""
tests/test_repository.py — openframe-adapters-db-redis
========================================================
Contract tests (RepositoryContractTests) run first, then adapter-specific
unit tests covering Redis error mapping and key/serialisation behaviour.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
import redis.exceptions

from openframe.adapters.db.redis import RedisRepository, RedisSettings
from openframe.core.exceptions import AdapterQueryError, AdapterTimeoutError
from openframe.core.ports import BaseRepository
from openframe.core.testing import RepositoryContractTests


# ── Contract tests — must pass for every BaseRepository implementation ─────

class TestRedisRepositoryContracts(RepositoryContractTests):
    """
    RedisRepository passes the full openframe contract suite.

    All 18 RepositoryContractTests run against a stateful in-memory mock.
    The mock tracks SET/GET/DELETE so that create→get, list pagination,
    update, and delete all behave like a real Redis instance.
    No real Redis server required.
    """

    @pytest.fixture
    def repository(self, mock_settings: RedisSettings) -> RedisRepository:
        import openframe.adapters.db.redis.connection as conn_module

        # In-memory store: full_key → json_string
        store: dict[str, str] = {}

        client = MagicMock()

        # ping / info for health checks inside the repo
        client.ping = AsyncMock(return_value=True)
        client.info = AsyncMock(return_value={"redis_version": "7.0.0"})
        client.aclose = AsyncMock()

        async def _get(key: str) -> str | None:
            return store.get(key)

        async def _set(key: str, value: str, nx: bool = False, xx: bool = False, ex: int | None = None) -> bool | None:
            if nx and key in store:
                return None
            if xx and key not in store:
                return None
            store[key] = value
            return True

        async def _delete(*keys: str) -> int:
            deleted = 0
            for k in keys:
                if k in store:
                    del store[k]
                    deleted += 1
            return deleted

        async def _mget(*keys: str) -> list[str | None]:
            return [store.get(k) for k in keys]

        async def _scan_iter(match: str = "*", count: int = 100):
            prefix = match.rstrip("*")
            for key in list(store.keys()):
                if key.startswith(prefix):
                    yield key

        client.get = AsyncMock(side_effect=_get)
        client.set = AsyncMock(side_effect=_set)
        client.delete = AsyncMock(side_effect=_delete)
        client.mget = AsyncMock(side_effect=_mget)
        client.scan_iter = _scan_iter

        conn_module._client_cache[mock_settings.redis_url] = client
        r = RedisRepository(mock_settings)
        yield r
        conn_module._client_cache.clear()
        store.clear()

    @pytest.fixture
    def port(self, repository):
        return repository

    @pytest.fixture
    def make_entity(self):
        def _make(id: str, name: str = "test") -> dict:
            return {"id": id, "name": name}
        return _make


# ── Adapter-specific tests ─────────────────────────────────────────────────

class TestProtocolConformance:
    def test_isinstance_base_repository(self, repo: RedisRepository) -> None:
        assert isinstance(repo, BaseRepository)


class TestMakeKey:
    def test_key_format_is_prefix_colon_id(self, repo: RedisRepository) -> None:
        assert repo._make_key("abc") == "openframe:abc"

    def test_custom_prefix_from_settings(self, mock_settings: RedisSettings, mock_redis: MagicMock) -> None:
        import openframe.adapters.db.redis.connection as conn_module
        settings = RedisSettings(
            redis_url="redis://localhost:6379/0",
            redis_key_prefix="myapp",
        )
        conn_module._client_cache[settings.redis_url] = mock_redis
        r = RedisRepository(settings)
        assert r._make_key("item-1") == "myapp:item-1"
        conn_module._client_cache.clear()


class TestGet:
    async def test_get_returns_none_for_missing_key(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.get.return_value = None
        result = await repo.get("missing")
        assert result is None

    async def test_get_deserialises_json(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.get.return_value = json.dumps({"id": "1", "name": "test"})
        result = await repo.get("1")
        assert result == {"id": "1", "name": "test"}

    async def test_get_timeout_raises_adapter_timeout_error(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.get.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get("1")
        assert exc_info.value.operation == "get"

    async def test_get_response_error_raises_adapter_query_error(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.get.side_effect = redis.exceptions.ResponseError("err")
        with pytest.raises(AdapterQueryError):
            await repo.get("1")


class TestCreate:
    async def test_create_uses_set_nx(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        """create() must use SET NX (set-if-not-exists)."""
        mock_redis.set.return_value = True
        await repo.create({"id": "1", "name": "test"})
        call_kwargs = mock_redis.set.call_args
        assert call_kwargs.kwargs.get("nx") is True

    async def test_create_serialises_entity_to_json(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.set.return_value = True
        entity = {"id": "1", "name": "test"}
        await repo.create(entity)
        stored_value = mock_redis.set.call_args.args[1]
        assert json.loads(stored_value) == entity

    async def test_create_raises_query_error_when_key_exists(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        """SET NX returns None/False when key already exists."""
        mock_redis.set.return_value = None
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.create({"id": "1", "name": "test"})
        assert exc_info.value.operation == "create"

    async def test_create_raises_query_error_when_entity_has_no_id(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.create({"name": "no id here"})
        assert exc_info.value.operation == "create"

    async def test_create_with_ttl_passes_ex_to_set(
        self, mock_settings: RedisSettings, mock_redis: MagicMock
    ) -> None:
        import openframe.adapters.db.redis.connection as conn_module
        settings = RedisSettings(
            redis_url="redis://localhost:6379/0",
            redis_default_ttl=300,
        )
        conn_module._client_cache[settings.redis_url] = mock_redis
        mock_redis.set.return_value = True
        repo = RedisRepository(settings)
        await repo.create({"id": "ttl-test", "name": "x"})
        assert mock_redis.set.call_args.kwargs.get("ex") == 300
        conn_module._client_cache.clear()

    async def test_create_with_zero_ttl_passes_ex_none(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        """TTL of 0 means no expiry — ex= should be None."""
        mock_redis.set.return_value = True
        await repo.create({"id": "1", "name": "test"})
        assert mock_redis.set.call_args.kwargs.get("ex") is None


class TestUpdate:
    async def test_update_uses_set_xx(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        """update() must use SET XX (set-only-if-exists)."""
        mock_redis.set.return_value = True
        await repo.update({"id": "1", "name": "updated"})
        assert mock_redis.set.call_args.kwargs.get("xx") is True

    async def test_update_returns_entity_on_success(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.set.return_value = True
        entity = {"id": "1", "name": "updated"}
        result = await repo.update(entity)
        assert result == entity

    async def test_update_returns_none_when_key_does_not_exist(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.set.return_value = None
        result = await repo.update({"id": "nonexistent", "name": "x"})
        assert result is None


class TestDelete:
    async def test_delete_returns_true_when_key_deleted(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.delete.return_value = 1
        result = await repo.delete("1")
        assert result is True

    async def test_delete_returns_false_when_key_missing(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.delete.return_value = 0
        result = await repo.delete("missing")
        assert result is False

    async def test_delete_timeout_raises_adapter_timeout_error(
        self, repo: RedisRepository, mock_redis: MagicMock
    ) -> None:
        mock_redis.delete.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.delete("1")
