"""
tests/test_config.py — openframe-adapters-db-redis
=====================================================
Unit tests for RedisSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.db.redis import RedisSettings
from openframe.core.config import BaseAdapterSettings


class TestRedisSettings:
    def test_instantiates_with_required_fields(self):
        s = RedisSettings(redis_url="redis://localhost:6379/0")
        assert s.redis_url == "redis://localhost:6379/0"

    def test_missing_redis_url_raises_validation_error(self):
        with pytest.raises(ValidationError):
            RedisSettings()

    def test_redis_max_connections_default(self):
        s = RedisSettings(redis_url="redis://localhost:6379/0")
        assert s.redis_max_connections == 10

    def test_redis_socket_timeout_default(self):
        s = RedisSettings(redis_url="redis://localhost:6379/0")
        assert s.redis_socket_timeout == 5.0

    def test_redis_socket_connect_timeout_default(self):
        s = RedisSettings(redis_url="redis://localhost:6379/0")
        assert s.redis_socket_connect_timeout == 5.0

    def test_redis_key_prefix_default(self):
        s = RedisSettings(redis_url="redis://localhost:6379/0")
        assert s.redis_key_prefix == "openframe"

    def test_redis_default_ttl_default(self):
        s = RedisSettings(redis_url="redis://localhost:6379/0")
        assert s.redis_default_ttl == 0

    def test_adapter_name_default(self):
        s = RedisSettings(redis_url="redis://localhost:6379/0")
        assert s.adapter_name == "redis"

    def test_connection_timeout_inherited_default(self):
        s = RedisSettings(redis_url="redis://localhost:6379/0")
        assert s.connection_timeout == 30.0

    def test_reads_redis_max_connections_from_env(self, monkeypatch):
        monkeypatch.setenv("REDIS_MAX_CONNECTIONS", "20")
        s = RedisSettings(redis_url="redis://localhost:6379/0")
        assert s.redis_max_connections == 20

    def test_is_subclass_of_base_adapter_settings(self):
        assert issubclass(RedisSettings, BaseAdapterSettings)
