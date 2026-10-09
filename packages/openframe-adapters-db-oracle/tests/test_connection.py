"""
tests/test_connection.py
==========================
Unit tests for get_oracle_pool() and _pool_cache.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import oracledb

from openframe.adapters.db.oracle import OracleSettings
from openframe.adapters.db.oracle.connection import _cache_key, _pool_cache, get_oracle_pool
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)


def make_oracle_error(exc_cls, full_code: str, message: str = "boom"):
    """
    Build a real oracledb exception instance shaped the way the driver
    actually raises them: exc.args[0] is an object exposing .full_code
    (verified against the installed driver — see connection.py's module
    docstring for the investigation that established this shape).
    """
    err = MagicMock()
    err.full_code = full_code
    err.code = 0
    err.message = message
    return exc_cls(err)


@pytest.fixture(autouse=True)
def clear_pool_cache():
    """Ensure _pool_cache is clean before and after every test."""
    _pool_cache.clear()
    yield
    _pool_cache.clear()


@pytest.fixture
def settings() -> OracleSettings:
    return OracleSettings(oracle_dsn="u/p@localhost:1521/db")


@pytest.fixture
def settings_alt() -> OracleSettings:
    return OracleSettings(oracle_dsn="u/p@localhost:1521/db2")


class TestGetOraclePool:
    async def test_returns_cached_pool_on_second_call(self, settings: OracleSettings) -> None:
        fake_pool = MagicMock()
        with patch(
            "openframe.adapters.db.oracle.connection.oracledb.create_pool_async",
            new=AsyncMock(return_value=fake_pool),
        ):
            pool1 = await get_oracle_pool(settings)
            pool2 = await get_oracle_pool(settings)

        assert pool1 is pool2
        assert pool1 is fake_pool

    async def test_different_dsns_produce_different_pools(
        self, settings: OracleSettings, settings_alt: OracleSettings
    ) -> None:
        pool_a = MagicMock(name="pool_a")
        pool_b = MagicMock(name="pool_b")
        create_pool_mock = AsyncMock(side_effect=[pool_a, pool_b])
        with patch(
            "openframe.adapters.db.oracle.connection.oracledb.create_pool_async",
            new=create_pool_mock,
        ):
            p1 = await get_oracle_pool(settings)
            p2 = await get_oracle_pool(settings_alt)

        assert p1 is not p2
        assert p1 is pool_a
        assert p2 is pool_b

    async def test_same_dsn_different_pool_max_produces_different_pools(
        self, settings: OracleSettings
    ) -> None:
        """
        Regression test: two Settings for the SAME oracle_dsn but different
        pool_max must NOT share a pool — the second caller must not
        silently inherit the first caller's pool configuration.
        """
        settings_bigger_pool = OracleSettings(oracle_dsn=settings.oracle_dsn, pool_max=50)
        pool_small = MagicMock(name="pool_small")
        pool_big = MagicMock(name="pool_big")
        create_pool_mock = AsyncMock(side_effect=[pool_small, pool_big])
        with patch(
            "openframe.adapters.db.oracle.connection.oracledb.create_pool_async",
            new=create_pool_mock,
        ):
            p1 = await get_oracle_pool(settings)
            p2 = await get_oracle_pool(settings_bigger_pool)

        assert p1 is not p2
        assert p1 is pool_small
        assert p2 is pool_big
        # A third call with settings matching the second config reuses it.
        with patch(
            "openframe.adapters.db.oracle.connection.oracledb.create_pool_async",
            new=AsyncMock(side_effect=AssertionError("should not create a third pool")),
        ):
            p3 = await get_oracle_pool(settings_bigger_pool)
        assert p3 is pool_big

    async def test_pool_stored_in_cache_after_creation(self, settings: OracleSettings) -> None:
        fake_pool = MagicMock()
        with patch(
            "openframe.adapters.db.oracle.connection.oracledb.create_pool_async",
            new=AsyncMock(return_value=fake_pool),
        ):
            await get_oracle_pool(settings)

        assert _pool_cache[_cache_key(settings)] is fake_pool

    async def test_timeout_raises_adapter_timeout_error(self, settings: OracleSettings) -> None:
        with patch(
            "openframe.adapters.db.oracle.connection.oracledb.create_pool_async",
            new=AsyncMock(side_effect=asyncio.TimeoutError()),
        ):
            with pytest.raises(AdapterTimeoutError) as exc_info:
                await get_oracle_pool(settings)

        assert exc_info.value.operation == "connect"

    async def test_dpy6_connection_error_raises_adapter_connection_error(
        self, settings: OracleSettings
    ) -> None:
        """DPY-6xxx (connection failed) — verified against oracledb 26.0.1."""
        err = make_oracle_error(oracledb.OperationalError, "DPY-6005")
        with patch(
            "openframe.adapters.db.oracle.connection.oracledb.create_pool_async",
            new=AsyncMock(side_effect=err),
        ):
            with pytest.raises(AdapterConnectionError) as exc_info:
                await get_oracle_pool(settings)

        assert exc_info.value.cause is err

    async def test_dpy4_config_error_raises_adapter_configuration_error(
        self, settings: OracleSettings
    ) -> None:
        """DPY-4xxx (malformed connect string) — verified: DPY-4027 in the installed driver."""
        err = make_oracle_error(oracledb.DatabaseError, "DPY-4027")
        with patch(
            "openframe.adapters.db.oracle.connection.oracledb.create_pool_async",
            new=AsyncMock(side_effect=err),
        ):
            with pytest.raises(AdapterConfigurationError) as exc_info:
                await get_oracle_pool(settings)

        assert exc_info.value.operation == "init"

    async def test_dns_resolution_failure_raises_adapter_connection_error(
        self, settings: OracleSettings
    ) -> None:
        """
        Real gotcha found during the research spike: a DNS resolution
        failure escapes oracledb.connect_async() as a raw socket.gaierror
        (an OSError subclass), NOT an oracledb.Error. This must be caught
        explicitly or it would propagate untranslated past the adapter
        boundary.
        """
        import socket

        with patch(
            "openframe.adapters.db.oracle.connection.oracledb.create_pool_async",
            new=AsyncMock(side_effect=socket.gaierror("nodename nor servname provided")),
        ):
            with pytest.raises(AdapterConnectionError):
                await get_oracle_pool(settings)
