"""
tests/test_connection.py
==========================
Unit tests for get_cassandra_session(), _session_cache, and the
ResponseFuture -> asyncio bridge.
"""
from __future__ import annotations

import asyncio
import threading
import time
from unittest.mock import MagicMock, patch

import cassandra
import pytest

from openframe.adapters.db.cassandra import CassandraSettings
from openframe.adapters.db.cassandra.connection import (
    _cache_key,
    _response_future_to_asyncio,
    _session_cache,
    get_cassandra_session,
)
from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)


@pytest.fixture(autouse=True)
def clear_session_cache():
    """Ensure _session_cache is clean before and after every test."""
    _session_cache.clear()
    yield
    _session_cache.clear()


@pytest.fixture
def settings() -> CassandraSettings:
    return CassandraSettings(cassandra_contact_points=["10.0.0.1"])


@pytest.fixture
def settings_alt() -> CassandraSettings:
    return CassandraSettings(cassandra_contact_points=["10.0.0.2"])


class TestGetCassandraSession:
    async def test_returns_cached_session_on_second_call(
        self, settings: CassandraSettings
    ) -> None:
        fake_session = MagicMock()
        with patch(
            "openframe.adapters.db.cassandra.connection._build_session",
            return_value=fake_session,
        ):
            s1 = await get_cassandra_session(settings)
            s2 = await get_cassandra_session(settings)

        assert s1 is s2
        assert s1 is fake_session

    async def test_different_contact_points_produce_different_sessions(
        self, settings: CassandraSettings, settings_alt: CassandraSettings
    ) -> None:
        session_a = MagicMock(name="session_a")
        session_b = MagicMock(name="session_b")
        with patch(
            "openframe.adapters.db.cassandra.connection._build_session",
            side_effect=[session_a, session_b],
        ):
            s1 = await get_cassandra_session(settings)
            s2 = await get_cassandra_session(settings_alt)

        assert s1 is not s2
        assert s1 is session_a
        assert s2 is session_b

    async def test_same_contact_points_different_keyspace_produces_different_sessions(
        self, settings: CassandraSettings
    ) -> None:
        """
        Regression test: two Settings for the SAME contact points but
        different keyspace must NOT share a session — the second caller
        must not silently inherit the first caller's connection config.
        """
        settings_other_keyspace = CassandraSettings(
            cassandra_contact_points=settings.cassandra_contact_points,
            cassandra_keyspace="other_ks",
        )
        session_small = MagicMock(name="session_default_ks")
        session_big = MagicMock(name="session_other_ks")
        with patch(
            "openframe.adapters.db.cassandra.connection._build_session",
            side_effect=[session_small, session_big],
        ):
            p1 = await get_cassandra_session(settings)
            p2 = await get_cassandra_session(settings_other_keyspace)

        assert p1 is not p2
        assert p1 is session_small
        assert p2 is session_big

        # A third call with settings matching the second config reuses it.
        with patch(
            "openframe.adapters.db.cassandra.connection._build_session",
            side_effect=AssertionError("should not build a third session"),
        ):
            p3 = await get_cassandra_session(settings_other_keyspace)
        assert p3 is session_big

    async def test_empty_contact_points_raises_configuration_error(self) -> None:
        settings = CassandraSettings(cassandra_contact_points=[])
        with pytest.raises(AdapterConfigurationError):
            await get_cassandra_session(settings)

    async def test_no_host_available_raises_adapter_connection_error(
        self, settings: CassandraSettings
    ) -> None:
        with patch(
            "openframe.adapters.db.cassandra.connection._build_session",
            side_effect=cassandra.cluster.NoHostAvailable("no hosts", {}),
        ):
            with pytest.raises(AdapterConnectionError) as exc_info:
                await get_cassandra_session(settings)
        assert exc_info.value.retryable is True

    async def test_authentication_failed_raises_non_retryable_connection_error(
        self, settings: CassandraSettings
    ) -> None:
        with patch(
            "openframe.adapters.db.cassandra.connection._build_session",
            side_effect=cassandra.AuthenticationFailed("bad creds"),
        ):
            with pytest.raises(AdapterConnectionError) as exc_info:
                await get_cassandra_session(settings)
        assert exc_info.value.retryable is False

    async def test_os_error_raises_adapter_connection_error(
        self, settings: CassandraSettings
    ) -> None:
        with patch(
            "openframe.adapters.db.cassandra.connection._build_session",
            side_effect=OSError("DNS resolution failed"),
        ):
            with pytest.raises(AdapterConnectionError):
                await get_cassandra_session(settings)

    async def test_timeout_raises_adapter_timeout_error(
        self, settings: CassandraSettings
    ) -> None:
        def _slow_build(_settings):
            time.sleep(0.2)
            return MagicMock()

        settings.connection_timeout = 0.01
        with patch(
            "openframe.adapters.db.cassandra.connection._build_session",
            side_effect=_slow_build,
        ):
            with pytest.raises(AdapterTimeoutError):
                await get_cassandra_session(settings)

    async def test_session_stored_in_cache_after_creation(
        self, settings: CassandraSettings
    ) -> None:
        fake_session = MagicMock()
        with patch(
            "openframe.adapters.db.cassandra.connection._build_session",
            return_value=fake_session,
        ):
            await get_cassandra_session(settings)

        assert _session_cache[_cache_key(settings)] is fake_session


class TestResponseFutureToAsyncioBridge:
    """
    Exercises the real bridging logic with a background thread invoking the
    callback — the same threading shape cassandra-driver's own reactor
    uses (callbacks fire off the event-loop thread).
    """

    async def test_success_callback_resolves_future(self) -> None:
        class ThreadedResponseFuture:
            def add_callbacks(self, callback, errback, **_kw):
                def fire():
                    time.sleep(0.02)
                    callback(["row1", "row2"])

                threading.Thread(target=fire, daemon=True).start()

        loop = asyncio.get_running_loop()
        aio_future = _response_future_to_asyncio(ThreadedResponseFuture(), loop)
        result = await aio_future
        assert result == ["row1", "row2"]

    async def test_error_callback_raises_on_future(self) -> None:
        class ThreadedResponseFuture:
            def add_callbacks(self, callback, errback, **_kw):
                def fire():
                    time.sleep(0.02)
                    errback(RuntimeError("boom"))

                threading.Thread(target=fire, daemon=True).start()

        loop = asyncio.get_running_loop()
        aio_future = _response_future_to_asyncio(ThreadedResponseFuture(), loop)
        with pytest.raises(RuntimeError, match="boom"):
            await aio_future

    async def test_synchronous_callback_also_resolves(self) -> None:
        """Callback invoked synchronously (same thread) must also work."""

        class SyncResponseFuture:
            def add_callbacks(self, callback, errback, **_kw):
                callback(42)

        loop = asyncio.get_running_loop()
        aio_future = _response_future_to_asyncio(SyncResponseFuture(), loop)
        result = await aio_future
        assert result == 42
