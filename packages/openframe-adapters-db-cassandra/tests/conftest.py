"""
tests/conftest.py — openframe-adapters-db-cassandra
=======================================================
OTel reset fixtures are provided by openframe.core.testing.fixtures.
This file contains only adapter-specific fixtures.

All tests run with zero network calls. cassandra-driver is mocked at the
``openframe.adapters.db.cassandra.connection`` import level so no real
Cassandra cluster is needed. ``ResponseFuture.add_callbacks`` is faked to
invoke its callback/errback synchronously (on the same thread, immediately)
rather than from a background thread — this is a valid simplification for
single-threaded test assertions and keeps tests deterministic without
losing coverage of the bridging logic in
``connection._response_future_to_asyncio()`` (which is exercised directly,
with real threads, in ``test_connection.py``).
"""
from __future__ import annotations

# Canonical OTel reset fixtures from openframe-core v3.0.
# Provides (autouse): reset_telemetry_state
# Provides (on-demand): span_exporter, metric_reader
from openframe.core.testing.fixtures import *  # noqa: F401, F403

import pytest
from unittest.mock import AsyncMock, MagicMock


class FakeResponseFuture:
    """
    A minimal stand-in for ``cassandra.cluster.ResponseFuture``.

    ``add_callbacks`` invokes the success/error callback immediately
    (synchronously), simulating the driver's reactor thread having already
    delivered the result by the time the test asserts on it.
    """

    def __init__(self, result: object = None, error: Exception | None = None) -> None:
        self._result = result
        self._error = error

    def add_callbacks(
        self,
        callback,
        errback,
        callback_args=(),
        callback_kwargs=None,
        errback_args=(),
        errback_kwargs=None,
    ) -> None:
        if self._error is not None:
            errback(self._error, *errback_args, **(errback_kwargs or {}))
        else:
            callback(self._result, *callback_args, **(callback_kwargs or {}))


@pytest.fixture
def make_response_future():
    """Factory fixture returning a FakeResponseFuture for a given outcome."""

    def _make(result: object = None, error: Exception | None = None) -> FakeResponseFuture:
        return FakeResponseFuture(result=result, error=error)

    return _make


@pytest.fixture
def mock_session() -> MagicMock:
    """A fully mocked cassandra-driver Session."""
    session = MagicMock()
    session.execute_async = MagicMock(return_value=FakeResponseFuture(result=[]))
    cluster = MagicMock()
    cluster.shutdown = MagicMock()
    session.cluster = cluster
    return session


@pytest.fixture
def mock_settings() -> object:
    """A CassandraSettings instance with dummy contact points."""
    from openframe.adapters.db.cassandra import CassandraSettings

    return CassandraSettings(cassandra_contact_points=["127.0.0.1"])


@pytest.fixture
def repo(mock_settings: object, mock_session: MagicMock, monkeypatch: pytest.MonkeyPatch):
    """
    A CassandraRepository wired to a mocked session.

    The mock session is injected directly into ``_session_cache`` so
    ``get_cassandra_session()`` never attempts a real connection.
    """
    from openframe.adapters.db.cassandra import CassandraRepository
    import openframe.adapters.db.cassandra.connection as conn_module

    conn_module._session_cache[conn_module._cache_key(mock_settings)] = mock_session  # type: ignore[attr-defined]
    r = CassandraRepository(mock_settings, table="items", id_column="id")  # type: ignore[arg-type]
    yield r
    conn_module._session_cache.clear()
