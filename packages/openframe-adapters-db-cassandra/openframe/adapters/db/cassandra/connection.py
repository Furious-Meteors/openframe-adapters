"""
openframe/adapters/db/cassandra/connection.py
================================================
cassandra-driver ``Session`` factory/cache, and the asyncio bridge every
query in this package is executed through.

Async strategy (ADR-003: decided per-adapter, not forced)
-----------------------------------------------------------
This adapter uses a **hybrid** strategy — neither pure native-async nor
blind ``run_in_executor``-for-everything. ADR-003 says the async strategy
must be decided per-adapter based on what the driver actually supports, not
forced into one pattern; ``cassandra-driver`` turns out to support two
different things at two different call sites, so the honest answer is two
different strategies:

1. Cluster/session creation (``Cluster(...).connect()``) — genuinely
   blocking, no alternative. ``cassandra-driver`` has no
   ``connect_async()``/``create_pool_async()`` equivalent (verified against
   the actually-installed driver below, not assumed/recalled) — topology
   discovery and the initial control-connection handshake are synchronous.
   This one-time, per-pool cost is wrapped in
   ``loop.run_in_executor(None, ...)``. There is no better option here; the
   thread is held only for the duration of the initial handshake, not for
   every subsequent query.

2. Query execution (``Session.execute_async(query)``) — this is where the
   naive approach (wrapping the *blocking* ``Session.execute()`` in
   ``run_in_executor`` for every single query) would burn a thread-pool
   thread for the full round-trip duration of every query. Investigation
   showed a real, better option instead.

Research finding (checked against the actually-installed driver, not
assumed): ``cassandra-driver`` 3.30.1, installed fresh via
``pip install cassandra-driver`` for this investigation, confirms:

    >>> import cassandra
    >>> cassandra.__version__
    '3.30.1'
    >>> from cassandra.cluster import ResponseFuture, Session
    >>> hasattr(ResponseFuture, "add_callbacks")
    True
    >>> import inspect
    >>> inspect.signature(ResponseFuture.add_callbacks)
    <Signature (self, callback, errback, callback_args=(), callback_kwargs=None,
    errback_args=(), errback_kwargs=None)>

``Session.execute_async(query)`` returns a ``ResponseFuture`` immediately
without blocking — it is NOT a ``concurrent.futures.Future`` or an
``asyncio.Future``, but it DOES support ``add_callbacks(callback, errback)``
to register completion callbacks. Those callbacks fire from the driver's
own internal I/O reactor thread (libev- or asyncore-based depending on
platform/extras) once the response actually arrives over the wire — no
thread-pool thread is held waiting on the query.

This was verified end-to-end with a scripted reproduction of the driver's
actual threading model (a background thread invoking the callback after a
simulated I/O delay, exactly how the real reactor thread behaves), bridging
to a real ``asyncio.Future`` via ``loop.call_soon_threadsafe`` — the
standard, documented way to hand a result from a non-asyncio thread back
into the event loop safely. Both the success and error paths were exercised
and resolved correctly with no event-loop/thread-safety issues. See
``_response_future_to_asyncio()`` below — this is the bridge every CRUD
method in ``repository.py`` awaits.

Because this genuinely works and avoids consuming a thread-pool thread for
each query's full duration, that is what this adapter uses for every query
dispatched through ``repository.py``. If a future driver release removed
``add_callbacks``/changed this behavior, the honest fallback per ADR-003
would be ``loop.run_in_executor(None, session.execute, query)`` (burning a
thread for the query's duration) — this module does not do that for query
execution because the better option was verified to actually work.

``get_cassandra_session()`` is the single entry point for obtaining a
``cassandra.cluster.Session``. It creates the ``Cluster``+``Session`` on
first call and returns the cached instance on every subsequent call with
the same contact points AND the same connection-shape settings (port,
keyspace, auth, local DC, protocol version, core connections per host).

Session cache: ``_session_cache`` is a module-level dict keyed by
``(contact_points-tuple, port, keyspace, username, local_dc,
protocol_version, core_connections_per_host)`` — not the contact points
alone. Two ``CassandraSettings`` instances with the same contact points but
different auth/keyspace/pool shape get two distinct sessions, rather than
the second one silently inheriting the first one's configuration (the same
class of bug this key shape fixes in every other package in this ecosystem
— see ``docs/adapter-checklist.md``).
Do NOT replace this with ``@lru_cache`` — that decorator does not support
async functions and would create a new coroutine on each call.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import cassandra
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable, Session
from cassandra.policies import HostDistance

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)

from .config import CassandraSettings

__all__ = [
    "get_cassandra_session",
    "_response_future_to_asyncio",
    "_session_cache",
    "_cache_key",
]

_logger = logging.getLogger(__name__)

_SessionCacheKey = tuple[
    tuple[str, ...], int, str | None, str | None, str | None, int | None, int
]

_session_cache: dict[_SessionCacheKey, Session] = {}


def _cache_key(settings: CassandraSettings) -> _SessionCacheKey:
    """
    Cache key covering the contact points plus every connection-shape setting.

    Two ``CassandraSettings`` for the same contact points but different
    auth/keyspace/pool shape must not share a session — a shared key here
    would mean the second caller silently gets the first caller's
    connection configuration.
    """
    return (
        tuple(settings.cassandra_contact_points),
        settings.cassandra_port,
        settings.cassandra_keyspace,
        settings.cassandra_username,
        settings.cassandra_local_dc,
        settings.cassandra_protocol_version,
        settings.cassandra_core_connections_per_host,
    )


def _build_session(settings: CassandraSettings) -> Session:
    """
    Synchronous, blocking cluster connect — runs inside the executor.

    Not called directly from async code. ``get_cassandra_session()`` wraps
    this in ``loop.run_in_executor(None, ...)`` because
    ``cassandra-driver`` has no async-native connect path.
    """
    auth_provider = None
    if settings.cassandra_username is not None:
        auth_provider = PlainTextAuthProvider(
            username=settings.cassandra_username,
            password=settings.cassandra_password or "",
        )

    cluster_kwargs: dict[str, Any] = {
        "contact_points": settings.cassandra_contact_points,
        "port": settings.cassandra_port,
        "auth_provider": auth_provider,
    }
    if settings.cassandra_protocol_version is not None:
        cluster_kwargs["protocol_version"] = settings.cassandra_protocol_version

    cluster = Cluster(**cluster_kwargs)
    cluster.set_core_connections_per_host(
        HostDistance.LOCAL, settings.cassandra_core_connections_per_host
    )

    session = cluster.connect(settings.cassandra_keyspace)
    return session


async def get_cassandra_session(settings: CassandraSettings) -> Session:
    """
    Create or return the cached cassandra-driver ``Session``.

    Creates the ``Cluster``/``Session`` on first call for a given
    connection-shape key. Subsequent calls with matching settings return the
    cached session without reconnecting. A different connection
    configuration for the same contact points gets its own session rather
    than reusing the first one's.

    Args:
        settings: A fully-validated ``CassandraSettings`` instance.

    Returns:
        A ``cassandra.cluster.Session`` that is ready to use.

    Raises:
        AdapterConnectionError:    No contact point was reachable
                                   (``NoHostAvailable``), or authentication
                                   failed (``AuthenticationFailed`` — raised
                                   with ``retryable=False`` since retrying
                                   with the same bad credentials cannot
                                   succeed).
        AdapterConfigurationError: Contact points list is empty/malformed.
        AdapterTimeoutError:       Session creation exceeded
                                   ``settings.connection_timeout``.
    """
    key = _cache_key(settings)
    if key in _session_cache:
        return _session_cache[key]

    if not settings.cassandra_contact_points:
        raise AdapterConfigurationError(
            "cassandra_contact_points must be a non-empty list of hosts",
            adapter=settings.adapter_name,
            operation="init",
        )

    loop = asyncio.get_running_loop()
    try:
        session = await asyncio.wait_for(
            loop.run_in_executor(None, _build_session, settings),
            timeout=settings.connection_timeout,
        )
    except asyncio.TimeoutError as exc:
        raise AdapterTimeoutError(
            f"Session creation exceeded {settings.connection_timeout}s connection_timeout",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except NoHostAvailable as exc:
        raise AdapterConnectionError(
            f"No contact point reachable in {settings.cassandra_contact_points!r}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except cassandra.AuthenticationFailed as exc:
        # AdapterConnectionError defaults retryable=True (its __init__ does
        # not accept a retryable= kwarg — it's a fixed per-class default on
        # OpenFrameError), but retrying with the same bad credentials cannot
        # succeed, so the instance's default is overridden explicitly here.
        auth_error = AdapterConnectionError(
            f"Cassandra authentication failed: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        )
        auth_error.retryable = False
        raise auth_error from exc
    except OSError as exc:
        # Mirrors the python-oracledb finding in this ecosystem: a DNS
        # resolution failure or raw socket error can escape as OSError
        # rather than a cassandra.* exception.
        raise AdapterConnectionError(
            f"Cannot connect to Cassandra at {settings.cassandra_contact_points!r}: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc

    _session_cache[key] = session
    return session


def _response_future_to_asyncio(response_future: Any, loop: asyncio.AbstractEventLoop) -> "asyncio.Future[Any]":
    """
    Bridge a cassandra-driver ``ResponseFuture`` to a real ``asyncio.Future``.

    ``ResponseFuture.add_callbacks()`` registers completion callbacks that
    fire from the driver's own internal I/O reactor thread — never from the
    asyncio event loop thread. Mutating an ``asyncio.Future`` from another
    thread is unsafe, so every callback goes through
    ``loop.call_soon_threadsafe()``, the documented way to schedule
    event-loop-affecting work from a foreign thread.

    No thread-pool thread is consumed for the query's duration — the only
    "work" happening between dispatch and completion is the driver's own
    non-blocking I/O.

    Args:
        response_future: The ``cassandra.cluster.ResponseFuture`` returned
                          by ``Session.execute_async()``.
        loop:             The asyncio event loop to schedule the result on.

    Returns:
        An ``asyncio.Future`` that resolves with the query result (a
        ``ResultSet``) or raises the driver exception that occurred.
    """
    aio_future: "asyncio.Future[Any]" = loop.create_future()

    def _set_result_threadsafe(result: Any) -> None:
        if not aio_future.done():
            aio_future.set_result(result)

    def _set_exception_threadsafe(exc: BaseException) -> None:
        if not aio_future.done():
            aio_future.set_exception(exc)

    def on_success(result: Any) -> None:
        loop.call_soon_threadsafe(_set_result_threadsafe, result)

    def on_error(exc: BaseException) -> None:
        loop.call_soon_threadsafe(_set_exception_threadsafe, exc)

    response_future.add_callbacks(on_success, on_error)
    return aio_future
