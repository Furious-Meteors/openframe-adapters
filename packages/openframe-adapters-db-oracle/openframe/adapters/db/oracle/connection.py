"""
openframe/adapters/db/oracle/connection.py
============================================
python-oracledb async connection pool factory and cache.

Async strategy (ADR-003: decided per-adapter, not forced)
-----------------------------------------------------------
This adapter uses **native async**, not the ``run_in_executor`` fallback.

Research finding (checked against the actually-installed driver, not
assumed): ``oracledb`` 26.0.1 ("python-oracledb"), installed fresh via
``pip install oracledb`` for this investigation, exposes a real, documented
async API surface in thin mode (no Oracle Client libraries required):
``oracledb.connect_async()``, ``oracledb.create_pool_async()``,
``oracledb.AsyncConnection``, ``oracledb.AsyncConnectionPool``, and
``oracledb.AsyncCursor``. These are not experimental additions bolted onto
the sync driver — they are documented factory functions with full
docstrings, and ``AsyncConnection``/``AsyncCursor`` both support
``async with``. Verified directly:

    >>> import oracledb
    >>> oracledb.__version__
    '26.0.1'
    >>> hasattr(oracledb, "connect_async"), hasattr(oracledb, "create_pool_async")
    (True, True)

Because a real, usable native-async surface exists, this adapter is built
the same shape as ``openframe-adapters-db-postgres``/``-mysql`` (async pool
cached by ``(dsn, pool-config-tuple)``, async CRUD methods) — not the
executor-wrapped-sync shape ``openframe-adapters-db-cassandra`` will need
(``cassandra-driver`` has no native async at all). If a future `oracledb`
release regressed this surface, the honest move per ADR-003 would be to
fall back to ``asyncio.get_running_loop().run_in_executor(None, sync_fn)``
here — this module does not do that because the surface genuinely exists
in the installed version.

A second, real gotcha found during the same investigation and worth
documenting here since it affects this module directly: a DNS resolution
failure while connecting (unresolvable host in the connect string) does
**not** raise ``oracledb.Error`` — it escapes as a raw ``socket.gaierror``
(an ``OSError`` subclass), confirmed against both ``oracledb.connect()``
and ``oracledb.connect_async()`` in thin mode. This module therefore
catches ``OSError`` explicitly alongside ``oracledb.Error`` wherever a
connection is established — omitting it would let a raw driver-adjacent
exception escape past the adapter boundary.

``get_oracle_pool()`` is the single entry point for obtaining an
``oracledb.AsyncConnectionPool``. It creates the pool on first call and
returns the cached instance on every subsequent call with the same
``oracle_dsn`` AND the same pool-relevant settings (``pool_min``,
``pool_max``, ``pool_increment``, ``pool_timeout``). Multiple
``OracleRepository`` instances constructed with matching settings share
the same pool.

Pool cache: ``_pool_cache`` is a module-level dict keyed by
``(oracle_dsn, pool-config-tuple)`` — not the DSN alone. Two
``OracleSettings`` instances with the same ``oracle_dsn`` but different
pool sizing get two distinct pools, rather than the second one silently
inheriting the first one's configuration (the bug this key shape fixes —
see ``docs/adapter-checklist.md``).
Do NOT replace this with ``@lru_cache`` — that decorator does not support
async functions and would create a new coroutine on each call.
"""
from __future__ import annotations

import asyncio

import oracledb

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterTimeoutError,
)

from .config import OracleSettings

__all__ = ["get_oracle_pool", "_pool_cache", "_cache_key"]

_PoolCacheKey = tuple[str, tuple[int, int, int, int]]

_pool_cache: dict[_PoolCacheKey, oracledb.AsyncConnectionPool] = {}

# DPY-4xxx / DPY-2xxx: driver-side connect-string/param parsing failures
# (bad EasyConnect syntax, missing config dir, invalid connect descriptor).
# Verified against the installed driver: a syntactically malformed DSN
# (e.g. "not_a_valid_dsn_at_all") raises oracledb.DatabaseError with
# full_code == "DPY-4027" ("no configuration directory specified") in this
# version — not a connection-class failure, a configuration one.
_CONFIG_ERROR_PREFIXES = ("DPY-4", "DPY-2")

# DPY-6xxx: driver-side connection failures (listener refused, cannot
# connect). Verified: connecting to an unreachable host with a syntactically
# valid DSN raises oracledb.OperationalError with full_code == "DPY-6005"
# ("cannot connect to database").
_CONNECTION_ERROR_PREFIXES = ("DPY-6",)


def _cache_key(settings: OracleSettings) -> _PoolCacheKey:
    """
    Cache key covering the DSN plus every pool-shape setting.

    Two ``OracleSettings`` for the same ``oracle_dsn`` but different pool
    sizing must not share a pool — a shared key here would mean the second
    caller silently gets the first caller's pool configuration.
    """
    return (
        settings.oracle_dsn,
        (
            settings.pool_min,
            settings.pool_max,
            settings.pool_increment,
            settings.pool_timeout,
        ),
    )


def _classify_connect_error(exc: Exception) -> str:
    """
    Classify a connect/pool-creation-time exception as "config" or
    "connection". Returns "connection" by default — anything not
    positively identified as a config/parse error is treated as a
    (retryable) connection failure, matching the conservative default the
    Postgres/MySQL adapters use for their own connect-time classification.
    """
    if isinstance(exc, oracledb.Error) and exc.args:
        err = exc.args[0]
        full_code = getattr(err, "full_code", "") or ""
        if full_code.startswith(_CONFIG_ERROR_PREFIXES):
            return "config"
        if full_code.startswith(_CONNECTION_ERROR_PREFIXES):
            return "connection"
    return "connection"


async def get_oracle_pool(settings: OracleSettings) -> oracledb.AsyncConnectionPool:
    """
    Create or return the cached ``oracledb.AsyncConnectionPool``.

    Creates the pool on first call for a given ``(oracle_dsn, pool config)``
    pair. Subsequent calls with matching DSN and pool settings return the
    cached pool without re-connecting. A different pool configuration for
    the same DSN gets its own pool rather than reusing the first one's.

    Args:
        settings: A fully-validated ``OracleSettings`` instance.

    Returns:
        An ``oracledb.AsyncConnectionPool`` that is ready to use.

    Raises:
        AdapterConnectionError:    Pool creation failed — host unreachable,
                                   listener refused, bad credentials, DNS
                                   resolution failure, etc.
        AdapterConfigurationError: ``oracle_dsn`` is syntactically invalid
                                   or unparsable.
        AdapterTimeoutError:       Pool creation exceeded
                                   ``settings.connection_timeout``.
    """
    key = _cache_key(settings)
    if key in _pool_cache:
        return _pool_cache[key]

    try:
        async with asyncio.timeout(settings.connection_timeout):
            pool = await oracledb.create_pool_async(
                dsn=settings.oracle_dsn,
                min=settings.pool_min,
                max=settings.pool_max,
                increment=settings.pool_increment,
                timeout=settings.pool_timeout,
            )
    except asyncio.TimeoutError as exc:
        raise AdapterTimeoutError(
            f"Pool creation exceeded {settings.connection_timeout}s connection_timeout",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc
    except (oracledb.Error, OSError) as exc:
        # OSError (e.g. socket.gaierror from an unresolvable host) is caught
        # explicitly — see the module docstring's "second real gotcha".
        if _classify_connect_error(exc) == "config":
            raise AdapterConfigurationError(
                f"ORACLE_DSN is syntactically invalid: {exc}",
                adapter=settings.adapter_name,
                operation="init",
                cause=exc,
            ) from exc
        raise AdapterConnectionError(
            f"Cannot connect to Oracle: {exc}",
            adapter=settings.adapter_name,
            operation="connect",
            cause=exc,
        ) from exc

    _pool_cache[key] = pool
    return pool
