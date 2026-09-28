"""
openframe/adapters/queue/nats/errors.py
==========================================
Shared driver-exception translation for the NATS JetStream queue adapter.

``_wrap_nats()`` is the single place that classifies a raw ``nats-py``
exception as connection-class (retryable) vs query/publish-class
(non-retryable). Both ``NatsProducer`` and ``NatsConsumer`` import and reuse
this helper rather than repeating the classification inline per method —
see ``docs/adapter-checklist.md`` in the ``openframe-adapters`` monorepo.

Verified against ``nats-py``'s actual exception hierarchy (installed
version, not guessed):

    nats.errors.Error                    (base — everything below IS-A this)
        NoServersError                   — no server reachable at connect/publish time
        ConnectionClosedError            — client was closed
        ConnectionDrainingError          — client is draining
        ConnectionReconnectingError       — mid-reconnect, request can't be sent
        StaleConnectionError             — server considers the connection stale
            UnexpectedEOF                — TCP connection dropped mid-read
        OutboundBufferLimitError         — write buffer overflowed while disconnected
        TimeoutError (also IS-A builtins.TimeoutError == asyncio.TimeoutError)
            DrainTimeoutError
        AuthorizationError, BadSubjectError, BadHeaderError, MaxPayloadError,
        NoRespondersError, ProtocolError, JsonParseError, ... — in-band,
        non-connection failures.

    nats.js.errors.Error(nats.errors.Error)   — JetStream errors ALSO subclass
                                                 nats.errors.Error (confirmed via
                                                 ``Error.__mro__``), so a single
                                                 ``except nats.errors.Error`` at
                                                 the call site catches both families.
        APIError, BadRequestError, NotFoundError, ServerError,
        ServiceUnavailableError, ... — in-band JetStream API failures.
        FetchTimeoutError(TimeoutError) — also IS-A nats.errors.TimeoutError.

Because ``nats.errors.TimeoutError`` itself subclasses the builtin
``TimeoutError`` (identical to ``asyncio.TimeoutError`` on Python 3.11+),
a single ``except asyncio.TimeoutError`` clause at each call site — placed
*before* the ``_wrap_nats()`` call — catches both our own
``asyncio.timeout()`` budget expiring AND any driver-raised timeout
(ack-wait, flush, fetch, drain). ``_wrap_nats()`` itself is therefore only
ever asked to classify non-timeout ``nats.errors.Error``/``nats.js.errors.Error``
instances into connection-class vs query-class.
"""
from __future__ import annotations

import nats.errors
import nats.js.errors

from openframe.core.exceptions import AdapterConnectionError, AdapterQueryError

__all__ = ["wrap_nats"]

# Connection-class failures — the request never made it to (or a response
# never came back from) a healthy server. Retryable.
#
# Deliberately excludes anything TimeoutError-shaped (nats.errors.TimeoutError
# and its subclasses DrainTimeoutError/FetchTimeoutError) — those are caught
# by the caller's own `except asyncio.TimeoutError` clause first, since
# nats.errors.TimeoutError IS-A builtins.TimeoutError.
_CONNECTION_ERRORS: tuple[type[Exception], ...] = (
    nats.errors.NoServersError,
    nats.errors.ConnectionClosedError,
    nats.errors.ConnectionDrainingError,
    nats.errors.ConnectionReconnectingError,
    nats.errors.StaleConnectionError,  # also catches UnexpectedEOF (subclass)
    nats.errors.OutboundBufferLimitError,
)


def wrap_nats(
    exc: Exception,
    operation: str,
    *,
    adapter: str,
    resource: str,
) -> AdapterConnectionError | AdapterQueryError:
    """
    Map a ``nats.errors.Error`` (or ``nats.js.errors.Error`` — a subclass) to
    the appropriate ``AdapterError`` subclass.

    Distinguishes a lost/unreachable connection (``AdapterConnectionError`` —
    retryable) from an in-band publish/ack/API failure such as an invalid
    subject or a JetStream API rejection (``AdapterQueryError`` — not
    retryable by default). Caller must ``raise ... from exc`` at the call site.

    Args:
        exc:       The caught ``nats.errors.Error``/``nats.js.errors.Error``.
        operation: The operation that failed, e.g. "publish", "ack".
        adapter:   Adapter identifier (``settings.adapter_name``).
        resource:  The NATS subject/stream involved, for the error message.
    """
    if isinstance(exc, _CONNECTION_ERRORS):
        return AdapterConnectionError(
            f"{operation} failed — connection to NATS was lost or unavailable: {exc}",
            adapter=adapter,
            operation=operation,
            cause=exc,
        )
    return AdapterQueryError(
        f"{operation} failed on subject {resource!r}: {exc}",
        adapter=adapter,
        operation=operation,
        cause=exc,
    )
