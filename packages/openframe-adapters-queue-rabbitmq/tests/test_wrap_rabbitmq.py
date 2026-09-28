"""
tests/test_wrap_rabbitmq.py — openframe-adapters-queue-rabbitmq
====================================================================
Parametrized tests for _wrap_rabbitmq()'s connection-vs-query
classification, on both RabbitmqProducer and RabbitmqConsumer.

Mirrors the pattern used in openframe-adapters-db-postgres's
test_repository.py for `_wrap_asyncpg()` — verify the classification
against multiple real driver exception types, not just one representative
instance.
"""
from __future__ import annotations

import aio_pika.exceptions
import pytest

from openframe.adapters.queue.rabbitmq import RabbitmqConsumer, RabbitmqProducer, RabbitmqSettings
from openframe.core.exceptions import AdapterConnectionError, AdapterQueryError


@pytest.fixture
def settings() -> RabbitmqSettings:
    return RabbitmqSettings(rabbitmq_url="amqp://guest:guest@localhost:5672/")


# Connection-class exceptions: broker unreachable/lost/auth-rejected before
# a connection was established. All are subclasses of
# aio_pika.exceptions.AMQPConnectionError (itself a subclass of the builtin
# ConnectionError) — verified against the installed aio-pika package, not
# assumed.
_CONNECTION_CLASS_EXCEPTIONS = [
    lambda: aio_pika.exceptions.AMQPConnectionError("connection refused"),
    lambda: aio_pika.exceptions.ConnectionClosed(None),
    lambda: aio_pika.exceptions.AuthenticationError(),
    lambda: aio_pika.exceptions.ProbableAuthenticationError(),
    lambda: ConnectionError("raw socket connection error"),
    lambda: OSError("Connect call failed"),
]

# Query/operation-class exceptions: the connection is fine, but the
# operation itself failed (channel closed by broker policy, undeliverable
# message, malformed frame). None of these are AMQPConnectionError
# subclasses.
_QUERY_CLASS_EXCEPTIONS = [
    lambda: aio_pika.exceptions.ChannelClosed(None, 406, "PRECONDITION_FAILED"),
    lambda: aio_pika.exceptions.ChannelNotFoundEntity(None, 404, "NOT_FOUND"),
    lambda: aio_pika.exceptions.MessageProcessError("publish failed", "reason"),
    lambda: aio_pika.exceptions.ProtocolSyntaxError("bad frame"),
]


@pytest.mark.parametrize("make_exc", _CONNECTION_CLASS_EXCEPTIONS)
def test_producer_wrap_rabbitmq_classifies_connection_errors(
    settings: RabbitmqSettings, make_exc
) -> None:
    producer = RabbitmqProducer(settings)
    wrapped = producer._wrap_rabbitmq(make_exc(), "publish")
    assert isinstance(wrapped, AdapterConnectionError)
    assert wrapped.retryable is True
    assert wrapped.operation == "publish"


@pytest.mark.parametrize("make_exc", _QUERY_CLASS_EXCEPTIONS)
def test_producer_wrap_rabbitmq_classifies_query_errors(
    settings: RabbitmqSettings, make_exc
) -> None:
    producer = RabbitmqProducer(settings)
    wrapped = producer._wrap_rabbitmq(make_exc(), "publish")
    assert isinstance(wrapped, AdapterQueryError)
    assert not isinstance(wrapped, AdapterConnectionError)
    assert wrapped.operation == "publish"


@pytest.mark.parametrize("make_exc", _CONNECTION_CLASS_EXCEPTIONS)
def test_consumer_wrap_rabbitmq_classifies_connection_errors(
    settings: RabbitmqSettings, make_exc
) -> None:
    consumer = RabbitmqConsumer(settings)
    wrapped = consumer._wrap_rabbitmq(make_exc(), "subscribe")
    assert isinstance(wrapped, AdapterConnectionError)
    assert wrapped.retryable is True
    assert wrapped.operation == "subscribe"


@pytest.mark.parametrize("make_exc", _QUERY_CLASS_EXCEPTIONS)
def test_consumer_wrap_rabbitmq_classifies_query_errors(
    settings: RabbitmqSettings, make_exc
) -> None:
    consumer = RabbitmqConsumer(settings)
    wrapped = consumer._wrap_rabbitmq(make_exc(), "subscribe")
    assert isinstance(wrapped, AdapterQueryError)
    assert not isinstance(wrapped, AdapterConnectionError)
    assert wrapped.operation == "subscribe"


def test_wrap_rabbitmq_cause_chaining_preserved() -> None:
    """The original driver exception must be reachable as .cause for debugging."""
    settings = RabbitmqSettings(rabbitmq_url="amqp://guest:guest@localhost:5672/")
    producer = RabbitmqProducer(settings)
    original = aio_pika.exceptions.ChannelClosed(None, 406, "PRECONDITION_FAILED")
    wrapped = producer._wrap_rabbitmq(original, "publish")
    assert wrapped.cause is original
