# openframe-adapters-queue-nats

NATS JetStream queue adapter for the **OpenFrame Microservice Development Suite**.

Part of the [`openframe-adapters`](https://github.com/Furious-Meteors/openframe-adapters) monorepo.

---

## What it provides

| Symbol | Purpose |
|---|---|
| `NatsSettings` | Pydantic-settings subclass — reads all config from env vars |
| `NatsProducer[T]` | Generic async message producer — `BaseProducer[T]` |
| `NatsConsumer[T]` | Generic async message consumer — `BaseConsumer[T]` |
| `NatsPlugin` | `BasePort` (Identity + Lifecycle) — structured lifecycle via `PluginRegistry`/`ApplicationBootstrap` |

## Why JetStream, not core NATS

Core NATS pub/sub is fire-and-forget (at-most-once): there is no
persistence, no publish acknowledgement, and no concept of redelivery — a
message a subscriber isn't currently listening for is simply gone. That
means there is nothing meaningful for `BaseConsumer.ack()`/`nack()` to do
on top of it.

**JetStream** adds a persistent stream plus a durable, acknowledgeable
consumer, giving `ack()`/`nack()` real at-least-once semantics: a handler
failure (`nack()` → `msg.nak()`) triggers redelivery according to the
durable consumer's `ack_wait`/`max_deliver` config — the same guarantee
`KafkaConsumer` provides via manual offset commit. This adapter targets
JetStream exclusively for that reason; there is no core-NATS mode.

## Installation

```bash
# Via meta-package (recommended)
pip install "openframe-adapters[nats]"

# Or directly
pip install openframe-adapters-queue-nats
```

## Quick start

```python
from openframe.adapters.queue.nats import NatsSettings, NatsProducer, NatsConsumer

settings = NatsSettings(nats_servers="nats://localhost:4222")

# Produce
producer = NatsProducer(settings)
await producer.start()
await producer.publish({"event": "item.created", "id": "abc"})
await producer.publish_batch([{"event": "x"}, {"event": "y"}])
await producer.close()

# Consume
consumer = NatsConsumer(settings)

async def handle(event: dict) -> None:
    print(f"Received: {event}")

await consumer.subscribe(handle)   # runs until consumer.close() called
```

## Configuration

| Env var | Default | Description |
|---|---|---|
| `NATS_SERVERS` | **required** | `nats://host:port,nats://host:port` |
| `NATS_SUBJECT` | `"openframe"` | Default subject for producer and consumer |
| `NATS_STREAM` | `"OPENFRAME"` | JetStream stream name (auto-created, idempotent) |
| `NATS_DURABLE_NAME` | `"openframe-consumer"` | Durable JetStream consumer name |
| `NATS_DELIVER_POLICY` | `"all"` | `"all"` or `"new"` |
| `NATS_ACK_WAIT_SECONDS` | `30.0` | Seconds before an un-acked message is redelivered |
| `NATS_MAX_DELIVER` | `5` | Max redelivery attempts before JetStream gives up |
| `NATS_MAX_RECONNECT_ATTEMPTS` | `60` | NATS client reconnect attempts |
| `NATS_USER` / `NATS_PASSWORD` | `""` | Basic auth, if the server requires it |
| `NATS_TOKEN` | `""` | Token auth, if the server requires it |

## Typed domain objects

```python
from openframe.adapters.queue.nats import NatsProducer, NatsConsumer, NatsSettings
from dataclasses import dataclass, asdict

@dataclass
class OrderEvent:
    order_id: str
    event_type: str

class OrderProducer(NatsProducer[OrderEvent]):
    def _serialise(self, message: OrderEvent) -> bytes:
        import json
        return json.dumps(asdict(message)).encode("utf-8")

class OrderConsumer(NatsConsumer[OrderEvent]):
    def _deserialise(self, raw: bytes) -> OrderEvent:
        import json
        return OrderEvent(**json.loads(raw.decode("utf-8")))
```

## Plugin lifecycle (optional)

`NatsPlugin` is a `BasePort` — wire it up with `ApplicationBootstrap.compose()`
(requires `openframe-core>=3.3`), the recommended zero-subclass entry point:

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.queue.nats import NatsPlugin, NatsSettings

nats_plugin = NatsPlugin(NatsSettings())

async with ApplicationBootstrap.compose(nats_plugin) as app:
    plugin = app.get(Capability.QUEUE)
    producer = plugin.get_producer()
    await producer.publish({"event": "item.created"})

    consumer = plugin.make_consumer()
    await consumer.subscribe(handler)
```

`NatsPlugin` doubles as both the producer and consumer port for the `QUEUE`
capability (`get_producer()` / `make_consumer()`), so a single instance is
usually enough. If a service registers a separate producer-only and
consumer-only `NatsPlugin` (e.g. different subjects/settings for each), pass
both to `compose()`: `ApplicationBootstrap.compose(producer_plugin,
consumer_plugin)`. Reach for a subclassed `ApplicationBootstrap` with
`configure()` only when you need per-port `config=`/`init_timeout=` or
conditional registration order, and use `app.registry` as an escape hatch
for anything neither tier covers.

## Consumer acknowledgement semantics

| Outcome | Behaviour |
|---|---|
| Handler returns | `msg.ack()` called → JetStream marks the message delivered |
| Handler raises | `msg.nak()` called → JetStream redelivers per `ack_wait`/`max_deliver` |
| `consumer.close()` | subscription unsubscribed, connection closed cleanly |

Note: unlike `KafkaConsumer`'s `ack()`/`nack()` (which commit/skip a global
offset and ignore their `message` argument), `NatsConsumer.ack()`/`nack()`
take the **raw NATS message** (not the deserialised payload) — JetStream
acks are per-message, so the exact delivered message object is required to
target the right ack. See `consumer.py`'s module docstring for the full
rationale.

## Resilience (optional — requires `openframe-core>=3.4`)

`openframe.core.resilience` ships a `CircuitBreakerProxy` that composes
around any `BasePort`, including this adapter's producer:

```python
from openframe.core.resilience import CircuitBreakerProxy
from openframe.core.tracing import TracingProxy

producer = CircuitBreakerProxy(
    TracingProxy(plugin.get_producer(), prefix="nats.producer"),
    failure_threshold=5,
    reset_timeout=30.0,
)
```

Compose the circuit breaker *around* the traced producer, not the reverse —
a short-circuited call should never produce a misleading adapter span for a
call that never reached NATS. No adapter code changes are needed to support
this; it wraps from the outside exactly like `TracingProxy`.

## License

MIT — © Furious Meteors Engineering
