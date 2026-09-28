# openframe-adapters-queue-kafka

Apache Kafka queue adapter for the **OpenFrame Microservice Development Suite**.

Part of the [`openframe-adapters`](https://github.com/Furious-Meteors/openframe-adapters) monorepo.

---

## What it provides

| Symbol | Purpose |
|---|---|
| `KafkaSettings` | Pydantic-settings subclass — reads all config from env vars |
| `KafkaProducer[T]` | Generic async message producer — `BaseProducer[T]` |
| `KafkaConsumer[T]` | Generic async message consumer — `BaseConsumer[T]` |
| `KafkaPlugin` | `BasePort` (Identity + Lifecycle) — structured lifecycle via `PluginRegistry`/`ApplicationBootstrap` |

## Installation

```bash
# Via meta-package (recommended)
pip install "openframe-adapters[kafka]"

# Or directly
pip install openframe-adapters-queue-kafka
```

## Quick start

```python
from openframe.adapters.queue.kafka import KafkaSettings, KafkaProducer, KafkaConsumer

settings = KafkaSettings(kafka_bootstrap_servers="localhost:9092")

# Produce
producer = KafkaProducer(settings)
await producer.start()
await producer.publish({"event": "item.created", "id": "abc"})
await producer.publish_batch([{"event": "x"}, {"event": "y"}])
await producer.close()

# Consume
consumer = KafkaConsumer(settings)

async def handle(event: dict) -> None:
    print(f"Received: {event}")

await consumer.subscribe(handle)   # runs until consumer.close() called
```

## Configuration

| Env var | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | **required** | `host:port,host:port` |
| `KAFKA_TOPIC` | `"openframe"` | Default topic for producer and consumer |
| `KAFKA_GROUP_ID` | `"openframe-group"` | Consumer group ID |
| `KAFKA_AUTO_OFFSET_RESET` | `"earliest"` | `"earliest"` or `"latest"` |
| `KAFKA_MAX_POLL_RECORDS` | `10` | Max messages per poll |
| `KAFKA_SESSION_TIMEOUT_MS` | `30000` | Consumer session timeout |
| `KAFKA_REQUEST_TIMEOUT_MS` | `30000` | Broker request timeout |
| `KAFKA_SECURITY_PROTOCOL` | `"PLAINTEXT"` | `"PLAINTEXT"`, `"SSL"`, `"SASL_PLAINTEXT"` |
| `KAFKA_SASL_MECHANISM` | `""` | `"PLAIN"`, `"SCRAM-SHA-256"`, etc. |
| `KAFKA_SASL_USERNAME` | `""` | SASL username |
| `KAFKA_SASL_PASSWORD` | `""` | SASL password |

## Typed domain objects

```python
from openframe.adapters.queue.kafka import KafkaProducer, KafkaConsumer, KafkaSettings
from dataclasses import dataclass, asdict

@dataclass
class OrderEvent:
    order_id: str
    event_type: str

class OrderProducer(KafkaProducer[OrderEvent]):
    def _serialise(self, message: OrderEvent) -> bytes:
        import json
        return json.dumps(asdict(message)).encode("utf-8")

class OrderConsumer(KafkaConsumer[OrderEvent]):
    def _deserialise(self, raw: bytes) -> OrderEvent:
        import json
        return OrderEvent(**json.loads(raw.decode("utf-8")))
```

## Plugin lifecycle (optional)

`KafkaPlugin` is a `BasePort` — wire it up with `ApplicationBootstrap.compose()`,
the recommended zero-subclass entry point:

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.queue.kafka import KafkaPlugin, KafkaSettings

kafka = KafkaPlugin(KafkaSettings())

async with ApplicationBootstrap.compose(kafka) as app:
    plugin = app.get(Capability.QUEUE)
    producer = plugin.get_producer()
    await producer.publish({"event": "item.created"})

    consumer = plugin.make_consumer()
    await consumer.subscribe(handler)
```

`KafkaPlugin` doubles as both the producer and consumer port for the `QUEUE`
capability (`get_producer()` / `make_consumer()`), so a single instance is
usually enough. If a service registers a separate producer-only and
consumer-only `KafkaPlugin` (e.g. different topics/settings for each), pass
both to `compose()`: `ApplicationBootstrap.compose(producer_plugin,
consumer_plugin)`. Reach for a subclassed `ApplicationBootstrap` with
`configure()` only when you need per-port `config=`/`init_timeout=` or
conditional registration order, and use `app.registry` as an escape hatch
for anything neither tier covers.

## Consumer acknowledgement semantics

| Outcome | Behaviour |
|---|---|
| Handler returns | `ack()` called → offset committed → message consumed |
| Handler raises | `nack()` called → no commit → message redelivered |
| `consumer.close()` | polling loop exits → consumer stopped cleanly |

## License

MIT — © Furious Meteors Engineering
