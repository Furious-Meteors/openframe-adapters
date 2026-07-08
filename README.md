<p align="center">
  <img src="docs/assets/images/banner.png" alt="openframe-adapters" width="100%" />
</p>

<p align="center">
  <a href="https://pypi.org/project/openframe-adapters/"><img src="https://img.shields.io/pypi/v/openframe-adapters?color=6DB33F&labelColor=1a1a1a&label=PyPI" alt="PyPI version"/></a>
  <a href="https://pypi.org/project/openframe-adapters/"><img src="https://img.shields.io/pypi/pyversions/openframe-adapters?color=6DB33F&labelColor=1a1a1a" alt="Python versions"/></a>
  <a href="https://github.com/Furious-Meteors/openframe-adapters/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Furious-Meteors/openframe-adapters/ci.yml?branch=production&color=6DB33F&labelColor=1a1a1a&label=tests" alt="Tests"/></a>
  <a href="https://github.com/Furious-Meteors/openframe-adapters/blob/production/LICENSE"><img src="https://img.shields.io/badge/license-MIT-6DB33F?labelColor=1a1a1a" alt="License"/></a>
  <a href="https://furious-meteors.github.io/openframe-adapters/"><img src="https://img.shields.io/badge/docs-live-6DB33F?labelColor=1a1a1a" alt="Docs"/></a>
</p>

<p align="center">
  <a href="https://furious-meteors.github.io/openframe-adapters/">Documentation</a> ·
  <a href="https://furious-meteors.github.io/openframe-adapters/getting-started/">Getting Started</a> ·
  <a href="https://pypi.org/project/openframe-adapters/">PyPI</a> ·
  <a href="https://github.com/Furious-Meteors/openframe-adapters/blob/production/.github/CHANGELOG.md">Changelog</a>
</p>

---

`openframe-adapters` is the database and queue adapter family of the OpenFrame Microservice Development Suite. Each adapter implements the `BaseRepository[T]` / `BaseProducer[T]` / `BaseConsumer[T]` port contracts from `openframe-core` — which, as of core v3.0 (ADR-006), each extend the unified `BasePort` (`Identity` + `Lifecycle`) contract — handles connection lifecycle and error translation, and gets out of the way — giving you direct access to the underlying driver for anything the port contract does not cover.

`postgres`, `mongo`, `redis`, and `kafka` pin `openframe-core>=3.0,<4` as of this release; other adapters in this family may still be on an earlier major until migrated — check each package's own `pyproject.toml`. The major version is the stability contract.

`openframe-adapters-db-postgres`, `-mongo`, and `-redis` are at **2.0.0** as of this release — `ping()`/`is_ready()` (deprecated in the prior minor) have been removed; `health()` is now the sole health check. `openframe-adapters-queue-kafka` is at **1.4.0** (non-breaking).

---

## Installation

### Granular — one specific adapter

```bash
# Relational
pip install openframe-adapters[postgres]
pip install openframe-adapters[mysql]

# Key-value
pip install openframe-adapters[redis]
pip install openframe-adapters[dynamodb]

# Document
pip install openframe-adapters[mongo]

# Columnar
pip install openframe-adapters[cassandra]

# Time-series
pip install openframe-adapters[influxdb]

# Vector
pip install openframe-adapters[milvus]
pip install openframe-adapters[chromadb]
pip install openframe-adapters[qdrant]
pip install openframe-adapters[faiss]
pip install openframe-adapters[falkordb]

# Queues
pip install openframe-adapters[kafka]
pip install openframe-adapters[nats]
pip install openframe-adapters[rabbitmq]
```

### Group — one category

```bash
pip install openframe-adapters[db]        # all 7 DB adapters (relational + document + columnar + time-series)
pip install openframe-adapters[vector]    # milvus + chromadb + qdrant + faiss + falkordb
pip install openframe-adapters[queue]     # kafka + nats + rabbitmq
```

### Convenience bundles

```bash
pip install openframe-adapters[rest-min]      # postgres + redis — REST API minimum
pip install openframe-adapters[rag-stack]     # milvus + falkordb + redis — RAG retrieval stack
pip install openframe-adapters[research-min]  # mongo + redis — Research Vault Phase 1
```

### Everything

```bash
pip install openframe-adapters[all]       # all 15 adapter packages
```

---

## Adapter inventory

| Extra | Package | Driver | Async | Category |
|---|---|---|---|---|
| `[postgres]` | `openframe-adapters-db-postgres` | asyncpg | native | Relational |
| `[mysql]` | `openframe-adapters-db-mysql` | aiomysql | native | Relational |
| `[redis]` | `openframe-adapters-db-redis` | redis-py | native | Key-value |
| `[dynamodb]` | `openframe-adapters-db-dynamodb` | aioboto3 | wrapper | Key-value |
| `[mongo]` | `openframe-adapters-db-mongo` | motor | native | Document |
| `[cassandra]` | `openframe-adapters-db-cassandra` | cassandra-driver | executor | Columnar |
| `[influxdb]` | `openframe-adapters-db-influxdb` | influxdb-client | native | Time-series |
| `[milvus]` | `openframe-adapters-db-milvus` | pymilvus | executor | Vector |
| `[chromadb]` | `openframe-adapters-db-chromadb` | chromadb | native | Vector |
| `[qdrant]` | `openframe-adapters-db-qdrant` | qdrant-client | native | Vector |
| `[faiss]` | `openframe-adapters-db-faiss` | faiss-cpu | executor | Vector |
| `[falkordb]` | `openframe-adapters-db-falkordb` | falkordb | executor | Graph |
| `[kafka]` | `openframe-adapters-queue-kafka` | aiokafka | native | Queue |
| `[nats]` | `openframe-adapters-queue-nats` | nats-py | native | Queue |
| `[rabbitmq]` | `openframe-adapters-queue-rabbitmq` | aio-pika | native | Queue |

---

## Quick start

### Import — identical regardless of how you installed

```python
# Whether you ran pip install openframe-adapters[postgres]
# or pip install openframe-adapters[all]
# the import is always the same:
from openframe.adapters.db.postgres import PostgresRepository, PostgresSettings
from openframe.adapters.db.mongo    import MongoRepository, MongoSettings
from openframe.adapters.db.redis    import RedisAdapter, RedisSettings
from openframe.adapters.queue.kafka import KafkaProducer, KafkaConsumer, KafkaSettings
```

The install granularity is invisible to application code.

### Postgres — standard CRUD via the port

```python
from openframe.adapters.db.postgres import PostgresRepository, PostgresSettings
from openframe.core.tracing import TracingProxy

class ItemRepository(PostgresRepository[Item]):
    _table     = "items"
    _id_column = "id"

    def _row_to_entity(self, row) -> Item:
        return Item(**dict(row))

    def _entity_to_row(self, entity: Item) -> dict:
        return entity.model_dump()

# Settings read from env vars — DATABASE_URL required
settings = PostgresSettings()
repo     = TracingProxy(ItemRepository(settings), prefix="repository.item")

# Standard port — inherited from PostgresRepository
item = await repo.get("abc-123")
all_items, total = await repo.list(limit=20, offset=0)
created = await repo.create(Item(name="hello"))
```

### Postgres — raw driver access for niche features

```python
# The adapter never hides asyncpg. Access it directly for anything
# the port does not cover — COPY, advisory locks, custom codecs, etc.
import asyncpg
from openframe.adapters.db.postgres import get_postgres_pool

class OrderRepository(PostgresRepository[Order]):
    _table     = "orders"
    _id_column = "id"

    async def bulk_copy(self, orders: list[Order]) -> None:
        """asyncpg COPY — 10x faster than individual INSERTs."""
        pool = await get_postgres_pool(self._settings)
        async with pool.acquire() as conn:
            await conn.copy_records_to_table(
                "orders",
                records=[(o.id, o.total) for o in orders],
                columns=["id", "total"],
            )
```

### MongoDB — document store with Atlas Search

```python
from openframe.adapters.db.mongo import MongoRepository, MongoSettings
from motor.motor_asyncio import AsyncIOMotorCollection

class ArtifactRepository(MongoRepository[Artifact]):
    _collection = "artifacts"

    def _doc_to_entity(self, doc: dict) -> Artifact:
        return Artifact(**doc)

    def _entity_to_doc(self, entity: Artifact) -> dict:
        return entity.model_dump()

    # Standard port — inherited
    # get(), list(), create(), update(), delete() just work

    # Raw motor access for Atlas-specific features
    async def vector_search(self, embedding: list[float], limit: int = 10) -> list[Artifact]:
        col: AsyncIOMotorCollection = self._get_collection()
        pipeline = [{
            "$vectorSearch": {
                "index": "embedding_index",
                "path": "embedding",
                "queryVector": embedding,
                "numCandidates": limit * 10,
                "limit": limit,
            }
        }]
        docs = await col.aggregate(pipeline).to_list(length=limit)
        return [self._doc_to_entity(self._serialise_doc(d)) for d in docs]
```

### Kafka — producer and consumer

```python
from openframe.adapters.queue.kafka import KafkaProducer, KafkaConsumer, KafkaSettings

settings = KafkaSettings()   # reads KAFKA_BOOTSTRAP_SERVERS from env

# Producer
producer = KafkaProducer(settings)
await producer.publish("events", {"type": "artifact.created", "id": "abc"})
await producer.publish_batch("events", [msg1, msg2, msg3])

# Consumer — integrates with FastAPI lifespan
consumer = KafkaConsumer(settings)

async def handle(message: dict) -> None:
    print(f"Received: {message}")

# subscribe() runs until close() is called
await consumer.subscribe("events", handler=handle)
```

### Health checks — built into every adapter

As of `openframe-core` v3.0 (ADR-006), the canonical health check lives on
`health()`, part of the unified `BasePort` (`Identity` + `Lifecycle`)
contract every `*Plugin` and repository/producer/consumer satisfies. It
returns a single `PluginHealth` snapshot and never raises:

```python
from openframe.adapters.db.postgres import PostgresPlugin, PostgresSettings
from openframe.core.ports import BasePort, PluginContext, PluginHealth

plugin = PostgresPlugin(PostgresSettings(), table="items")
assert isinstance(plugin, BasePort)   # True — Identity + Lifecycle

await plugin.initialize(PluginContext(config={}, plugin_name=plugin.name))
health: PluginHealth = await plugin.health()   # never raises
```

`repo.health()` works the same way directly on the repository — it is the
**sole** health check as of `openframe-adapters-db-postgres`/`-mongo`/
`-redis` **2.0.0**. `ping()`/`is_ready()` were deprecated in the prior
minor release and have now been **removed** — calling them raises
`AttributeError`.

```python
from openframe.adapters.db.postgres import PostgresRepository, PostgresSettings

repo = PostgresRepository(PostgresSettings(), table="items", id_column="id")

health = await repo.health()   # the only health check — never raises
```

---

## Design philosophy

Adapters handle three things and nothing else:

1. **Connection lifecycle** — settings from env vars, pool or client created once, health check protocol implemented
2. **Port conformance** — `BaseRepository[T]` methods so the hexagonal boundary holds
3. **Error translation** — driver exceptions become `AdapterError` subclasses so services have one catch point

Everything else — aggregation pipelines, bulk operations, vendor-specific features, advanced query patterns — the developer accesses directly through the underlying driver. The adapter gets out of the way.

```python
# openframe handles this:
settings = PostgresSettings()          # env var validation at startup
pool     = get_postgres_pool(settings) # pool created once, shared
entity   = await repo.get("abc-123")  # port contract, OTel span, error translation

# You handle this directly:
async with pool.acquire() as conn:
    await conn.execute("VACUUM ANALYZE items")   # raw asyncpg — no abstraction needed
```

---

## Error handling

Every adapter raises only `AdapterError` subclasses — never raw driver exceptions. Services have one catch point regardless of which adapter is wired in.

```python
from openframe.core.exceptions import (
    AdapterError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterNotFoundError,
    AdapterTimeoutError,
)

try:
    entity = await repo.get(entity_id)
except AdapterNotFoundError:
    raise HTTPException(status_code=404)
except AdapterTimeoutError:
    raise HTTPException(status_code=504)
except AdapterError as exc:
    logger.error("Adapter failure: %s", exc)   # [postgres.get] message — caused by: ...
    raise HTTPException(status_code=500)
```

Swap Postgres for MongoDB — the `except` blocks are unchanged. The adapter translates; the service doesn't care.

---

## Environment variables

Each adapter reads its own env vars via its `Settings` class. Common fields inherited from `BaseAdapterSettings`:

| Variable | Default | Description |
|---|---|---|
| `CONNECTION_TIMEOUT` | `30.0` | Seconds to wait when establishing a connection |
| `OPERATION_TIMEOUT` | `10.0` | Seconds to wait for a single operation |
| `MAX_RETRIES` | `3` | Maximum retry attempts on transient failure |

Adapter-specific variables — see each package's documentation:

| Adapter | Key env vars |
|---|---|
| Postgres | `DATABASE_URL`, `POOL_SIZE` |
| MongoDB | `MONGO_URL`, `MONGO_DATABASE`, `MONGO_MAX_POOL_SIZE` |
| Redis | `REDIS_URL` |
| Kafka | `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_GROUP_ID` |
| Milvus | `MILVUS_HOST`, `MILVUS_PORT`, `MILVUS_COLLECTION` |

---

## Wiring in deps.py

Every OpenFrame template wires adapters the same way — settings from env, repository constructed, wrapped with `TracingProxy` for automatic OTel spans:

```python
# src/deps.py
from functools import lru_cache
from openframe.adapters.db.postgres import PostgresRepository, PostgresSettings
from openframe.core.tracing import TracingProxy

@lru_cache(maxsize=1)
def _get_settings() -> PostgresSettings:
    return PostgresSettings()   # raises ValidationError at startup if env vars missing

@lru_cache(maxsize=1)
def _get_repository() -> PostgresRepository:
    return PostgresRepository(_get_settings(), table="items", id_column="id")

def get_repository() -> TracingProxy:
    return TracingProxy(_get_repository(), prefix="repository.item")
    # Every call to get_repository().get(...) now creates span "repository.item.get"
```

Swap the adapter — change two lines in `deps.py`, update one env var. Zero changes to business logic.

---

## Ecosystem

| Package | Install | Description |
|---|---|---|
| [`openframe-core`](https://github.com/Furious-Meteors/openframe-core) | `pip install openframe-core` | Ports · tracing · telemetry · exceptions · config |
| [`openframe-protocol`](https://github.com/Furious-Meteors/openframe-protocol) | `pip install openframe-protocol[sse]` | WebSocket · SSE · gRPC · MCP · webhooks |
| [`openframe-infra`](https://github.com/Furious-Meteors/openframe-infra) | `pip install openframe-infra[storage]` | Storage · auth · secrets · observability · feature flags |
| [`openframe-ai`](https://github.com/Furious-Meteors/openframe-ai) | `pip install openframe-ai[serving]` | LangChain · LlamaIndex · CrewAI · model serving · training |
| [`openframe-suite`](https://github.com/Furious-Meteors/openframe-suite) | `pip install openframe-suite[all]` | Full platform — installs everything |

---

## License

MIT — see [LICENSE](LICENSE).