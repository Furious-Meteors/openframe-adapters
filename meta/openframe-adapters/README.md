# openframe-adapters

A metadata-only package that provides named install shortcuts for the entire
`openframe-adapters` ecosystem. Install one adapter, a category, or everything
— all with a single `pip install` command.

---

## Install surface

### Individual adapters

```bash
# Relational
pip install openframe-adapters[postgres]    # PostgreSQL via asyncpg
pip install openframe-adapters[mysql]       # MySQL via aiomysql

# Key-value
pip install openframe-adapters[redis]       # Redis via redis-py
pip install openframe-adapters[dynamodb]    # DynamoDB via aiobotocore

# Document
pip install openframe-adapters[mongo]       # MongoDB via Motor

# Columnar
pip install openframe-adapters[cassandra]   # Cassandra via cassandra-driver

# Time-series
pip install openframe-adapters[influxdb]    # InfluxDB via influxdb-client

# Vector
pip install openframe-adapters[milvus]      # Milvus via pymilvus
pip install openframe-adapters[chromadb]    # Chroma via chromadb
pip install openframe-adapters[qdrant]      # Qdrant via qdrant-client
pip install openframe-adapters[faiss]       # FAISS via faiss-cpu
pip install openframe-adapters[falkordb]    # FalkorDB via falkordb

# Queues
pip install openframe-adapters[kafka]       # Kafka via aiokafka
pip install openframe-adapters[nats]        # NATS via nats-py
pip install openframe-adapters[rabbitmq]    # RabbitMQ via aio-pika
```

### Groups — one category

```bash
pip install openframe-adapters[db]       # all 7 DB adapters (relational + document + specialist)
pip install openframe-adapters[vector]   # all 5 vector DB adapters
pip install openframe-adapters[queue]    # all 3 queue adapters
```

### Everything

```bash
pip install openframe-adapters[all]      # all 15 individual adapter packages
```

### Convenience combinations

```bash
pip install openframe-adapters[rest-min]      # postgres + redis  (REST API minimum)
pip install openframe-adapters[rag-stack]     # milvus + falkordb + redis  (RAG / inference)
pip install openframe-adapters[research-min]  # mongo + redis  (Research Vault Phase 1)
```

---

## Import paths

The import path is identical regardless of how you installed:

```python
# Whether you ran:
#   pip install openframe-adapters[postgres]
# or:
#   pip install openframe-adapters[all]
# the import is always the same:
from openframe.adapters.db.postgres import PostgresRepository
from openframe.adapters.db.mongo import MongoRepository
```

Each individual adapter package uses PEP 420 implicit namespace packages
under `openframe.adapters.*` (uniformly across all four migrated packages
as of this release), so all adapters share the same top-level namespace
without any conflicts.

---

## Wiring adapters into your service

Every adapter wires into your service through a single file — `deps.py` or
`bootstrap/dependencies.py`. This file is the only place in your codebase
that knows which adapter is active. Routes and services never import adapters
directly.

The rule is simple:

> **One adapter** — wire directly with `lru_cache`.
> **Two or more adapters** — use `PluginRegistry`.
> The trigger to upgrade is adding a second adapter.

### Stage 1 — One adapter

Install one adapter and wire it directly. Four lines. No registry needed.

```python
# bootstrap/dependencies.py
from functools import lru_cache
from openframe.adapters.db.postgres import PostgresRepository, PostgresSettings
from openframe.core.tracing import TracingProxy
from src.adapters.item_repository import ItemPostgresRepository
from src.application.services.item_service import ItemService

@lru_cache(maxsize=1)
def _get_repository() -> ItemPostgresRepository:
    return ItemPostgresRepository(PostgresSettings())

def get_item_service() -> ItemService:
    return ItemService(TracingProxy(_get_repository(), prefix="repository.item"))
```

`PostgresSettings()` reads `DATABASE_URL` from env at startup.
`lru_cache(maxsize=1)` constructs the repository once per process.
`TracingProxy` wraps it for automatic OTel spans on every call.

### Stage 2 — Two or more adapters

When a second adapter is needed, replace `lru_cache` with `PluginRegistry`.
The registry handles startup ordering, health aggregation, and graceful
shutdown across all adapters.

```python
# bootstrap/dependencies.py
from openframe.adapters.db.postgres import PostgresPlugin, PostgresSettings
from openframe.adapters.db.redis import RedisPlugin, RedisSettings
from openframe.core.plugins import PluginRegistry
from openframe.core.tracing import TracingProxy
from src.application.services.item_service import ItemService
from src.application.services.session_service import SessionService

_registry: PluginRegistry | None = None

async def initialise() -> None:
    global _registry
    _registry = PluginRegistry()
    _registry.register(PostgresPlugin(PostgresSettings()))  # capability: Capability.PERSISTENCE
    _registry.register(RedisPlugin(RedisSettings()))        # capability: Capability.CACHE
    await _registry.initialize_all()   # fails fast if any backend unreachable

async def shutdown() -> None:
    if _registry:
        await _registry.shutdown_all()  # never raises

def get_item_service() -> ItemService:
    repo = TracingProxy(
        _registry.get("persistence").get_repository(),
        prefix="repository.item",
    )
    return ItemService(repo)

def get_session_service() -> SessionService:
    cache = TracingProxy(
        _registry.get("cache").get_repository(),
        prefix="cache.session",
    )
    return SessionService(cache)
```

Wire `initialise()` and `shutdown()` in your FastAPI lifespan:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from openframe.core.middleware import TelemetryMiddleware
from openframe.core.telemetry import setup_telemetry
from src.bootstrap import dependencies

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_telemetry()
    await dependencies.initialise()
    yield
    await dependencies.shutdown()

app = FastAPI(lifespan=lifespan)
app.add_middleware(TelemetryMiddleware)
```

### Plugin capabilities

Every `*Plugin` class declares a `capability` attribute — a typed
`openframe.core.contracts.Capability` enum member (not a raw string as of
`openframe-core` v3.0). This is the key used by `registry.get()`:

```python
from openframe.core.contracts import Capability

registry.get(Capability.PERSISTENCE)  # → PostgresPlugin / MongoPlugin
registry.get(Capability.CACHE)        # → RedisPlugin
```

The capability taxonomy is a closed enum shared across the entire OpenFrame
ecosystem:

| Capability | Adapters | Use for |
|---|---|---|
| `Capability.PERSISTENCE` | Postgres, Mongo, MySQL, DynamoDB, Cassandra | Primary data store |
| `Capability.CACHE` | Redis | Fast ephemeral store, sessions, rate limits |
| `Capability.QUEUE` | Kafka, NATS, RabbitMQ | Message publishing and consumption |
| `Capability.SEARCH` | Milvus, Qdrant, ChromaDB, FAISS, FalkorDB | Vector similarity search |

The closed `Capability` enum (`openframe.core.contracts.Capability`) has no
dedicated time-series member as of core v3.0 — InfluxDB and other
time-series adapters do not yet have an assigned capability in this
taxonomy.

`Capability` also compares equal to its string value (e.g.
`Capability.PERSISTENCE == "persistence"`), so existing string comparisons
keep working, but new code should key on the enum member directly.

### The env-var swap exception

Switching between adapters via an environment variable (`PERSISTENCE_BACKEND=postgres`
vs `PERSISTENCE_BACKEND=mongo`) is still Stage 1 — because only one adapter
is active at any moment. Use `lru_cache` direct wiring with a conditional:

```python
@lru_cache(maxsize=1)
def _get_repository():
    backend = os.environ.get("PERSISTENCE_BACKEND", "postgres")
    if backend == "postgres":
        return ItemPostgresRepository(PostgresSettings())
    elif backend == "mongo":
        return ItemMongoRepository(MongoSettings())
    raise ValueError(f"Unknown PERSISTENCE_BACKEND: {backend!r}")
```

`PluginRegistry` is for services that need multiple adapters simultaneously,
not for services that swap between adapters via configuration.

### Full wiring reference

For the complete guide — upgrade path from Stage 1 to Stage 2, lifespan
wiring, and architecture diagrams — see the
[Composition Root](https://furious-meteors.github.io/openframe-core/developer-guide/composition-root/)
page in the `openframe-core` documentation.

---

## Package inventory

| Extra | `pip install` | Package installed | Async driver |
|---|---|---|---|
| `postgres` | `openframe-adapters[postgres]` | `openframe-adapters-db-postgres` | `asyncpg` |
| `mysql` | `openframe-adapters[mysql]` | `openframe-adapters-db-mysql` | `aiomysql` |
| `redis` | `openframe-adapters[redis]` | `openframe-adapters-db-redis` | `redis-py` (asyncio) |
| `dynamodb` | `openframe-adapters[dynamodb]` | `openframe-adapters-db-dynamodb` | `aiobotocore` |
| `mongo` | `openframe-adapters[mongo]` | `openframe-adapters-db-mongo` | `Motor` |
| `cassandra` | `openframe-adapters[cassandra]` | `openframe-adapters-db-cassandra` | `cassandra-driver` |
| `influxdb` | `openframe-adapters[influxdb]` | `openframe-adapters-db-influxdb` | `influxdb-client` |
| `milvus` | `openframe-adapters[milvus]` | `openframe-adapters-db-milvus` | `pymilvus` |
| `chromadb` | `openframe-adapters[chromadb]` | `openframe-adapters-db-chromadb` | `chromadb` |
| `qdrant` | `openframe-adapters[qdrant]` | `openframe-adapters-db-qdrant` | `qdrant-client` |
| `faiss` | `openframe-adapters[faiss]` | `openframe-adapters-db-faiss` | `faiss-cpu` |
| `falkordb` | `openframe-adapters[falkordb]` | `openframe-adapters-db-falkordb` | `falkordb` |
| `kafka` | `openframe-adapters[kafka]` | `openframe-adapters-queue-kafka` | `aiokafka` |
| `nats` | `openframe-adapters[nats]` | `openframe-adapters-queue-nats` | `nats-py` |
| `rabbitmq` | `openframe-adapters[rabbitmq]` | `openframe-adapters-queue-rabbitmq` | `aio-pika` |
| `db` | `openframe-adapters[db]` | all 7 DB adapters above | — |
| `vector` | `openframe-adapters[vector]` | all 5 vector adapters above | — |
| `queue` | `openframe-adapters[queue]` | all 3 queue adapters above | — |
| `all` | `openframe-adapters[all]` | all 15 adapter packages | — |

---

## Core dependency

[`openframe-core`](https://pypi.org/project/openframe-core/) is installed
automatically as a transitive dependency — you never need to declare it
separately. As of this release, `postgres`, `mongo`, `redis`, and `kafka`
pin `openframe-core>=3.0,<4` (the ADR-006 unified port/lifecycle contract
layer — `BasePort`, typed `Capability`, `openframe.core.contracts`). Other
adapter packages in this meta-package may still be on an older `core` pin
until they are migrated; check each package's own `pyproject.toml` for its
exact range.

---

## Versioning

Each adapter package is versioned independently and published to PyPI under
its own name (e.g. `openframe-adapters-db-postgres`). This meta-package pins
each one to its own major-version range (see the `[project.optional-dependencies]`
table in `pyproject.toml`), so patch and minor releases are picked up
automatically the next time you run `pip install --upgrade`. Only a major
version bump in an individual adapter requires a meta-package update.

**Breaking change in this release**: `postgres`, `mongo`, and `redis` are
now pinned to `>=2.0,<3` — `ping()`/`is_ready()` were removed from their
repository classes (deprecated in the prior minor release; `health()` is
now the sole health check). `kafka` remains non-breaking at `>=1.4,<2`.

When a new adapter is added to the ecosystem, only this `pyproject.toml`
changes — one new line in `[project.optional-dependencies]` and an update to
the relevant group. No other file in the monorepo is touched.
