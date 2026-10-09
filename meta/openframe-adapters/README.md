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
pip install openframe-adapters[cockroachdb] # CockroachDB via asyncpg
pip install openframe-adapters[mysql]       # MySQL via aiomysql
pip install openframe-adapters[mariadb]     # MariaDB via aiomysql
pip install openframe-adapters[oracle]      # Oracle via python-oracledb (thin mode async)

# Key-value
pip install openframe-adapters[redis]       # Redis via redis-py
pip install openframe-adapters[dynamodb]    # DynamoDB via aioboto3

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
pip install openframe-adapters[db]       # all 10 DB adapters (relational + document + specialist)
pip install openframe-adapters[vector]   # all 5 vector DB adapters
pip install openframe-adapters[queue]    # all 3 queue adapters
```

### Everything

```bash
pip install openframe-adapters[all]      # all 18 individual adapter packages
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

Wiring goes through `openframe-core`'s `ApplicationBootstrap`
(requires `openframe-core>=3.3`) — one recommended class, at two levels of
ceremony:

> **One or a few adapters, no per-adapter config needed** — `ApplicationBootstrap.compose(*plugins)`. No subclass.
> **An adapter needs its own `config=`/`init_timeout=`, or registration order depends on a runtime condition** — subclass with `configure()`.
> Either way, `ApplicationBootstrap` manages startup ordering, health aggregation, and graceful shutdown (including telemetry flush) across all adapters — you never hand-roll that part.

### One adapter — `compose()`, no subclass

```python
# bootstrap/dependencies.py
from openframe.adapters.db.postgres import PostgresPlugin, PostgresSettings
from openframe.core.ports import Capability
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.tracing import TracingProxy
from src.adapters.item_repository import ItemPostgresRepository
from src.application.services.item_service import ItemService

app = ApplicationBootstrap.compose(
    PostgresPlugin(PostgresSettings(), repository_class=ItemPostgresRepository)
)

def get_item_service() -> ItemService:
    repo = TracingProxy(app.get(Capability.PERSISTENCE).get_repository(), prefix="repository.item")
    return ItemService(repo)
```

`PostgresSettings()` reads `DATABASE_URL` from env at startup.
`TracingProxy` wraps the repository for automatic OTel spans on every call.

### Two or more adapters — still `compose()`, or subclass if you need per-adapter config

`compose()` accepts any number of ports — pass them all in one call:

```python
# bootstrap/dependencies.py
from openframe.adapters.db.postgres import PostgresPlugin, PostgresSettings
from openframe.adapters.db.redis import RedisPlugin, RedisSettings
from openframe.core.ports import Capability
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.tracing import TracingProxy
from src.application.services.item_service import ItemService
from src.application.services.session_service import SessionService

app = ApplicationBootstrap.compose(
    PostgresPlugin(PostgresSettings()),   # capability: Capability.PERSISTENCE
    RedisPlugin(RedisSettings()),          # capability: Capability.CACHE
)

def get_item_service() -> ItemService:
    repo = TracingProxy(app.get(Capability.PERSISTENCE).get_repository(), prefix="repository.item")
    return ItemService(repo)

def get_session_service() -> SessionService:
    cache = TracingProxy(app.get(Capability.CACHE).get_repository(), prefix="cache.session")
    return SessionService(cache)
```

Reach for a subclass instead once an adapter needs its own `config=` mapping,
a per-adapter `init_timeout=`, or registration order that depends on a
runtime condition:

```python
class AppBootstrap(ApplicationBootstrap):
    def configure(self) -> None:
        self.register(PostgresPlugin(PostgresSettings()), config={"pool_hint": "primary"})
        self.register(RedisPlugin(RedisSettings()), init_timeout=5.0)

app = AppBootstrap()
```

For the deliberate multi-port-per-capability case (e.g. primary + replica
Postgres), use `app.get_all(Capability.PERSISTENCE)` or, for anything neither
covers, the underlying registry directly via `app.registry`.

Wire `start()` and `stop()` in your FastAPI lifespan:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from openframe.core.middleware import TelemetryMiddleware
from openframe.core.telemetry import setup_telemetry
from src.bootstrap.dependencies import app as bootstrap

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_telemetry()
    await bootstrap.start()   # initializes every registered adapter — fails fast if any backend unreachable
    yield
    await bootstrap.stop()    # shuts down every adapter, then flushes telemetry — never raises

app = FastAPI(lifespan=lifespan)
app.add_middleware(TelemetryMiddleware)
```

### Domain subclass registration

Every `*Plugin` supports registering a domain-specific subclass instead of
the plain base adapter class — `repository_class` (Postgres, Mongo, Redis),
`producer_class` and `consumer_class` (Kafka):

```python
app = ApplicationBootstrap.compose(
    PostgresPlugin(PostgresSettings(), table="items", repository_class=ItemRepository),
    RedisPlugin(RedisSettings(), repository_class=SessionRepository),
    KafkaPlugin(KafkaSettings(), producer_class=ArtifactEventProducer, consumer_class=OrderEventConsumer),
)
```

`get_repository()` / `get_producer()` / `make_consumer()` then return an
instance of the subclass you passed in, not the plain base class — so
overridden entity mapping (`_doc_to_entity`, `_entity_to_doc`, `_serialise`,
`_deserialise`, etc.) is preserved end-to-end through the registry.

### Plugin capabilities

Every `*Plugin` class declares a `capability` attribute — a typed
`openframe.core.ports.Capability` enum member (not a raw string as of
`openframe-core` v3.0; the module was `openframe.core.contracts` before
v3.1, which merged it into `openframe.core.ports`). This is the key used
by `ApplicationBootstrap.get()` / `PluginRegistry.get()`:

```python
from openframe.core.ports import Capability

app.get(Capability.PERSISTENCE)  # → PostgresPlugin / MongoPlugin
app.get(Capability.CACHE)        # → RedisPlugin
```

The capability taxonomy is a closed enum shared across the entire OpenFrame
ecosystem:

| Capability | Adapters | Use for |
|---|---|---|
| `Capability.PERSISTENCE` | Postgres, Mongo, MySQL, DynamoDB, Cassandra, InfluxDB | Primary data store |
| `Capability.CACHE` | Redis | Fast ephemeral store, sessions, rate limits |
| `Capability.QUEUE` | Kafka, NATS, RabbitMQ | Message publishing and consumption |
| `Capability.SEARCH` | Milvus, Qdrant, ChromaDB, FAISS, FalkorDB | Vector similarity search |

The closed `Capability` enum (`openframe.core.ports.Capability`) has no
dedicated time-series member as of core v3.0. InfluxDB registers under
`Capability.PERSISTENCE` rather than a dedicated member — adding one to a
closed, ecosystem-wide enum needs its own ADR-level justification, and
nothing about InfluxDB's registry-lookup behavior requires it. See
`openframe-adapters-db-influxdb`'s `plugin.py` module docstring and its
README's "InfluxDB vs. row-based CRUD" section for the real mismatch that
distinguishes this adapter from the rest of the `PERSISTENCE` group —
it is not a drop-in row-store replacement despite the shared capability.

`Capability` also compares equal to its string value (e.g.
`Capability.PERSISTENCE == "persistence"`), so existing string comparisons
keep working, but new code should key on the enum member directly.

### The env-var swap exception

Switching between adapters via an environment variable (`PERSISTENCE_BACKEND=postgres`
vs `PERSISTENCE_BACKEND=mongo`) still only ever has **one** adapter active at
a time — `compose()` still applies, just with the chosen plugin decided
before the call:

```python
def _make_plugin():
    backend = os.environ.get("PERSISTENCE_BACKEND", "postgres")
    if backend == "postgres":
        return PostgresPlugin(PostgresSettings(), repository_class=ItemPostgresRepository)
    elif backend == "mongo":
        return MongoPlugin(MongoSettings(), repository_class=ItemMongoRepository)
    raise ValueError(f"Unknown PERSISTENCE_BACKEND: {backend!r}")

app = ApplicationBootstrap.compose(_make_plugin())
```

Both backends still register under the same `Capability.PERSISTENCE` — the
rest of your service (`app.get(Capability.PERSISTENCE)`) never knows which
one is active.

### Full wiring reference

For the complete guide — the three-tier model (`compose()` → `configure()`
subclass → `.registry` escape hatch), lifespan wiring, and architecture
diagrams — see [How It Works § Choosing a Wiring Pattern](https://furious-meteors.github.io/openframe-core/developer-guide/how-it-works/#choosing-a-wiring-pattern)
in the `openframe-core` documentation.

### Resilience — circuit breaking under sustained failure

`openframe-core>=3.4` ships `openframe.core.resilience.CircuitBreakerProxy` — wrap a repository/producer to short-circuit calls after repeated failures instead of blocking every caller until `operation_timeout` during a sustained outage:

```python
from openframe.core.resilience import CircuitBreakerProxy
from openframe.core.tracing import TracingProxy

repo = CircuitBreakerProxy(
    TracingProxy(app.get(Capability.PERSISTENCE).get_repository(), prefix="repository.item"),
    failure_threshold=5,
    reset_timeout=30.0,
)
```

Wrap the traced repository, not the reverse — a short-circuited call never
reaches the adapter, so it shouldn't produce a misleading adapter span.

---

## Package inventory

| Extra | `pip install` | Package installed | Async driver |
|---|---|---|---|
| `postgres` | `openframe-adapters[postgres]` | `openframe-adapters-db-postgres` | `asyncpg` |
| `cockroachdb` | `openframe-adapters[cockroachdb]` | `openframe-adapters-db-cockroachdb` | `asyncpg` |
| `mysql` | `openframe-adapters[mysql]` | `openframe-adapters-db-mysql` | `aiomysql` |
| `mariadb` | `openframe-adapters[mariadb]` | `openframe-adapters-db-mariadb` | `aiomysql` |
| `oracle` | `openframe-adapters[oracle]` | `openframe-adapters-db-oracle` | `oracledb` (thin mode async) |
| `redis` | `openframe-adapters[redis]` | `openframe-adapters-db-redis` | `redis-py` (asyncio) |
| `dynamodb` | `openframe-adapters[dynamodb]` | `openframe-adapters-db-dynamodb` | `aioboto3` (aiohttp-backed via aiobotocore) |
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
| `db` | `openframe-adapters[db]` | all 10 DB adapters above | — |
| `vector` | `openframe-adapters[vector]` | all 5 vector adapters above | — |
| `queue` | `openframe-adapters[queue]` | all 3 queue adapters above | — |
| `all` | `openframe-adapters[all]` | all 18 adapter packages | — |

---

## Core dependency

[`openframe-core`](https://pypi.org/project/openframe-core/) is installed
automatically as a transitive dependency — you never need to declare it
separately. As of this release, `postgres`, `mongo`, `redis`, and `kafka`
pin `openframe-core>=3.3,<4` (v3.3.0 added `ApplicationBootstrap.compose()`/
`get_all()`/`registry`, which every package's own wiring documentation now
uses as the default example). Other adapter packages in this meta-package
may still be on an older `core` pin until they are migrated; check each
package's own `pyproject.toml` for its exact range.

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
