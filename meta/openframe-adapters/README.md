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

Each individual adapter package uses Python namespace packages under
`openframe.adapters.*`, so all adapters share the same top-level namespace
without any conflicts.

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
separately. Every individual adapter package pins `openframe-core>=1.0,<2`,
so installing any extra brings core in as part of the resolution.

---

## Versioning

Each adapter package is versioned independently and published to PyPI under
its own name (e.g. `openframe-adapters-db-postgres`). This meta-package pins
all of them at `>=1.0,<2`, so patch and minor releases are picked up
automatically the next time you run `pip install --upgrade`. Only a major
version bump in an individual adapter requires a meta-package update.

When a new adapter is added to the ecosystem, only this `pyproject.toml`
changes — one new line in `[project.optional-dependencies]` and an update to
the relevant group. No other file in the monorepo is touched.
