# openframe-adapters-db-mongo

MongoDB document store adapter for the **OpenFrame Microservice Suite**.

Part of the `openframe-adapters` monorepo. Implements `BaseRepository[T]` and
`HealthCheck` from `openframe-core` using `motor` (`AsyncIOMotorClient`).

---

## Installation

```bash
pip install openframe-adapters-db-mongo
```

Required env vars:

```
MONGO_URL=mongodb://user:password@host:27017
MONGO_DATABASE=mydb
```

Atlas SRV URIs work too:

```
MONGO_URL=mongodb+srv://user:password@cluster.mongodb.net
```

---

## Quick start

### Raw dict mode

```python
from openframe.adapters.db.mongo import MongoSettings, MongoRepository

settings = MongoSettings()  # reads MONGO_URL and MONGO_DATABASE from env
repo = MongoRepository(settings, collection="artifacts")

doc = await repo.get("507f1f77bcf86cd799439011")   # dict | None
docs, total = await repo.list(10, 0)                # ([dict, ...], int)
created = await repo.create({"title": "paper"})
updated = await repo.update({"_id": "...", "title": "updated"})
deleted = await repo.delete("507f1f77bcf86cd799439011")  # bool
```

### Typed domain mode

```python
from dataclasses import dataclass
from openframe.adapters.db.mongo import MongoSettings, MongoRepository

@dataclass
class Artifact:
    id: str
    title: str
    artifact_type: str

class ArtifactRepository(MongoRepository[Artifact]):
    _collection = "artifacts"

    def _doc_to_entity(self, doc: dict) -> Artifact:
        return Artifact(
            id=doc["_id"],
            title=doc["title"],
            artifact_type=doc["artifact_type"],
        )

    def _entity_to_doc(self, entity: Artifact) -> dict:
        return {
            "_id": entity.id,
            "title": entity.title,
            "artifact_type": entity.artifact_type,
        }

settings = MongoSettings()
repo = ArtifactRepository(settings)
artifact: Artifact | None = await repo.get("507f1f77bcf86cd799439011")
```

---

## `_id` handling

MongoDB uses `_id` as the document identifier; `BaseRepository` uses
`entity_id: str`. The adapter bridges this transparently:

- **Reading:** `_id` is always returned as `str` — `ObjectId` is never
  exposed to callers. A convenience `id` key is also added mirroring `_id`.
- **Writing:** If an entity dict has an `id` key but no `_id` key, the
  adapter maps `id` → `_id` before insertion.
- **Filtering:** String entity IDs that are valid 24-char hex are parsed
  as `ObjectId` for indexed lookups; other strings are used as plain string
  `_id` values.

---

## Configuration

All settings are read from environment variables.

| Env var | Type | Default | Description |
|---|---|---|---|
| `MONGO_URL` | `str` | **required** | Motor/pymongo connection string |
| `MONGO_DATABASE` | `str` | **required** | Database name |
| `MONGO_MIN_POOL_SIZE` | `int` | `5` | Minimum pool connections |
| `MONGO_MAX_POOL_SIZE` | `int` | `20` | Maximum pool connections |
| `MONGO_SERVER_SELECTION_TIMEOUT_MS` | `int` | `5000` | Server selection timeout (ms) |
| `MONGO_TLS` | `bool` | `False` | Enable TLS/SSL |
| `MONGO_TLS_ALLOW_INVALID_CERTS` | `bool` | `False` | Skip cert validation (dev only) |
| `CONNECTION_TIMEOUT` | `float` | `30.0` | Pool creation timeout (s) |
| `OPERATION_TIMEOUT` | `float` | `10.0` | Per-operation timeout (s) |
| `MAX_RETRIES` | `int` | `3` | Max retry attempts |

---

## Timeout strategy

Two layers of timeout on every operation:

1. `asyncio.timeout(settings.operation_timeout)` — cancels the Python coroutine.
2. `max_time_ms=int(settings.operation_timeout * 1000)` passed to motor query
   methods — instructs the MongoDB server to abort the query.

---

## Health checks

`MongoRepository` implements the `HealthCheck` protocol from `openframe-core`.

```python
alive = await repo.ping()       # admin.command("ping") — fast liveness check
ready = await repo.is_ready()   # list_collection_names() — full readiness check
```

Both methods return `False` on any failure and never raise.

---

## Exception hierarchy

All exceptions are `AdapterError` subclasses from `openframe.core.exceptions`.
Raw motor/pymongo exceptions never escape the adapter.

| Situation | Exception |
|---|---|
| Cannot connect to MongoDB | `AdapterConnectionError` |
| Invalid `MONGO_URL` syntax | `AdapterConfigurationError` |
| Query failed (constraint, auth, etc.) | `AdapterQueryError` |
| Operation exceeded timeout | `AdapterTimeoutError` |

---

## Development

```bash
# from the package directory
pip install -e ".[dev]"
pytest tests/ -v
```

---

## Protocol conformance

```python
from openframe.core.ports import BaseRepository
from openframe.core.health import HealthCheck

repo = MongoRepository(settings, collection="artifacts")
assert isinstance(repo, BaseRepository)   # True — structural check
assert isinstance(repo, HealthCheck)      # True — structural check
```

No inheritance from either Protocol is required or used.

---

## License

MIT
