# openframe-adapters-db-milvus

Milvus vector-database adapter for the **OpenFrame Microservice Suite**.

Part of the `openframe-adapters` monorepo. Implements `BaseVectorStore[T]`
(which extends `BaseRepository[T]`) from `openframe-core` using
`pymilvus.AsyncMilvusClient`.

---

## Installation

```bash
pip install openframe-adapters-db-milvus
```

Required env var:

```
MILVUS_URI=http://localhost:19530
```

Milvus Lite (local file, no server) and Zilliz Cloud endpoints both work
with the same variable — see `MilvusSettings` for format examples.

---

## Quick start

### Raw dict mode

```python
from openframe.adapters.db.milvus import MilvusSettings, MilvusRepository

settings = MilvusSettings()  # reads MILVUS_URI from env
repo = MilvusRepository(settings, collection_name="items")

item = await repo.get("abc-123")              # dict | None
items, total = await repo.list(10, 0)         # ([dict, ...], int)
created = await repo.create({"id": "1", "vector": [0.1, 0.2, 0.3], "name": "x"})
updated = await repo.update({"id": "1", "vector": [0.1, 0.2, 0.3], "name": "y"})
deleted = await repo.delete("1")              # bool
results = await repo.search(query_vector=[0.1, 0.2, 0.3], k=5)  # list[dict]
```

### Typed domain mode

```python
from dataclasses import dataclass
from openframe.adapters.db.milvus import MilvusSettings, MilvusRepository

@dataclass
class Item:
    id: str
    vector: list[float]
    name: str

class ItemRepository(MilvusRepository[Item]):
    _collection_name = "items"

    def _row_to_entity(self, row) -> Item:
        return Item(**row)

    def _entity_to_row(self, entity: Item) -> dict:
        return {"id": entity.id, "vector": entity.vector, "name": entity.name}

settings = MilvusSettings()
repo = ItemRepository(settings)
item: Item | None = await repo.get("abc-123")
```

---

## Wiring into an application

For a real service, wire `MilvusPlugin` (the `BasePort`-satisfying plugin
class) through `ApplicationBootstrap.compose()` from `openframe-core`. This
gives you proper lifecycle management — `initialize()` / `health()` /
`shutdown()` — for free, instead of constructing `MilvusRepository`
directly and managing the client yourself:

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.db.milvus import MilvusPlugin, MilvusSettings

settings = MilvusSettings()  # reads MILVUS_URI from env
plugin = MilvusPlugin(settings, collection_name="items")

async with ApplicationBootstrap.compose(plugin) as app:
    repo = app.get(Capability.SEARCH)   # -> MilvusRepository
    results = await repo.search(query_vector=[0.1, 0.2, 0.3], k=5)
# client is closed automatically on exit (plugin.shutdown() ran)
```

`compose()` calls `plugin.initialize()` on entry and `plugin.shutdown()` on
exit, so the client is created, health-checked, and torn down without any
manual lifecycle code. Requires `openframe-core>=3.3`.

Reach for a subclassed `ApplicationBootstrap` (with a `configure()` method)
only when you need per-port `config=`/`init_timeout=` or conditional
registration order; use `app.registry` as an escape hatch for anything
neither tier covers. The `MilvusRepository(settings)` construction shown
above under "Quick start" remains valid for tests, scripts, or any context
that doesn't need plugin lifecycle management.

---

## Configuration

All settings are read from environment variables.

| Env var | Type | Default | Description |
|---|---|---|---|
| `MILVUS_URI` | `str` | **required** | Server URI, Milvus Lite file path, or Zilliz Cloud endpoint |
| `MILVUS_TOKEN` | `str` | `""` | API key or `"user:password"` credential |
| `MILVUS_DB_NAME` | `str` | `"default"` | Milvus database name within the instance |
| `MILVUS_METRIC_TYPE` | `str` | `"COSINE"` | Similarity metric (`COSINE`, `L2`, `IP`) |
| `MILVUS_VECTOR_DIM` | `int` | `128` | Embedding dimensionality |
| `CONNECTION_TIMEOUT` | `float` | `30.0` | Connectivity-probe timeout (s) |
| `OPERATION_TIMEOUT` | `float` | `10.0` | Per-operation timeout (s) |
| `MAX_RETRIES` | `int` | `3` | Max retry attempts |

---

## Async strategy

This adapter uses **native async** — `pymilvus.AsyncMilvusClient` — not a
`run_in_executor` wrapper around the sync client. The installed driver
(`pymilvus` 3.0.2) ships a real `async def` surface for every CRUD +
search method this adapter needs, verified directly against the installed
package rather than assumed. See `connection.py`'s module docstring for
the full verification trail, including a documented finding that
`AsyncMilvusClient.__init__` is synchronous and lazy (no network I/O until
the first awaited call), and that real connection failures surface as a
bare `pymilvus.MilvusException` rather than the `ConnectError`/
`MilvusUnavailableException` subclasses pymilvus's own docstrings suggest.

---

## Health checks

`MilvusRepository`/`MilvusPlugin` both expose `health()`, returning a
`PluginHealth` snapshot. Never raises.

```python
health = await repo.health()   # PluginHealth(status=..., message=...)
```

---

## Exception hierarchy

All exceptions are `AdapterError` subclasses from `openframe.core.exceptions`.
Raw `pymilvus` exceptions never escape the adapter.

| Situation | Exception |
|---|---|
| Cannot connect to Milvus | `AdapterConnectionError` |
| Invalid `MILVUS_URI` | `AdapterConfigurationError` |
| Query failed (bad filter, dimension mismatch, etc.) | `AdapterQueryError` |
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
from openframe.core.ports import BaseRepository, BaseVectorStore

repo = MilvusRepository(settings, collection_name="items")
assert isinstance(repo, BaseRepository)    # True — structural check
assert isinstance(repo, BaseVectorStore)   # True — structural check
```

No inheritance from either Protocol is required or used.

---

## Resilience (optional — `openframe-core>=3.4`)

`CircuitBreakerProxy` can be composed around the traced repository to
short-circuit calls to a failing Milvus instance:

```python
from openframe.core.resilience import CircuitBreakerProxy
from openframe.core.telemetry import TracingProxy

repo = CircuitBreakerProxy(
    TracingProxy(plugin.get_repository(), prefix="repository.item"),
    failure_threshold=5,
    reset_timeout=30.0,
)
```

No adapter code needs to change to support this — `CircuitBreakerProxy`
wraps from the outside, exactly like `TracingProxy`.

---

## License

MIT
