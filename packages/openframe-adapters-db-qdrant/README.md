# openframe-adapters-db-qdrant

Qdrant vector-store adapter for the **OpenFrame Microservice Suite**.

Part of the `openframe-adapters` monorepo. Implements `BaseVectorStore[T]`
(which extends `BaseRepository[T]`) from `openframe-core` using
`qdrant-client`'s `AsyncQdrantClient`.

---

## Installation

```bash
pip install openframe-adapters-db-qdrant
```

Required env var:

```
QDRANT_URL=http://localhost:6333
```

---

## Quick start

### Raw dict mode

```python
from openframe.adapters.db.qdrant import QdrantSettings, QdrantVectorStore

settings = QdrantSettings()  # reads QDRANT_URL from env
store = QdrantVectorStore(settings, collection="documents")

item = await store.get("abc-123")                      # dict | None
items, total = await store.list(10, 0)                 # ([dict, ...], int)
created = await store.create({"id": "abc-123", "vector": [0.1, 0.2, 0.3], "text": "hello"})
updated = await store.update({"id": "abc-123", "vector": [0.1, 0.2, 0.3], "text": "bye"})
deleted = await store.delete("abc-123")                 # bool
results = await store.search(query_vector=[0.1, 0.2, 0.3], k=5)  # list[dict]
```

### Typed domain mode

```python
from dataclasses import dataclass
from qdrant_client.http.models import PointStruct
from openframe.adapters.db.qdrant import QdrantSettings, QdrantVectorStore

@dataclass
class Document:
    id: str
    vector: list[float]
    text: str

class DocumentStore(QdrantVectorStore[Document]):
    _collection = "documents"

    def _point_to_entity(self, point) -> Document:
        return Document(id=point.id, vector=point.vector, text=(point.payload or {}).get("text", ""))

    def _entity_to_point(self, entity: Document) -> PointStruct:
        return PointStruct(id=entity.id, vector=entity.vector, payload={"text": entity.text})

settings = QdrantSettings()
store = DocumentStore(settings)
doc: Document | None = await store.get("abc-123")
```

---

## Wiring into an application

For a real service, wire `QdrantPlugin` (the `BasePort`-satisfying plugin
class) through `ApplicationBootstrap.compose()` from `openframe-core`. This
gives you proper lifecycle management — `initialize()` / `health()` /
`shutdown()` — for free, instead of constructing `QdrantVectorStore`
directly and managing the client yourself:

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.db.qdrant import QdrantPlugin, QdrantSettings

settings = QdrantSettings()  # reads QDRANT_URL from env
plugin = QdrantPlugin(settings, collection="documents")

async with ApplicationBootstrap.compose(plugin) as app:
    store = app.get(Capability.SEARCH)   # -> QdrantVectorStore
    results = await store.search(query_vector=[0.1, 0.2, 0.3], k=5)
# client is closed automatically on exit (plugin.shutdown() ran)
```

`compose()` calls `plugin.initialize()` on entry and `plugin.shutdown()` on
exit, so the client is created, health-checked, and torn down without any
manual lifecycle code. Requires `openframe-core>=3.3`.

Reach for a subclassed `ApplicationBootstrap` (with a `configure()` method)
only when you need per-port `config=`/`init_timeout=` or conditional
registration order; use `app.registry` as an escape hatch for anything
neither tier covers. The `QdrantVectorStore(settings, collection=...)`
construction shown above under "Quick start" remains valid for tests,
scripts, or any context that doesn't need plugin lifecycle management.

### Resilience (optional)

`openframe-core>=3.4` ships `openframe.core.resilience`. Compose
`CircuitBreakerProxy` around the (optionally traced) store — never the
reverse, so a short-circuited call never produces a misleading adapter span
for a call that never reached Qdrant:

```python
from openframe.core.resilience import CircuitBreakerProxy
from openframe.core.observability import TracingProxy

store = CircuitBreakerProxy(
    TracingProxy(plugin.get_repository(), prefix="vectorstore.documents"),
    failure_threshold=5,
    reset_timeout=30.0,
)
```

No adapter code needs to change to support this — `CircuitBreakerProxy`
wraps from the outside, exactly like `TracingProxy`.

---

## Configuration

All settings are read from environment variables.

| Env var | Type | Default | Description |
|---|---|---|---|
| `QDRANT_URL` | `str` | **required** | Base URL of the Qdrant server (`http://host:6333`) |
| `QDRANT_API_KEY` | `str \| None` | `None` | API key for Qdrant Cloud / secured instances |
| `QDRANT_PREFER_GRPC` | `bool` | `False` | Use the gRPC transport instead of REST |
| `QDRANT_HTTPS` | `bool \| None` | `None` | Force TLS; `None` infers from `QDRANT_URL` |
| `CONNECTION_TIMEOUT` | `float` | `30.0` | Connectivity-probe timeout (s) |
| `OPERATION_TIMEOUT` | `float` | `10.0` | Per-operation timeout (s) |
| `MAX_RETRIES` | `int` | `3` | Max retry attempts |

---

## API mapping and caveats

`QdrantVectorStore` maps `BaseVectorStore[T]` onto Qdrant's
`AsyncQdrantClient` (native async, verified against the installed
`qdrant-client` — see `connection.py`'s module docstring for how).

| Port method | Qdrant call |
|---|---|
| `get(id)` | `client.retrieve(collection, ids=[id], with_vectors=True)` |
| `list(limit, offset)` | `client.scroll(...)` + `client.count(...)` — **see caveat below** |
| `create(entity)` | `client.upsert(collection, points=[PointStruct(...)])` |
| `update(entity)` | existence check via `get()`, then the same `upsert()` |
| `delete(id)` | existence check via `get()`, then `client.delete(...)` |
| `search(query_vector, k)` | `client.query_points(collection, query=query_vector, limit=k)` |

**`list()` offset caveat:** Qdrant's own `scroll()` offset is a point-id
cursor ("skip points with id less than this"), not an integer skip-count.
This adapter implements a compatibility shim — it scrolls
`limit + offset` points id-ascending from the start and slices the first
`offset` off in Python — to honour `BaseRepository.list(limit, offset)`'s
documented integer-offset contract. This is correctness-preserving for
test-sized and small collections but is **not** performant for a large
`offset`, since there is no server-side "skip N" cursor and every call
re-fetches and discards the skipped prefix. For efficient pagination over
a large collection, page by Qdrant's own id-cursor `next_page_offset`
directly via the raw driver (`store.client` / `get_qdrant_client()`), not
via this method's `offset`.

**`search()` note:** Qdrant 1.19's `AsyncQdrantClient` has no `search()`
method — it was replaced by the universal query endpoint `query_points()`.
This adapter's `search()` is implemented against `query_points()`. The
similarity metric used is whatever metric the target collection was
created with (Qdrant stores the metric per-collection at creation time) —
this adapter does not create collections or choose a metric itself.

---

## Health checks

`QdrantVectorStore.health()` never raises — it returns a `PluginHealth`
snapshot (`PluginStatus.READY` or `PluginStatus.FAILED` with a message)
based on a low-cost `get_collections()` call.

---

## Exception hierarchy

All exceptions are `AdapterError` subclasses from `openframe.core.exceptions`.
Raw `qdrant_client` exceptions never escape the adapter.

| Situation | Exception |
|---|---|
| Cannot connect to Qdrant (transport-level failure) | `AdapterConnectionError` |
| Qdrant server error (5xx / unrecognised response) | `AdapterConnectionError` |
| Invalid `QDRANT_URL` | `AdapterConfigurationError` |
| Query failed (4xx — e.g. vector dimensionality mismatch) | `AdapterQueryError` |
| Operation exceeded timeout | `AdapterTimeoutError` |

See `repository.py`'s `_wrap_qdrant()` for the full classification logic.

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
from openframe.core.ports import BaseVectorStore, BaseRepository

store = QdrantVectorStore(settings, collection="documents")
assert isinstance(store, BaseVectorStore)   # True — structural check
assert isinstance(store, BaseRepository)    # True — BaseVectorStore extends it
```

No inheritance from either Protocol is required or used.

---

## License

MIT
