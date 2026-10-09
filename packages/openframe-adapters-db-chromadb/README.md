# openframe-adapters-db-chromadb

ChromaDB vector-store adapter for the **OpenFrame Microservice Suite**.

Part of the `openframe-adapters` monorepo. Implements `BaseVectorStore[T]`
(which extends `BaseRepository[T]`) from `openframe-core` using `chromadb`'s
native async HTTP client (`chromadb.AsyncHttpClient`).

This adapter targets Chroma's **client-server (HTTP) mode** exclusively.
There is no embedded/persistent-local-mode support (`chromadb.Client()`/
`PersistentClient()`) — that mode has no server to connect to and is out of
scope here, matching how this ecosystem's other adapters (Postgres, Mongo,
Redis) assume a running backend server rather than an embedded/local-file
mode.

---

## Installation

```bash
pip install openframe-adapters-db-chromadb
```

No required env vars — every setting has a default suitable for a locally
run Chroma server (`chroma run`, which listens on `localhost:8000`). You
must set a collection name before using a repository, either via
`CHROMA_COLLECTION` or the `collection=` constructor argument.

---

## Async client research finding

`chromadb` 1.5.9 ships a genuine, documented native-async client-server
API — not an experimental bolt-on over the sync client. Verified directly
against the installed package:

```python
>>> import chromadb, inspect
>>> chromadb.__version__
'1.5.9'
>>> inspect.iscoroutinefunction(chromadb.AsyncHttpClient)
True
```

Every method this adapter calls on the collection object
`AsyncClientAPI.get_or_create_collection(...)` returns is a real coroutine
function (`inspect.iscoroutinefunction` confirms this for `AsyncCollection.add`,
`.get`, `.update`, `.upsert`, `.delete`, `.query`, `.count`, as well as
`AsyncClientAPI.get_or_create_collection`/`.create_collection`/`.get_collection`
themselves).

Because that surface genuinely exists, this adapter is built the same shape
as `openframe-adapters-db-postgres`/`-oracle` — a client cached by
connection-parameter tuple, native `async def` CRUD methods — not the
`run_in_executor`-wrapped-sync shape a backend with no real async client
would need (per ADR-003: the async strategy is decided per-adapter against
the actually-installed driver, not forced). See
`openframe/adapters/db/chromadb/connection.py`'s module docstring for the
full finding, including the `httpx`-exception classification detail below.

---

## Quick start

### Raw dict mode

```python
from openframe.adapters.db.chromadb import ChromaDBSettings, ChromaDBRepository

settings = ChromaDBSettings(chroma_collection="items")  # reads CHROMA_* from env
repo = ChromaDBRepository(settings)

item = await repo.get("abc-123")                 # dict | None
items, total = await repo.list(10, 0)             # ([dict, ...], int)
created = await repo.create({"id": "abc-123", "vector": [0.1, 0.2, 0.3], "metadata": {"name": "x"}})
updated = await repo.update({"id": "abc-123", "vector": [0.1, 0.2, 0.3], "metadata": {"name": "y"}})
deleted = await repo.delete("abc-123")             # bool
results = await repo.search(query_vector=[0.1, 0.2, 0.3], k=5)  # list[dict], nearest first
```

Rows are shaped `{"id": ..., "vector": [...], "metadata": {...}, "document": ...}`
— the plain-dict shape `_row_to_entity()`/`_entity_to_row()` transpose to/from
Chroma's own columnar (parallel-list) wire format. See "Columnar wire format"
below.

### Typed domain mode

```python
from dataclasses import dataclass
from openframe.adapters.db.chromadb import ChromaDBSettings, ChromaDBRepository

@dataclass
class Item:
    id: str
    vector: list[float]
    name: str

class ItemRepository(ChromaDBRepository[Item]):
    _collection = "items"

    def _row_to_entity(self, row) -> Item:
        return Item(id=row["id"], vector=row["vector"], name=row["metadata"].get("name", ""))

    def _entity_to_row(self, entity: Item) -> dict:
        return {"id": entity.id, "vector": entity.vector, "metadata": {"name": entity.name}}

settings = ChromaDBSettings()
repo = ItemRepository(settings)
item: Item | None = await repo.get("abc-123")
```

---

## Columnar wire format

ChromaDB's `collection.get(...)`/`collection.query(...)` do not return
row/record objects the way `asyncpg`/`pymongo` do — they return a dict of
**parallel lists**, one list per field (`ids`, `embeddings`, `metadatas`,
`documents`), all the same length and ordered together positionally.
`query()` nests each list one level deeper (one list-of-lists per query
embedding, since Chroma's query API supports batched multi-vector queries);
this adapter always sends exactly one query embedding, so it always
unwraps index `[0]`.

`ChromaDBRepository._get_result_to_rows()`/`._query_result_to_rows()`
transpose that columnar shape into plain per-entity row dicts before
`_row_to_entity()` ever sees them — the single most unusual part of this
adapter relative to a row-oriented driver.

---

## `create()` vs `upsert()`

`create()` uses `collection.add()`, which **fails on a duplicate id**
(`IDAlreadyExistsError`/`DuplicateIDError` from the server) — matching
`BaseRepository.create()`'s "persist a NEW entity" contract, the same way a
unique-constraint violation would surface in Postgres. `update()` uses
`collection.update()` after a `collection.get(ids=[id])` existence check,
since Chroma's own `update()` has no documented raise-if-missing behaviour.
`collection.upsert()` is intentionally unused by either method — it would
make `create()` silently overwrite on a duplicate id, hiding exactly the
bug class `BaseRepository.create()`'s contract exists to catch.

---

## Wiring into an application

For a real service, wire `ChromaDBPlugin` (the `BasePort`-satisfying plugin
class) through `ApplicationBootstrap.compose()` from `openframe-core`. This
gives you proper lifecycle management — `initialize()` / `health()` /
`shutdown()` — for free, instead of constructing `ChromaDBRepository`
directly and managing the client yourself:

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.db.chromadb import ChromaDBPlugin, ChromaDBSettings

settings = ChromaDBSettings(chroma_collection="items")
plugin = ChromaDBPlugin(settings, collection="items")

async with ApplicationBootstrap.compose(plugin) as app:
    repo = app.get(Capability.SEARCH)        # -> ChromaDBRepository
    results = await repo.search(query_vector=[0.1, 0.2, 0.3], k=5)
# client handle released automatically on exit (plugin.shutdown() ran)
```

`compose()` calls `plugin.initialize()` on entry and `plugin.shutdown()` on
exit, so the collection handle is created, health-checked, and released
without any manual lifecycle code. Requires `openframe-core>=3.5`.

Reach for a subclassed `ApplicationBootstrap` (with a `configure()` method)
only when you need per-port `config=`/`init_timeout=` or conditional
registration order; use `app.registry` as an escape hatch for anything
neither tier covers. The `ChromaDBRepository(settings)` construction shown
above under "Quick start" remains valid for tests, scripts, or any context
that doesn't need plugin lifecycle management.

### Optional: circuit breaker

`openframe-core>=3.4` ships `openframe.core.resilience`. Compose
`CircuitBreakerProxy` around the (optionally traced) repository — never the
reverse, so a short-circuited call never produces a misleading adapter span
for a call that never reached the adapter:

```python
from openframe.core.resilience import CircuitBreakerProxy
from openframe.core.tracing import TracingProxy

repo = plugin.get_repository()
protected = CircuitBreakerProxy(
    TracingProxy(repo, prefix="repository.item"),
    failure_threshold=5,
    reset_timeout=30.0,
)
```

No adapter code needs to change to support this — both proxies wrap from
the outside.

---

## Configuration

All settings are read from environment variables. None are strictly
required, but `chroma_collection` must be set (here or via `collection=`)
before any repository operation will succeed.

| Env var | Type | Default | Description |
|---|---|---|---|
| `CHROMA_HOST` | `str` | `localhost` | Chroma HTTP server hostname |
| `CHROMA_PORT` | `int` | `8000` | Chroma HTTP server port |
| `CHROMA_SSL` | `bool` | `False` | Use HTTPS when talking to the server |
| `CHROMA_TENANT` | `str` | `default_tenant` | Tenant name |
| `CHROMA_DATABASE` | `str` | `default_database` | Database name within the tenant |
| `CHROMA_COLLECTION` | `str` | `""` | Default collection name (required to use a repository) |
| `CONNECTION_TIMEOUT` | `float` | `30.0` | Client creation timeout (s) |
| `OPERATION_TIMEOUT` | `float` | `10.0` | Per-operation timeout (s) |
| `MAX_RETRIES` | `int` | `3` | Max retry attempts |

---

## Health checks

`ChromaDBRepository`/`ChromaDBPlugin` implement the `BasePort` lifecycle
from `openframe-core`.

```python
health = await repo.health()   # PluginHealth(status=PluginStatus.READY, message="")
```

`health()` never raises — it returns `PluginHealth(status=PluginStatus.FAILED, ...)`
on any internal failure.

---

## Exception hierarchy

All exceptions are `AdapterError` subclasses from `openframe.core.exceptions`.
Raw `chromadb`/`httpx` exceptions never escape the adapter.

| Situation | Exception |
|---|---|
| Cannot connect to Chroma / connection lost mid-call | `AdapterConnectionError` |
| No collection name configured | `AdapterConfigurationError` |
| Query failed (duplicate id, dimension mismatch, etc.) | `AdapterQueryError` |
| Operation exceeded timeout | `AdapterTimeoutError` |

`httpx.TimeoutException` is checked before the broader `httpx.TransportError`
at every call site — it is itself a `TransportError` subclass in the
installed version, so checking order matters or every timeout would be
misclassified as a generic connection failure. `chromadb.errors.ChromaError`
and all its subclasses (`IDAlreadyExistsError`, `InvalidDimensionException`,
`NotFoundError`, etc.) are flat — always query-class, never connection-class
— confirmed against the installed 1.5.9's exception hierarchy.

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

repo = ChromaDBRepository(settings, collection="items")
assert isinstance(repo, BaseVectorStore)  # True — structural check
assert isinstance(repo, BaseRepository)  # True — BaseVectorStore extends it
```

No inheritance from either Protocol is required or used.

---

## License

MIT
