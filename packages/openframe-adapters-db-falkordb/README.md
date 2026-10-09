# openframe-adapters-db-falkordb

FalkorDB graph-database adapter for the **OpenFrame Microservice Development Suite**.

Part of the [`openframe-adapters`](https://github.com/Furious-Meteors/openframe-adapters) monorepo.

---

## What it provides

| Symbol | Purpose |
|---|---|
| `FalkorDBSettings` | Pydantic-settings subclass — reads all config from env vars |
| `FalkorDBRepository[T]` | Generic async graph repository — `BaseGraphStore[T]` (extends `BaseRepository[T]`) |
| `get_falkordb_client()` | Async factory — creates and caches the `falkordb.asyncio.FalkorDB` client |
| `FalkorDBPlugin` | `BasePort` (Identity + Lifecycle) — registered via `PluginRegistry`/`ApplicationBootstrap` |

Requires `openframe-core>=3.6,<4` — the `BaseGraphStore[T]` port this package implements was introduced in openframe-core v3.6.0.

## Installation

```bash
# Via meta-package (recommended)
pip install "openframe-adapters[falkordb]"

# Or directly
pip install openframe-adapters-db-falkordb
```

## Addressing scheme — read this before writing queries against this adapter

**Nodes are addressed by an application-level `id` property, never by FalkorDB's own internal `id()` Cypher function.**

FalkorDB's own `id()` function is explicitly documented upstream as unstable: when a node is deleted, higher internal ids can be migrated down to fill the vacated lower ones, so an id captured before some unrelated deletion elsewhere in the graph can silently point at a *different* node afterwards. Using `id(n)` as a stable addressing key is a real, documented footgun — this adapter never does it.

Instead, every node this repository manages carries an application-level `id` property, backed by a real FalkorDB unique constraint (which transitively creates the backing range index) on `(label, id)`. This adapter creates that constraint itself — idempotently, lazily, on the first CRUD call a repository instance makes, and also eagerly from `initialize()` for the plugin/lifecycle-managed path — it does **not** assume an operator created it out-of-band. Without this constraint/index, every `get()`/`update()`/`delete()` degrades silently to an unindexed full-label scan.

All nodes a given `FalkorDBRepository` instance manages share one Cypher label, configured via `FalkorDBSettings.falkordb_node_label` (default `"Entity"`). Because Cypher has no parameterized syntax for labels, this value is interpolated directly into every query string the adapter builds — so it is validated as a safe identifier (`^[A-Za-z_][A-Za-z0-9_]*$`) at `FalkorDBSettings` construction time, not per-query. An unsafe label raises `pydantic_core.ValidationError` immediately at startup.

### Relationships and edges — `traverse()` is the only way

`BaseGraphStore` models only nodes as first-class, CRUD-addressable entities. It does **not** give relationships/edges their own method — `traverse()` is the *only* way to create, update, or query them, via arbitrary Cypher:

```python
# Create an edge between two existing nodes
await repo.traverse(
    "MATCH (a:Entity) WHERE a.id = $a_id "
    "MATCH (b:Entity) WHERE b.id = $b_id "
    "CREATE (a)-[:KNOWS]->(b)",
    {"a_id": "1", "b_id": "2"},
)

# Traverse one hop
friends = await repo.traverse(
    "MATCH (a:Entity)-[:KNOWS]->(b:Entity) WHERE a.id = $id RETURN b",
    {"id": "1"},
)
```

`traverse()` maps each result row's **first** column to a domain entity via `_node_to_entity()` when it is a FalkorDB `Node`, or passes the raw value through otherwise (scalars, aggregates). A multi-column `RETURN` only has its first column mapped — split multi-column traversals into separate calls, or post-process the raw rows yourself if you need more.

## Quick start

```python
from openframe.adapters.db.falkordb import FalkorDBSettings, FalkorDBRepository

settings = FalkorDBSettings(falkordb_host="localhost", falkordb_port=6379)
repo = FalkorDBRepository(settings)

# Store and retrieve
await repo.create({"id": "user-1", "name": "Alice"})
user = await repo.get("user-1")          # {"id": "user-1", "name": "Alice"}
await repo.update({"id": "user-1", "name": "Alice B."})
await repo.delete("user-1")              # True
```

## Configuration

| Env var | Default | Description |
|---|---|---|
| `FALKORDB_HOST` | `"localhost"` | FalkorDB/Redis host |
| `FALKORDB_PORT` | `6379` | FalkorDB/Redis port |
| `FALKORDB_PASSWORD` | `None` | Optional auth password |
| `FALKORDB_SSL` | `false` | Use TLS |
| `FALKORDB_SOCKET_TIMEOUT` | `5.0` | Socket read/write timeout (seconds) |
| `FALKORDB_SOCKET_CONNECT_TIMEOUT` | `5.0` | Connection timeout (seconds) |
| `FALKORDB_GRAPH_NAME` | `"openframe"` | Name of the graph selected on the server |
| `FALKORDB_NODE_LABEL` | `"Entity"` | Cypher label shared by every node this repository manages — validated as a safe identifier at config-load time |

## Typed domain objects

```python
from dataclasses import dataclass
from openframe.adapters.db.falkordb import FalkorDBRepository, FalkorDBSettings

@dataclass
class Person:
    id: str
    name: str

class PersonRepository(FalkorDBRepository[Person]):
    def _node_to_entity(self, node) -> Person:
        return Person(**node.properties)
    def _entity_to_properties(self, entity: Person) -> dict:
        return {"id": entity.id, "name": entity.name}
```

## Plugin lifecycle (optional)

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.db.falkordb import FalkorDBPlugin, FalkorDBSettings

async with ApplicationBootstrap.compose(FalkorDBPlugin(FalkorDBSettings())) as app:
    repo = app.get(Capability.SEARCH).get_repository()
    await repo.create({"id": "1", "name": "Alice"})
    await repo.health()
# shutdown() is called automatically on exit from the `async with` block.
```

`ApplicationBootstrap.compose()` is the recommended entry point for wiring
one or a few ports — no `config=`/`init_timeout=` boilerplate needed. Reach
for a `configure()` subclass only when you need per-port config/timeouts or
conditional registration order, and for `app.registry` (the underlying
`PluginRegistry`) only for what neither tier covers.

## Resilience (optional — requires `openframe-core>=3.4`)

`openframe.core.resilience` ships `CircuitBreakerProxy`/`TracingProxy`, composable
around any repository from the outside — no adapter code changes needed:

```python
from openframe.core.resilience import CircuitBreakerProxy, TracingProxy

repo = CircuitBreakerProxy(
    TracingProxy(plugin.get_repository(), prefix="falkordb"),
    failure_threshold=5,
    reset_timeout=30.0,
)
```

Compose `CircuitBreakerProxy` around the traced repository (not the reverse) so a
short-circuited call never produces a misleading adapter span for a call that
never reached the adapter.

## License

MIT — © Furious Meteors Engineering
