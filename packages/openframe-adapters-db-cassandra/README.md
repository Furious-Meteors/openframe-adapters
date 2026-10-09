# openframe-adapters-db-cassandra

Cassandra database adapter for the **OpenFrame Microservice Suite**.

Part of the `openframe-adapters` monorepo. Implements `BaseRepository[T]`
from `openframe-core` using `cassandra-driver`.

---

## Async strategy

`cassandra-driver` has no native `asyncio` support. This adapter uses a
**hybrid** strategy rather than blindly wrapping every call in
`run_in_executor`:

- **Session creation** (`Cluster(...).connect()`) is genuinely blocking —
  the driver has no async-native connect path — so it is wrapped in
  `loop.run_in_executor(None, ...)`. This cost is paid once per cached
  session, not per query.
- **Every query** is dispatched through `Session.execute_async()`, which
  returns a `cassandra.cluster.ResponseFuture` immediately without
  blocking. Its `add_callbacks()` method is bridged to a real
  `asyncio.Future` via `loop.call_soon_threadsafe()` — no thread-pool
  thread is held for the duration of the query, unlike a naive
  `run_in_executor(None, session.execute, query)` would require.

See `openframe/adapters/db/cassandra/connection.py`'s module docstring for
the full research finding (verified against the actually-installed
`cassandra-driver` 3.30.1) and the honest fallback this adapter would use
if that callback API ever stopped working as documented.

---

## Installation

```bash
pip install openframe-adapters-db-cassandra
```

Required env var:

```
CASSANDRA_CONTACT_POINTS=["10.0.0.1", "10.0.0.2"]
```

---

## Quick start

### Raw dict mode

```python
from openframe.adapters.db.cassandra import CassandraSettings, CassandraRepository

settings = CassandraSettings()  # reads CASSANDRA_CONTACT_POINTS from env
repo = CassandraRepository(settings, table="items", id_column="id")

item = await repo.get("abc-123")          # dict | None
items, total = await repo.list(10, 0)     # ([dict, ...], int)
created = await repo.create({"id": "abc-123", "name": "x"})
updated = await repo.update({"id": "abc-123", "name": "y"})
deleted = await repo.delete("abc-123")    # bool
```

### Typed domain mode

```python
from dataclasses import dataclass
from openframe.adapters.db.cassandra import CassandraSettings, CassandraRepository

@dataclass
class Item:
    id: str
    name: str

class ItemRepository(CassandraRepository[Item]):
    _table = "items"
    _id_column = "id"

    def _row_to_entity(self, row) -> Item:
        return Item(**dict(row._asdict())) if not isinstance(row, dict) else Item(**row)

    def _entity_to_row(self, entity: Item) -> dict:
        return {"id": entity.id, "name": entity.name}

settings = CassandraSettings()
repo = ItemRepository(settings)
item: Item | None = await repo.get("abc-123")
```

Note on CRUD semantics: Cassandra's `INSERT`/`UPDATE`/`DELETE` statements
have no `RETURNING` clause and report no "rows matched" count. `create()`
and `update()` therefore return the entity as submitted (not a
database-generated row), and `delete()` always returns `True` on a
successful statement. See the docstrings in `repository.py` for details.

---

## Wiring into an application

For a real service, wire `CassandraPlugin` (the `BasePort`-satisfying
plugin class) through `ApplicationBootstrap.compose()` from
`openframe-core`. This gives you proper lifecycle management —
`initialize()` / `health()` / `shutdown()` — for free, instead of
constructing `CassandraRepository` directly and managing the session
yourself:

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.db.cassandra import CassandraPlugin, CassandraSettings

settings = CassandraSettings()  # reads CASSANDRA_CONTACT_POINTS from env
plugin = CassandraPlugin(settings, table="items", id_column="id")

async with ApplicationBootstrap.compose(plugin) as app:
    repo = app.get(Capability.PERSISTENCE)   # -> CassandraRepository
    item = await repo.get("abc-123")
# session/cluster is shut down automatically on exit (plugin.shutdown() ran)
```

`compose()` calls `plugin.initialize()` on entry and `plugin.shutdown()` on
exit, so the session is created, health-checked, and torn down without any
manual lifecycle code. Requires `openframe-core>=3.3`.

Reach for a subclassed `ApplicationBootstrap` (with a `configure()` method)
only when you need per-port `config=`/`init_timeout=` or conditional
registration order; use `app.registry` as an escape hatch for anything
neither tier covers. The `CassandraRepository(settings)` construction shown
above under "Quick start" remains valid for tests, scripts, or any context
that doesn't need plugin lifecycle management.

### Optional: circuit breaker

`openframe-core>=3.4` ships `openframe.core.resilience`. Compose
`CircuitBreakerProxy` around the (optionally traced) repository — never the
reverse, so a short-circuited call never produces a misleading adapter span
for a call that never reached the adapter:

```python
from openframe.core.resilience import CircuitBreakerProxy
from openframe.core.telemetry import TracingProxy

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

All settings are read from environment variables.

| Env var | Type | Default | Description |
|---|---|---|---|
| `CASSANDRA_CONTACT_POINTS` | `list[str]` (JSON array) | **required** | Seed node hostnames/IPs |
| `CASSANDRA_PORT` | `int` | `9042` | Native protocol port |
| `CASSANDRA_KEYSPACE` | `str \| None` | `None` | Keyspace to use |
| `CASSANDRA_USERNAME` | `str \| None` | `None` | `PlainTextAuthProvider` username |
| `CASSANDRA_PASSWORD` | `str \| None` | `None` | `PlainTextAuthProvider` password |
| `CASSANDRA_LOCAL_DC` | `str \| None` | `None` | Local datacenter name |
| `CASSANDRA_PROTOCOL_VERSION` | `int \| None` | `None` | Explicit native protocol version |
| `CASSANDRA_CORE_CONNECTIONS_PER_HOST` | `int` | `2` | Connections kept open per host |
| `CONNECTION_TIMEOUT` | `float` | `30.0` | Session creation timeout (s) |
| `OPERATION_TIMEOUT` | `float` | `10.0` | Per-operation timeout (s) |
| `MAX_RETRIES` | `int` | `3` | Max retry attempts |

---

## Exception hierarchy

All exceptions are `AdapterError` subclasses from `openframe.core.exceptions`.
Raw `cassandra-driver` exceptions never escape the adapter.

| Situation | Exception | Retryable |
|---|---|---|
| No contact point reachable (`NoHostAvailable`) | `AdapterConnectionError` | Yes |
| Connection lost mid-query (`ConnectionException`) | `AdapterConnectionError` | Yes |
| Not enough replicas available (`Unavailable`) | `AdapterConnectionError` | Yes |
| Authentication rejected (`AuthenticationFailed`) | `AdapterConnectionError` | **No** |
| Coordinator/client timeout (`OperationTimedOut`/`ReadTimeout`/`WriteTimeout`) | `AdapterTimeoutError` | Yes |
| Malformed CQL (`InvalidRequest`) | `AdapterQueryError` | No |
| Replica explicitly failed (`ReadFailure`/`WriteFailure`) | `AdapterQueryError` | No |
| `CASSANDRA_CONTACT_POINTS` empty | `AdapterConfigurationError` | No |
| Operation exceeded `OPERATION_TIMEOUT` | `AdapterTimeoutError` | Yes |

---

## Development

```bash
# from the package directory
uv venv .venv && source .venv/bin/activate
uv pip install -e ".[dev]"
python -m pytest tests/ -v
```

---

## Protocol conformance

```python
from openframe.core.ports import BaseRepository

repo = CassandraRepository(settings, table="items", id_column="id")
assert isinstance(repo, BaseRepository)   # True — structural check
```

No inheritance from the Protocol is required or used.

---

## License

MIT
