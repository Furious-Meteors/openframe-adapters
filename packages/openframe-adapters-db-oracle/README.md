# openframe-adapters-db-oracle

Oracle database adapter for the **OpenFrame Microservice Suite**.

Part of the `openframe-adapters` monorepo. Implements `BaseRepository[T]`
from `openframe-core` using `python-oracledb`'s native async ("thin mode")
API.

---

## Async strategy — read this first

Unlike `cassandra-driver` (which has no async API at all and will need an
`asyncio.get_running_loop().run_in_executor(None, sync_fn)` wrapper when
built), `python-oracledb` **does** ship a real, documented native-async
surface in thin mode — no Oracle Client libraries required:
`oracledb.connect_async()`, `oracledb.create_pool_async()`,
`oracledb.AsyncConnection`, `oracledb.AsyncConnectionPool`,
`oracledb.AsyncCursor`.

This was verified directly against a freshly installed `oracledb==26.0.1`
(not assumed from documentation alone) before writing a single line of this
adapter — see `openframe/adapters/db/oracle/connection.py`'s module
docstring for the full investigation notes, including a real gotcha found
along the way: a DNS resolution failure while connecting escapes as a raw
`socket.gaierror` rather than an `oracledb.Error`, so this adapter catches
`OSError` explicitly at every connection boundary.

Because the native-async surface genuinely exists and works, this adapter
is built **native-async**, matching the shape of
`openframe-adapters-db-postgres`/`-mysql` — not the executor-wrapped shape
`-cassandra` will need. Per ADR-003, this decision is made per-adapter
based on actual driver support, not forced one way across the ecosystem.

---

## Installation

```bash
pip install openframe-adapters-db-oracle
```

Required env var:

```
ORACLE_DSN=app/secret@db.example.com:1521/orclpdb
```

`ORACLE_DSN` matches the format `oracledb.connect_async()`'s own `dsn`
parameter documents: `user/password@host:port/service_name`. A bare
connect descriptor without embedded credentials also works if you keep
credentials elsewhere.

---

## Quick start

### Raw dict mode

```python
from openframe.adapters.db.oracle import OracleSettings, OracleRepository

settings = OracleSettings()  # reads ORACLE_DSN from env
repo = OracleRepository(settings, table="items", id_column="id")

item = await repo.get("abc-123")          # dict | None
items, total = await repo.list(10, 0)     # ([dict, ...], int)
created = await repo.create({"id": "1", "name": "x"})
updated = await repo.update({"id": "1", "name": "y"})
deleted = await repo.delete("1")          # bool
```

### Typed domain mode

```python
from dataclasses import dataclass
from openframe.adapters.db.oracle import OracleSettings, OracleRepository

@dataclass
class Item:
    id: str
    name: str

class ItemRepository(OracleRepository[Item]):
    _table = "items"
    _id_column = "id"

    def _row_to_entity(self, row: dict) -> Item:
        return Item(**row)

    def _entity_to_row(self, entity: Item) -> dict:
        return {"id": entity.id, "name": entity.name}

settings = OracleSettings()
repo = ItemRepository(settings)
item: Item | None = await repo.get("abc-123")
```

---

## Wiring into an application

For a real service, wire `OraclePlugin` (the `BasePort`-satisfying plugin
class) through `ApplicationBootstrap.compose()` from `openframe-core`. This
gives you proper lifecycle management — `initialize()` / `health()` /
`shutdown()` — for free, instead of constructing `OracleRepository`
directly and managing the pool yourself:

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.db.oracle import OraclePlugin, OracleSettings

settings = OracleSettings()  # reads ORACLE_DSN from env
plugin = OraclePlugin(settings, table="items", id_column="id")

async with ApplicationBootstrap.compose(plugin) as app:
    repo = app.get(Capability.PERSISTENCE)   # -> OracleRepository
    item = await repo.get("abc-123")
# pool is closed automatically on exit (plugin.shutdown() ran)
```

`compose()` calls `plugin.initialize()` on entry and `plugin.shutdown()` on
exit, so the pool is created, health-checked, and torn down without any
manual lifecycle code. Requires `openframe-core>=3.3`.

Reach for a subclassed `ApplicationBootstrap` (with a `configure()` method)
only when you need per-port `config=`/`init_timeout=` or conditional
registration order; use `app.registry` as an escape hatch for anything
neither tier covers. The `OracleRepository(settings)` construction shown
above under "Quick start" remains valid for tests, scripts, or any context
that doesn't need plugin lifecycle management.

### Resilience — circuit breaking under sustained failure

`openframe-core>=3.4` ships `openframe.core.resilience.CircuitBreakerProxy`
— wrap a repository to short-circuit calls after repeated failures instead
of blocking every caller until `operation_timeout` during a sustained
outage:

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
reaches the adapter, so it shouldn't produce a misleading adapter span. No
adapter code changes to support this — `CircuitBreakerProxy` wraps from the
outside, exactly like `TracingProxy`.

---

## Configuration

All settings are read from environment variables.

| Env var | Type | Default | Description |
|---|---|---|---|
| `ORACLE_DSN` | `str` | **required** | `user/password@host:port/service_name` |
| `POOL_MIN` | `int` | `1` | Minimum pool size |
| `POOL_MAX` | `int` | `10` | Maximum pool size |
| `POOL_INCREMENT` | `int` | `1` | Connections opened per pool growth step |
| `POOL_TIMEOUT` | `int` | `60` | Seconds an idle pooled connection may sit before being closed |
| `CONNECTION_TIMEOUT` | `float` | `30.0` | Pool creation timeout (s) |
| `OPERATION_TIMEOUT` | `float` | `10.0` | Per-operation timeout (s) |
| `MAX_RETRIES` | `int` | `3` | Max retry attempts |

---

## Health checks

`OracleRepository` implements the unified `BasePort` lifecycle from
`openframe-core`.

```python
health = await repo.health()   # PluginHealth — never raises
```

---

## Exception hierarchy

All exceptions are `AdapterError` subclasses from `openframe.core.exceptions`.
Raw `oracledb` exceptions (and the raw `OSError`/`socket.gaierror` a DNS
failure can raise — see "Async strategy" above) never escape the adapter.

| Situation | Exception |
|---|---|
| Cannot connect to Oracle (host unreachable, listener refused, DNS failure) | `AdapterConnectionError` |
| `ORACLE_DSN` is syntactically invalid | `AdapterConfigurationError` |
| Query failed (constraint, syntax, etc.) | `AdapterQueryError` |
| Connection lost mid-query (ORA-03113, ORA-03114, ORA-12541, ORA-12154, etc.) | `AdapterConnectionError` |
| Operation exceeded timeout | `AdapterTimeoutError` |

See `openframe/adapters/db/oracle/repository.py`'s module-level comment for
the exact ORA/DPY code classification, including which codes were verified
against the installed driver and which are documented-but-unverified
standard Oracle networking codes (no live Oracle server was available
during development — this is stated honestly rather than implied to be
fully verified).

---

## Development

```bash
# from the package directory
uv venv .venv && source .venv/bin/activate
uv pip install -e ".[dev]"
python -m pytest tests/ -q
```

All tests run with zero real network calls — `oracledb` is fully mocked.

---

## Protocol conformance

```python
from openframe.core.ports import BaseRepository

repo = OracleRepository(settings, table="items", id_column="id")
assert isinstance(repo, BaseRepository)   # True — structural check
```

No inheritance from the Protocol is required or used.

---

## License

MIT
