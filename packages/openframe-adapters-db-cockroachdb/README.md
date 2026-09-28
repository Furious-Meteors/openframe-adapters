# openframe-adapters-db-cockroachdb

CockroachDB database adapter for the **OpenFrame Microservice Suite**.

Part of the `openframe-adapters` monorepo. Implements `BaseRepository[T]` and
`HealthCheck` from `openframe-core` using `asyncpg` — the same driver used by
`openframe-adapters-db-postgres`, because CockroachDB speaks the PostgreSQL
wire protocol. This package is an adaptation of the Postgres adapter, not a
from-scratch build; see "CockroachDB vs. Postgres differences" below for
everything that is genuinely different.

---

## Installation

```bash
pip install openframe-adapters-db-cockroachdb
```

Required env var:

```
COCKROACHDB_URL=postgresql://user:password@host:26257/dbname
```

Note the scheme is still `postgresql://` — asyncpg only speaks the wire
protocol, it has no notion of "CockroachDB" as a distinct backend. Point the
URL at your CockroachDB node or load balancer exactly as you would a
Postgres primary.

---

## Quick start

### Raw dict mode

```python
from openframe.adapters.db.cockroachdb import CockroachdbSettings, CockroachdbRepository

settings = CockroachdbSettings()  # reads COCKROACHDB_URL from env
repo = CockroachdbRepository(settings, table="items", id_column="id")

item = await repo.get("abc-123")          # dict | None
items, total = await repo.list(10, 0)     # ([dict, ...], int)
created = await repo.create({"name": "x"})
updated = await repo.update({"id": "abc-123", "name": "y"})
deleted = await repo.delete("abc-123")    # bool
```

### Typed domain mode

```python
from dataclasses import dataclass
from openframe.adapters.db.cockroachdb import CockroachdbSettings, CockroachdbRepository

@dataclass
class Item:
    id: str
    name: str

class ItemRepository(CockroachdbRepository[Item]):
    _table = "items"
    _id_column = "id"

    def _row_to_entity(self, row) -> Item:
        return Item(**dict(row))

    def _entity_to_row(self, entity: Item) -> dict:
        return {"id": entity.id, "name": entity.name}

settings = CockroachdbSettings()
repo = ItemRepository(settings)
item: Item | None = await repo.get("abc-123")
```

---

## Wiring into an application

For a real service, wire `CockroachdbPlugin` (the `BasePort`-satisfying
plugin class) through `ApplicationBootstrap.compose()` from `openframe-core`.
This gives you proper lifecycle management — `initialize()` / `health()` /
`shutdown()` — for free, instead of constructing `CockroachdbRepository`
directly and managing the pool yourself:

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.db.cockroachdb import CockroachdbPlugin, CockroachdbSettings

settings = CockroachdbSettings()  # reads COCKROACHDB_URL from env
plugin = CockroachdbPlugin(settings, table="items", id_column="id")

async with ApplicationBootstrap.compose(plugin) as app:
    repo = app.get(Capability.PERSISTENCE)   # -> CockroachdbRepository
    item = await repo.get("abc-123")
# pool is closed automatically on exit (plugin.shutdown() ran)
```

`compose()` calls `plugin.initialize()` on entry and `plugin.shutdown()` on
exit, so the pool is created, health-checked, and torn down without any
manual lifecycle code. Requires `openframe-core>=3.3`.

Reach for a subclassed `ApplicationBootstrap` (with a `configure()` method)
only when you need per-port `config=`/`init_timeout=` or conditional
registration order; use `app.registry` as an escape hatch for anything
neither tier covers. The `CockroachdbRepository(settings)` construction
shown above under "Quick start" remains valid for tests, scripts, or any
context that doesn't need plugin lifecycle management.

---

## Raw driver access for niche features

The adapter never hides asyncpg. Access it directly for anything the port
does not cover:

```python
from openframe.adapters.db.cockroachdb import get_cockroachdb_pool

class OrderRepository(CockroachdbRepository[Order]):
    _table     = "orders"
    _id_column = "id"

    async def bulk_upsert(self, orders: list[Order]) -> None:
        pool = await get_cockroachdb_pool(self._settings)
        async with pool.acquire() as conn:
            async with conn.transaction():
                for o in orders:
                    await conn.execute(
                        "UPSERT INTO orders (id, total) VALUES ($1, $2)",
                        o.id, o.total,
                    )
```

---

## CockroachDB vs. Postgres differences

CockroachDB speaks the same wire protocol as Postgres, so connection
pooling, the asyncpg exception hierarchy used for connection-vs-query
classification, and health checks (`SELECT 1`) are all identical to the
Postgres adapter. Two real differences matter at the schema/transaction
boundary:

### No `SERIAL`/`BIGSERIAL`

CockroachDB does not implement Postgres's `SERIAL`/`BIGSERIAL`
auto-increment column types the same way. If your schema uses `SERIAL`
against a real CockroachDB cluster, it likely will not behave as you
expect. The idiomatic CockroachDB primary-key patterns are:

```sql
-- UUID primary key (recommended default)
id UUID PRIMARY KEY DEFAULT gen_random_uuid()

-- or, if you want an integer key
id INT PRIMARY KEY DEFAULT unique_rowid()
```

This adapter does not translate or rewrite DDL — it only issues the DML
your repository methods construct against whatever schema already exists.
Design your `CREATE TABLE` statements with CockroachDB's own primary-key
conventions, not a copy-pasted Postgres schema.

### CockroachDB transaction retries (SQLSTATE 40001)

CockroachDB always runs at `SERIALIZABLE` isolation (there is no lower
isolation level to opt into). Under contention this can produce a
retryable "transaction retry error" — SQLSTATE `40001`, with
`restart transaction` in the error message — that requires the **client**
to retry the *entire* transaction, not just the failing statement.

This adapter's basic CRUD methods (`get`/`list`/`create`/`update`/`delete`)
each execute as an implicit single-statement transaction, so this class of
error is rare in practice for simple single-statement operations — there is
no multi-statement transaction for CockroachDB to need to restart.

However, if you use this package's "raw driver access" pattern (above) to
run your **own** explicit multi-statement transaction via
`conn.transaction()`, you are responsible for retrying it on SQLSTATE
`40001`:

```python
import asyncpg

async def transfer_funds(pool, from_id: str, to_id: str, amount: int) -> None:
    for attempt in range(5):
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
                        amount, from_id,
                    )
                    await conn.execute(
                        "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
                        amount, to_id,
                    )
            return
        except asyncpg.PostgresError as exc:
            if getattr(exc, "sqlstate", None) == "40001":
                continue  # retry the whole transaction
            raise
    raise RuntimeError("transfer_funds: exhausted retries on SQLSTATE 40001")
```

This adapter deliberately does **not** implement automatic retry inside its
own CRUD methods — that would be a surprising, undocumented behavior change
from how the Postgres adapter behaves for the same driver calls. The
difference is documented here so callers doing their own multi-statement
transactions know it exists and can implement the retry loop themselves.

Everything else about this adapter — connection pooling, asyncpg exception
classification, health checks — is unchanged from the Postgres adapter.

---

## Configuration

All settings are read from environment variables.

| Env var | Type | Default | Description |
|---|---|---|---|
| `COCKROACHDB_URL` | `str` | **required** | Full asyncpg DSN (`postgresql://` scheme) pointed at a CockroachDB cluster |
| `POOL_SIZE` | `int` | `10` | Pool min/max size |
| `POOL_MAX_INACTIVE_CONN_LIFETIME` | `float` | `300.0` | Idle connection TTL (s) |
| `POOL_COMMAND_TIMEOUT` | `float` | `60.0` | Per-statement timeout (s) |
| `POOL_MAX_QUERIES` | `int` | `50000` | Queries per connection before recycle |
| `CONNECTION_TIMEOUT` | `float` | `30.0` | Pool creation timeout (s) |
| `OPERATION_TIMEOUT` | `float` | `10.0` | Per-operation timeout (s) |
| `MAX_RETRIES` | `int` | `3` | Max retry attempts |

---

## Health checks

`CockroachdbRepository` implements the `HealthCheck` protocol from `openframe-core`.

```python
health = await repo.health()   # PluginHealth snapshot — the sole health check, never raises
```

---

## Exception hierarchy

All exceptions are `AdapterError` subclasses from `openframe.core.exceptions`.
Raw `asyncpg` exceptions never escape the adapter.

| Situation | Exception |
|---|---|
| Cannot connect to CockroachDB | `AdapterConnectionError` |
| Invalid `COCKROACHDB_URL` catalog | `AdapterConfigurationError` |
| Query failed (constraint, syntax, SQLSTATE 40001, etc.) | `AdapterQueryError` |
| Entity not found | `AdapterNotFoundError` |
| Operation exceeded timeout | `AdapterTimeoutError` |

A CockroachDB SQLSTATE `40001` transaction-retry error surfaces as
`AdapterQueryError` like any other in-band query failure — see "CockroachDB
transaction retries" above for why this adapter does not retry it
automatically, and how to retry it yourself for explicit multi-statement
transactions.

---

## Development

```bash
# from the package directory
pip install -e ".[dev]"
python -m pytest tests/ -v
```

---

## Protocol conformance

```python
from openframe.core.ports import BaseRepository
from openframe.core.health import HealthCheck

repo = CockroachdbRepository(settings, table="items", id_column="id")
assert isinstance(repo, BaseRepository)   # True — structural check
assert isinstance(repo, HealthCheck)      # True — structural check
```

No inheritance from either Protocol is required or used.

---

## Resilience — circuit breaking under sustained failure

`openframe-core>=3.4` ships `openframe.core.resilience.CircuitBreakerProxy` —
wrap the repository to short-circuit calls after repeated failures instead
of blocking every caller until `operation_timeout` during a sustained
outage. No adapter code changes are needed to support this — it composes
from the outside exactly like `TracingProxy`:

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

## License

MIT
