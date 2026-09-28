# openframe-adapters-db-postgres

PostgreSQL database adapter for the **OpenFrame Microservice Suite**.

Part of the `openframe-adapters` monorepo. Implements `BaseRepository[T]` and
`HealthCheck` from `openframe-core` using `asyncpg`.

---

## Installation

```bash
pip install openframe-adapters-db-postgres
```

Required env var:

```
DATABASE_URL=postgresql://user:password@host:5432/dbname
```

---

## Quick start

### Raw dict mode

```python
from openframe.adapters.db.postgres import PostgresSettings, PostgresRepository

settings = PostgresSettings()  # reads DATABASE_URL from env
repo = PostgresRepository(settings, table="items", id_column="id")

item = await repo.get("abc-123")          # dict | None
items, total = await repo.list(10, 0)     # ([dict, ...], int)
created = await repo.create({"name": "x"})
updated = await repo.update({"id": "abc-123", "name": "y"})
deleted = await repo.delete("abc-123")    # bool
```

### Typed domain mode

```python
from dataclasses import dataclass
from openframe.adapters.db.postgres import PostgresSettings, PostgresRepository

@dataclass
class Item:
    id: str
    name: str

class ItemRepository(PostgresRepository[Item]):
    _table = "items"
    _id_column = "id"

    def _row_to_entity(self, row) -> Item:
        return Item(**dict(row))

    def _entity_to_row(self, entity: Item) -> dict:
        return {"id": entity.id, "name": entity.name}

settings = PostgresSettings()
repo = ItemRepository(settings)
item: Item | None = await repo.get("abc-123")
```

---

## Wiring into an application

For a real service, wire `PostgresPlugin` (the `BasePort`-satisfying plugin
class) through `ApplicationBootstrap.compose()` from `openframe-core`. This
gives you proper lifecycle management — `initialize()` / `health()` /
`shutdown()` — for free, instead of constructing `PostgresRepository`
directly and managing the pool yourself:

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.db.postgres import PostgresPlugin, PostgresSettings

settings = PostgresSettings()  # reads DATABASE_URL from env
plugin = PostgresPlugin(settings, table="items", id_column="id")

async with ApplicationBootstrap.compose(plugin) as app:
    repo = app.get(Capability.PERSISTENCE)   # -> PostgresRepository
    item = await repo.get("abc-123")
# pool is closed automatically on exit (plugin.shutdown() ran)
```

`compose()` calls `plugin.initialize()` on entry and `plugin.shutdown()` on
exit, so the pool is created, health-checked, and torn down without any
manual lifecycle code. Requires `openframe-core>=3.3`.

Reach for a subclassed `ApplicationBootstrap` (with a `configure()` method)
only when you need per-port `config=`/`init_timeout=` or conditional
registration order; use `app.registry` as an escape hatch for anything
neither tier covers. The `PostgresRepository(settings)` construction shown
above under "Quick start" remains valid for tests, scripts, or any context
that doesn't need plugin lifecycle management.

---

## Configuration

All settings are read from environment variables.

| Env var | Type | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | `str` | **required** | Full asyncpg DSN |
| `POOL_SIZE` | `int` | `10` | Pool min/max size |
| `POOL_MAX_INACTIVE_CONN_LIFETIME` | `float` | `300.0` | Idle connection TTL (s) |
| `POOL_COMMAND_TIMEOUT` | `float` | `60.0` | Per-statement timeout (s) |
| `POOL_MAX_QUERIES` | `int` | `50000` | Queries per connection before recycle |
| `CONNECTION_TIMEOUT` | `float` | `30.0` | Pool creation timeout (s) |
| `OPERATION_TIMEOUT` | `float` | `10.0` | Per-operation timeout (s) |
| `MAX_RETRIES` | `int` | `3` | Max retry attempts |

---

## Health checks

`PostgresRepository` implements the `HealthCheck` protocol from `openframe-core`.

```python
alive = await repo.ping()       # SELECT 1 — fast liveness check
ready = await repo.is_ready()   # pg_tables query — full readiness check
```

Both methods return `False` on any failure and never raise.

---

## Exception hierarchy

All exceptions are `AdapterError` subclasses from `openframe.core.exceptions`.
Raw `asyncpg` exceptions never escape the adapter.

| Situation | Exception |
|---|---|
| Cannot connect to Postgres | `AdapterConnectionError` |
| Invalid `DATABASE_URL` catalog | `AdapterConfigurationError` |
| Query failed (constraint, syntax, etc.) | `AdapterQueryError` |
| Entity not found | `AdapterNotFoundError` |
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

repo = PostgresRepository(settings, table="items", id_column="id")
assert isinstance(repo, BaseRepository)   # True — structural check
assert isinstance(repo, HealthCheck)      # True — structural check
```

No inheritance from either Protocol is required or used.

---

## License

MIT
