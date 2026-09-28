# openframe-adapters-db-mysql

MySQL database adapter for the **OpenFrame Microservice Suite**.

Part of the `openframe-adapters` monorepo. Implements `BaseRepository[T]` and
`HealthCheck` from `openframe-core` using `aiomysql` (which wraps `PyMySQL`).

---

## Installation

```bash
pip install openframe-adapters-db-mysql
```

Required env var:

```
DATABASE_URL=mysql://user:password@host:3306/dbname
```

---

## Quick start

### Raw dict mode

```python
from openframe.adapters.db.mysql import MySQLSettings, MySQLRepository

settings = MySQLSettings()  # reads DATABASE_URL from env
repo = MySQLRepository(settings, table="items", id_column="id")

item = await repo.get("abc-123")          # dict | None
items, total = await repo.list(10, 0)     # ([dict, ...], int)
created = await repo.create({"name": "x"})
updated = await repo.update({"id": "abc-123", "name": "y"})
deleted = await repo.delete("abc-123")    # bool
```

### Typed domain mode

```python
from dataclasses import dataclass
from openframe.adapters.db.mysql import MySQLSettings, MySQLRepository

@dataclass
class Item:
    id: str
    name: str

class ItemRepository(MySQLRepository[Item]):
    _table = "items"
    _id_column = "id"

    def _row_to_entity(self, row) -> Item:
        return Item(**row)

    def _entity_to_row(self, entity: Item) -> dict:
        return {"id": entity.id, "name": entity.name}

settings = MySQLSettings()
repo = ItemRepository(settings)
item: Item | None = await repo.get("abc-123")
```

---

## Wiring into an application

For a real service, wire `MySQLPlugin` (the `BasePort`-satisfying plugin
class) through `ApplicationBootstrap.compose()` from `openframe-core`. This
gives you proper lifecycle management — `initialize()` / `health()` /
`shutdown()` — for free, instead of constructing `MySQLRepository`
directly and managing the pool yourself:

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.db.mysql import MySQLPlugin, MySQLSettings

settings = MySQLSettings()  # reads DATABASE_URL from env
plugin = MySQLPlugin(settings, table="items", id_column="id")

async with ApplicationBootstrap.compose(plugin) as app:
    repo = app.get(Capability.PERSISTENCE)   # -> MySQLRepository
    item = await repo.get("abc-123")
# pool is closed automatically on exit (plugin.shutdown() ran)
```

`compose()` calls `plugin.initialize()` on entry and `plugin.shutdown()` on
exit, so the pool is created, health-checked, and torn down without any
manual lifecycle code. Requires `openframe-core>=3.3`.

Reach for a subclassed `ApplicationBootstrap` (with a `configure()` method)
only when you need per-port `config=`/`init_timeout=` or conditional
registration order; use `app.registry` as an escape hatch for anything
neither tier covers. The `MySQLRepository(settings)` construction shown
above under "Quick start" remains valid for tests, scripts, or any context
that doesn't need plugin lifecycle management.

---

## Configuration

All settings are read from environment variables.

| Env var | Type | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | `str` | **required** | Full DSN: `mysql://user:pass@host:port/dbname` |
| `POOL_SIZE` | `int` | `10` | Pool minsize/maxsize |
| `POOL_RECYCLE` | `float` | `300.0` | Seconds before a pooled connection is recycled |
| `POOL_CONNECT_TIMEOUT` | `float` | `10.0` | Per-connection TCP connect timeout (s) |
| `CONNECTION_TIMEOUT` | `float` | `30.0` | Pool creation timeout (s) |
| `OPERATION_TIMEOUT` | `float` | `10.0` | Per-operation timeout (s) |
| `MAX_RETRIES` | `int` | `3` | Max retry attempts |

---

## Health checks

`MySQLRepository` implements the `HealthCheck` protocol from `openframe-core`.

```python
alive = await repo.health()   # PluginHealth snapshot — SELECT 1 liveness check
```

`health()` never raises — it returns a `PluginHealth` with `status=FAILED` on
any failure instead.

---

## Exception hierarchy

All exceptions are `AdapterError` subclasses from `openframe.core.exceptions`.
Raw PyMySQL/aiomysql exceptions never escape the adapter.

| Situation | Exception |
|---|---|
| Cannot connect to MySQL (unreachable host, dropped socket, auth failure) | `AdapterConnectionError` |
| `DATABASE_URL` references a non-existent database | `AdapterConfigurationError` |
| Query failed (constraint violation, syntax error, lock-wait timeout) | `AdapterQueryError` |
| Operation exceeded timeout | `AdapterTimeoutError` |

**A note on `pymysql.err.OperationalError`:** PyMySQL raises this single
exception class for both genuine connection failures (error codes
2002/2003/2006/2013 — unreachable host, dropped socket, server gone away)
*and* purely in-band query failures such as error code 1205 ("Lock wait
timeout exceeded"). The adapter inspects the numeric MySQL error code
(`exc.args[0]`) to classify each occurrence correctly instead of trusting
the exception's Python type alone — see `repository.py`'s `_wrap_pymysql()`
and `connection.py`'s module docstring for the full code list.

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

repo = MySQLRepository(settings, table="items", id_column="id")
assert isinstance(repo, BaseRepository)   # True — structural check
assert isinstance(repo, HealthCheck)      # True — structural check
```

No inheritance from either Protocol is required or used.

---

## Resilience (optional)

`openframe-core>=3.4` ships `openframe.core.resilience`. Wrap the repository
from the outside — exactly like `TracingProxy` — with no adapter code
changes required:

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

Compose the circuit breaker around the traced repository (not the reverse)
so a short-circuited call never produces a misleading adapter span for a
call that never reached the adapter.

---

## License

MIT
