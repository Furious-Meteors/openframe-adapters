# openframe-adapters-db-mariadb

MariaDB database adapter for the **OpenFrame Microservice Suite**.

Part of the `openframe-adapters` monorepo. Implements `BaseRepository[T]` and
`HealthCheck` from `openframe-core` using `aiomysql` (which wraps `PyMySQL`)
— the same driver used by `openframe-adapters-db-mysql`, since MariaDB is
wire-protocol-compatible with MySQL and there is no separate MariaDB Python
driver in play here.

---

## Installation

```bash
pip install openframe-adapters-db-mariadb
```

Required env var:

```
DATABASE_URL=mysql://user:password@host:3306/dbname
```

Note the DSN scheme is `mysql://`, not `mariadb://` — `aiomysql`/`PyMySQL`
don't define a separate scheme, since they speak the shared wire protocol.
See "Configuration" below for why this package keeps the `DATABASE_URL`
field name rather than introducing a MariaDB-specific one.

---

## Quick start

### Raw dict mode

```python
from openframe.adapters.db.mariadb import MariadbSettings, MariadbRepository

settings = MariadbSettings()  # reads DATABASE_URL from env
repo = MariadbRepository(settings, table="items", id_column="id")

item = await repo.get("abc-123")          # dict | None
items, total = await repo.list(10, 0)     # ([dict, ...], int)
created = await repo.create({"name": "x"})
updated = await repo.update({"id": "abc-123", "name": "y"})
deleted = await repo.delete("abc-123")    # bool
```

### Typed domain mode

```python
from dataclasses import dataclass
from openframe.adapters.db.mariadb import MariadbSettings, MariadbRepository

@dataclass
class Item:
    id: str
    name: str

class ItemRepository(MariadbRepository[Item]):
    _table = "items"
    _id_column = "id"

    def _row_to_entity(self, row) -> Item:
        return Item(**row)

    def _entity_to_row(self, entity: Item) -> dict:
        return {"id": entity.id, "name": entity.name}

settings = MariadbSettings()
repo = ItemRepository(settings)
item: Item | None = await repo.get("abc-123")
```

---

## Wiring into an application

For a real service, wire `MariadbPlugin` (the `BasePort`-satisfying plugin
class) through `ApplicationBootstrap.compose()` from `openframe-core`. This
gives you proper lifecycle management — `initialize()` / `health()` /
`shutdown()` — for free, instead of constructing `MariadbRepository`
directly and managing the pool yourself:

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.db.mariadb import MariadbPlugin, MariadbSettings

settings = MariadbSettings()  # reads DATABASE_URL from env
plugin = MariadbPlugin(settings, table="items", id_column="id")

async with ApplicationBootstrap.compose(plugin) as app:
    repo = app.get(Capability.PERSISTENCE)   # -> MariadbRepository
    item = await repo.get("abc-123")
# pool is closed automatically on exit (plugin.shutdown() ran)
```

`compose()` calls `plugin.initialize()` on entry and `plugin.shutdown()` on
exit, so the pool is created, health-checked, and torn down without any
manual lifecycle code. Requires `openframe-core>=3.3`.

Reach for a subclassed `ApplicationBootstrap` (with a `configure()` method)
only when you need per-port `config=`/`init_timeout=` or conditional
registration order; use `app.registry` as an escape hatch for anything
neither tier covers. The `MariadbRepository(settings)` construction shown
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

### Field-naming decision: `database_url`, not `mariadb_url`

This package intentionally reuses the `database_url` field name (and
`DATABASE_URL` env var) from `openframe-adapters-db-mysql` rather than
introducing a MariaDB-specific `mariadb_url`. Rationale:

- The DSN shape is identical between the two packages
  (`mysql://user:pass@host:port/dbname` — `aiomysql`/`PyMySQL` have no
  separate `mariadb://` scheme).
- Exactly one of `openframe-adapters-db-mysql` / `openframe-adapters-db-mariadb`
  is installed per service, so there's no ambiguity about which backend
  `DATABASE_URL` targets within a given deployment.
- Keeping the field name identical means a service can migrate between a
  MySQL server and a MariaDB server (or vice versa) by swapping the
  installed adapter package and import — zero environment/config changes.

### Authentication plugin differences

Recent MySQL server versions default to the `caching_sha2_password`
authentication plugin. MariaDB servers have historically defaulted to
`mysql_native_password`, or to MariaDB-specific plugins such as
`ed25519`/`unix_socket`. This adapter doesn't special-case authentication —
`aiomysql`/`PyMySQL` negotiate whichever plugin the server offers for the
connecting user — but if you hit an auth-negotiation failure against a
MariaDB server, check the server's (or the connecting user's)
authentication plugin configuration before assuming the `DATABASE_URL`
itself is wrong.

---

## Health checks

`MariadbRepository` implements the `HealthCheck` protocol from
`openframe-core`.

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
| Cannot connect to MariaDB (unreachable host, dropped socket, auth failure) | `AdapterConnectionError` |
| `DATABASE_URL` references a non-existent database | `AdapterConfigurationError` |
| Query failed (constraint violation, syntax error, lock-wait timeout) | `AdapterQueryError` |
| Operation exceeded timeout | `AdapterTimeoutError` |

**A note on `pymysql.err.OperationalError`:** PyMySQL raises this single
exception class for both genuine connection failures (error codes
2002/2003/2006/2013 — unreachable host, dropped socket, server gone away)
*and* purely in-band query failures such as error code 1205 ("Lock wait
timeout exceeded"). The adapter inspects the numeric error code
(`exc.args[0]`) to classify each occurrence correctly instead of trusting
the exception's Python type alone — see `repository.py`'s `_wrap_pymysql()`
and `connection.py`'s module docstring for the full code list.

**MySQL/MariaDB error-code parity:** MariaDB and MySQL trace back to a
shared early codebase and a largely-compatible wire protocol, so the
specific codes this adapter classifies on (2002/2003/2006/2013/1045/1049/
1205) are pre-fork, universally-shared codes and behave identically against
both server families in the adapter's own testing. This is **not** a
guarantee of permanent 100% parity, though: MySQL versions released after
the MySQL/MariaDB fork have introduced their own new error codes that may
not exist identically on a MariaDB server, and MariaDB has its own
extensions with no MySQL equivalent (its own storage engines such as Aria,
and MariaDB-specific system variables/error conditions). If you extend this
adapter's classification logic to cover additional error codes, verify their
meaning against the actual server family you're targeting.

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

repo = MariadbRepository(settings, table="items", id_column="id")
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
