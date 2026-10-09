# openframe-adapters-db-dynamodb

DynamoDB database adapter for the **OpenFrame Microservice Suite**.

Part of the `openframe-adapters` monorepo. Implements `BaseRepository[T]` and
`HealthCheck` from `openframe-core` using `aioboto3` (which wraps
`boto3`/`botocore` with genuine `aiohttp`-backed async I/O via
`aiobotocore` — see "A note on async style" below).

---

## Installation

```bash
pip install openframe-adapters-db-dynamodb
```

Required env vars:

```
AWS_REGION=us-east-1
DYNAMODB_TABLE_NAME=items
```

Optional, for local development against a local DynamoDB process instead of
real AWS:

```
ENDPOINT_URL=http://localhost:8000
AWS_ACCESS_KEY_ID=local
AWS_SECRET_ACCESS_KEY=local
```

---

## Quick start

### Raw dict mode

```python
from openframe.adapters.db.dynamodb import DynamoDBSettings, DynamoDBRepository

settings = DynamoDBSettings()  # reads AWS_REGION / DYNAMODB_TABLE_NAME from env
repo = DynamoDBRepository(settings, id_column="id")

item = await repo.get("abc-123")          # dict | None
items, total = await repo.list(10, 0)     # ([dict, ...], int)
created = await repo.create({"id": "abc-123", "name": "x"})
updated = await repo.update({"id": "abc-123", "name": "y"})
deleted = await repo.delete("abc-123")    # bool
```

### Typed domain mode

```python
from dataclasses import dataclass
from openframe.adapters.db.dynamodb import DynamoDBSettings, DynamoDBRepository

@dataclass
class Item:
    id: str
    name: str

class ItemRepository(DynamoDBRepository[Item]):
    _id_column = "id"

    def _row_to_entity(self, row) -> Item:
        return Item(**row)

    def _entity_to_row(self, entity: Item) -> dict:
        return {"id": entity.id, "name": entity.name}

settings = DynamoDBSettings()
repo = ItemRepository(settings)
item: Item | None = await repo.get("abc-123")
```

---

## Wiring into an application

For a real service, wire `DynamoDBPlugin` (the `BasePort`-satisfying plugin
class) through `ApplicationBootstrap.compose()` from `openframe-core`. This
gives you proper lifecycle management — `initialize()` / `health()` /
`shutdown()` — for free, instead of constructing `DynamoDBRepository`
directly and managing the resource yourself:

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.db.dynamodb import DynamoDBPlugin, DynamoDBSettings

settings = DynamoDBSettings()  # reads AWS_REGION / DYNAMODB_TABLE_NAME from env
plugin = DynamoDBPlugin(settings, id_column="id")

async with ApplicationBootstrap.compose(plugin) as app:
    repo = app.get(Capability.PERSISTENCE)   # -> DynamoDBRepository
    item = await repo.get("abc-123")
# resource is closed automatically on exit (plugin.shutdown() ran)
```

`compose()` calls `plugin.initialize()` on entry and `plugin.shutdown()` on
exit, so the resource is created, health-checked, and torn down without any
manual lifecycle code. Requires `openframe-core>=3.3`.

Reach for a subclassed `ApplicationBootstrap` (with a `configure()` method)
only when you need per-port `config=`/`init_timeout=` or conditional
registration order; use `app.registry` as an escape hatch for anything
neither tier covers. The `DynamoDBRepository(settings)` construction shown
above under "Quick start" remains valid for tests, scripts, or any context
that doesn't need plugin lifecycle management.

---

## Configuration

All settings are read from environment variables.

| Env var | Type | Default | Description |
|---|---|---|---|
| `AWS_REGION` | `str` | **required** | AWS region, e.g. `us-east-1` |
| `DYNAMODB_TABLE_NAME` | `str` | **required** | DynamoDB table name |
| `ENDPOINT_URL` | `str \| None` | `None` | Override endpoint, e.g. `http://localhost:8000` for local DynamoDB |
| `AWS_ACCESS_KEY_ID` | `str \| None` | `None` | Explicit access key; omit to use the standard AWS credential chain |
| `AWS_SECRET_ACCESS_KEY` | `str \| None` | `None` | Explicit secret key |
| `AWS_SESSION_TOKEN` | `str \| None` | `None` | Explicit session token (temporary creds) |
| `CONNECTION_TIMEOUT` | `float` | `30.0` | Resource creation timeout (s) |
| `OPERATION_TIMEOUT` | `float` | `10.0` | Per-operation timeout (s) |
| `MAX_RETRIES` | `int` | `3` | Max retry attempts |

---

## A note on async style, and what "connection caching" means here

`aioboto3` is classified as a **wrapper**-style driver in this ecosystem's
driver taxonomy (as opposed to natively-async drivers like `asyncpg`, or
executor-style drivers that thread-wrap a blocking C library). That
classification is about the *shape* of the API, not about whether the I/O
is real: under the hood, `aioboto3` delegates to `aiobotocore`, which
replaces botocore's blocking `urllib3` HTTP stack with genuine
`aiohttp`-backed async I/O (see `aiobotocore.httpsession.AIOHTTPSession`,
verified against the installed package). Calls made through this adapter
are not thread-wrapped synchronous `boto3` calls — they are real
non-blocking network requests.

DynamoDB itself, unlike Postgres/MySQL, has no concept of a persistent TCP
connection pool — it's a managed, stateless, HTTP-based AWS service. This
package's `connection.py` still caches something per settings
(`get_dynamodb_table()` / `_table_cache`), but what it caches is a
**session/resource/Table object**, not a connection pool. Entering
`aioboto3.Session().resource("dynamodb", ...)` sets up an internal
`aiohttp.ClientSession` but performs no network call by itself — the first
actual round-trip happens on the first real operation. The cache exists to
avoid re-creating that session and re-resolving credentials on every call,
not to bound concurrent connections the way a Postgres pool's `pool_size`
does; aiohttp's own connector handles concurrent-request pooling
transparently underneath the single cached `Table` object.

---

## Why the resource interface, not the client interface

`aioboto3` exposes DynamoDB through two interfaces: the low-level
**client** (`session.client("dynamodb", ...)`), whose methods require
building DynamoDB's verbose `{"S": "value"}`-style attribute-value maps by
hand, and the higher-level **resource** (`session.resource("dynamodb",
...)`), whose `Table` object exposes `get_item`/`put_item`/`delete_item`/
`query`/`scan` working directly with plain Python dicts via boto3's
built-in type serializer/deserializer. This package uses the resource
interface's `Table` object exclusively — it maps directly onto
`BaseRepository[T]`'s plain-dict CRUD shape, with no manual
attribute-value marshalling required.

---

## DynamoDB-specific repository semantics

- **`list(limit, offset)`**: DynamoDB has no native integer offset — scans
  paginate via an opaque `LastEvaluatedKey`, not a skip count. This
  implementation scans the full table (honouring the `(limit, offset)`
  contract exactly) and discards the first `offset` items in Python. Correct,
  but its cost scales with `offset + limit` items scanned — not a substitute
  for DynamoDB's own key-based pagination in a latency-sensitive path.
- **`update(entity)`**: a plain `put_item` would silently *create* a new
  item if the key doesn't exist. To honour `BaseRepository`'s "returns
  `None` for a missing entity" contract, `update()` issues a conditional
  `put_item` (`ConditionExpression="attribute_exists(...)"`) and translates
  a `ConditionalCheckFailedException` into `None` instead of raising.
- **`create(entity)`**: `put_item` has no response body, and DynamoDB has
  no auto-increment equivalent, so `create()` returns the entity exactly as
  passed in rather than re-fetching it.

---

## Health checks

`DynamoDBRepository` implements the `HealthCheck` protocol from
`openframe-core`.

```python
alive = await repo.health()   # PluginHealth snapshot -- describe_table liveness check
```

`health()` never raises — it returns a `PluginHealth` with `status=FAILED` on
any failure instead.

---

## Exception hierarchy

All exceptions are `AdapterError` subclasses from `openframe.core.exceptions`.
Raw `botocore`/`aioboto3` exceptions never escape the adapter.

| Situation | DynamoDB error code | Exception |
|---|---|---|
| Cannot reach DynamoDB (network-level, request never sent) | `EndpointConnectionError`/`ConnectionError` | `AdapterConnectionError` |
| Request throttled / capacity exceeded (transient) | `ProvisionedThroughputExceededException`, `ThrottlingException`, `RequestLimitExceeded` | `AdapterConnectionError` (retryable) |
| Missing/invalid region or credentials | `NoCredentialsError`/`NoRegionError`/`PartialCredentialsError` | `AdapterConfigurationError` |
| Bad input / table doesn't exist / other service rejection | `ValidationException`, `ResourceNotFoundException`, etc. | `AdapterQueryError` |
| `update()` target doesn't exist | `ConditionalCheckFailedException` | returns `None` (not raised) |
| Operation exceeded timeout | — | `AdapterTimeoutError` |

**A note on `botocore.exceptions.ClientError`:** DynamoDB funnels nearly
every *service-side* error — a missing table, bad input, throttling, a
failed conditional check — through this single exception class. The only
way to tell them apart is `exc.response["Error"]["Code"]`, never the Python
exception type. Genuine *local*/network-level failures, where the request
never reached AWS at all, raise a completely different hierarchy
(`EndpointConnectionError`/`ConnectionError`) and are checked first — see
`repository.py`'s `_wrap_botocore()` and `connection.py`'s module docstring
for the full classification.

Throttling errors (`ProvisionedThroughputExceededException`/
`ThrottlingException`) are mapped onto `AdapterConnectionError` rather than
`AdapterQueryError`: `openframe-core`'s exception taxonomy has no dedicated
"throttled" subclass, and `AdapterConnectionError`'s `retryable=True`
default communicates exactly what callers need — back off and retry — even
though no actual connection was lost.

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

repo = DynamoDBRepository(settings, id_column="id")
assert isinstance(repo, BaseRepository)   # True -- structural check
assert isinstance(repo, HealthCheck)      # True -- structural check
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
