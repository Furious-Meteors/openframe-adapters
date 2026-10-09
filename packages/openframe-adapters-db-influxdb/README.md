# openframe-adapters-db-influxdb

InfluxDB 2.x time-series database adapter for the **OpenFrame Microservice
Suite**.

Part of the `openframe-adapters` monorepo. Implements `BaseRepository[T]`
from `openframe-core` using `influxdb-client`'s native async client
(`InfluxDBClientAsync`).

---

## InfluxDB vs. row-based CRUD — read this first

`BaseRepository[T]` (`get`/`list`/`create`/`update`/`delete`) was designed
for row-based stores with a primary key. InfluxDB is a **time-series**
database: data is immutable points — a measurement name, a tag set, a
field set, and a timestamp — written via line protocol and queried with
Flux, not SQL `CRUD`. There is no primary key. This adapter maps
`BaseRepository` onto that model by picking explicit interpretations,
documented honestly here (and in `repository.py`'s module docstring, which
is the canonical source if the two ever drift):

| Method | What it actually does | How well it fits |
|---|---|---|
| `create(entity)` | Writes one new point tagged with the entity's id. | Good fit. |
| `get(entity_id)` | Returns the **most recent point** tagged with `entity_id`, within a bounded time range (`lookback`, default `-30d`). | No natural fit — InfluxDB has no primary key. "Most recent point for this id" is a chosen interpretation, not the only valid one. |
| `list(limit, offset)` | `limit` caps the result count (real). `offset` skips rows in Python after fetching `limit + offset` points. | `offset` is a **compatibility shim**, not idiomatic InfluxDB access. Real callers should prefer a time-range query via the raw driver (see below), not an arbitrary row offset into a time-ordered result. |
| `update(entity)` | Looks up the existing point for the id, then writes a **new point with the identical tag set and identical timestamp** — InfluxDB's storage engine resolves that as a last-write-wins overwrite of the exact same point. | Not a real update — points are immutable. This is the honest mechanism, not a workaround that pretends otherwise. |
| `delete(entity_id)` | Checks the point exists, then issues a predicate + time-range delete (`delete_api().delete(...)`). | Implementable, but coarser-grained than SQL `DELETE … WHERE id = ?` — it deletes by predicate across a time range, not a single row. |

If your use case is "look up the latest reading for a sensor" or "write one
reading," this adapter fits well. If it's real time-series analysis —
aggregations, downsampling, multi-field joins across time, continuous
queries — use the raw-driver escape hatch below instead of forcing
`BaseRepository` to do something it was never shaped for.

---

## Installation

```bash
pip install openframe-adapters-db-influxdb
```

Required env vars:

```
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=your-api-token
INFLUXDB_ORG=your-org
INFLUXDB_BUCKET=your-bucket
```

---

## Quick start

### Raw dict mode

```python
from openframe.adapters.db.influxdb import InfluxDBSettings, InfluxDBRepository

settings = InfluxDBSettings()  # reads INFLUXDB_* from env
repo = InfluxDBRepository(settings, measurement="readings", id_tag="sensor_id")

reading = await repo.get("sensor-1")                 # dict | None — most recent point
readings, total = await repo.list(10, 0)              # ([dict, ...], int)
created = await repo.create({"sensor_id": "sensor-1", "temperature": 21.5})
updated = await repo.update({"sensor_id": "sensor-1", "temperature": 22.0})
deleted = await repo.delete("sensor-1")               # bool
```

### Typed domain mode

```python
from dataclasses import dataclass
from openframe.adapters.db.influxdb import InfluxDBSettings, InfluxDBRepository

@dataclass
class Reading:
    sensor_id: str
    temperature: float

class ReadingRepository(InfluxDBRepository[Reading]):
    _measurement = "readings"
    _id_tag = "sensor_id"

    def _record_to_entity(self, record) -> Reading:
        return Reading(sensor_id=record["sensor_id"], temperature=record["temperature"])

    def _entity_to_point(self, entity: Reading, *, time_override=None):
        from influxdb_client.client.write.point import Point
        point = Point(self._measurement).tag("sensor_id", entity.sensor_id).field("temperature", entity.temperature)
        if time_override is not None:
            point = point.time(time_override)
        return point

settings = InfluxDBSettings()
repo = ReadingRepository(settings)
reading: Reading | None = await repo.get("sensor-1")
```

---

## Raw driver access (escape hatch)

Every method above is a compatibility layer over a database that was never
row-shaped. For anything beyond simple single-id lookups/overwrites — real
Flux aggregations, time-range queries, downsampling — use the cached
`InfluxDBClientAsync` directly, the same "escape hatch for niche features"
pattern this ecosystem's Postgres adapter documents for raw SQL:

```python
client = await repo.client()
tables = await client.query_api().query(
    'from(bucket: "my-bucket") |> range(start: -1h) '
    '|> filter(fn: (r) => r._measurement == "readings") '
    '|> aggregateWindow(every: 5m, fn: mean)',
    org=settings.influxdb_org,
)
```

---

## Wiring into an application

For a real service, wire `InfluxDBPlugin` (the `BasePort`-satisfying plugin
class) through `ApplicationBootstrap.compose()` from `openframe-core`. This
gives you proper lifecycle management — `initialize()` / `health()` /
`shutdown()` — for free, instead of constructing `InfluxDBRepository`
directly and managing the client yourself:

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.db.influxdb import InfluxDBPlugin, InfluxDBSettings

settings = InfluxDBSettings()  # reads INFLUXDB_* from env
plugin = InfluxDBPlugin(settings, measurement="readings", id_tag="sensor_id")

async with ApplicationBootstrap.compose(plugin) as app:
    repo = app.get(Capability.PERSISTENCE)   # -> InfluxDBRepository
    reading = await repo.get("sensor-1")
# client is closed automatically on exit (plugin.shutdown() ran)
```

`compose()` calls `plugin.initialize()` on entry and `plugin.shutdown()` on
exit, so the client is created, health-checked, and torn down without any
manual lifecycle code. Requires `openframe-core>=3.3`.

Reach for a subclassed `ApplicationBootstrap` (with a `configure()` method)
only when you need per-port `config=`/`init_timeout=` or conditional
registration order; use `app.registry` as an escape hatch for anything
neither tier covers. The `InfluxDBRepository(settings)` construction shown
above under "Quick start" remains valid for tests, scripts, or any context
that doesn't need plugin lifecycle management.

This adapter registers under `Capability.PERSISTENCE` rather than a
dedicated time-series capability — that enum is closed by design, and
nothing about InfluxDB creates the kind of registry-lookup ambiguity that
would justify adding a member to it. See `plugin.py`'s module docstring.

### Resilience — circuit breaking under sustained failure

`openframe-core>=3.4` ships `openframe.core.resilience.CircuitBreakerProxy`
— wrap a repository to short-circuit calls after repeated failures instead
of blocking every caller until `operation_timeout` during a sustained
outage:

```python
from openframe.core.resilience import CircuitBreakerProxy
from openframe.core.tracing import TracingProxy

repo = CircuitBreakerProxy(
    TracingProxy(app.get(Capability.PERSISTENCE).get_repository(), prefix="repository.reading"),
    failure_threshold=5,
    reset_timeout=30.0,
)
```

Wrap the traced repository, not the reverse — a short-circuited call never
reaches the adapter, so it shouldn't produce a misleading adapter span. No
adapter code changes are needed to support this — `CircuitBreakerProxy`
wraps from the outside, exactly like `TracingProxy`.

---

## Async strategy

This adapter uses `influxdb-client`'s native async client
(`InfluxDBClientAsync`, backed by `aiohttp`) — the same native-async shape
as the Postgres/Oracle adapters, not the `run_in_executor` fallback Cassandra
needs. See `connection.py`'s module docstring for the full research finding
(what was verified against the actually-installed driver, and the real
gotcha found along the way: `aiohttp` connection-level failures never
become `influxdb_client.rest.ApiException`, so they're caught explicitly).

---

## Configuration

All settings are read from environment variables.

| Env var | Type | Default | Description |
|---|---|---|---|
| `INFLUXDB_URL` | `str` | **required** | Server URL |
| `INFLUXDB_TOKEN` | `str` | **required** | API token |
| `INFLUXDB_ORG` | `str` | **required** | Organization name or ID |
| `INFLUXDB_BUCKET` | `str` | **required** | Default bucket name |
| `CLIENT_TIMEOUT_MS` | `int` | `10000` | Per-request timeout (ms) |
| `LOOKBACK` | `str` | `-30d` | Flux `range(start: ...)` window for `get()`/`list()` |
| `CONNECTION_TIMEOUT` | `float` | `30.0` | Client creation/connectivity-check timeout (s) |
| `OPERATION_TIMEOUT` | `float` | `10.0` | Per-operation timeout (s) |
| `MAX_RETRIES` | `int` | `3` | Max retry attempts |

---

## Exception hierarchy

All exceptions are `AdapterError` subclasses from `openframe.core.exceptions`.
Raw `influxdb_client`/`aiohttp` exceptions never escape the adapter.

| Situation | Exception |
|---|---|
| Cannot connect (DNS, refused, `aiohttp.ClientError`) | `AdapterConnectionError` |
| Token/org rejected (HTTP 401/403) | `AdapterConfigurationError` |
| Bad Flux query / missing bucket (HTTP 400/404) | `AdapterQueryError` |
| InfluxDB server error (HTTP 5xx) | `AdapterConnectionError` (treated as transient — see `repository.py`) |
| Operation exceeded timeout | `AdapterTimeoutError` |

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

repo = InfluxDBRepository(settings, measurement="readings", id_tag="sensor_id")
assert isinstance(repo, BaseRepository)   # True — structural check
```

No inheritance from the Protocol is required or used.

---

## License

MIT
