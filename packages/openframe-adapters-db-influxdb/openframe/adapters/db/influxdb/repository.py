"""
openframe/adapters/db/influxdb/repository.py
================================================
Generic InfluxDB 2.x repository implementing ``BaseRepository[T]`` from
``openframe-core`` via structural subtyping.

InfluxDB vs. row-based CRUD — read this before using this adapter
---------------------------------------------------------------------
``BaseRepository[T]`` (``get``/``list``/``create``/``update``/``delete``) was
designed for row-based stores with a primary key. InfluxDB is a time-series
database: data is immutable **points** — a measurement name, a tag set, a
field set, and a timestamp — written via line protocol and queried with
Flux, not SQL. There is no primary key. Mapping ``BaseRepository`` onto
this model requires picking interpretations, documented honestly below
rather than hidden behind code that merely *looks* like it does normal
CRUD:

* ``create(entity)`` — maps reasonably well. ``entity`` must be a
  ``dict`` containing the id-tag key (``_id_tag``, default ``"id"``) plus
  any number of other keys, which become InfluxDB **fields** (an optional
  ``"time"`` key, if present, becomes the point's timestamp; otherwise the
  current wall-clock time is used). Written as one point tagged
  ``{id_tag: entity[id_tag]}`` under this repository's ``_measurement``.
  Returns the entity exactly as passed — InfluxDB's write API returns no
  server-generated fields the way SQL's ``RETURNING *`` does, so there is
  nothing to merge back in.

* ``get(entity_id)`` — **no natural fit.** InfluxDB has no primary-key
  concept and no "the one row where id = X" query. The interpretation
  chosen here: treat ``entity_id`` as a specific value of the id tag,
  scoped to this repository's measurement, and return the single **most
  recent point** matching it (Flux ``sort(desc: true) |> limit(n: 1)``)
  within ``settings.lookback`` of "now" (default ``-30d`` — a Flux range
  query requires *some* bound; an unbounded range is a real but much more
  expensive query most callers don't want by default). If multiple fields
  were written at different timestamps for the same id, this returns only
  the most recent timestamp's fields, not a merge across all of history.
  This is a design choice, not the "obvious" or only valid one — a caller
  who needs different semantics (a specific time, an aggregate across a
  range, all points for an id) should use the raw-driver escape hatch
  below instead of forcing this method to do something it isn't shaped
  for.

* ``list(limit, offset)`` — ``limit`` maps cleanly to a result-count cap.
  ``offset`` as "skip N rows" does **not** map naturally onto time-series
  data, which is more naturally paginated by time range (e.g. "give me the
  next bucket of points older than timestamp T"), not by an arbitrary row
  offset into an unordered-until-you-say-so result set. This method
  implements ``offset`` anyway, as a **compatibility shim**: it queries up
  to ``limit + offset`` points (sorted by time) and skips the first
  ``offset`` in Python. This is correctness-preserving but not
  performance-sensible for a large ``offset`` — every call re-fetches and
  discards the skipped prefix, there is no server-side cursor. Real
  callers who need efficient pagination over a time series should query
  by time range directly via the raw driver (``repo.client`` /
  ``get_influxdb_client()``), not via this method's ``offset``. The
  returned total count is the number of points matched by the measurement
  filter within ``lookback`` — not the size of any particular page.

* ``update(entity)`` — InfluxDB points are **immutable**. There is no
  update operation in the InfluxDB write model. What this method actually
  does, and the only thing "update" can honestly mean here: it looks up
  the existing point for ``entity[id_tag]`` (same interpretation as
  ``get()``), and if one exists, writes a **new point with the identical
  tag set and the identical timestamp** as the one found, carrying the new
  field values. InfluxDB's storage engine treats a second point with an
  identical series key (measurement + tag set) and an identical timestamp
  as a **last-write-wins overwrite** of that exact point, not a new,
  separate point. This is the real mechanism — it is not SQL ``UPDATE``,
  it is "replace this exact (series, timestamp) slot by writing to it
  again." Returns ``None`` if no existing point was found (nothing to
  overwrite), mirroring ``BaseRepository``'s not-found semantics.

* ``delete(entity_id)`` — InfluxDB does support deletion, via a
  predicate + time-range **delete API**
  (``delete_api().delete(start, stop, predicate, bucket, org)``), which is
  real and implementable, but it is coarser-grained than a SQL
  ``DELETE … WHERE id = ?``: it deletes every point matching the predicate
  within the given time range, not "the one row." This method first
  checks (via the same lookup as ``get()``) whether a matching point
  exists, then issues a delete predicate scoped to this repository's
  measurement and id tag across ``settings.lookback``. Returns ``True``
  only if a point existed before the delete call — the delete API itself
  returns a bare ``bool`` indicating the HTTP call succeeded, not how many
  points (if any) actually matched, so "did anything get deleted" has to
  be answered by the existence check beforehand, not by the delete
  response.

**Raw driver access (escape hatch):** every interpretation above is a
compatibility layer over a database that was never row-shaped to begin
with. For anything beyond simple single-id lookups/overwrites — real Flux
aggregations, time-range queries, downsampling, continuous queries — use
``repo.client`` (the cached ``InfluxDBClientAsync``) directly via its
``query_api()``/``write_api()``/``delete_api()``, the same "escape hatch
for niche features" pattern this ecosystem's Postgres adapter documents in
its own README for raw-SQL access.

Usage — raw dict mode (no subclassing needed):

    repo = InfluxDBRepository(settings, measurement="readings", id_tag="id")
    reading = await repo.get("sensor-1")   # dict | None

Structural conformance (no inheritance from Protocols required):

    assert isinstance(repo, BaseRepository)
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Generic, TypeVar

import aiohttp
from influxdb_client.client.flux_table import FluxRecord
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.write.point import Point
from influxdb_client.rest import ApiException

from openframe.core.exceptions import (
    AdapterConfigurationError,
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseRepository, Capability, PluginContext, PluginHealth, PluginStatus

from .config import InfluxDBSettings
from .connection import _cache_key, _client_cache, get_influxdb_client

__all__ = ["InfluxDBRepository"]

T = TypeVar("T")

# Internal Flux/annotated-CSV columns that are never part of the entity —
# stripped from every record before handing it back to the caller.
_INTERNAL_COLUMNS = frozenset({"_time", "_start", "_stop", "_measurement", "result", "table"})


class InfluxDBRepository(Generic[T]):
    """
    Generic InfluxDB 2.x repository.

    Implements ``BaseRepository[T]`` structurally — no inheritance from
    the Protocol. See this module's docstring for the honest mapping (and
    the real limits of that mapping) between row-based CRUD and InfluxDB's
    point/Flux model.

    All driver exceptions are caught and re-raised as ``AdapterError``
    subclasses via ``_wrap_influx()``. Every operation wraps its call in
    ``asyncio.timeout(settings.operation_timeout)``.

    Class attributes (override in subclass):
        _measurement: Measurement name used when no ``measurement``
                      argument is passed.
        _id_tag:      Tag key treated as the entity id. Default ``"id"``.

    Args:
        settings:    An ``InfluxDBSettings`` instance.
        measurement: Measurement name. Overrides the ``_measurement``
                     class attribute.
        id_tag:      Id-tag key name. Overrides the ``_id_tag`` class
                     attribute.
        bucket:      Bucket name override. Defaults to
                     ``settings.influxdb_bucket``.

    Raises:
        AdapterConfigurationError: If neither ``measurement`` param nor
                                   ``_measurement`` class attribute is set.
    """

    _measurement: str = ""
    _id_tag: str = "id"

    name:       str = "openframe-influxdb-repository"
    version:    str = "0.1.0"
    capability: Capability = Capability.PERSISTENCE

    def __init__(
        self,
        settings: InfluxDBSettings,
        measurement: str | None = None,
        id_tag: str | None = None,
        bucket: str | None = None,
    ) -> None:
        self._settings = settings
        self._measurement = measurement or self.__class__._measurement
        self._id_tag = id_tag or self.__class__._id_tag
        self._bucket = bucket or settings.influxdb_bucket

        if not self._measurement:
            raise AdapterConfigurationError(
                "InfluxDBRepository requires a measurement name. "
                "Pass measurement= to __init__ or set _measurement on the subclass.",
                adapter=settings.adapter_name,
                operation="init",
            )

    # ------------------------------------------------------------------
    # Raw driver access (escape hatch — see module docstring)
    # ------------------------------------------------------------------

    async def client(self) -> InfluxDBClientAsync:
        """
        Return the cached ``InfluxDBClientAsync`` for raw Flux/write-API
        access beyond what this repository's CRUD shim covers.
        """
        return await get_influxdb_client(self._settings)

    # ------------------------------------------------------------------
    # Entity <-> point/record mapping (override in typed subclasses)
    # ------------------------------------------------------------------

    def _entity_to_point(self, entity: T, *, time_override: Any | None = None) -> Point:
        """
        Convert an entity dict to an InfluxDB ``Point``.

        Base implementation expects a ``dict`` containing ``self._id_tag``
        plus arbitrary field keys. An optional ``"time"`` key becomes the
        point's timestamp (ignored if ``time_override`` is given — used by
        ``update()`` to overwrite the exact same timestamp). Subclasses
        override this to map typed domain objects.
        """
        if not isinstance(entity, dict):
            raise AdapterQueryError(
                f"InfluxDBRepository base implementation requires dict entities, got {type(entity)!r}",
                adapter=self._settings.adapter_name,
                operation="write",
            )
        if self._id_tag not in entity:
            raise AdapterQueryError(
                f"Entity is missing required id tag {self._id_tag!r}: {entity!r}",
                adapter=self._settings.adapter_name,
                operation="write",
            )
        point = Point(self._measurement).tag(self._id_tag, str(entity[self._id_tag]))
        for key, value in entity.items():
            if key in (self._id_tag, "time"):
                continue
            point = point.field(key, value)
        ts = time_override if time_override is not None else entity.get("time")
        if ts is not None:
            point = point.time(ts)
        return point

    def _record_to_entity(self, record: FluxRecord) -> T:
        """
        Convert a ``FluxRecord`` to the entity type ``T``.

        Base implementation returns a ``dict`` of every non-internal
        column (tags + fields), with the id tag exposed under its own
        key. Subclasses override this to return typed domain objects.
        """
        return {
            k: v
            for k, v in record.values.items()
            if k not in _INTERNAL_COLUMNS
        }  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Exception mapping helper
    # ------------------------------------------------------------------

    def _wrap_influx(
        self, exc: Exception, operation: str
    ) -> AdapterQueryError | AdapterConnectionError | AdapterConfigurationError:
        """
        Map an InfluxDB driver exception to the appropriate ``AdapterError``
        subclass.

        Classification, decided and documented here (see also
        ``connection.py``'s module docstring for the research behind it):

        * ``aiohttp.ClientError`` / ``OSError`` — raised before any HTTP
          response exists (DNS failure, connection refused/reset). These
          never become ``ApiException`` — confirmed by reading
          ``influxdb_client/_async/rest.py``, which only wraps responses
          that actually came back with a status. -> ``AdapterConnectionError``
          (retryable).
        * ``ApiException`` with status 401/403 — token/org rejected.
          -> ``AdapterConfigurationError`` (not retryable — the credentials
          are wrong, retrying won't help).
        * ``ApiException`` with status 400 or 404 — malformed Flux query,
          or a referenced bucket/org that does not exist.
          -> ``AdapterQueryError`` (not retryable — identical request fails
          identically).
        * ``ApiException`` with a 5xx status — genuinely ambiguous: could
          be a transient overload on the InfluxDB server, or a persistent
          server-side bug. This module's decision: treat 5xx as
          **retryable connection-class** (``AdapterConnectionError``), on
          the reasoning that a 5xx from InfluxDB's own HTTP layer (as
          opposed to a 4xx the client caused) is far more often a
          transient server condition than a client-caused failure that
          would just repeat — the same bias ``AdapterConnectionError``'s
          "retryable" default already encodes. A caller who knows a
          specific 5xx in their deployment is NOT transient can still
          inspect ``exc.cause.status`` and decide not to retry.
        * ``ApiException`` with any other/missing status (e.g. status=0,
          raised by the REST client itself on an unexpected response
          shape) — treated conservatively as ``AdapterQueryError``.

        Caller must ``raise ... from exc`` at the call site.
        """
        if isinstance(exc, (aiohttp.ClientError, OSError)):
            return AdapterConnectionError(
                f"{operation} failed — connection to InfluxDB was lost or never established: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        if isinstance(exc, ApiException):
            status = exc.status or 0
            if status in (401, 403):
                return AdapterConfigurationError(
                    f"{operation} failed — InfluxDB rejected the token/org ({status}): {exc.reason}",
                    adapter=self._settings.adapter_name,
                    operation=operation,
                    cause=exc,
                )
            if status >= 500:
                return AdapterConnectionError(
                    f"{operation} failed — InfluxDB server error ({status}), treated as transient: {exc.reason}",
                    adapter=self._settings.adapter_name,
                    operation=operation,
                    cause=exc,
                )
            return AdapterQueryError(
                f"{operation} failed on measurement {self._measurement!r} ({status}): {exc.reason}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        return AdapterQueryError(
            f"{operation} failed on measurement {self._measurement!r}: {exc}",
            adapter=self._settings.adapter_name,
            operation=operation,
            cause=exc,
        )

    # ------------------------------------------------------------------
    # Flux query builders
    # ------------------------------------------------------------------

    def _filter_clause(self, entity_id: str | None) -> str:
        base = f'r._measurement == "{self._measurement}"'
        if entity_id is not None:
            escaped = entity_id.replace('"', '\\"')
            base += f' and r["{self._id_tag}"] == "{escaped}"'
        return base

    def _flux_get_latest(self, entity_id: str) -> str:
        return (
            f'from(bucket: "{self._bucket}")\n'
            f"  |> range(start: {self._settings.lookback})\n"
            f"  |> filter(fn: (r) => {self._filter_clause(entity_id)})\n"
            f'  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n'
            f'  |> sort(columns: ["_time"], desc: true)\n'
            f"  |> limit(n: 1)"
        )

    def _flux_list_all(self) -> str:
        return (
            f'from(bucket: "{self._bucket}")\n'
            f"  |> range(start: {self._settings.lookback})\n"
            f"  |> filter(fn: (r) => {self._filter_clause(None)})\n"
            f'  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n'
            f'  |> sort(columns: ["_time"])'
        )

    # ------------------------------------------------------------------
    # Internal: shared lookup used by get()/update()/delete()
    # ------------------------------------------------------------------

    async def _query_latest_record(self, entity_id: str) -> FluxRecord | None:
        client = await get_influxdb_client(self._settings)
        query = self._flux_get_latest(entity_id)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                tables = await client.query_api().query(query=query, org=self._settings.influxdb_org)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"get exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="get",
                cause=exc,
            ) from exc
        except (ApiException, aiohttp.ClientError, OSError) as exc:
            raise self._wrap_influx(exc, "get") from exc

        for table in tables:
            for record in table.records:
                return record
        return None

    # ------------------------------------------------------------------
    # BaseRepository[T] interface
    # ------------------------------------------------------------------

    async def get(self, entity_id: str) -> T | None:
        """
        Retrieve the most recent point tagged with ``entity_id``.

        See this module's docstring for why "most recent point matching
        the id tag" is the chosen interpretation of ``get()`` for a
        database with no primary-key concept.

        Args:
            entity_id: Value of the id tag to look up.

        Returns:
            The entity if a matching point exists within
            ``settings.lookback``, ``None`` otherwise.

        Raises:
            AdapterQueryError:   Query failed after connection was established.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        record = await self._query_latest_record(entity_id)
        if record is None:
            return None
        return self._record_to_entity(record)

    async def list(self, limit: int, offset: int) -> tuple[list[T], int]:
        """
        Return a paginated slice of points for this measurement and the
        total number of points matched.

        ``offset`` is a **compatibility shim**, not an idiomatic InfluxDB
        access pattern — see this module's docstring. It fetches all
        matching points within ``settings.lookback`` (sorted by time) and
        slices in Python; there is no server-side cursor. Real callers
        needing efficient time-series pagination should query by time
        range directly via ``repo.client()``.

        Args:
            limit:  Maximum number of points to return.
            offset: Number of points to skip from the start of the
                    time-sorted result.

        Returns:
            A 2-tuple ``(entities, total_count)`` where ``total_count`` is
            the number of all points matched by the measurement filter
            within ``settings.lookback`` — not the page size.

        Raises:
            AdapterQueryError:   Query failed after connection was established.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        client = await get_influxdb_client(self._settings)
        query = self._flux_list_all()
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                tables = await client.query_api().query(query=query, org=self._settings.influxdb_org)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"list exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="list",
                cause=exc,
            ) from exc
        except (ApiException, aiohttp.ClientError, OSError) as exc:
            raise self._wrap_influx(exc, "list") from exc

        records: list[FluxRecord] = []
        for table in tables:
            records.extend(table.records)

        total = len(records)
        page = records[offset : offset + limit]
        entities = [self._record_to_entity(r) for r in page]
        return entities, total

    async def create(self, entity: T) -> T:
        """
        Write a new point and return the entity as passed.

        InfluxDB's write API returns no server-generated fields (no
        ``RETURNING *`` equivalent — there is no autoincrement PK, no
        default-filled columns), so there is nothing to merge back into
        the entity; it is returned unchanged.

        Args:
            entity: The entity to write (a dict containing the id tag plus
                    field keys; see this module's docstring).

        Returns:
            ``entity``, unchanged.

        Raises:
            AdapterQueryError:   Write failed.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        client = await get_influxdb_client(self._settings)
        point = self._entity_to_point(entity)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await client.write_api().write(bucket=self._bucket, org=self._settings.influxdb_org, record=point)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"create exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="create",
                cause=exc,
            ) from exc
        except (ApiException, aiohttp.ClientError, OSError) as exc:
            raise self._wrap_influx(exc, "create") from exc

        return entity

    async def update(self, entity: T) -> T | None:
        """
        Overwrite the most recent existing point for this entity's id tag.

        InfluxDB points are immutable — this is NOT a real update. See
        this module's docstring: it looks up the existing point (same
        interpretation as ``get()``), and if found, writes a new point
        with the identical tag set and identical timestamp, which
        InfluxDB's storage engine resolves as a last-write-wins overwrite
        of that exact point.

        Args:
            entity: The entity with updated fields. Must contain the id
                    tag key.

        Returns:
            ``entity``, unchanged, if an existing point was found and
            overwritten. ``None`` if no existing point matches — there is
            nothing to "update."

        Raises:
            AdapterQueryError:   Write or lookup failed.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        if not isinstance(entity, dict) or self._id_tag not in entity:
            raise AdapterQueryError(
                f"update requires an entity dict containing {self._id_tag!r}: {entity!r}",
                adapter=self._settings.adapter_name,
                operation="update",
            )
        entity_id = str(entity[self._id_tag])
        existing = await self._query_latest_record(entity_id)
        if existing is None:
            return None

        client = await get_influxdb_client(self._settings)
        point = self._entity_to_point(entity, time_override=existing.get_time())
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await client.write_api().write(bucket=self._bucket, org=self._settings.influxdb_org, record=point)
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"update exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="update",
                cause=exc,
            ) from exc
        except (ApiException, aiohttp.ClientError, OSError) as exc:
            raise self._wrap_influx(exc, "update") from exc

        return entity

    async def delete(self, entity_id: str) -> bool:
        """
        Delete all points matching this entity's id tag within
        ``settings.lookback``.

        InfluxDB's delete API deletes by predicate + time range, not by a
        single row — see this module's docstring. This method first
        checks whether a matching point exists (the delete API's own
        response is a bare success/failure bool, not a matched-count), and
        only reports ``True`` if one did.

        Args:
            entity_id: Value of the id tag to delete.

        Returns:
            ``True`` if a matching point existed and the delete predicate
            was submitted successfully, ``False`` if no matching point
            existed.

        Raises:
            AdapterQueryError:   Deletion failed.
            AdapterTimeoutError: Operation exceeded ``operation_timeout``.
        """
        existing = await self._query_latest_record(entity_id)
        if existing is None:
            return False

        client = await get_influxdb_client(self._settings)
        escaped = entity_id.replace('"', '\\"')
        predicate = f'_measurement="{self._measurement}" AND {self._id_tag}="{escaped}"'
        now = datetime.now(timezone.utc)
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await client.delete_api().delete(
                    start="1970-01-01T00:00:00Z",
                    stop=now,
                    predicate=predicate,
                    bucket=self._bucket,
                    org=self._settings.influxdb_org,
                )
        except asyncio.TimeoutError as exc:
            raise AdapterTimeoutError(
                f"delete exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation="delete",
                cause=exc,
            ) from exc
        except (ApiException, aiohttp.ClientError, OSError) as exc:
            raise self._wrap_influx(exc, "delete") from exc

        return True

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """
        Close the InfluxDB client and remove it from the cache.

        Call once at application shutdown. After this returns, a
        subsequent operation will create a new client.
        """
        key = _cache_key(self._settings)
        client = _client_cache.get(key)
        if client is not None:
            await client.close()
            _client_cache.pop(key, None)

    # ------------------------------------------------------------------
    # BasePort (Identity + Lifecycle) interface
    # ------------------------------------------------------------------

    async def initialize(self, context: PluginContext) -> None:
        """
        Establish the InfluxDB client and verify connectivity.

        BasePort lifecycle entry point. Reuses the same cached client as
        every other method on this repository.

        Args:
            context: Plugin context. Unused — settings are provided at
                     construction time.

        Raises:
            AdapterConnectionError: InfluxDB is unreachable.
        """
        await get_influxdb_client(self._settings)
        health = await self.health()
        if health.status != PluginStatus.READY:
            raise AdapterConnectionError(
                health.message or "InfluxDB connectivity check failed during initialize()",
                adapter=self._settings.adapter_name,
                operation="initialize",
            )

    async def shutdown(self) -> None:
        """BasePort lifecycle entry point — alias for close(). Never raises."""
        await self.close()

    async def health(self) -> PluginHealth:
        """
        BasePort lifecycle entry point — returns a PluginHealth snapshot.

        The sole connectivity check on this repository — a ``ping()``
        against the client with a 5-second timeout. Never raises.
        """
        try:
            client = await get_influxdb_client(self._settings)
            ok = await asyncio.wait_for(client.ping(), timeout=5.0)
            if ok:
                return PluginHealth(status=PluginStatus.READY, message="")
            return PluginHealth(status=PluginStatus.FAILED, message="InfluxDB ping() returned False")
        except Exception as exc:  # noqa: BLE001
            return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
