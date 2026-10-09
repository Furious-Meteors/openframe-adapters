"""
openframe/adapters/db/falkordb/repository.py
================================================
Generic FalkorDB graph repository implementing ``BaseGraphStore[T]`` from
``openframe-core`` (>=3.6) via structural subtyping.

Addressing scheme — read this before touching ``get``/``update``/``delete``
------------------------------------------------------------------------
Nodes are addressed by an application-level ``id`` **property**, never by
FalkorDB's own internal ``id()`` Cypher function. FalkorDB's upstream docs
explicitly document ``id()`` as unstable: when a node is deleted, higher
internal ids can be migrated down to fill the vacated lower ids, so an id
captured before a deletion elsewhere in the graph can silently point at a
different node afterwards. ``openframe-core``'s own
``openframe.core.ports.outbound.repository`` module docstring spells out
the same reasoning for ``BaseGraphStore`` generally — see it for the full
rationale.

The standard, FalkorDB-documented workaround is exactly what this adapter
does: address nodes by a unique application-level property (here, ``id``,
matching ``BaseRepository``'s own convention) backed by a real unique
constraint/index, so every ``get``/``update``/``delete`` is an indexed
lookup rather than an unindexed full-label scan. ``_ensure_constraint()``
below creates that constraint (via ``AsyncGraph.create_node_unique_constraint``,
which transitively creates the backing range index) idempotently, lazily,
on the first CRUD call this repository instance makes (and also eagerly
from ``initialize()`` for the plugin/lifecycle-managed path) — it is NOT
assumed to exist already, and this adapter does not expect an operator to
have created it out-of-band.

Edges / relationships
----------------------
This port (``BaseGraphStore``) models only nodes as first-class entities.
Relationships/edges have no CRUD method of their own — ``traverse()`` is
the *only* way to create, update, or query them, via arbitrary Cypher
(e.g. ``MATCH (a) WHERE a.id = $a_id MATCH (b) WHERE b.id = $b_id CREATE
(a)-[:KNOWS]->(b)``). See :meth:`FalkorDBRepository.traverse`'s own
docstring.

The base class works with raw ``dict[str, Any]`` node properties. Domain
adapters subclass it and override ``_entity_to_properties()`` and
``_node_to_entity()`` to map between FalkorDB ``Node`` objects and typed
domain objects — matching the ``_row_to_entity``/``_entity_to_row``
override convention ``PostgresRepository`` already uses.

Usage — raw dict mode::

    repo = FalkorDBRepository(settings)
    node = await repo.get("abc-123")   # dict | None

Usage — typed domain mode::

    class PersonRepository(FalkorDBRepository[Person]):
        def _node_to_entity(self, node: Node) -> Person:
            return Person(**node.properties)
        def _entity_to_properties(self, entity: Person) -> dict[str, Any]:
            return entity.model_dump()

Structural conformance (no inheritance from Protocols required)::

    assert isinstance(repo, BaseGraphStore)
"""
from __future__ import annotations

import asyncio
from typing import Any, Generic, TypeVar

import redis.exceptions
from falkordb.node import Node

from openframe.core.exceptions import (
    AdapterConnectionError,
    AdapterQueryError,
    AdapterTimeoutError,
)
from openframe.core.ports import BaseGraphStore, Capability, PluginContext, PluginHealth, PluginStatus

from .config import FalkorDBSettings
from .connection import _cache_key, _client_cache, get_falkordb_client

__all__ = ["FalkorDBRepository"]

T = TypeVar("T")


class FalkorDBRepository(Generic[T]):
    """
    Generic FalkorDB graph repository.

    Implements ``BaseGraphStore[T]`` structurally — no inheritance from
    the Protocol. All ``falkordb``/``redis`` exceptions are caught and
    re-raised as ``AdapterError`` subclasses. Every operation wraps its
    FalkorDB call in ``asyncio.timeout(settings.operation_timeout)``.

    Health check: ``health()`` is the sole health check on this repository.
    It verifies backend connectivity and returns a ``PluginHealth``
    snapshot describing the result — never raises.

    All nodes this repository manages share one Cypher label
    (``settings.falkordb_node_label``), validated as a safe Cypher
    identifier at ``FalkorDBSettings`` construction time (Cypher has no
    parameterized label syntax, so it is interpolated directly into every
    query string below — safe only because it was already validated).

    Base implementation works with ``dict[str, Any]`` node properties.
    Subclasses override ``_node_to_entity()``/``_entity_to_properties()``
    for typed domain objects.
    """

    name:       str = "openframe-falkordb-repository"
    version:    str = "0.1.0"
    capability: Capability = Capability.SEARCH

    def __init__(self, settings: FalkorDBSettings) -> None:
        self._settings = settings
        self._label = settings.falkordb_node_label
        self._constraint_ensured = False

    # ------------------------------------------------------------------
    # Graph access
    # ------------------------------------------------------------------

    async def _get_graph(self):
        """
        Return this repository's ``AsyncGraph`` handle.

        ``FalkorDB.select_graph()`` is confirmed synchronous and lazy (no
        I/O — see ``connection.py``'s module docstring), so this is cheap
        to call on every operation; only the underlying client connection
        is cached.
        """
        client = await get_falkordb_client(self._settings)
        return client.select_graph(self._settings.falkordb_graph_name)

    async def _ensure_ready(self) -> None:
        """
        Ensure the ``(label, id)`` unique constraint/index exists.

        Called lazily at the top of every CRUD method (idempotent via the
        ``_constraint_ensured`` flag — only does real work once per
        repository instance) so that direct construct-and-use callers
        (no ``initialize()``) still get the constraint. Also called
        eagerly from ``initialize()`` for the plugin/lifecycle-managed
        path, so the first real traffic never pays this cost.
        """
        if self._constraint_ensured:
            return

        graph = await self._get_graph()
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                await graph.create_node_unique_constraint(self._label, "id")
        except redis.exceptions.ResponseError as exc:
            # Verified by reading falkordb's source: create_node_unique_
            # constraint() already swallows "already exists"-type failures
            # from its own internal range-index creation step, but the
            # GRAPH.CONSTRAINT CREATE command itself can still fail with an
            # "already exists"-shaped error on a repeated call (no real
            # FalkorDB server was available to observe the exact server-side
            # message in this sandboxed environment — this adapter treats
            # any ResponseError whose message mentions "already" as the
            # expected idempotent no-op, and re-raises anything else).
            if "already" not in str(exc).lower():
                raise self._wrap_falkordb(exc, "ensure_constraint") from exc
        except (
            asyncio.TimeoutError,
            redis.exceptions.ConnectionError,
            redis.exceptions.AuthenticationError,
            redis.exceptions.TimeoutError,
        ) as exc:
            raise self._wrap_falkordb(exc, "ensure_constraint") from exc

        self._constraint_ensured = True

    # ------------------------------------------------------------------
    # Node ↔ entity mapping (override in typed subclasses)
    # ------------------------------------------------------------------

    def _entity_to_properties(self, entity: T) -> dict[str, Any]:
        """
        Convert the entity type ``T`` to a plain dict of node properties.

        Base implementation returns the entity unchanged if it is already
        a dict, or falls back to ``vars(entity)`` for simple objects.
        Subclasses override this to serialise typed domain objects
        correctly.
        """
        if isinstance(entity, dict):
            return dict(entity)
        return vars(entity)

    def _node_to_entity(self, node: Node) -> T:
        """
        Convert a ``falkordb.node.Node`` to the entity type ``T``.

        Base implementation returns ``dict(node.properties)``. Subclasses
        override this to return typed domain objects. Note: ``node.id`` is
        FalkorDB's own internal (unstable) id — this method intentionally
        ignores it and relies entirely on ``node.properties["id"]``, the
        application-level id this adapter addresses nodes by.
        """
        return dict(node.properties)  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Exception mapping helper
    # ------------------------------------------------------------------

    def _wrap_falkordb(
        self, exc: Exception, operation: str
    ) -> AdapterQueryError | AdapterConnectionError | AdapterTimeoutError:
        """
        Map a falkordb/redis exception to the appropriate ``AdapterError``
        subclass.

        Independently verified exception surface (see ``connection.py``'s
        module docstring for the detail): FalkorDB wraps redis-py directly
        and defines no query-specific exception type of its own
        (``falkordb.exceptions`` only has ``SchemaVersionMismatchException``).
        A malformed/failing Cypher query (syntax error, constraint
        violation) surfaces as a plain ``redis.exceptions.ResponseError``;
        a broken/lost connection surfaces as ``redis.exceptions.ConnectionError``/
        ``AuthenticationError``/``TimeoutError``. This mirrors
        ``RedisRepository._wrap_redis()``'s classification exactly, since
        FalkorDB *is* a Redis module under the hood.

        Caller must ``raise ... from exc`` at the call site.
        """
        if isinstance(exc, asyncio.TimeoutError):
            return AdapterTimeoutError(
                f"{operation} exceeded {self._settings.operation_timeout}s operation_timeout",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        if isinstance(
            exc,
            (
                redis.exceptions.ConnectionError,
                redis.exceptions.AuthenticationError,
                redis.exceptions.TimeoutError,
            ),
        ):
            return AdapterConnectionError(
                f"{operation} failed — cannot reach FalkorDB: {exc}",
                adapter=self._settings.adapter_name,
                operation=operation,
                cause=exc,
            )
        return AdapterQueryError(
            f"{operation} failed on FalkorDB: {exc}",
            adapter=self._settings.adapter_name,
            operation=operation,
            cause=exc,
        )

    # ------------------------------------------------------------------
    # BaseGraphStore[T] interface — id-addressable node CRUD
    # ------------------------------------------------------------------

    async def get(self, entity_id: str) -> T | None:
        """
        Retrieve a single node by its application-level ``id`` property.

        Args:
            entity_id: The node's ``id`` property value.

        Returns:
            The entity if a matching node exists, ``None`` otherwise.

        Raises:
            AdapterConnectionError: FalkorDB is unreachable.
            AdapterQueryError:      Query failed.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        await self._ensure_ready()
        graph = await self._get_graph()
        query = f"MATCH (n:{self._label}) WHERE n.id = $id RETURN n LIMIT 1"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                result = await graph.query(query, {"id": entity_id})
        except (
            asyncio.TimeoutError,
            redis.exceptions.ResponseError,
            redis.exceptions.ConnectionError,
            redis.exceptions.AuthenticationError,
            redis.exceptions.TimeoutError,
        ) as exc:
            raise self._wrap_falkordb(exc, "get") from exc

        if not result.result_set:
            return None
        node = result.result_set[0][0]
        return self._node_to_entity(node)

    async def list(self, limit: int, offset: int) -> tuple[list[T], int]:
        """
        Return a paginated slice of nodes and the total node count.

        Args:
            limit:  Maximum number of nodes to return.
            offset: Number of nodes to skip.

        Returns:
            A 2-tuple ``(entities, total_count)`` where ``total_count`` is
            the number of all nodes with this repository's label.

        Raises:
            AdapterConnectionError: FalkorDB is unreachable.
            AdapterQueryError:      Query failed.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        await self._ensure_ready()
        graph = await self._get_graph()
        rows_query = (
            f"MATCH (n:{self._label}) RETURN n "
            f"ORDER BY n.id SKIP $offset LIMIT $limit"
        )
        count_query = f"MATCH (n:{self._label}) RETURN count(n)"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                rows_result = await graph.query(rows_query, {"offset": offset, "limit": limit})
                count_result = await graph.query(count_query, {})
        except (
            asyncio.TimeoutError,
            redis.exceptions.ResponseError,
            redis.exceptions.ConnectionError,
            redis.exceptions.AuthenticationError,
            redis.exceptions.TimeoutError,
        ) as exc:
            raise self._wrap_falkordb(exc, "list") from exc

        entities = [self._node_to_entity(row[0]) for row in rows_result.result_set]
        total = int(count_result.result_set[0][0]) if count_result.result_set else 0
        return entities, total

    async def create(self, entity: T) -> T:
        """
        Create a new node.

        Serialises ``entity`` to a properties dict (must contain ``id``)
        and executes ``CREATE (n:{label}) SET n = $props RETURN n``. The
        ``(label, id)`` unique constraint (created by ``_ensure_ready()``)
        rejects a duplicate ``id`` with a constraint-violation error,
        surfaced here as ``AdapterQueryError``.

        Args:
            entity: The entity to create. Must have an ``id`` field.

        Returns:
            The entity as stored.

        Raises:
            AdapterQueryError:      Entity has no ``id`` field, or a node
                                    with that ``id`` already exists.
            AdapterConnectionError: FalkorDB is unreachable.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        await self._ensure_ready()
        properties = self._entity_to_properties(entity)
        entity_id = str(properties.get("id", ""))
        if not entity_id:
            raise AdapterQueryError(
                "Entity must have an 'id' field to create",
                adapter=self._settings.adapter_name,
                operation="create",
            )

        graph = await self._get_graph()
        query = f"CREATE (n:{self._label}) SET n = $props RETURN n"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                result = await graph.query(query, {"props": properties})
        except (
            asyncio.TimeoutError,
            redis.exceptions.ResponseError,
            redis.exceptions.ConnectionError,
            redis.exceptions.AuthenticationError,
            redis.exceptions.TimeoutError,
        ) as exc:
            raise self._wrap_falkordb(exc, "create") from exc

        node = result.result_set[0][0]
        return self._node_to_entity(node)

    async def update(self, entity: T) -> T | None:
        """
        Update an existing node's properties.

        Executes ``MATCH (n:{label}) WHERE n.id = $id SET n += $props
        RETURN n`` — ``SET n +=`` merges the given properties onto the
        existing node rather than replacing it wholesale. Returns ``None``
        if no node matched the ``id``.

        Args:
            entity: The entity with updated fields. Must have an ``id`` field.

        Returns:
            The updated entity as stored, or ``None`` if no node matched.

        Raises:
            AdapterConnectionError: FalkorDB is unreachable.
            AdapterQueryError:      Query failed.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        await self._ensure_ready()
        properties = self._entity_to_properties(entity)
        entity_id = str(properties.get("id", ""))

        graph = await self._get_graph()
        query = f"MATCH (n:{self._label}) WHERE n.id = $id SET n += $props RETURN n"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                result = await graph.query(query, {"id": entity_id, "props": properties})
        except (
            asyncio.TimeoutError,
            redis.exceptions.ResponseError,
            redis.exceptions.ConnectionError,
            redis.exceptions.AuthenticationError,
            redis.exceptions.TimeoutError,
        ) as exc:
            raise self._wrap_falkordb(exc, "update") from exc

        if not result.result_set:
            return None
        node = result.result_set[0][0]
        return self._node_to_entity(node)

    async def delete(self, entity_id: str) -> bool:
        """
        Delete a node by its application-level ``id`` property.

        Executes ``MATCH (n:{label}) WHERE n.id = $id DETACH DELETE n`` —
        ``DETACH`` removes incident relationships first, since FalkorDB
        (like most graph backends) rejects deleting a node that still has
        edges. Success is determined from ``QueryResult.nodes_deleted``
        (confirmed to exist on the driver's ``QueryResult`` class), not
        from a ``RETURN`` clause — a deleted node cannot be returned.

        Args:
            entity_id: The node's ``id`` property value.

        Returns:
            ``True`` if a node was deleted, ``False`` if none matched.

        Raises:
            AdapterConnectionError: FalkorDB is unreachable.
            AdapterQueryError:      Query failed.
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        await self._ensure_ready()
        graph = await self._get_graph()
        query = f"MATCH (n:{self._label}) WHERE n.id = $id DETACH DELETE n"
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                result = await graph.query(query, {"id": entity_id})
        except (
            asyncio.TimeoutError,
            redis.exceptions.ResponseError,
            redis.exceptions.ConnectionError,
            redis.exceptions.AuthenticationError,
            redis.exceptions.TimeoutError,
        ) as exc:
            raise self._wrap_falkordb(exc, "delete") from exc

        return result.nodes_deleted > 0

    # ------------------------------------------------------------------
    # BaseGraphStore[T] interface — traverse (also the edge escape hatch)
    # ------------------------------------------------------------------

    async def traverse(self, query: str, params: dict[str, object] | None = None) -> list[T]:
        """
        Execute an arbitrary Cypher query against this repository's graph.

        This is the only way to create, update, or query relationships/
        edges — ``BaseGraphStore`` does not give edges their own method
        (see this module's own docstring and
        ``openframe.core.ports.outbound.repository``'s module docstring
        for the full rationale). Example edge creation::

            await repo.traverse(
                "MATCH (a:Entity) WHERE a.id = $a_id "
                "MATCH (b:Entity) WHERE b.id = $b_id "
                "CREATE (a)-[:KNOWS]->(b)",
                {"a_id": "1", "b_id": "2"},
            )

        Each result row's first column is mapped to a domain entity via
        ``_node_to_entity()`` when it is a FalkorDB ``Node``, or passed
        through unchanged otherwise (e.g. scalar/aggregate ``RETURN``
        clauses). Only the first column is mapped — ``BaseGraphStore``'s
        ``traverse()`` returns ``list[T]``, one entity type, not
        ``list[tuple]``; a multi-column ``RETURN`` should be split into
        several ``traverse()`` calls or post-processed by the caller.

        Args:
            query:  A Cypher query string.
            params: Named parameters substituted into the query. ``None``
                    if the query has no parameters.

        Returns:
            The entities (or raw scalar values) the query's ``RETURN``
            clause produces. Empty list if the query matches nothing.

        Raises:
            AdapterConnectionError: FalkorDB is unreachable.
            AdapterQueryError:      The query is malformed or fails (e.g.
                                    a syntax error, or a constraint
                                    violation on a write).
            AdapterTimeoutError:    Operation exceeded ``operation_timeout``.
        """
        graph = await self._get_graph()
        try:
            async with asyncio.timeout(self._settings.operation_timeout):
                result = await graph.query(query, params or {})
        except (
            asyncio.TimeoutError,
            redis.exceptions.ResponseError,
            redis.exceptions.ConnectionError,
            redis.exceptions.AuthenticationError,
            redis.exceptions.TimeoutError,
        ) as exc:
            raise self._wrap_falkordb(exc, "traverse") from exc

        entities: list[T] = []
        for row in result.result_set:
            if not row:
                continue
            cell = row[0]
            if isinstance(cell, Node):
                entities.append(self._node_to_entity(cell))
            else:
                entities.append(cell)
        return entities

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """
        Close the FalkorDB client and remove it from the cache.

        Call once at application shutdown. After this returns the client
        is closed; a subsequent operation will create a new client.
        """
        client = _client_cache.pop(_cache_key(self._settings), None)
        if client is not None:
            await client.aclose()

    # ------------------------------------------------------------------
    # BasePort (Identity + Lifecycle) interface
    # ------------------------------------------------------------------

    async def initialize(self, context: PluginContext) -> None:
        """
        Establish the FalkorDB client, verify connectivity, and ensure
        the ``(label, id)`` unique constraint exists.

        BasePort lifecycle entry point. Reuses the same cached client as
        every other method on this repository.

        Args:
            context: Plugin context. Unused — settings are provided at
                     construction time.

        Raises:
            AdapterConnectionError: FalkorDB is unreachable.
            AdapterQueryError:      Constraint creation failed for a
                                    reason other than "already exists".
        """
        await get_falkordb_client(self._settings)
        await self._ensure_ready()
        health = await self.health()
        if health.status != PluginStatus.READY:
            raise AdapterConnectionError(
                health.message or "FalkorDB connectivity check failed during initialize()",
                adapter=self._settings.adapter_name,
                operation="initialize",
            )

    async def shutdown(self) -> None:
        """BasePort lifecycle entry point — alias for close(). Never raises."""
        await self.close()

    async def health(self) -> PluginHealth:
        """
        BasePort lifecycle entry point — returns a PluginHealth snapshot.

        The sole connectivity check on this repository — the Redis
        ``PING`` command (via the underlying connection FalkorDB wraps)
        with a 5-second timeout. Never raises.
        """
        try:
            client = await get_falkordb_client(self._settings)
            await asyncio.wait_for(client.connection.ping(), timeout=5.0)
            return PluginHealth(status=PluginStatus.READY, message="")
        except Exception as exc:  # noqa: BLE001
            return PluginHealth(status=PluginStatus.FAILED, message=str(exc))
