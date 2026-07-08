## [postgres 2.0.1 / mongo 2.0.1 / redis 2.0.1 / kafka 1.4.1] - 2026-07-08

### Changed
- Updated all imports from the now-removed `openframe.core.contracts` to
  `openframe.core.ports`, tracking the openframe-core v3.1.0 merge of
  `contracts` into `ports` (no compatibility shim was provided at the old
  path). Pure import-path update — no class names, method signatures, or
  behavior changed.
- Added the (previously missing) `from openframe.core.ports import
  BaseRepository` to `postgres/repository.py` and `from openframe.core.ports
  import BaseConsumer` to `kafka/consumer.py`, matching the pattern already
  present in the other repository/producer modules.
- The existing `openframe-core>=3.0,<4` pin is unchanged — it already
  covers 3.1.0. These are patch releases.

## [postgres 2.0.0 / mongo 2.0.0 / redis 2.0.0 / kafka 1.4.0] - 2026-07-08

### Breaking
- `ping()` and `is_ready()` removed from `PostgresRepository`,
  `MongoRepository`, and `RedisRepository` (deprecated in the prior
  release; this is the removal). `health()` is now the sole health check
  on these classes — it returns a `PluginHealth` snapshot and never
  raises. This is why `postgres`/`mongo`/`redis` bump to **2.0.0**; `kafka`
  is unaffected by this removal and bumps only to **1.4.0**.

### Changed
- `PostgresPlugin`/`MongoPlugin`/`RedisPlugin` `initialize()` and
  `health()` rewritten to delegate to `self._repo.health()` (returns
  `PluginHealth` directly, no bool translation) instead of
  `self._repo.ping()`.
- `PostgresRepository`/`MongoRepository`/`RedisRepository` `initialize()`
  and `health()` now perform their connectivity check directly (`SELECT
  1` / `admin.command("ping")` / Redis `PING`) rather than delegating to
  the removed `ping()`.
- Namespace packaging standardized to PEP 420 implicit namespace packages
  across all four packages — removed the pkgutil-style `__init__.py`
  shims previously present at the intermediate `openframe/`,
  `openframe/adapters/`, and `openframe/adapters/db|queue/` levels (this
  also reverses the pkgutil-style files briefly added to `postgres`/
  `mongo` earlier in the v3 migration).
- `openframe-adapters-queue-kafka`'s `tests/conftest.py` gained the
  "Canonical OTel reset fixtures from openframe-core v3.0" comment,
  matching the other three packages (it never had one before).

### Removed
- `test_health.py` in all three DB packages — they exclusively tested the
  now-removed `ping()`/`is_ready()` methods.
- Now-dead mock scaffolding (`client.info`, `list_collection_names`)
  exclusively used by the removed `is_ready()` in `conftest.py`/
  `test_repository.py`.

## [postgres 1.3.0 / mongo 1.3.0 / redis 1.2.0 / kafka 1.3.0] - 2026-07-08

### Changed
- All four adapters now depend on `openframe-core>=3.0,<4` (was `>=2.0,<3`),
  tracking the ADR-006 unified port/lifecycle contract layer.
- `PluginContext`/`PluginHealth`/`PluginStatus` are now imported from
  `openframe.core.contracts` instead of `openframe.core.plugins` in every
  `plugin.py`.
- `capability` on every plugin class is now a typed
  `openframe.core.contracts.Capability` enum member (`Capability.PERSISTENCE`,
  `Capability.CACHE`, `Capability.QUEUE`) instead of a raw string.
- `PostgresPlugin`, `MongoPlugin`, `RedisPlugin`, and `KafkaPlugin` each
  explicitly declare `BasePort` (`openframe.core.contracts.BasePort`) as
  their base, replacing the old `OpenFramePlugin` protocol.
- `KafkaConsumer`/`KafkaProducer` trace propagation now goes through the
  shared `openframe.core.tracing.propagation` `inject()`/`extract()`
  helpers instead of each adapter holding its own
  `TraceContextTextMapPropagator` instance. `KafkaProducer` gained a
  `_inject_trace_headers()` helper (mirroring the consumer's extraction)
  and now attaches `traceparent` headers on every `publish()`/
  `publish_batch()` call.
- `PostgresRepository`, `MongoRepository`, `RedisRepository`,
  `KafkaProducer`, and `KafkaConsumer` each gained `name`/`version`/
  `capability` plus `initialize()`/`shutdown()`/`health()`, since
  `BaseRepository`/`BaseProducer`/`BaseConsumer` now extend `BasePort` in
  core v3. The new methods delegate to each class's existing connection/
  `ping()`/`close()` logic — no behavioural change to `get`/`list`/
  `create`/`update`/`delete`/`publish`/`subscribe`.
- Test suites for all four packages now inherit `PortContractTests`
  (`openframe.core.testing.contracts`) on their plugin test classes, and
  `RepositoryContractTests`/`ProducerContractTests`/`ConsumerContractTests`
  gained the `port` fixture alias needed now that those contracts require
  full `BasePort` conformance.

### Deprecated
- `ping()`/`is_ready()` on `PostgresRepository`, `MongoRepository`, and
  `RedisRepository` are deprecated in favour of `plugin.health()`, the
  canonical health check in core v3. Kept for this minor version; removal
  is reserved for the next major version of each adapter package.

### Removed
- The dead `openframe.core.health.HealthCheck` import/assertion from
  `mongo`/`redis` `repository.py` and their `test_repository.py` files —
  the class was removed from core v3 (absorbed into `Lifecycle.health()`).

## [1.2.0] - 2026-06-19

### Added
- `MongoPlugin(settings, collection, repository_class=MongoRepository)` —
  new `repository_class` parameter allows registering a domain-specific
  `MongoRepository` subclass through the plugin registry. Previously the
  plugin always constructed the base `MongoRepository`, silently discarding
  any overridden `_doc_to_entity()`/`_entity_to_doc()` on subclasses.
- `PostgresPlugin(..., repository_class=PostgresRepository)` — same fix
  for Postgres.
- `KafkaPlugin(..., producer_class=KafkaProducer)` — same fix for Kafka
  producer serialisation overrides.

### Fixed
- Domain-specific repository/producer subclasses registered via
  `PluginRegistry` were silently replaced with the base adapter class,
  causing `_doc_to_entity()`/`_entity_to_doc()`/`_serialise()` overrides
  to be ignored. Discovered via live Docker validation of a 3-adapter
  service — manifested as `AttributeError: 'dict' object has no attribute
  '...'` when application code expected a typed domain object.
