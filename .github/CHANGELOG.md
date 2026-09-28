## [postgres 2.0.3 / mongo 2.0.3 / redis 2.0.4 / kafka 1.4.4] - 2026-09-28

### Fixed
- `PostgresRepository` (postgres only) — `get`/`list`/`create`/`update`/`delete`
  no longer misclassify a connection lost mid-query as `AdapterQueryError`.
  Added a `_wrap_asyncpg()` helper (mirroring `MongoRepository`'s/
  `RedisRepository`'s existing connection-vs-query distinction) that
  correctly raises retryable `AdapterConnectionError` for
  `ConnectionDoesNotExistError`/`ConnectionFailureError`/`InterfaceError`/
  `TooManyConnectionsError`. `PostgresRepository.version` bumped `1.3.0`
  → `1.3.1` to reflect the logic change (package version tracks the
  package as a whole; this class's own `version` attribute tracks the
  last version its own logic changed, per existing convention).
- Two stale `OpenFramePlugin` references (redis, kafka `README.md`) —
  `OpenFramePlugin` was deleted from `openframe-core` in v3.0.0 (ADR-006:
  "a plugin is just a registered `BasePort`"); corrected to describe
  `RedisPlugin`/`KafkaPlugin` as `BasePort` (Identity + Lifecycle)
  implementations instead.
- Broken `.../developer-guide/composition-root/` doc link (all four
  `plugin.py` module-level comments, plus the meta package's own
  `README.md`) — the actual `openframe-core` guide lives at
  `developer-guide/how-it-works/#choosing-a-wiring-pattern`; corrected.

### Changed
- **Adopted `openframe-core` v3.3.0's `ApplicationBootstrap.compose()`
  wiring convention across all four packages and the meta package**,
  replacing `PluginRegistry` direct construction and `deps.py`+`lru_cache`
  examples — both are no longer documented as peer alternatives upstream
  (see `openframe-core`'s own v3.3.0 changelog: "Add ApplicationBootstrap.compose()/get_all()/registry").
  - Added or rewrote a wiring example in every package's `README.md` and
    every `plugin.py` module docstring, based on each package's `*Plugin`
    class (not the raw repository/producer), showing real
    `initialize()`/`health()`/`shutdown()` lifecycle via
    `async with ApplicationBootstrap.compose(...) as app:`.
  - Meta package `README.md`'s "Wiring adapters into your service"
    section — the most substantial rewrite: replaced the entire
    "Stage 1 (`lru_cache`) → Stage 2 (`PluginRegistry`)" upgrade-path
    model with the "`compose()` → `configure()` subclass → `.registry`
    escape hatch" model. Also fixed 4 examples using raw string capability
    keys (`registry.get("persistence")`, `registry.get("cache")`) instead
    of the typed `Capability` enum members those examples were otherwise
    demonstrating correctly elsewhere in the same file — a real,
    independent bug in the example code, not just wiring-pattern
    staleness.
  - Top-level `README.md`'s "Wiring in deps.py" section retitled and
    rewritten to the same convention.
- **Dependency floor**: all four packages' `openframe-core` pin tightened
  from `>=3.0,<4` to `>=3.3,<4`, since each package's own docs now show a
  `compose()` example that requires 3.3+. Verified meaningful (not just
  cosmetic) by confirming `compose`/`get_all`/`registry` genuinely don't
  exist on `openframe-core==3.2.1` before re-testing against a local
  3.3.0 install.
- Meta package (`openframe-adapters`) bumped to `2.0.3` — no dependency
  version pins changed (extras still pin sub-packages by major range,
  e.g. `openframe-adapters-db-postgres>=2.0,<3`, which already covers the
  2.0.3 patch releases above), bumped for its own substantial `README.md`
  rewrite.

### Verification
- All four packages reinstalled against a local editable `openframe-core`
  3.3.0 build (not yet on the package index) to confirm the new examples
  actually work, not just read correctly. Full suite re-run after every
  change: **383 tests passed** (94 postgres + 109 mongo + 85 redis + 95
  kafka), zero failures, across the version bump, the docstring fixes, and
  the meta README rewrite.

## [redis 2.0.3 / kafka 1.4.3] - 2026-07-08

### Added
- `RedisPlugin(settings, repository_class=RedisRepository)` — new
  `repository_class` parameter, mirroring `PostgresPlugin`/`MongoPlugin`.
  `get_repository()` now returns the domain subclass passed in rather
  than always constructing the plain base `RedisRepository`.
- `KafkaPlugin(..., consumer_class=KafkaConsumer)` — new `consumer_class`
  parameter, mirroring the existing `producer_class` pattern.
  `make_consumer()` now returns the configured subclass rather than
  always constructing a plain `KafkaConsumer`.

Both gaps surfaced during the `ApplicationBootstrap` migration of all 7
validation framework services. Additive-only — default behaviour for
callers that don't pass the new parameter is unchanged.

### Other
- Meta package (`openframe-adapters`) bumped to `2.0.2`.
- `.github/workflows/auto-docs.yml` disabled (renamed to `.disabled`) —
  `docs/` has no `mkdocs.yml` or markdown content yet, so the build step
  had nothing to build.

## [postgres 2.0.2 / mongo 2.0.2 / redis 2.0.2 / kafka 1.4.2] - 2026-07-08

### Changed
- Docstring cleanup only — no logic, test, or public API changes.
- Fixed 7 raw-string `registry.get("persistence"|"cache"|"queue")`
  examples across all four `plugin.py` docstrings to use the typed
  `registry.get(Capability.X)` form, with the matching
  `from openframe.core.ports import Capability` import shown in each
  example.
- Replaced the stale "`ping()` and `is_ready()` were removed in v2.0;
  use `health()`" sentence in the three DB `repository.py` class
  docstrings with a clean description of what `health()` returns —
  the migration is complete and the sentence had no remaining audience.

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
