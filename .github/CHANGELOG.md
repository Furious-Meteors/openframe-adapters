## [cockroachdb 0.1.0 / mysql 0.1.0 / mariadb 0.1.0 / meta 2.2.0] - 2026-09-28

### Added
- **`openframe-adapters-db-cockroachdb`** (new package, `0.1.0`) — `CockroachdbRepository[T]`/`CockroachdbPlugin`, an adaptation of `openframe-adapters-db-postgres` (same `asyncpg` driver — CockroachDB speaks the Postgres wire protocol). Documents two real dialect/semantics differences rather than assuming 100% Postgres parity: no `SERIAL` type (use `gen_random_uuid()`/`unique_rowid()` instead), and default `SERIALIZABLE` isolation requiring client-side retry on `SQLSTATE 40001` for any multi-statement transaction a caller builds via raw `asyncpg` access — documented, not silently auto-retried inside the adapter. 96 tests, all passing; same-URL-different-pool-config cache regression verified red-then-green.
- **`openframe-adapters-db-mysql`** (new package, `0.1.0`) — `MySQLRepository[T]`/`MySQLPlugin`, built on `aiomysql`/`pymysql`. Investigated and correctly resolved a real classification gotcha: `pymysql.err.OperationalError` is used for BOTH genuine connection failures (codes 2002/2003/2006/2013) AND non-connection issues like lock-wait-timeout (code 1205) — a naive `isinstance` check would misclassify the latter as a connection failure. `_wrap_pymysql()` inspects the actual numeric error code (`exc.args[0]`) rather than just the exception class. Verified `pymysql.err.MySQLError` (unlike `asyncpg.PostgresError`) genuinely is the common base of every driver exception, so a single `except MySQLError` per call site is sufficient — confirmed against the installed package, not assumed. 101 tests, all passing; lock-wait-timeout misclassification regression verified red-then-green.
- **`openframe-adapters-db-mariadb`** (new package, `0.1.0`) — `MariadbRepository[T]`/`MariadbPlugin`, an adaptation of the MySQL package (same `aiomysql` driver, same error-code classification — MariaDB and MySQL share the pre-fork wire protocol and error codes this adapter relies on). Deliberately kept the `database_url`/`DATABASE_URL` field name (not `mariadb_url`) so a service can migrate between MySQL and MariaDB backends by swapping only the installed package, zero config changes — documented as an explicit decision, not an oversight. Also documents (without code changes) that MySQL now defaults to `caching_sha2_password` auth while MariaDB has historically defaulted to `mysql_native_password`/`ed25519`/`unix_socket`, in case of auth-negotiation issues. 101 tests, all passing.
- All three packages follow `docs/adapter-checklist.md` in full, including its CI/CD section — added to both `.github/workflows/app-test.yml` and `python-build.yml`'s matrices, and to the meta package's `[project.optional-dependencies]`, on the first attempt this time (no follow-up gap, unlike the nats/rabbitmq release).
- Three concurrent agents built these packages in parallel and all three touched the same 5 shared files (both CI workflows, meta `pyproject.toml`, both top-level `README.md`s) — each re-read the current file state immediately before editing rather than assuming stale content, so no edits were lost or clobbered. Verified directly after all three completed: both workflow matrices and the meta package's extras all contain clean, complete entries for `cockroachdb`, `mysql`, and `mariadb` with no corruption.

### Changed
- Bumped meta package (`openframe-adapters`) to `2.2.0` (minor — three new extras that previously installed nothing real now install working adapters). Top-level and meta `README.md`'s adapter-count callouts updated from 8/16 (after the CockroachDB addition) to 9/17 (after MariaDB, the last of the three to land).

---

## [nats 0.1.0 / rabbitmq 0.1.0 / meta 2.1.0] - 2026-09-28

### Added
- **`openframe-adapters-queue-nats`** (new package, `0.1.0`) — `NatsProducer[T]`/`NatsConsumer[T]`/`NatsPlugin`, built against JetStream (not core NATS) specifically because core NATS pub/sub has no ack/nack/persistence — `BaseConsumer.ack()`/`.nack()` would be meaningless without it. Uses `manual_ack=True` durable consumers for real at-least-once semantics matching what `KafkaConsumer` provides via manual offset commit. New `errors.py` module holds a shared `wrap_nats()` classification helper (Kafka's single-file-per-role structure didn't need one since it only has two call sites; NATS producer+consumer share it to avoid duplicating the connection-error tuple). Exception hierarchy (`nats.errors.*`/`nats.js.errors.*`) verified against the actually-installed driver, not guessed. 117 tests, all passing; two real regressions (connection-vs-query misclassification, plugin discarding the injected subclass) verified red-then-green.
- **`openframe-adapters-queue-rabbitmq`** (new package, `0.1.0`) — `RabbitmqProducer[T]`/`RabbitmqConsumer[T]`/`RabbitmqPlugin`, built on `aio-pika`'s `connect_robust()` (auto-reconnecting, fully async-native). Consumer uses `queue.iterator()` so `subscribe()` keeps the same `async for msg in ...` polling shape as Kafka. Found and regression-tested a genuine `_wrap_rabbitmq()` gotcha analogous to Postgres's `asyncpg.InterfaceError` issue: `connect_robust()` can raise a raw `OSError` before `aio_pika` wraps it in an `AMQPConnectionError` — confirmed by reading `aio_pika`'s own `CONNECTION_EXCEPTIONS` tuple in its source, which explicitly includes `OSError` for the same reason. 128 tests, all passing; verified red-then-green the same way as NATS.
- Both packages follow `docs/adapter-checklist.md` in full — first real proof the checklist holds up against genuinely new drivers, not just the four packages it was extracted from.

### Fixed
- Meta package (`openframe-adapters`) — the `nats`/`rabbitmq` extras had placeholder pins (`>=1.1,<2`) left over from before either package existed for real; corrected to `>=0.1,<1` to match the packages' actual starting version. Bumped meta package to `2.1.0` (minor — two extras that previously installed nothing real now install working adapters, a meaningful new capability from the meta package's own consumer-facing perspective).
- **`.github/workflows/app-test.yml` and `python-build.yml` didn't include either new package** — both hardcode an explicit package matrix, and neither had been updated when the packages were added, so CI would have silently never run their tests, and `python-build.yml` would never have built or published them to PyPI. Caught only because the user asked directly whether the workflows had been updated — not caught by the checklist, which had no CI/CD section. Fixed both matrices; added a new "CI/CD" section to `docs/adapter-checklist.md` (repo-wide files, easy to forget since they live outside the new package's own directory) so this doesn't recur for future packages.

---

## [postgres 2.0.4 / mongo 2.0.4 / redis 2.0.5 / kafka 1.4.5] - 2026-09-28

### Fixed
- **Connection-cache correctness (postgres, mongo, redis)** — the
  module-level pool/client cache in each package's `connection.py`
  (`_pool_cache`/`_client_cache`) was keyed only by connection URL. Two
  `Settings` instances for the same URL but different pool sizing
  (`pool_size`, `mongo_max_pool_size`, `redis_max_connections`, etc.)
  silently shared one pool/client — the second caller's pool
  configuration was discarded without warning. A real footgun for any
  multi-tenant or multi-config deployment. Fixed by widening the cache
  key to `(url, pool-config-tuple)` via a new `_cache_key()` helper in
  each `connection.py`, exported and reused by the repository's `close()`
  and every test that injects into the cache directly. Same URL + same
  config still hits the cache exactly as before (no behavior change for
  the common case); same URL + different config now gets its own
  pool/client instead of silently inheriting the first one's.
- **Kafka `nack()` fragility** — `nack()` does nothing but log; its
  correctness depends entirely on `subscribe()`'s hardcoded
  `enable_auto_commit=False` never changing, and nothing tested that
  invariant directly (only that *this adapter* never calls `commit()`
  itself after a handler failure — which would still pass even if
  auto-commit were silently enabled at the client level). Added a
  code comment cross-referencing the coupling at both the flag and at
  `nack()`'s own docstring, plus a regression test asserting
  `enable_auto_commit=False` is actually passed to the `AIOKafkaConsumer`
  constructor. Verified the new test fails on a temporarily-reverted
  version of the flag and passes once restored.

### Added
- **`docs/adapter-checklist.md`** — the concrete robustness checklist
  every adapter package in this repo follows, extracted from the two
  fixes above plus prior fixes this release cycle (Postgres's
  connection/query exception misclassification, the `RedisPlugin`/
  `KafkaPlugin` domain-subclass-injection gaps). Each item cites the
  specific real bug it prevents rather than reading as abstract best
  practice. Linked from the top-level `README.md`'s new "Adapter
  development" section. Will serve as the spec for a later, separate
  effort to build out the 11 other advertised-but-unimplemented adapter
  packages.
- **Resilience documentation** — both `README.md` (top-level and meta
  package) now document `openframe-core>=3.4`'s new
  `openframe.core.resilience.CircuitBreakerProxy` as an optional
  composition-root addition (wrap the traced repository, not the
  reverse, so a short-circuited call never produces a misleading adapter
  span). No adapter code changes required — it's an opt-in wrapper, like
  `TracingProxy`.
- Corrected the meta package's `README.md` "Core dependency" section,
  which still said `openframe-core>=3.0,<4` — stale since the pin was
  actually tightened to `>=3.3,<4` in the prior release.

---

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
