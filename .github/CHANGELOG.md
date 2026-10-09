## [redis 2.0.6] - 2026-10-09

### Fixed
- **`openframe-adapters-db-redis`**'s dependency declaration, `redis[asyncio]>=5.0`, named an extra (`[asyncio]`) that no longer exists in current `redis-py` (confirmed against installed `8.1.0` — the `asyncio` extra was merged into the base package in an earlier release). `pip`/`uv` silently warn rather than fail on an unknown extra, so this never broke installs, but it's misleading packaging metadata — the adapter's own code imports `redis.asyncio` directly (built into the base package), no extra was ever actually needed. Corrected to `redis>=5.0`. Found while auditing `.github/requirements/test.txt` for staleness, not during any functional testing — a pure packaging-metadata fix, no runtime behavior change, hence the patch bump rather than minor.
- Local-dev convenience file `.github/requirements/test.txt` (explicitly not used by CI) carried the same stale `redis[asyncio]` pin — corrected there too.
- 86 tests, all passing (including the version-string assertion test, updated to match).

### Changed
- Bumped `openframe-adapters-db-redis` `2.0.5` → `2.0.6` (patch — dependency-metadata correction only, no behavior change).

---

## [falkordb 0.1.0 / meta 2.7.0] - 2026-10-09

### Added
- **`openframe-adapters-db-falkordb`** (`0.1.0`) — `FalkorDBGraphStore[T]`/`FalkorDBPlugin`, the first adapter built on `BaseGraphStore[T]` (shipped in `openframe-core` v3.6.0; see that repo's own changelog). Native async via `falkordb.asyncio.FalkorDB` (confirmed via `inspect.iscoroutinefunction` against installed `falkordb==1.7.1` — `AsyncGraph.query`/`create_node_range_index`/`create_node_unique_constraint` are all genuine coroutines; the client wraps `redis-py` directly, confirmed via `inspect.getsource`).
- **Addressing scheme, documented explicitly in the README and `connection.py`'s docstring**: nodes are addressed by an application-level `id` property with a backing unique constraint this adapter creates itself (lazily on first CRUD call, eagerly from `initialize()`) — never FalkorDB's own internal `id()` function, which is documented upstream as unstable (deleted nodes' higher internal ids can be migrated down to fill vacated lower ones). The Cypher node label is configurable via `Settings` and validated as a safe identifier at config-load time (not per-query), since Cypher doesn't support parameterized labels and this value gets string-interpolated into every query — a real injection-shaped risk if left unvalidated, verified with a hard regression check (disabled the validator, confirmed 3 tests failed including an actual injection payload, restored, confirmed green).
- **Exception classification found via direct source-reading, not assumption**: `falkordb.exceptions` defines no query-error type of its own — `GRAPH.QUERY` is itself a Redis command, so Cypher-level failures (syntax errors, constraint violations) surface as a plain `redis.exceptions.ResponseError`; connection-level failures (`ConnectionError`/`AuthenticationError`/`TimeoutError`) are the same ones plain `redis-py` raises, since the async client constructs a `redis.Redis` connection internally.
- **`list()` adds an explicit `ORDER BY n.id`** beyond the originally-planned query shape — Cypher gives no ordering guarantee without one, which `RepositoryContractTests`' offset/pagination assertions require deterministically. `delete()`'s success signal comes from `QueryResult.nodes_deleted` rather than a post-delete `RETURN`, since a deleted node can't be returned.
- `traverse()` is a thin passthrough to `graph.query(query, params)`, mapping `Node` cells to the domain entity via an overridable `_node_to_entity()` (matching the `_row_to_entity` override convention every SQL-family adapter uses) — documented as this adapter's only way to create or query relationships/edges, since `BaseGraphStore` deliberately gives edges no method of their own.
- `Capability.SEARCH` reused (no new taxonomy member), same precedent as the vector-store adapters.
- 111 tests, all passing. Two regressions verified the hard way: the label-injection validator (disabled → 3 failures including an actual injection payload → restored → green) and the connection-vs-query exception split (disabled the connection-class branch → 2 failures → restored → green).
- Followed `docs/adapter-checklist.md` in full, including CI/CD.
- FalkorDB was one of the original 15 advertised packages — `meta/openframe-adapters/pyproject.toml`'s placeholder `falkordb` pin corrected to `>=0.1,<1`. Top-level `README.md`'s `falkordb` inventory row's async-style column fixed (said `executor`, actually `native`); `meta/openframe-adapters/README.md`'s row was already correct.
- Full ecosystem test count after this package: **1,781** (`pytest packages/` from the repo root).

### Caveat
- **The unique-constraint idempotency behavior (does `GRAPH.CONSTRAINT CREATE` no-op or raise when the constraint already exists?) is based on source-reading and defensive error-message matching, not an observed live-server response** — this sandbox had no real FalkorDB server to test against, per this session's zero-real-network-calls testing convention. `_ensure_ready()` defensively swallows `ResponseError`s whose message contains "already" (case-insensitive) and re-raises anything else. **Recommend a live-server smoke test before this adapter is used in production.**

### Changed
- Bumped meta package (`openframe-adapters`) to `2.7.0` (minor — one extra that previously pointed at a placeholder pin for a nonexistent package now installs a real, working adapter).

---

## [qdrant 0.1.0 / milvus 0.1.0 / chromadb 0.1.0 / meta 2.6.0] - 2026-10-09

### Added
- **Wave F, the first real adapters built on `BaseVectorStore[T]`** (shipped in `openframe-core` v3.5.0 as F.0, see that repo's own changelog) — three packages, built via three parallel subagents, each verifying its driver's async support directly against the installed package rather than assuming it:
  - **`openframe-adapters-db-qdrant`** (`0.1.0`) — `QdrantVectorStore[T]`/`QdrantPlugin`, native async via `qdrant_client.AsyncQdrantClient` (confirmed via `inspect.iscoroutinefunction` against the installed `qdrant-client==1.19.1`). Found `AsyncQdrantClient.search()` doesn't exist in this version — replaced by `query_points()`, whose `QueryResponse.points` is the actual result list; `search()` is implemented against that. `list()`'s `offset` is honestly documented as a correctness-preserving but non-performant shim, since Qdrant's own `scroll()` offset is a point-id cursor, not an integer skip-count — same documented-mismatch pattern InfluxDB used for its own offset semantics. Exception classification splits on `ResponseHandlingException` (transport-class → `AdapterConnectionError`) vs `UnexpectedResponse` (in-band HTTP, 4xx → `AdapterQueryError`, 5xx → retryable `AdapterConnectionError`). 114 tests, all passing.
  - **`openframe-adapters-db-milvus`** (`0.1.0`) — `MilvusRepository[T]`/`MilvusPlugin`, native async via `pymilvus.AsyncMilvusClient` (confirmed via `inspect.signature`/`inspect.getsource` against installed `pymilvus==3.0.2` — every CRUD+search method is a genuine coroutine). Found that real connection failures surface as a bare, undifferentiated `MilvusException(code=2, ...)` rather than the documented `ConnectError`/`MilvusUnavailableException` subclasses (which exist but were never observed reachable from the async client in this version) — handled with a message-phrase fallback matcher in addition to the documented subclasses, verified red-then-green. `update()` does a pre-`get()` existence check since Milvus's `upsert()` doesn't report whether it created or replaced a row, needed to satisfy `BaseRepository`'s "update returns None if missing" contract. 114 tests, all passing.
  - **`openframe-adapters-db-chromadb`** (`0.1.0`) — `ChromaDBVectorStore[T]`/`ChromaDBPlugin`, native async via `chromadb.AsyncHttpClient` (confirmed via `inspect.iscoroutinefunction` against installed `chromadb==1.5.9`). The most structurally unusual of the three: Chroma's wire format is columnar (`GetResult`/`QueryResult` return parallel lists, not row objects), so `repository.py` has dedicated transpose helpers (`_get_result_to_rows`/`_query_result_to_rows`) with their own direct unit tests. `create()` deliberately uses `collection.add()` (fails on duplicate id) rather than the idempotent `collection.upsert()`, to preserve `BaseRepository.create()`'s non-idempotent contract — verified with a real regression check (swapped to `upsert()`, confirmed the duplicate-id test went red, reverted). Exception classification checks `httpx.TimeoutException` before `httpx.TransportError` deliberately, since the former subclasses the latter. 111 tests, all passing.
  - All three packages were among the **original 15 advertised but unimplemented packages** — `meta/openframe-adapters/pyproject.toml`'s placeholder pins (`>=1.1,<2`) for all three corrected to `>=0.1,<1`, not duplicated. Top-level and meta READMEs' inventory rows corrected where stale (milvus's async-style column was wrong — said `executor`, actually `native` — and its env-var column named a nonexistent `MILVUS_HOST/PORT/COLLECTION` scheme instead of the real `MILVUS_URI/TOKEN/DB_NAME`).
  - Three concurrent subagents, each touching the same shared CI/CD and meta/README files, merged additively with zero conflicts — each re-read the shared files fresh immediately before editing, per the pattern established in Wave D.
  - Each independently followed `docs/adapter-checklist.md` in full, including CI/CD and the pythonpath/extraPaths maintenance item, and each verified the root-level `pytest packages/` mechanism actually works with their package installed into the shared root `.venv`, not just that the config edits look right.
  - **FAISS and FalkorDB remain explicitly out of scope** for this wave, per the earlier descoping decision recorded in the build plan — not part of this count, to be revisited separately.
  - Full ecosystem test count after this wave: **1,670** (`pytest packages/` from the repo root).

### Changed
- Bumped meta package (`openframe-adapters`) to `2.6.0` (minor — three extras that previously pointed at placeholder pins for nonexistent packages now install real, working adapters, same precedent as prior waves' placeholder-to-real corrections).

---

## [influxdb 0.1.0 / meta 2.5.0] - 2026-10-08

### Added
- **`openframe-adapters-db-influxdb`** (new package, `0.1.0`) — `InfluxDBRepository[T]`/`InfluxDBPlugin`, built on `influxdb-client[async]`'s `InfluxDBClientAsync` (confirmed via direct inspection to be genuinely `aiohttp`-backed end-to-end, not a sync-wrapped facade — the bare `influxdb-client` install doesn't even expose the async client without the `[async]` extra, which is now pinned explicitly). Found and handled a real gotcha: the driver's async request path never wraps `aiohttp`'s own connection-level exceptions (`ClientConnectorError`, `ServerTimeoutError`, the whole `aiohttp.ClientError` family) into its own `ApiException` — they propagate raw and would have escaped the adapter boundary untranslated had they not been caught explicitly.
- **Documents the CRUD/time-series impedance mismatch honestly, in both the module docstring and a dedicated README section**, rather than silently forcing InfluxDB's point-based model into `BaseRepository[T]`'s row-based shape: `get()` has no natural primary-key equivalent (interpreted as "most recent point matching this tag, within a bounded lookback window" — stated as a design choice, not the only valid one); `list(limit, offset)`'s `offset` is an explicit compatibility shim (no server-side cursor, skips in Python) with a pointer to the raw-client escape hatch for real time-range queries; `update()` is **not a real update** — it's documented as writing a new point with an identical tag set and timestamp, which InfluxDB's storage engine resolves as last-write-wins overwrite, not a row-level UPDATE; `delete()` uses the real predicate/time-range delete API, coarser-grained than row delete.
- `Capability.PERSISTENCE` reused (no new taxonomy member) per the decision already recorded in the build plan — one-sentence rationale in `plugin.py`'s docstring, and the meta README's `Capability.PERSISTENCE` taxonomy table/paragraph corrected to actually list InfluxDB under it (previously said InfluxDB had no assigned capability at all, which was stale).
- 107 tests, all passing; the `aiohttp.ClientError`/`OSError` connection-classification regression verified red-then-green.
- Followed `docs/adapter-checklist.md` in full, including CI/CD.

### Fixed
- **A real, repo-wide gap found by chance, not by the checklist**: root `pyproject.toml`'s `[tool.pytest.ini_options]` `pythonpath` list and root `pyrightconfig.json`'s `extraPaths` list — both intended to let the ENTIRE monorepo's tests run in one `pytest packages/` invocation from the repo root, distinct from the per-package CI matrix — had never been updated for any of the 9 packages built since Wave A (nats, rabbitmq, cockroachdb, mysql, mariadb, oracle, dynamodb, cassandra; only postgres/mongo/redis/kafka were listed). Found only because the InfluxDB build's own report prompted a direct check rather than assuming the config was self-evidently fine. Fixed both lists to include all 13 real packages, then **actually ran `pytest packages/` from the repo root to verify the mechanism works** (not just that the config is syntactically valid) — 1,331 tests collected and passed in one invocation, matching the sum of all 13 packages' individually-verified counts exactly.
- **While verifying the above, found and fixed a real test-isolation bug in `openframe-adapters-queue-rabbitmq`**: `test_consumer.py`/`test_consumer_propagation.py` used `from conftest import _make_mock_queue_iterator` — a bare import that only works under pytest's default/prepend import mode (which inserts the test file's directory onto `sys.path` as a side effect) and silently breaks under `--import-mode=importlib` (the mode the root `pyproject.toml` uses for cross-package runs, to avoid "import file mismatch" errors when multiple packages share identically-named test files). Fixed by converting the helper into a `@pytest.fixture`, matching the Kafka package's own `conftest.py` (RabbitMQ's actual template), which never used a bare importable helper in the first place. This was a real latent bug in a previously-"all green" package — its own isolated per-package venv run never exercised the import mode that broke it. Re-verified: 128/128 still passing in RabbitMQ's own venv after the fix, plus the full 1,331-test root run.
- Added a new checklist section to `docs/adapter-checklist.md` covering both the `pythonpath`/`extraPaths` maintenance item and the "shared test helpers must be fixtures, never bare importable names" lesson, so this doesn't recur for Wave F and beyond.

### Changed
- Bumped meta package (`openframe-adapters`) to `2.5.0` (minor — one new extra now installs a working adapter).

---

## [dynamodb 0.1.0 / cassandra 0.1.0 / meta 2.4.0] - 2026-10-08

### Added
- **`openframe-adapters-db-dynamodb`** (new package, `0.1.0`) — `DynamoDBRepository[T]`/`DynamoDBPlugin`, built on `aioboto3`'s **resource** interface (`Table` objects with dict-based `get_item`/`put_item`/`delete_item`/`query`/`scan`), not the lower-level client interface's verbose attribute-value maps. Research spike confirmed `aiobotocore` (which `aioboto3` delegates to) genuinely opens real `aiohttp.ClientSession` objects — not a thread pool wrapping blocking `boto3` calls — so the ecosystem's existing "wrapper" classification for this driver is about API shape, not fake I/O. Also confirmed empirically that resource creation is lazy (no real network call until the first operation). `connection.py`'s cache holds a session/resource/`Table` object, explicitly documented as not a TCP connection pool — DynamoDB has no such concept, and the docstring says so rather than forcing Postgres's pooling model where it doesn't fit. Throttling errors (`ProvisionedThroughputExceededException`, `ThrottlingException`, `RequestLimitExceeded`) classified as retryable `AdapterConnectionError` since this ecosystem's taxonomy has no dedicated "throttled" type and retryability is what callers actually need. 103 tests, all passing.
- **`openframe-adapters-db-cassandra`** (new package, `0.1.0`) — `CassandraRepository[T]`/`CassandraPlugin`. The most substantial async-strategy research of any package so far: `cassandra-driver` is fundamentally synchronous, but `Session.execute_async()` returns a `ResponseFuture` with a real `add_callbacks()` method — verified directly (not assumed) with a scripted reproduction bridging it to `asyncio.Future` via `loop.call_soon_threadsafe()`, confirmed thread-safe on both the success and error paths. Landed on a **hybrid strategy**: one-time session/cluster creation wrapped in `run_in_executor` (genuinely blocking, no alternative), but every query goes through the callback-bridge — no thread-pool thread held for a query's duration, unlike naive `run_in_executor(None, session.execute, ...)` on every call. Also found that `cassandra.cluster.NoHostAvailable`/`ConnectionException` are **not** subclasses of `cassandra.DriverException` (the same class of gotcha as `asyncpg.InterfaceError`/`oracledb.InterfaceError` found in earlier packages) — caught explicitly rather than assumed covered. Added a third exception bucket, `AdapterTimeoutError`, for `OperationTimedOut`/`ReadTimeout`/`WriteTimeout` rather than forcing the connection/query binary where it didn't fit — a deliberate, documented deviation from the checklist's simpler framing, not an oversight. 104 tests, all passing.
- Both packages are among the **original 15 advertised but unimplemented packages** — `meta/openframe-adapters/pyproject.toml` already had placeholder pins (`>=1.1,<2`) for both; corrected to `>=0.1,<1` rather than adding new lines. Both top-level README inventory rows already existed; the `dynamodb` row's driver name was corrected from an inconsistent `aiobotocore` to `aioboto3` (matching the package's actual direct dependency), and the `cassandra` row's async-style column corrected from `executor` to `hybrid (callback-bridge + executor connect)` to reflect the actual implementation.
- Two concurrent agents built these in parallel and both touched the same shared files (CI workflows, meta `pyproject.toml`, both READMEs) without disturbing each other's lines — verified directly after both completed, zero corruption, matching the pattern from Wave B.
- Both followed `docs/adapter-checklist.md` in full, including CI/CD — sixth and seventh packages in a row to get it right on the first attempt.

### Changed
- Bumped meta package (`openframe-adapters`) to `2.4.0` (minor — two extras that previously pointed at placeholder pins for nonexistent packages now install real, working adapters — same precedent as the Wave A nats/rabbitmq bump).

---

## [oracle 0.1.0 / meta 2.3.0] - 2026-09-28

### Added
- **`openframe-adapters-db-oracle`** (new package, `0.1.0`) — `OracleRepository[T]`/`OraclePlugin`, built on `python-oracledb` (import name `oracledb`). Preceded by a genuine research spike, not an assumption: installed `oracledb` fresh (resolved to `26.0.1`) and directly verified it exposes a real, documented native-async surface in thin mode — `connect_async`, `create_pool_async`, `AsyncConnection`, `AsyncCursor` — before deciding to build this as a native-async adapter (matching Postgres/MySQL's shape) rather than falling back to `run_in_executor` per ADR-003's per-adapter decision principle. The module docstring documents the counterfactual honestly: if this async surface hadn't existed, the correct move would have been the executor-wrapped fallback planned for Cassandra.
- Two real driver gotchas found and empirically verified against the installed driver (not guessed):
  - DNS resolution failures escape as a raw `socket.gaierror` (an `OSError`), not `oracledb.Error`, from both `connect()` and `connect_async()` — caught explicitly at every connection boundary.
  - `oracledb.InterfaceError` is **not** a subclass of `oracledb.DatabaseError` (confirmed via `__mro__`) — the same class of gotcha as `asyncpg.InterfaceError` escaping `asyncpg.PostgresError`, found earlier this session. Every `_wrap_oracledb()` call site explicitly catches `(DatabaseError, InterfaceError, OSError)`.
  - ORA/DPY connection-class error codes (DPY-6005, DPY-4027, ORA-03113/03114) were pulled directly from the installed driver's own `oracledb.errors` module, not from memory. The TNS/listener codes (ORA-12541/12154/12170/12537) are included but honestly flagged as standard documented Oracle knowledge, **not** empirically verified against a live Oracle server (none was available) — stated explicitly in both code comments and the README rather than glossed over as equally verified.
- 87 tests, all passing; both the connection-error classification and the pool-cache-key regression verified red-then-green.
- Followed `docs/adapter-checklist.md` in full, including CI/CD (fourth package in a row to get this right on the first attempt since the section was added).

### Changed
- Bumped meta package (`openframe-adapters`) to `2.3.0` (minor — one new extra, `oracle`, now installs a working adapter). Adapter-count callouts updated 9/17 → 10/18 in both top-level and meta `README.md`.

---

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
