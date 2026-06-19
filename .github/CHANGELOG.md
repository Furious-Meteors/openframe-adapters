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
