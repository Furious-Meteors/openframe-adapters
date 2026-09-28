# openframe-adapters-db-redis

Redis key-value adapter for the **OpenFrame Microservice Development Suite**.

Part of the [`openframe-adapters`](https://github.com/Furious-Meteors/openframe-adapters) monorepo.

---

## What it provides

| Symbol | Purpose |
|---|---|
| `RedisSettings` | Pydantic-settings subclass — reads all config from env vars |
| `RedisRepository[T]` | Generic async repository — `BaseRepository[T]` + `HealthCheck` |
| `get_redis_client()` | Async factory — creates and caches the `redis.asyncio.Redis` client |
| `RedisPlugin` | `BasePort` (Identity + Lifecycle) — registered via `PluginRegistry`/`ApplicationBootstrap` |

## Installation

```bash
# Via meta-package (recommended)
pip install "openframe-adapters[redis]"

# Or directly
pip install openframe-adapters-db-redis
```

## Quick start

```python
from openframe.adapters.db.redis import RedisSettings, RedisRepository

settings = RedisSettings(redis_url="redis://localhost:6379/0")
repo = RedisRepository(settings)

# Store and retrieve
await repo.create({"id": "user-1", "name": "Alice"})
user = await repo.get("user-1")         # {"id": "user-1", "name": "Alice"}
await repo.update({"id": "user-1", "name": "Alice B."})
await repo.delete("user-1")             # True
```

## Configuration

| Env var | Default | Description |
|---|---|---|
| `REDIS_URL` | **required** | `redis://[:password@]host[:port][/db]` or `rediss://` for TLS |
| `REDIS_MAX_CONNECTIONS` | `10` | Max connections in pool |
| `REDIS_SOCKET_TIMEOUT` | `5.0` | Socket read/write timeout (seconds) |
| `REDIS_SOCKET_CONNECT_TIMEOUT` | `5.0` | Connection timeout (seconds) |
| `REDIS_KEY_PREFIX` | `"openframe"` | Key namespace — keys stored as `{prefix}:{id}` |
| `REDIS_DEFAULT_TTL` | `0` | TTL in seconds; `0` = no expiry |

## Typed domain objects

```python
from dataclasses import dataclass
from openframe.adapters.db.redis import RedisRepository, RedisSettings

@dataclass
class Session:
    id: str
    user_id: str
    token: str

class SessionRepository(RedisRepository[Session]):
    def _dict_to_entity(self, data: dict) -> Session:
        return Session(**data)
    def _entity_to_dict(self, entity: Session) -> dict:
        return {"id": entity.id, "user_id": entity.user_id, "token": entity.token}
```

## Plugin lifecycle (optional)

```python
from openframe.core.runtime import ApplicationBootstrap
from openframe.core.ports import Capability
from openframe.adapters.db.redis import RedisPlugin, RedisSettings

async with ApplicationBootstrap.compose(RedisPlugin(RedisSettings())) as app:
    repo = app.get(Capability.CACHE)
```

`ApplicationBootstrap.compose()` is the recommended entry point for wiring
one or a few ports — no `config=`/`init_timeout=` boilerplate needed. Reach
for a `configure()` subclass only when you need per-port config/timeouts or
conditional registration order, and for `app.registry` (the underlying
`PluginRegistry`) only for what neither tier covers.

## License

MIT — © Furious Meteors Engineering
