"""
Redis caching layer for high-frequency lookups.

Caches:
- Idempotency key checks (TTL-based, avoids hitting Postgres on every message)
- Fraud rule results for duplicate policy lookups
- Pipeline feature flags (lightweight key/value toggles)

Falls back gracefully to direct DB queries when Redis is unavailable —
the circuit breaker protects against cascading failures.
"""

import json
import time
from typing import Optional, Any

import structlog

logger = structlog.get_logger(__name__)

# Try to import redis; if not installed, degrade gracefully
try:
    import redis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False


class RedisCache:
    """Thin wrapper around Redis with TTL, namespacing, and graceful degradation."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        prefix: str = "ins_pipeline",
        default_ttl: int = 300,            # 5 minutes
        socket_timeout: float = 1.0,
        socket_connect_timeout: float = 1.0,
    ):
        self.prefix = prefix
        self.default_ttl = default_ttl
        self._available = False
        self._client = None

        if not _REDIS_AVAILABLE:
            logger.warning("redis package not installed — caching disabled")
            return

        try:
            self._client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_connect_timeout,
                decode_responses=True,
            )
            self._client.ping()
            self._available = True
            logger.info("Redis cache connected", host=host, port=port)
        except Exception as e:
            logger.warning("Redis unavailable — caching disabled", error=str(e))

    @property
    def available(self) -> bool:
        return self._available

    def _key(self, namespace: str, key: str) -> str:
        return f"{self.prefix}:{namespace}:{key}"

    # ------------------------------------------------------------------ #
    #  Core get / set / delete
    # ------------------------------------------------------------------ #
    def get(self, namespace: str, key: str) -> Optional[str]:
        if not self._available:
            return None
        try:
            return self._client.get(self._key(namespace, key))
        except Exception:
            return None

    def get_json(self, namespace: str, key: str) -> Optional[Any]:
        raw = self.get(namespace, key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None

    def set(self, namespace: str, key: str, value: str, ttl: Optional[int] = None):
        if not self._available:
            return
        try:
            self._client.setex(self._key(namespace, key), ttl or self.default_ttl, value)
        except Exception:
            pass

    def set_json(self, namespace: str, key: str, value: Any, ttl: Optional[int] = None):
        self.set(namespace, key, json.dumps(value), ttl)

    def delete(self, namespace: str, key: str):
        if not self._available:
            return
        try:
            self._client.delete(self._key(namespace, key))
        except Exception:
            pass

    def exists(self, namespace: str, key: str) -> bool:
        if not self._available:
            return False
        try:
            return bool(self._client.exists(self._key(namespace, key)))
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    #  Idempotency helpers
    # ------------------------------------------------------------------ #
    def check_idempotency(self, idempotency_key: str) -> bool:
        """Return True if the key was already seen (duplicate)."""
        return self.exists("idem", idempotency_key)

    def mark_idempotency(self, idempotency_key: str, claim_id: str, ttl: int = 86400):
        """Mark an idempotency key as processed (default 24h TTL)."""
        self.set("idem", idempotency_key, claim_id, ttl=ttl)

    # ------------------------------------------------------------------ #
    #  Feature flags
    # ------------------------------------------------------------------ #
    def get_feature_flag(self, flag_name: str, default: bool = False) -> bool:
        """Read a boolean feature flag from Redis."""
        val = self.get("flags", flag_name)
        if val is None:
            return default
        return val.lower() in ("1", "true", "yes", "on")

    def set_feature_flag(self, flag_name: str, enabled: bool):
        """Set a boolean feature flag (no TTL — permanent until changed)."""
        if not self._available:
            return
        try:
            self._client.set(self._key("flags", flag_name), "1" if enabled else "0")
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    #  Stats
    # ------------------------------------------------------------------ #
    def get_stats(self) -> dict:
        """Return Redis server info useful for observability."""
        if not self._available:
            return {"available": False}
        try:
            info = self._client.info(section="stats")
            mem = self._client.info(section="memory")
            return {
                "available": True,
                "connected_clients": info.get("connected_clients", 0),
                "used_memory_human": mem.get("used_memory_human", "?"),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "hit_rate": round(
                    info.get("keyspace_hits", 0)
                    / max(1, info.get("keyspace_hits", 0) + info.get("keyspace_misses", 0)),
                    4,
                ),
            }
        except Exception as e:
            return {"available": False, "error": str(e)}


def _build_cache() -> RedisCache:
    """Build cache from environment config."""
    import os
    return RedisCache(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        password=os.getenv("REDIS_PASSWORD"),
        default_ttl=int(os.getenv("REDIS_DEFAULT_TTL", "300")),
    )


# Module-level singleton — import and use as `from src.observability.cache import cache`
cache = _build_cache()
