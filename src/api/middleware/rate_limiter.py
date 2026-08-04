"""
Token Bucket Rate Limiting Middleware.

Enforces per-client rate limits and burst capacities using a token bucket algorithm.
"""

from __future__ import annotations

import time
import structlog
from typing import Dict, Tuple

logger = structlog.get_logger(__name__)


class TokenBucketRateLimiter:
    """Token bucket rate limiter per API key / IP address."""

    def __init__(self, rate_per_sec: float = 50.0, capacity: float = 100.0):
        self.rate = rate_per_sec
        self.capacity = capacity
        self._buckets: Dict[str, Tuple[float, float]] = {}  # client_id -> (tokens, last_update)

    def allow_request(self, client_id: str, tokens_requested: float = 1.0) -> bool:
        now = time.time()
        tokens, last_update = self._buckets.get(client_id, (self.capacity, now))

        # Replenish tokens
        elapsed = now - last_update
        tokens = min(self.capacity, tokens + elapsed * self.rate)

        if tokens >= tokens_requested:
            self._buckets[client_id] = (tokens - tokens_requested, now)
            return True

        self._buckets[client_id] = (tokens, now)
        logger.warning("Rate limit exceeded for client", client_id=client_id)
        return False
