"""
Circuit Breaker pattern for protecting unstable downstream dependencies.

States:
  CLOSED    — normal operation, calls pass through
  OPEN      — dependency is failing; calls are rejected immediately to prevent cascade
  HALF_OPEN — cooldown elapsed; one probe call allowed to test recovery

Usage:
    cb = CircuitBreaker("postgres", failure_threshold=5, recovery_timeout=30)
    with cb:
        db_session.commit()
"""

import time
import threading
from enum import Enum
from typing import Callable, Any, Optional
import structlog

from src.observability.metrics import CIRCUIT_BREAKER_STATE, CIRCUIT_BREAKER_TRIPS

logger = structlog.get_logger(__name__)


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpen(Exception):
    """Raised when a call is rejected because the circuit is OPEN."""
    pass


class CircuitBreaker:
    """
    Thread-safe circuit breaker for a named downstream dependency.

    Args:
        name:               Identifier used in metrics and logs (e.g. "postgres", "minio")
        failure_threshold:  Consecutive failures required to trip to OPEN state
        recovery_timeout:   Seconds to wait in OPEN before allowing one probe (HALF_OPEN)
        success_threshold:  Consecutive successes in HALF_OPEN needed to return to CLOSED
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        success_threshold: int = 2,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.Lock()

        # Initialise Prometheus gauge to CLOSED (1 = closed/healthy)
        CIRCUIT_BREAKER_STATE.labels(dependency=self.name).set(1)

    @property
    def state(self) -> CircuitState:
        return self._state

    def _set_state(self, new_state: CircuitState) -> None:
        old_state = self._state
        self._state = new_state
        # 1 = CLOSED (healthy), 0 = OPEN (tripped), 0.5 = HALF_OPEN (probing)
        state_value = {"closed": 1, "open": 0, "half_open": 0.5}[new_state.value]
        CIRCUIT_BREAKER_STATE.labels(dependency=self.name).set(state_value)
        logger.info("Circuit breaker state changed",
                    dependency=self.name, from_state=old_state, to_state=new_state)

    def _should_attempt_reset(self) -> bool:
        return (
            self._state == CircuitState.OPEN
            and self._last_failure_time is not None
            and (time.monotonic() - self._last_failure_time) >= self.recovery_timeout
        )

    def call(self, fn: Callable, *args, **kwargs) -> Any:
        """
        Execute *fn* with circuit breaker protection.
        Raises CircuitBreakerOpen if the circuit is OPEN and not yet in cooldown.
        """
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._set_state(CircuitState.HALF_OPEN)
                    self._success_count = 0
                else:
                    raise CircuitBreakerOpen(
                        f"Circuit '{self.name}' is OPEN — call rejected to protect the system. "
                        f"Recovery in ~{self._recovery_seconds_remaining():.0f}s."
                    )

        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except CircuitBreakerOpen:
            raise
        except Exception as exc:
            self._on_failure()
            raise exc

    def _on_success(self) -> None:
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._failure_count = 0
                    self._success_count = 0
                    self._set_state(CircuitState.CLOSED)
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    def _on_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self._state == CircuitState.HALF_OPEN or self._failure_count >= self.failure_threshold:
                CIRCUIT_BREAKER_TRIPS.labels(dependency=self.name).inc()
                self._failure_count = 0
                self._success_count = 0
                self._set_state(CircuitState.OPEN)
                logger.warning("Circuit breaker TRIPPED",
                               dependency=self.name,
                               recovery_in=self.recovery_timeout)

    def _recovery_seconds_remaining(self) -> float:
        if self._last_failure_time is None:
            return 0.0
        elapsed = time.monotonic() - self._last_failure_time
        return max(0.0, self.recovery_timeout - elapsed)

    def __enter__(self):
        # Support `with circuit_breaker:` syntax — just checks state
        with self._lock:
            if self._state == CircuitState.OPEN and not self._should_attempt_reset():
                raise CircuitBreakerOpen(
                    f"Circuit '{self.name}' is OPEN — skipping call."
                )
            if self._should_attempt_reset():
                self._set_state(CircuitState.HALF_OPEN)
                self._success_count = 0
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._on_success()
        elif exc_type is not CircuitBreakerOpen:
            self._on_failure()
        return False  # never suppress exceptions


# Module-level singletons — shared across the process lifetime
postgres_breaker = CircuitBreaker("postgres", failure_threshold=5, recovery_timeout=30)
minio_breaker = CircuitBreaker("minio", failure_threshold=3, recovery_timeout=20)
