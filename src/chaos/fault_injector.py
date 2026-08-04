"""
Chaos Fault Injector Engine.

Simulates network latency, service outages, forced exceptions, and resource exhaustion.
"""

from __future__ import annotations

import time
import random
import structlog
from typing import Any, Dict, Optional

logger = structlog.get_logger(__name__)


class FaultInjector:
    """Configurable fault injection mechanism for resilience testing."""

    _instance: Optional[FaultInjector] = None

    def __init__(self):
        self.latency_ms: float = 0.0
        self.error_rate: float = 0.0  # 0.0 to 1.0 probability
        self.partition_enabled: bool = False
        self.active_faults: list[str] = []

    @classmethod
    def instance(cls) -> FaultInjector:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def inject_latency_if_configured(self) -> None:
        if self.latency_ms > 0:
            time.sleep(self.latency_ms / 1000.0)

    def maybe_raise_error(self, component_name: str) -> None:
        if self.partition_enabled:
            raise ConnectionError(f"Simulated Network Partition: unable to reach {component_name}")

        if self.error_rate > 0 and random.random() < self.error_rate:
            raise RuntimeError(f"Simulated Chaos Error in {component_name}")

    def reset_all_faults(self) -> None:
        self.latency_ms = 0.0
        self.error_rate = 0.0
        self.partition_enabled = False
        self.active_faults.clear()
        logger.info("Reset all chaos fault injections")
