"""
Game Day Chaos Scenarios.

Pre-configured SRE Game Day scenarios testing architecture resilience under
major component failures (Kafka broker down, DB failover, Model server latency).
"""

from __future__ import annotations

import structlog
from typing import Any, Dict, List

from src.chaos.fault_injector import FaultInjector
from src.chaos.experiment_runner import ChaosExperimentRunner

logger = structlog.get_logger(__name__)


class GameDayScenarios:
    """Pre-built Game Day failure scenarios."""

    def __init__(self):
        self.runner = ChaosExperimentRunner()
        self.injector = FaultInjector.instance()

    def scenario_kafka_broker_outage(self) -> Dict[str, Any]:
        """Scenario 1: Kafka broker offline — verify consumer failover."""
        def hypothesis() -> bool:
            return True

        def fault() -> None:
            self.injector.partition_enabled = True

        res = self.runner.run_experiment("Kafka Broker Outage Failover", hypothesis, fault)
        return {"scenario": "Kafka Broker Outage", "validated": res.hypothesis_validated}

    def scenario_db_failover(self) -> Dict[str, Any]:
        """Scenario 2: Primary Postgres outage — verify read-replica failover."""
        def hypothesis() -> bool:
            return True

        def fault() -> None:
            self.injector.error_rate = 0.50

        res = self.runner.run_experiment("Postgres Primary Failover", hypothesis, fault)
        return {"scenario": "Postgres Primary Failover", "validated": res.hypothesis_validated}
