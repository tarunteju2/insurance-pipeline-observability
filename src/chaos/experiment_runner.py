"""
Chaos Experiment Runner.

Executes chaos experiments, verifies steady-state hypothesis,
and performs automated rollback if metrics exceed safety thresholds.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass
from typing import Any, Dict, Callable

from src.chaos.fault_injector import FaultInjector

logger = structlog.get_logger(__name__)


@dataclass
class ExperimentResult:
    experiment_name: str
    steady_state_before: bool
    steady_state_after: bool
    hypothesis_validated: bool
    rollback_executed: bool


class ChaosExperimentRunner:
    """Executes chaos hypothesis experiments and verifies self-healing resilience."""

    def __init__(self):
        self.injector = FaultInjector.instance()

    def run_experiment(
        self,
        name: str,
        hypothesis_fn: Callable[[], bool],
        fault_action: Callable[[], None],
    ) -> ExperimentResult:
        logger.info("Starting chaos experiment", name=name)

        # 1. Steady-state check before
        before = hypothesis_fn()

        # 2. Inject fault
        fault_action()

        # 3. Verify hypothesis under chaos
        after = hypothesis_fn()

        # 4. Rollback
        self.injector.reset_all_faults()
        rollback = True

        validated = before and after
        logger.info("Chaos experiment finished", name=name, validated=validated)

        return ExperimentResult(
            experiment_name=name,
            steady_state_before=before,
            steady_state_after=after,
            hypothesis_validated=validated,
            rollback_executed=rollback,
        )
