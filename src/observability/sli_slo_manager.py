"""
SLI / SLO & Error Budget Manager.

Tracks Service Level Indicators (SLI), Service Level Objectives (SLO),
error budget burn rates, and multi-window burn rate alerts per service/stage.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = structlog.get_logger(__name__)


@dataclass
class SLODefinition:
    slo_name: str
    target_pct: float  # e.g. 99.5%
    window_days: int = 30
    total_events: int = 0
    good_events: int = 0

    @property
    def current_sli_pct(self) -> float:
        if self.total_events == 0:
            return 100.0
        return round((self.good_events / self.total_events) * 100, 3)

    @property
    def error_budget_remaining_pct(self) -> float:
        allowed_bad_pct = 100.0 - self.target_pct
        if allowed_bad_pct <= 0:
            return 0.0
        actual_bad_pct = 100.0 - self.current_sli_pct
        remaining = max(0.0, (allowed_bad_pct - actual_bad_pct) / allowed_bad_pct) * 100
        return round(remaining, 2)

    @property
    def is_breached(self) -> bool:
        return self.current_sli_pct < self.target_pct


class SLISLOManager:
    """Enterprise SLI/SLO and Error Budget Manager."""

    _instance: Optional[SLISLOManager] = None

    def __init__(self):
        self._slos: Dict[str, SLODefinition] = {
            "validation_latency_p95": SLODefinition("validation_latency_p95", 99.5),
            "fraud_scoring_availability": SLODefinition("fraud_scoring_availability", 99.9),
            "end_to_end_pipeline_success": SLODefinition("end_to_end_pipeline_success", 99.0),
        }

    @classmethod
    def instance(cls) -> SLISLOManager:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def record_event(self, slo_name: str, is_good: bool) -> None:
        slo = self._slos.get(slo_name)
        if slo:
            slo.total_events += 1
            if is_good:
                slo.good_events += 1

            if slo.is_breached:
                logger.warning("SLO breached!", slo_name=slo_name, current_sli=slo.current_sli_pct, target=slo.target_pct)

    def get_slo_report(self) -> List[Dict[str, Any]]:
        return [
            {
                "slo_name": slo.slo_name,
                "target_pct": slo.target_pct,
                "current_sli_pct": slo.current_sli_pct,
                "error_budget_remaining_pct": slo.error_budget_remaining_pct,
                "total_events": slo.total_events,
                "is_breached": slo.is_breached,
            }
            for slo in self._slos.values()
        ]
