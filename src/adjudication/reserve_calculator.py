"""
Actuarial Reserve Calculator.

Estimates initial case reserves, Incurred But Not Reported (IBNR) reserves,
and ultimate loss projections using Bornhuetter-Ferguson and Monte Carlo simulation.
"""

from __future__ import annotations

import math
import random
import structlog
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)


@dataclass
class ReserveCalculationResult:
    claim_id: str
    initial_case_reserve: float
    ibnr_reserve: float
    ultimate_loss_estimate: float
    catastrophe_load: float
    confidence_95_max_loss: float
    method: str


class ActuarialReserveCalculator:
    """Actuarial engine computing case reserves, IBNR, and stochastic loss bounds."""

    def calculate_reserves(
        self,
        claim: InsuranceClaim,
        loss_development_factor: float = 1.35,
        expected_loss_ratio: float = 0.65,
    ) -> ReserveCalculationResult:
        base = claim.claim_amount
        initial_case = round(base * 0.70, 2)

        # Bornhuetter-Ferguson IBNR calculation
        unreported_factor = max(0.05, 1.0 - (1.0 / loss_development_factor))
        ibnr = round(base * expected_loss_ratio * unreported_factor, 2)

        # Catastrophe load
        cat_load = round(base * 0.05, 2) if (claim.enrichment_data or {}).get("is_catastrophe_claim") else 0.0

        ultimate = round(initial_case + ibnr + cat_load, 2)

        # Monte Carlo 95% Confidence Upper Bound simulation (100 iterations)
        simulated_losses = [
            ultimate * (1.0 + random.gauss(0.0, 0.15))
            for _ in range(100)
        ]
        simulated_losses.sort()
        confidence_95_max = round(simulated_losses[95], 2)

        return ReserveCalculationResult(
            claim_id=claim.claim_id,
            initial_case_reserve=initial_case,
            ibnr_reserve=ibnr,
            ultimate_loss_estimate=ultimate,
            catastrophe_load=cat_load,
            confidence_95_max_loss=confidence_95_max,
            method="Bornhuetter-Ferguson + Monte Carlo",
        )
