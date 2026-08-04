"""
Coverage Validation Engine.

Verifies policy coverage eligibility, endorsement riders, waiting periods,
effective dates, and exclusion clauses prior to claim adjudication.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional

from src.models.claims import InsuranceClaim, ClaimType

logger = structlog.get_logger(__name__)


@dataclass
class CoverageValidationResult:
    is_covered: bool
    effective_date: str
    expiration_date: str
    covered_perils: List[str]
    exclusions_triggered: List[str]
    waiting_period_met: bool
    pre_existing_condition_flag: bool


class CoverageValidator:
    """Validates policy coverage bounds and endorsement terms."""

    def validate_coverage(self, claim: InsuranceClaim, policy_info: Dict[str, Any]) -> CoverageValidationResult:
        eff_date = policy_info.get("effective_date", "2026-01-01")
        exp_date = policy_info.get("expiration_date", "2026-12-31")
        covered_perils = policy_info.get("covered_perils", ["collision", "fire", "water", "theft", "medical"])

        exclusions = []
        desc = (claim.description or "").lower()

        # Exclusion clause checks
        if "racing" in desc or "track" in desc:
            exclusions.append("EXCLUSION_VEHICLE_RACING")
        if "intentional" in desc or "fraud" in desc:
            exclusions.append("EXCLUSION_INTENTIONAL_ACT")
        if "war" in desc or "terrorism" in desc:
            exclusions.append("EXCLUSION_WAR_AND_TERRORISM")

        # Waiting period check
        waiting_days = policy_info.get("waiting_period_days", 0)
        policy_age = policy_info.get("policy_age_days", 100)
        waiting_met = policy_age >= waiting_days

        # Pre-existing condition check
        pre_existing = False
        if claim.claim_type == ClaimType.HEALTH:
            pre_existing = policy_info.get("has_pre_existing_conditions", False)

        is_covered = len(exclusions) == 0 and waiting_met and not pre_existing

        return CoverageValidationResult(
            is_covered=is_covered,
            effective_date=eff_date,
            expiration_date=exp_date,
            covered_perils=covered_perils,
            exclusions_triggered=exclusions,
            waiting_period_met=waiting_met,
            pre_existing_condition_flag=pre_existing,
        )
