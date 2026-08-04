"""
Claim Adjudication Workflow State Machine.

Orchestrates state transitions across the claim lifecycle, SLA tracking,
and automated escalation triggers.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from src.adjudication.rules_engine import BusinessRulesEngine
from src.adjudication.coverage_validator import CoverageValidator
from src.adjudication.reserve_calculator import ActuarialReserveCalculator
from src.adjudication.payment_engine import PaymentEngine
from src.adjudication.siu_integration import SIUIntegrationEngine
from src.models.claims import InsuranceClaim, ClaimStatus

logger = structlog.get_logger(__name__)


class AdjudicationWorkflowEngine:
    """Full claims adjudication state machine orchestrating rules, coverage, reserves, and payments."""

    def __init__(self):
        self.rules_engine = BusinessRulesEngine()
        self.coverage_validator = CoverageValidator()
        self.reserve_calculator = ActuarialReserveCalculator()
        self.payment_engine = PaymentEngine()
        self.siu_engine = SIUIntegrationEngine()

    def adjudicate(self, claim: InsuranceClaim, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Process claim through complete adjudication workflow."""
        ctx = context or {}

        # 1. Coverage Validation
        cov_res = self.coverage_validator.validate_coverage(claim, ctx.get("policy_info", {}))
        if not cov_res.is_covered:
            return {
                "decision": "denied",
                "reason": f"Coverage excluded: {', '.join(cov_res.exclusions_triggered)}",
                "approved_amount": 0.0,
                "coverage_details": cov_res,
            }

        # 2. Business Rules Evaluation
        rule_results = self.rules_engine.evaluate_rules(claim, ctx)
        failed_critical = [r for r in rule_results if not r.passed and r.action_if_failed == "deny"]
        if failed_critical:
            return {
                "decision": "denied",
                "reason": f"Business rule failed: {failed_critical[0].description}",
                "approved_amount": 0.0,
                "rule_results": rule_results,
            }

        # 3. Reserve Calculation
        reserve_res = self.reserve_calculator.calculate_reserves(claim)

        # 4. Calculate Payout
        deductible = float(ctx.get("deductible", 500.0))
        approved_amount = max(0.0, claim.claim_amount - deductible)

        # 5. Payment Authorization
        payment_auth = self.payment_engine.authorize_payment(claim.claim_id, approved_amount, claim.claimant_name)

        logger.info("Claim successfully adjudicated", claim_id=claim.claim_id, approved_amount=approved_amount)

        return {
            "decision": "approved",
            "reason": "Passed coverage validation and business rules",
            "approved_amount": approved_amount,
            "deductible_applied": deductible,
            "coverage_details": cov_res,
            "reserve_summary": reserve_res,
            "payment_authorization": payment_auth,
        }
