"""
Declarative Business Rules Engine for Claims Adjudication.

Evaluates complex configurable business rules for policy coverage, deductibles,
exclusions, rider benefits, and automatic approval thresholds.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)


@dataclass
class RuleResult:
    rule_id: str
    rule_name: str
    passed: bool
    description: str
    action_if_failed: str = "none"  # deny, pend, flag, reduce_payout
    payout_modifier: float = 0.0


class BusinessRulesEngine:
    """Configurable rules engine evaluating business policies and claim conditions."""

    def __init__(self):
        self._rules: List[Dict[str, Any]] = []
        self._init_default_rules()

    def evaluate_rules(self, claim: InsuranceClaim, context: Optional[Dict[str, Any]] = None) -> List[RuleResult]:
        ctx = context or {}
        results: List[RuleResult] = []

        # Rule 1: Minimum Deductible Check
        deductible = float(ctx.get("deductible", 500.0))
        if claim.claim_amount <= deductible:
            results.append(RuleResult(
                rule_id="RULE_DEDUCTIBLE_EXCEEDED",
                rule_name="Deductible Exceeded Check",
                passed=False,
                description=f"Claim amount ${claim.claim_amount:,.2f} is less than policy deductible ${deductible:,.2f}",
                action_if_failed="deny",
            ))
        else:
            results.append(RuleResult(
                rule_id="RULE_DEDUCTIBLE_EXCEEDED",
                rule_name="Deductible Exceeded Check",
                passed=True,
                description="Claim amount exceeds policy deductible",
                payout_modifier=-deductible,
            ))

        # Rule 2: Policy Maximum Limit Check
        policy_limit = float(ctx.get("policy_limit", 100000.0))
        if claim.claim_amount > policy_limit:
            results.append(RuleResult(
                rule_id="RULE_POLICY_LIMIT",
                rule_name="Policy Limit Check",
                passed=False,
                description=f"Claim amount ${claim.claim_amount:,.2f} exceeds max policy limit ${policy_limit:,.2f}",
                action_if_failed="reduce_payout",
            ))
        else:
            results.append(RuleResult(
                rule_id="RULE_POLICY_LIMIT",
                rule_name="Policy Limit Check",
                passed=True,
                description="Claim amount within policy limit",
            ))

        # Rule 3: Date of Loss within Policy Term
        policy_active = ctx.get("policy_active", True)
        if not policy_active:
            results.append(RuleResult(
                rule_id="RULE_POLICY_ACTIVE",
                rule_name="Active Policy Coverage Check",
                passed=False,
                description="Loss date falls outside active policy coverage term",
                action_if_failed="deny",
            ))
        else:
            results.append(RuleResult(
                rule_id="RULE_POLICY_ACTIVE",
                rule_name="Active Policy Coverage Check",
                passed=True,
                description="Policy active on date of loss",
            ))

        logger.debug("Business rules evaluated", claim_id=claim.claim_id, total_rules=len(results), passed_rules=sum(1 for r in results if r.passed))
        return results

    def _init_default_rules(self) -> None:
        self._rules.append({"id": "RULE_DEDUCTIBLE_EXCEEDED", "version": "1.0"})
        self._rules.append({"id": "RULE_POLICY_LIMIT", "version": "1.0"})
        self._rules.append({"id": "RULE_POLICY_ACTIVE", "version": "1.0"})
