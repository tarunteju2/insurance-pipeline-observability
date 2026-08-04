"""
Special Investigations Unit (SIU) Integration Engine.

Manages fraud referrals, investigation case tracking, evidence logs,
and outcome feedback loops for model retraining.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = structlog.get_logger(__name__)


@dataclass
class SIUCase:
    case_id: str
    claim_id: str
    risk_score: float
    referral_reason: str
    status: str = "open"  # open, under_investigation, confirmed_fraud, cleared, closed
    investigator_assigned: Optional[str] = None
    evidence_items: List[Dict[str, Any]] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class SIUIntegrationEngine:
    """Manages SIU referral workflow and investigation lifecycle."""

    def __init__(self):
        self._cases: Dict[str, SIUCase] = {}

    def create_referral(self, claim_id: str, risk_score: float, reason: str) -> SIUCase:
        case_id = f"SIU_{claim_id}"
        case = SIUCase(
            case_id=case_id,
            claim_id=claim_id,
            risk_score=risk_score,
            referral_reason=reason,
            investigator_assigned="SIU_Investigator_Lead",
        )
        self._cases[case_id] = case
        logger.warning("SIU referral created", case_id=case_id, claim_id=claim_id, risk_score=risk_score)
        return case

    def add_evidence(self, case_id: str, evidence_type: str, description: str) -> None:
        case = self._cases.get(case_id)
        if case:
            case.evidence_items.append({
                "type": evidence_type,
                "description": description,
                "added_at": datetime.utcnow().isoformat(),
            })

    def close_investigation(self, case_id: str, confirmed_fraud: bool) -> None:
        case = self._cases.get(case_id)
        if case:
            case.status = "confirmed_fraud" if confirmed_fraud else "cleared"
            logger.info("SIU investigation closed", case_id=case_id, confirmed_fraud=confirmed_fraud)
