"""
Regulatory Reporting Engine.

Generates NAIC Annual Statement schedules, State DOI market conduct reports,
loss ratio / combined ratio calculations, and Suspicious Activity Reports (SAR).
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = structlog.get_logger(__name__)


@dataclass
class NAICReportSchedule:
    schedule_name: str  # Schedule P Part 1 - Analysis of Losses
    line_of_business: str
    accident_year: int
    earned_premium: float
    incurred_losses: float
    defense_and_cost_containment: float
    loss_ratio_pct: float
    combined_ratio_pct: float


class RegulatoryReportingEngine:
    """Generates statutory insurance reports for state DOI and NAIC filing."""

    def calculate_loss_ratio(self, incurred_losses: float, earned_premium: float) -> float:
        """Calculate loss ratio percentage = (incurred losses / earned premium) * 100."""
        if earned_premium <= 0:
            return 0.0
        return round((incurred_losses / earned_premium) * 100, 2)

    def calculate_combined_ratio(
        self,
        incurred_losses: float,
        underwriting_expenses: float,
        earned_premium: float,
        written_premium: float,
    ) -> float:
        """
        Calculate combined ratio = Loss Ratio + Expense Ratio.
        A combined ratio < 100% indicates an underwriting profit.
        """
        loss_ratio = (incurred_losses / max(1.0, earned_premium)) * 100
        expense_ratio = (underwriting_expenses / max(1.0, written_premium)) * 100
        return round(loss_ratio + expense_ratio, 2)

    def generate_naic_schedule_p(
        self,
        lob_code: str,
        accident_year: int,
        earned_premium: float,
        incurred_losses: float,
        underwriting_expenses: float,
    ) -> NAICReportSchedule:
        loss_ratio = self.calculate_loss_ratio(incurred_losses, earned_premium)
        combined_ratio = self.calculate_combined_ratio(incurred_losses, underwriting_expenses, earned_premium, earned_premium)

        return NAICReportSchedule(
            schedule_name="Schedule P Part 1",
            line_of_business=lob_code,
            accident_year=accident_year,
            earned_premium=earned_premium,
            incurred_losses=incurred_losses,
            defense_and_cost_containment=round(incurred_losses * 0.08, 2),
            loss_ratio_pct=loss_ratio,
            combined_ratio_pct=combined_ratio,
        )

    def generate_sar_report(self, claim_id: str, fraud_score: float, details: Dict[str, Any]) -> Dict[str, Any]:
        """Generate FinCEN / NAIC Suspicious Activity Report (SAR) template for SIU referrals."""
        return {
            "sar_id": f"SAR_{claim_id}",
            "claim_id": claim_id,
            "suspicion_type": "Insurance Fraud / Extortion",
            "fraud_risk_score": fraud_score,
            "filing_institution": "Enterprise Insurance Carrier",
            "summary": f"Automated SIU fraud referral triggered for claim {claim_id} with score {fraud_score}",
            "details": details,
        }
