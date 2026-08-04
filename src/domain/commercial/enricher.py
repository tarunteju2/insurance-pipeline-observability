"""
Commercial Claims Enricher.

Enriches commercial claims with:
  - D&B business lookup (simulated)
  - Litigation history
  - Workers' comp experience modifier
  - NAICS/SIC classification
  - Loss control recommendations
"""

import hashlib
import random
import structlog
from datetime import date
from typing import Any, Dict

from src.models.claims import InsuranceClaim, ClaimType

logger = structlog.get_logger(__name__)


class CommercialClaimEnricher:
    """Enriches commercial insurance claims."""

    def enrich(self, claim: InsuranceClaim) -> Dict[str, Any]:
        enrichment: Dict[str, Any] = {}

        enrichment.update(self._business_lookup(claim))
        enrichment.update(self._litigation_check(claim))
        enrichment.update(self._experience_mod(claim))
        enrichment.update(self._loss_control(claim, enrichment))
        enrichment.update(self._reserve_analysis(claim, enrichment))

        return enrichment

    def _business_lookup(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Simulated D&B-style business information lookup."""
        seed = int(
            hashlib.md5(claim.policy_number.encode()).hexdigest()[:8], 16
        )
        rng = random.Random(seed)

        industries = [
            ("23", "Construction"), ("54", "Professional Services"),
            ("44", "Retail Trade"), ("72", "Accommodation & Food"),
            ("62", "Health Care"), ("31", "Manufacturing"),
        ]
        sector_code, sector_name = rng.choice(industries)

        return {
            "business_profile": {
                "duns_number": f"D{rng.randint(100000000, 999999999)}",
                "business_name": f"Enterprise Corp #{rng.randint(1000, 9999)}",
                "naics_code": f"{sector_code}{rng.randint(100, 999)}",
                "sector": sector_name,
                "employee_count": rng.choice([5, 15, 50, 150, 500, 2000]),
                "annual_revenue": rng.choice([
                    500_000, 2_000_000, 10_000_000, 50_000_000, 200_000_000
                ]),
                "years_in_business": rng.randint(1, 50),
                "credit_rating": rng.choice(["A+", "A", "B+", "B", "C"]),
                "bankruptcy_history": rng.random() < 0.05,
            },
            "naics_code": f"{sector_code}{rng.randint(100, 999)}",
        }

    def _litigation_check(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Check for pending litigation on the insured."""
        seed = int(
            hashlib.md5(claim.claim_id.encode()).hexdigest()[:8], 16
        )
        rng = random.Random(seed)

        has_litigation = rng.random() < 0.12
        cases = []
        if has_litigation:
            case_count = rng.randint(1, 3)
            for i in range(case_count):
                cases.append({
                    "case_number": f"CV-{rng.randint(2020, 2026)}-{rng.randint(10000, 99999)}",
                    "jurisdiction": rng.choice(["Federal", "State", "Arbitration"]),
                    "status": rng.choice(["active", "discovery", "trial_pending", "settlement"]),
                    "estimated_exposure": rng.choice([
                        50_000, 100_000, 250_000, 500_000, 1_000_000
                    ]),
                })

        return {
            "litigation_pending": has_litigation,
            "litigation_cases": cases,
            "litigation_total_exposure": sum(c["estimated_exposure"] for c in cases),
        }

    def _experience_mod(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Calculate experience modification rate for workers' comp."""
        if claim.claim_type != ClaimType.WORKERS_COMP:
            return {}

        seed = int(
            hashlib.md5(claim.policy_number.encode()).hexdigest()[:6], 16
        )
        rng = random.Random(seed)

        # EMR: 1.0 = industry average, <1.0 = better, >1.0 = worse
        emr = round(rng.gauss(1.05, 0.25), 2)
        emr = max(0.50, min(2.00, emr))

        prior_claims = rng.randint(0, 15)
        prior_cost = sum(rng.randint(5_000, 100_000) for _ in range(prior_claims))

        return {
            "experience_modification_rate": emr,
            "emr_effective_date": f"{date.today().year}-01-01",
            "prior_3yr_claims": prior_claims,
            "prior_3yr_incurred": prior_cost,
            "wc_class_code": rng.choice(list(
                __import__("src.domain.commercial.validator",
                           fromlist=["_WC_CLASS_CODES"])._WC_CLASS_CODES.keys()
            )),
        }

    def _loss_control(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate loss control recommendations."""
        profile = current.get("business_profile", {})
        emr = current.get("experience_modification_rate", 1.0)

        recommendations = []
        if emr > 1.20:
            recommendations.append({
                "priority": "high",
                "category": "workplace_safety",
                "description": "Implement comprehensive safety training program",
                "estimated_premium_impact": "-8% to -15%",
            })
        if claim.claim_amount > 50_000:
            recommendations.append({
                "priority": "medium",
                "category": "risk_engineering",
                "description": "Schedule on-site risk engineering survey",
                "estimated_premium_impact": "-5% to -10%",
            })
        if profile.get("employee_count", 0) > 100:
            recommendations.append({
                "priority": "medium",
                "category": "return_to_work",
                "description": "Establish modified-duty return-to-work program",
                "estimated_premium_impact": "-3% to -8%",
            })

        return {
            "loss_control_recommendations": recommendations,
            "loss_control_survey_required": emr > 1.30 or claim.claim_amount > 100_000,
        }

    def _reserve_analysis(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze reserve requirements for commercial claims."""
        litigation = current.get("litigation_pending", False)
        exposure = current.get("litigation_total_exposure", 0)

        # Commercial claims develop more slowly — higher initial reserves
        base_reserve = claim.claim_amount * 0.60
        litigation_load = exposure * 0.30 if litigation else 0
        total_reserve = base_reserve + litigation_load

        return {
            "reserve_analysis": {
                "base_reserve": round(base_reserve, 2),
                "litigation_load": round(litigation_load, 2),
                "total_initial_reserve": round(total_reserve, 2),
                "development_tail_months": 36 if litigation else 24,
            },
        }
