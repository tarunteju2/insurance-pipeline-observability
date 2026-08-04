"""
Health LOB definition — wires up validator, enricher, and medical review.
"""

from src.domain.lob_registry import (
    LineOfBusiness, SLATargets, RegulatoryConfig,
    FraudModelConfig, ReserveConfig,
)
from src.models.claims import ClaimType


class HealthLineOfBusiness(LineOfBusiness):
    """Health/Medical insurance line of business."""

    lob_code = "HEALTH"
    lob_name = "Health & Medical Insurance"
    claim_types = [ClaimType.HEALTH]

    sla = SLATargets(
        validation_latency_p95_ms=250.0,
        fraud_scoring_latency_p95_ms=600.0,
        enrichment_latency_p95_ms=1200.0,
        end_to_end_latency_p95_ms=4000.0,
        max_error_rate=0.03,
        max_dlq_rate=0.015,
    )

    regulatory = RegulatoryConfig(
        jurisdiction="US-ALL",
        reporting_frequency="quarterly",
        doi_filing_required=True,
        naic_annual_statement_line="1-3",
        statutory_reserve_method="tabular",
        prompt_pay_days=30,
        acknowledgment_days=15,
        investigation_days=45,
    )

    fraud_config = FraudModelConfig(
        heuristic_weight=0.20,
        ml_model_weight=0.45,
        anomaly_weight=0.20,
        network_weight=0.15,
        auto_approve_threshold=0.10,
        manual_review_threshold=0.38,
        siu_referral_threshold=0.68,
        model_id="health_xgb_v2",
    )

    reserve_config = ReserveConfig(
        initial_reserve_method="tabular",
        loss_development_factors={
            3: 1.90, 6: 1.60, 12: 1.35, 18: 1.20,
            24: 1.12, 36: 1.06, 48: 1.03, 60: 1.00,
        },
        ibnr_method="bornhuetter_ferguson",
        catastrophe_load_pct=0.02,
    )

    def __init__(self):
        super().__init__()
        from src.domain.health.validator import HealthClaimValidator
        from src.domain.health.enricher import HealthClaimEnricher
        self._validator = HealthClaimValidator()
        self._enricher = HealthClaimEnricher()
