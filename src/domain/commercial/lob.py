"""
Commercial LOB definition.
"""

from src.domain.lob_registry import (
    LineOfBusiness, SLATargets, RegulatoryConfig,
    FraudModelConfig, ReserveConfig,
)
from src.models.claims import ClaimType


class CommercialLineOfBusiness(LineOfBusiness):
    """Commercial Lines — GL, PL, Commercial Property, Umbrella."""

    lob_code = "COMMERCIAL"
    lob_name = "Commercial Lines Insurance"
    claim_types = [ClaimType.LIABILITY, ClaimType.WORKERS_COMP]

    sla = SLATargets(
        validation_latency_p95_ms=300.0,
        fraud_scoring_latency_p95_ms=700.0,
        enrichment_latency_p95_ms=2000.0,
        end_to_end_latency_p95_ms=5000.0,
        max_error_rate=0.04,
        max_dlq_rate=0.02,
    )

    regulatory = RegulatoryConfig(
        jurisdiction="US-ALL",
        reporting_frequency="annual",
        doi_filing_required=True,
        naic_annual_statement_line="17-17.3",
        statutory_reserve_method="case_basis",
        prompt_pay_days=45,
        acknowledgment_days=15,
        investigation_days=60,
    )

    fraud_config = FraudModelConfig(
        heuristic_weight=0.35,
        ml_model_weight=0.30,
        anomaly_weight=0.15,
        network_weight=0.20,
        auto_approve_threshold=0.10,
        manual_review_threshold=0.35,
        siu_referral_threshold=0.65,
        model_id="commercial_xgb_v1",
    )

    reserve_config = ReserveConfig(
        initial_reserve_method="case_basis",
        loss_development_factors={
            3: 2.50, 6: 2.10, 12: 1.70, 18: 1.45,
            24: 1.30, 36: 1.18, 48: 1.10, 60: 1.05,
        },
        ibnr_method="bornhuetter_ferguson",
        catastrophe_load_pct=0.08,
    )

    def __init__(self):
        super().__init__()
        from src.domain.commercial.validator import CommercialClaimValidator
        from src.domain.commercial.enricher import CommercialClaimEnricher
        self._validator = CommercialClaimValidator()
        self._enricher = CommercialClaimEnricher()
