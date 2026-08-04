"""
Auto LOB definition — wires up validator, enricher, and fraud scorer.
"""

from src.domain.lob_registry import (
    LineOfBusiness, SLATargets, RegulatoryConfig,
    FraudModelConfig, ReserveConfig,
)
from src.models.claims import ClaimType


class AutoLineOfBusiness(LineOfBusiness):
    """Personal and Commercial Automobile insurance line of business."""

    lob_code = "AUTO"
    lob_name = "Personal & Commercial Automobile"
    claim_types = [ClaimType.AUTO]

    sla = SLATargets(
        validation_latency_p95_ms=150.0,
        fraud_scoring_latency_p95_ms=400.0,
        enrichment_latency_p95_ms=800.0,
        end_to_end_latency_p95_ms=2500.0,
        max_error_rate=0.02,
        max_dlq_rate=0.01,
    )

    regulatory = RegulatoryConfig(
        jurisdiction="US-ALL",
        reporting_frequency="quarterly",
        doi_filing_required=True,
        naic_annual_statement_line="19.1-19.2",
        statutory_reserve_method="case_basis",
        prompt_pay_days=30,
        acknowledgment_days=15,
        investigation_days=30,
    )

    fraud_config = FraudModelConfig(
        heuristic_weight=0.25,
        ml_model_weight=0.40,
        anomaly_weight=0.20,
        network_weight=0.15,
        auto_approve_threshold=0.12,
        manual_review_threshold=0.40,
        siu_referral_threshold=0.72,
        model_id="auto_xgb_v3",
    )

    reserve_config = ReserveConfig(
        initial_reserve_method="average_paid",
        loss_development_factors={
            3: 1.65, 6: 1.42, 12: 1.22, 18: 1.12,
            24: 1.06, 36: 1.03, 48: 1.01, 60: 1.00,
        },
        ibnr_method="chain_ladder",
        catastrophe_load_pct=0.03,
    )

    def __init__(self):
        super().__init__()
        from src.domain.auto.validator import AutoClaimValidator
        from src.domain.auto.enricher import AutoClaimEnricher
        self._validator = AutoClaimValidator()
        self._enricher = AutoClaimEnricher()
