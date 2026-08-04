"""
Property LOB definition.
"""

from src.domain.lob_registry import (
    LineOfBusiness, SLATargets, RegulatoryConfig,
    FraudModelConfig, ReserveConfig,
)
from src.models.claims import ClaimType


class PropertyLineOfBusiness(LineOfBusiness):
    """Homeowners, Renters, and Commercial Property insurance."""

    lob_code = "PROPERTY"
    lob_name = "Property & Homeowners Insurance"
    claim_types = [ClaimType.PROPERTY]

    sla = SLATargets(
        validation_latency_p95_ms=200.0,
        fraud_scoring_latency_p95_ms=500.0,
        enrichment_latency_p95_ms=1500.0,
        end_to_end_latency_p95_ms=4000.0,
        max_error_rate=0.03,
        max_dlq_rate=0.02,
    )

    regulatory = RegulatoryConfig(
        jurisdiction="US-ALL",
        reporting_frequency="quarterly",
        doi_filing_required=True,
        naic_annual_statement_line="4-5.2",
        statutory_reserve_method="case_basis",
        prompt_pay_days=30,
        acknowledgment_days=15,
        investigation_days=45,
    )

    fraud_config = FraudModelConfig(
        heuristic_weight=0.30,
        ml_model_weight=0.35,
        anomaly_weight=0.20,
        network_weight=0.15,
        auto_approve_threshold=0.15,
        manual_review_threshold=0.45,
        siu_referral_threshold=0.75,
        model_id="property_xgb_v2",
    )

    reserve_config = ReserveConfig(
        initial_reserve_method="average_paid",
        loss_development_factors={
            3: 2.10, 6: 1.75, 12: 1.45, 18: 1.25,
            24: 1.15, 36: 1.08, 48: 1.04, 60: 1.00,
        },
        ibnr_method="bornhuetter_ferguson",
        catastrophe_load_pct=0.12,  # Higher CAT load for property
    )

    def __init__(self):
        super().__init__()
        from src.domain.property.validator import PropertyClaimValidator
        from src.domain.property.enricher import PropertyClaimEnricher
        self._validator = PropertyClaimValidator()
        self._enricher = PropertyClaimEnricher()
