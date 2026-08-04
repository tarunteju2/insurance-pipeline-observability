"""
Cyber LOB definition.
"""

from src.domain.lob_registry import (
    LineOfBusiness, SLATargets, RegulatoryConfig,
    FraudModelConfig, ReserveConfig,
)
from src.models.claims import ClaimType


class CyberLineOfBusiness(LineOfBusiness):
    """Cyber Liability & Data Breach insurance."""

    lob_code = "CYBER"
    lob_name = "Cyber Liability Insurance"
    claim_types = [ClaimType.LIFE]  # Reusing LIFE claim type enum for cyber liability

    sla = SLATargets(
        validation_latency_p95_ms=200.0,
        fraud_scoring_latency_p95_ms=800.0,
        enrichment_latency_p95_ms=2500.0,
        end_to_end_latency_p95_ms=6000.0,
        max_error_rate=0.05,
        max_dlq_rate=0.03,
    )

    regulatory = RegulatoryConfig(
        jurisdiction="US-ALL",
        reporting_frequency="annual",
        doi_filing_required=True,
        naic_annual_statement_line="17-OTHER",
        statutory_reserve_method="case_basis",
        prompt_pay_days=30,
        acknowledgment_days=10,  # Faster response for cyber events
        investigation_days=90,   # Complex digital forensics
    )

    fraud_config = FraudModelConfig(
        heuristic_weight=0.20,
        ml_model_weight=0.35,
        anomaly_weight=0.30,
        network_weight=0.15,
        auto_approve_threshold=0.08,
        manual_review_threshold=0.30,
        siu_referral_threshold=0.60,
        model_id="cyber_anomaly_v1",
    )

    reserve_config = ReserveConfig(
        initial_reserve_method="case_basis",
        loss_development_factors={
            3: 3.00, 6: 2.40, 12: 1.80, 18: 1.50,
            24: 1.30, 36: 1.15, 48: 1.08, 60: 1.02,
        },
        ibnr_method="bornhuetter_ferguson",
        catastrophe_load_pct=0.15,  # High for aggregation risk
    )

    def __init__(self):
        super().__init__()
        from src.domain.cyber.validator import CyberClaimValidator
        from src.domain.cyber.enricher import CyberClaimEnricher
        self._validator = CyberClaimValidator()
        self._enricher = CyberClaimEnricher()
