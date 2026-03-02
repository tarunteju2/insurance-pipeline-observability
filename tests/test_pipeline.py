"""
Tests for the Insurance Claims Pipeline.
"""

import pytest
import json
import time
import sys
import os
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models.claims import InsuranceClaim, ClaimType, ClaimStatus, RiskLevel
from src.processors.claims_validator import ClaimsValidator
from src.processors import claims_validator as claims_validator_module
from src.processors.fraud_detector import FraudDetector
from src.processors.claims_enricher import ClaimsEnricher
from src.observability.tracing import init_tracing


@pytest.fixture(autouse=True)
def setup_tracing():
    init_tracing("test-pipeline")


class TestInsuranceClaimModel:
    def test_create_valid_claim(self):
        claim = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="John Doe",
            claim_type=ClaimType.AUTO,
            claim_amount=5000.00,
            date_of_loss="2025-01-15",
        )
        assert claim.claim_id.startswith("CLM-")
        assert claim.status == ClaimStatus.SUBMITTED
        assert claim.fraud_score == 0.0

    def test_claim_serialization(self):
        claim = InsuranceClaim(
            policy_number="HLT-654321",
            claimant_name="Jane Smith",
            claim_type=ClaimType.HEALTH,
            claim_amount=12500.00,
            date_of_loss="2025-02-01",
            provider_name="Memorial Hospital",
            diagnosis_code="M54.5",
        )
        data = claim.to_kafka_dict()
        assert 'timestamp' in data
        assert data['policy_number'] == "HLT-654321"

        restored = InsuranceClaim.from_kafka_dict(data)
        assert restored.claim_id == claim.claim_id
        assert restored.claim_amount == 12500.00

    def test_invalid_amount_too_high(self):
        with pytest.raises(ValueError):
            InsuranceClaim(
                policy_number="AUT-123456",
                claimant_name="Test",
                claim_type=ClaimType.AUTO,
                claim_amount=50_000_000,
                date_of_loss="2025-01-01",
            )


class TestClaimsValidator:
    def setup_method(self):
        claims_validator_module.lineage_tracker.record_event = lambda **kwargs: None
        self.validator = ClaimsValidator()

    def test_valid_auto_claim(self):
        recent_loss = (date.today() - timedelta(days=10)).isoformat()
        claim = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="John Doe",
            claim_type=ClaimType.AUTO,
            claim_amount=5000.00,
            date_of_loss=recent_loss,
            description="Rear-end collision",
        )
        is_valid, result = self.validator.validate(claim)
        assert is_valid is True
        assert result.status == ClaimStatus.VALIDATED
        assert len(result.validation_errors) == 0

    def test_missing_policy_number(self):
        recent_loss = (date.today() - timedelta(days=5)).isoformat()
        claim = InsuranceClaim(
            policy_number="",
            claimant_name="John Doe",
            claim_type=ClaimType.AUTO,
            claim_amount=5000.00,
            date_of_loss=recent_loss,
        )
        is_valid, result = self.validator.validate(claim)
        assert is_valid is False
        assert result.status == ClaimStatus.VALIDATION_FAILED
        assert any("policy_number" in e for e in result.validation_errors)

    def test_invalid_policy_format(self):
        recent_loss = (date.today() - timedelta(days=5)).isoformat()
        claim = InsuranceClaim(
            policy_number="INVALID",
            claimant_name="John Doe",
            claim_type=ClaimType.AUTO,
            claim_amount=5000.00,
            date_of_loss=recent_loss,
        )
        is_valid, result = self.validator.validate(claim)
        assert is_valid is False

    def test_health_claim_requires_provider(self):
        recent_loss = (date.today() - timedelta(days=7)).isoformat()
        claim = InsuranceClaim(
            policy_number="HLT-123456",
            claimant_name="Jane Smith",
            claim_type=ClaimType.HEALTH,
            claim_amount=10000.00,
            date_of_loss=recent_loss,
            provider_name=None,
        )
        is_valid, result = self.validator.validate(claim)
        assert is_valid is False
        assert any("provider" in e.lower() for e in result.validation_errors)

    def test_future_date_rejected(self):
        claim = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="Test Person",
            claim_type=ClaimType.AUTO,
            claim_amount=3000.00,
            date_of_loss="2099-01-01",
        )
        is_valid, result = self.validator.validate(claim)
        assert is_valid is False
        assert any("future" in e.lower() for e in result.validation_errors)

    def test_date_filed_before_loss_rejected(self):
        loss_date = (date.today() - timedelta(days=10)).isoformat()
        filed_date = (date.today() - timedelta(days=15)).isoformat()
        claim = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="Date Test",
            claim_type=ClaimType.AUTO,
            claim_amount=5000.00,
            date_of_loss=loss_date,
            date_filed=filed_date,
            vehicle_vin="1HGCM82633A004352",
        )
        is_valid, result = self.validator.validate(claim)
        assert is_valid is False
        assert "FILED_BEFORE_LOSS" in result.processing_metadata.get('validation_error_codes', [])

    def test_invalid_auto_vin_rejected(self):
        recent_loss = (date.today() - timedelta(days=12)).isoformat()
        recent_filed = (date.today() - timedelta(days=10)).isoformat()
        claim = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="VIN Test",
            claim_type=ClaimType.AUTO,
            claim_amount=200000.00,
            date_of_loss=recent_loss,
            date_filed=recent_filed,
            vehicle_vin="INVALIDVIN123",
            description="Major front-end collision on interstate resulting in significant damage.",
        )
        is_valid, result = self.validator.validate(claim)
        assert is_valid is False
        assert "INVALID_VIN_FORMAT" in result.processing_metadata.get('validation_error_codes', [])

    def test_validation_error_details_are_structured(self):
        recent_loss = (date.today() - timedelta(days=8)).isoformat()
        claim = InsuranceClaim(
            policy_number="BAD",
            claimant_name="X",
            claim_type=ClaimType.AUTO,
            claim_amount=1000.00,
            date_of_loss=recent_loss,
        )
        is_valid, result = self.validator.validate(claim)
        assert is_valid is False

        details = result.processing_metadata.get('validation_error_details', [])
        assert len(details) > 0
        assert all('code' in item and 'field' in item and 'message' in item for item in details)


class TestFraudDetector:
    def setup_method(self):
        self.detector = FraudDetector()

    def test_low_risk_claim(self):
        claim = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="John Doe",
            claim_type=ClaimType.AUTO,
            claim_amount=2000.00,
            date_of_loss="2025-02-01",
            date_filed="2025-02-03",
            description="Minor fender bender in parking lot",
            vehicle_vin="1HGBH41JXMN109186",
        )
        score, result = self.detector.score_claim(claim)
        assert score < 0.5
        assert result.risk_level in [RiskLevel.LOW, RiskLevel.MEDIUM]

    def test_suspicious_high_amount(self):
        claim = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="Suspicious Person",
            claim_type=ClaimType.AUTO,
            claim_amount=200000.00,
            date_of_loss="2025-01-01",
            date_filed="2025-03-15",
            description="Major accident",
        )
        score, result = self.detector.score_claim(claim)
        assert score > 0.3  # Should be flagged as at least medium risk
        assert result.fraud_score == score

    def test_round_amount_flag(self):
        claim = InsuranceClaim(
            policy_number="PRP-123456",
            claimant_name="Round Amount Person",
            claim_type=ClaimType.PROPERTY,
            claim_amount=50000.00,
            date_of_loss="2025-02-01",
            description="Property damage claim",
        )
        score, result = self.detector.score_claim(claim)
        rules = result.processing_metadata.get('fraud_rules_triggered', [])
        assert 'round_amount' in rules


class TestClaimsEnricher:
    def setup_method(self):
        self.enricher = ClaimsEnricher()

    def test_enrichment_adds_fields(self):
        claim = InsuranceClaim(
            policy_number="HLT-123456",
            claimant_name="Jane Smith",
            claim_type=ClaimType.HEALTH,
            claim_amount=15000.00,
            date_of_loss="2025-02-01",
            fraud_score=0.2,
            risk_level=RiskLevel.LOW,
        )
        result = self.enricher.enrich(claim)
        assert result.status == ClaimStatus.ENRICHED
        assert 'policy' in result.enrichment_data
        assert 'claimant_history' in result.enrichment_data
        assert 'geo_risk' in result.enrichment_data
        assert 'reserve_estimate' in result.enrichment_data
        assert 'adjuster_recommendation' in result.enrichment_data

    def test_high_fraud_gets_siu(self):
        claim = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="Fraud Suspect",
            claim_type=ClaimType.AUTO,
            claim_amount=50000.00,
            date_of_loss="2025-02-01",
            fraud_score=0.85,
            risk_level=RiskLevel.CRITICAL,
        )
        result = self.enricher.enrich(claim)
        adjuster = result.enrichment_data['adjuster_recommendation']
        assert "SIU" in adjuster['assigned_team'] or adjuster['priority'] == 'urgent'


class TestEndToEndPipeline:
    """Integration tests for the full pipeline (without Kafka)."""

    def test_full_processing_pipeline(self):
        validator = ClaimsValidator()
        detector = FraudDetector()
        enricher = ClaimsEnricher()

        claim = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="Integration Test User",
            claim_type=ClaimType.AUTO,
            claim_amount=8500.00,
            date_of_loss="2025-02-10",
            description="Side-swipe in parking lot",
            vehicle_vin="1HGBH41JXMN109186",
        )

        # Step 1: Validate
        is_valid, claim = validator.validate(claim)
        assert is_valid
        assert claim.status == ClaimStatus.VALIDATED

        # Step 2: Fraud Detection
        score, claim = detector.score_claim(claim)
        assert 0 <= score <= 1
        assert claim.status in [ClaimStatus.SCORED, ClaimStatus.FLAGGED_FRAUD]

        # Step 3: Enrichment
        claim = enricher.enrich(claim)
        assert claim.status == ClaimStatus.ENRICHED
        assert len(claim.enrichment_data) > 0

        # Verify processing metadata trail
        assert 'validated_at' in claim.processing_metadata
        assert 'fraud_scored_at' in claim.processing_metadata
        assert 'enriched_at' in claim.processing_metadata

    def test_pipeline_latency_under_one_second(self):
        """Verify sub-second processing per claim."""
        validator = ClaimsValidator()
        detector = FraudDetector()
        enricher = ClaimsEnricher()

        claim = InsuranceClaim(
            policy_number="HLT-999999",
            claimant_name="Latency Test User",
            claim_type=ClaimType.HEALTH,
            claim_amount=25000.00,
            date_of_loss="2025-02-15",
            description="Emergency room visit",
            provider_name="Memorial Hospital",
            diagnosis_code="R10.9",
        )

        start = time.time()

        is_valid, claim = validator.validate(claim)
        if is_valid:
            _, claim = detector.score_claim(claim)
            claim = enricher.enrich(claim)

        elapsed_ms = (time.time() - start) * 1000

        assert elapsed_ms < 1000, f"Pipeline took {elapsed_ms:.1f}ms (should be <1000ms)"
        print(f"Pipeline latency: {elapsed_ms:.2f}ms")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])