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
# Phase 2 imports
from src.models.claims import ValidationSeverity, VALIDATION_SEVERITY_MAP, SCHEMA_VERSION


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
            date_of_loss=(date.today() - timedelta(days=10)).isoformat(),
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


# ===========================================================================
# Phase 2 tests
# ===========================================================================

class TestIdempotencyKey:
    """A1: Idempotency key is deterministic and unique per distinct claim."""

    def test_same_inputs_produce_same_key(self):
        c1 = InsuranceClaim(
            policy_number="AUT-111111",
            claimant_name="Alice",
            claim_type=ClaimType.AUTO,
            claim_amount=5000.00,
            date_of_loss="2025-03-01",
        )
        c2 = InsuranceClaim(
            policy_number="AUT-111111",
            claimant_name="Different Name",  # name does NOT affect idempotency key
            claim_type=ClaimType.HEALTH,     # type does NOT affect idempotency key
            claim_amount=5000.00,
            date_of_loss="2025-03-01",
        )
        assert c1.idempotency_key == c2.idempotency_key

    def test_different_amounts_produce_different_keys(self):
        c1 = InsuranceClaim(
            policy_number="AUT-111111",
            claimant_name="Alice",
            claim_type=ClaimType.AUTO,
            claim_amount=5000.00,
            date_of_loss="2025-03-01",
        )
        c2 = InsuranceClaim(
            policy_number="AUT-111111",
            claimant_name="Alice",
            claim_type=ClaimType.AUTO,
            claim_amount=5001.00,
            date_of_loss="2025-03-01",
        )
        assert c1.idempotency_key != c2.idempotency_key

    def test_key_length_is_32(self):
        c = InsuranceClaim(
            policy_number="HLT-999999",
            claimant_name="Bob",
            claim_type=ClaimType.HEALTH,
            claim_amount=1200.00,
            date_of_loss="2025-01-01",
        )
        assert c.idempotency_key is not None
        assert len(c.idempotency_key) == 32


class TestSchemaVersion:
    """B2: Schema version is embedded in serialized claim."""

    def test_to_kafka_dict_includes_schema_version(self):
        c = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="Carol",
            claim_type=ClaimType.AUTO,
            claim_amount=3000.00,
            date_of_loss="2025-02-01",
        )
        d = c.to_kafka_dict()
        assert "schema_version" in d
        assert d["schema_version"] == "v1"

    def test_from_kafka_dict_preserves_schema_version(self):
        c = InsuranceClaim(
            policy_number="PRP-654321",
            claimant_name="Dave",
            claim_type=ClaimType.PROPERTY,
            claim_amount=15000.00,
            date_of_loss="2025-01-10",
        )
        d = c.to_kafka_dict()
        restored = InsuranceClaim.from_kafka_dict(d)
        assert restored.schema_version == "v1"


class TestValidationSeverity:
    """C1: Each validation error carries a severity classification."""

    def setup_method(self):
        import src.processors.claims_validator as mod
        mod.lineage_tracker.record_event = lambda **kwargs: None
        self.validator = ClaimsValidator()

    def test_required_field_error_is_critical(self):
        claim = InsuranceClaim(
            policy_number="",
            claimant_name="Eve",
            claim_type=ClaimType.AUTO,
            claim_amount=5000.00,
            date_of_loss="2025-02-01",
        )
        _, result = self.validator.validate(claim)
        details = result.processing_metadata.get("validation_error_details", [])
        critical = [d for d in details if d.get("code") == "REQUIRED_FIELD_MISSING"]
        assert len(critical) > 0
        assert all(d["severity"] == "critical" for d in critical)

    def test_invalid_vin_is_medium_severity(self):
        from datetime import date, timedelta
        recent = (date.today() - timedelta(days=5)).isoformat()
        claim = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="Frank",
            claim_type=ClaimType.AUTO,
            claim_amount=5000.00,
            date_of_loss=recent,
            vehicle_vin="BADVIN",
            description="Minor accident on the highway",
        )
        _, result = self.validator.validate(claim)
        details = result.processing_metadata.get("validation_error_details", [])
        vin_errors = [d for d in details if d.get("code") == "INVALID_VIN_FORMAT"]
        assert len(vin_errors) > 0
        assert vin_errors[0]["severity"] == "medium"

    def test_error_details_always_have_severity(self):
        from datetime import date, timedelta
        recent = (date.today() - timedelta(days=5)).isoformat()
        claim = InsuranceClaim(
            policy_number="BAD",
            claimant_name="Grace",
            claim_type=ClaimType.AUTO,
            claim_amount=1000.00,
            date_of_loss=recent,
        )
        _, result = self.validator.validate(claim)
        details = result.processing_metadata.get("validation_error_details", [])
        assert all("severity" in d for d in details)


class TestStopTheLine:
    """C2: CRITICAL errors immediately reject the claim (stop-the-line)."""

    def setup_method(self):
        import src.processors.claims_validator as mod
        mod.lineage_tracker.record_event = lambda **kwargs: None
        self.validator = ClaimsValidator()

    def test_missing_required_fields_triggers_stop_the_line(self):
        claim = InsuranceClaim(
            policy_number="",
            claimant_name="",
            claim_type=ClaimType.AUTO,
            claim_amount=5000.00,
            date_of_loss="2025-01-01",
        )
        is_valid, result = self.validator.validate(claim)
        assert is_valid is False
        assert result.processing_metadata.get("has_critical_validation_error") is True
        # Stop-the-line means severity_summary.critical > 0
        severity_summary = result.processing_metadata.get("validation_severity_summary", {})
        assert severity_summary.get("critical", 0) > 0

    def test_severity_summary_always_present(self):
        from datetime import date, timedelta
        recent = (date.today() - timedelta(days=5)).isoformat()
        claim = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="Henry",
            claim_type=ClaimType.AUTO,
            claim_amount=3000.00,
            date_of_loss=recent,
        )
        _, result = self.validator.validate(claim)
        summary = result.processing_metadata.get("validation_severity_summary", {})
        assert set(summary.keys()) == {"critical", "high", "medium", "low"}

    def test_valid_claim_has_no_critical_flag(self):
        from datetime import date, timedelta
        recent = (date.today() - timedelta(days=5)).isoformat()
        claim = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="Iris",
            claim_type=ClaimType.AUTO,
            claim_amount=3000.00,
            date_of_loss=recent,
            description="Rear-end collision",
        )
        is_valid, result = self.validator.validate(claim)
        assert is_valid is True
        assert result.processing_metadata.get("has_critical_validation_error") is False


class TestPIIMasking:
    """D1: PII fields are correctly masked for safe logging."""

    def test_name_masked_to_initials(self):
        from src.observability.pii_masking import mask_name
        assert mask_name("John Michael Doe") == "J.M.D."
        assert mask_name("Alice") == "A."
        assert mask_name(None) == "***"

    def test_vin_shows_last_4_only(self):
        from src.observability.pii_masking import mask_vin
        masked = mask_vin("1HGBH41JXMN109186")
        assert masked.endswith("9186")
        assert "*" in masked

    def test_policy_number_shows_prefix_and_last_3(self):
        from src.observability.pii_masking import mask_policy_number
        assert mask_policy_number("AUT-123456") == "AUT-***456"
        assert mask_policy_number("HLT-789012") == "HLT-***012"

    def test_mask_claim_for_logging_does_not_mutate_original(self):
        from src.observability.pii_masking import mask_claim_for_logging
        original = {"claimant_name": "Jane Doe", "policy_number": "AUT-111111", "claim_amount": 5000}
        masked = mask_claim_for_logging(original)
        assert original["claimant_name"] == "Jane Doe"   # original unchanged
        assert masked["claimant_name"] != "Jane Doe"     # copy is masked


class TestCircuitBreaker:
    """A3: Circuit breaker trips after threshold failures and self-heals."""

    def test_circuit_trips_after_threshold(self):
        from src.observability.circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerOpen
        cb = CircuitBreaker("test-dep-trip", failure_threshold=3, recovery_timeout=60)

        def bad_fn():
            raise ConnectionError("simulated failure")

        for _ in range(3):
            try:
                cb.call(bad_fn)
            except ConnectionError:
                pass

        assert cb.state == CircuitState.OPEN
        with pytest.raises(CircuitBreakerOpen):
            cb.call(bad_fn)

    def test_circuit_recovers_after_timeout(self):
        from src.observability.circuit_breaker import CircuitBreaker, CircuitState
        cb = CircuitBreaker("test-dep-recover", failure_threshold=2, recovery_timeout=0.01)

        def bad_fn():
            raise ConnectionError("simulated")

        def good_fn():
            return "ok"

        for _ in range(2):
            try:
                cb.call(bad_fn)
            except ConnectionError:
                pass

        assert cb.state == CircuitState.OPEN

        import time
        time.sleep(0.05)  # wait past recovery_timeout (10ms)

        result = cb.call(good_fn)  # probe call — should succeed and move to HALF_OPEN
        # Second success needed to close (success_threshold=2)
        cb.call(good_fn)
        assert cb.state == CircuitState.CLOSED
        assert result == "ok"

    def test_closed_circuit_passes_through(self):
        from src.observability.circuit_breaker import CircuitBreaker, CircuitState
        cb = CircuitBreaker("test-dep-passthrough", failure_threshold=5)
        assert cb.call(lambda: 42) == 42
        assert cb.state == CircuitState.CLOSED


# ===========================================================================
# Phase 2b tests — new features
# ===========================================================================

class TestCorrelationID:
    """Correlation ID is auto-generated and survives Kafka round-trip."""

    def test_correlation_id_auto_generated(self):
        c = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="Test",
            claim_type=ClaimType.AUTO,
            claim_amount=1000.00,
            date_of_loss="2025-06-01",
        )
        assert c.correlation_id is not None
        assert len(c.correlation_id) == 32  # hex UUID without dashes

    def test_correlation_id_survives_kafka_roundtrip(self):
        c = InsuranceClaim(
            policy_number="AUT-654321",
            claimant_name="RoundTrip",
            claim_type=ClaimType.AUTO,
            claim_amount=2000.00,
            date_of_loss="2025-06-10",
        )
        original_cid = c.correlation_id
        d = c.to_kafka_dict()
        restored = InsuranceClaim.from_kafka_dict(d)
        assert restored.correlation_id == original_cid

    def test_two_claims_get_different_correlation_ids(self):
        c1 = InsuranceClaim(
            policy_number="AUT-111111",
            claimant_name="A",
            claim_type=ClaimType.AUTO,
            claim_amount=1000.00,
            date_of_loss="2025-06-01",
        )
        c2 = InsuranceClaim(
            policy_number="AUT-111111",
            claimant_name="A",
            claim_type=ClaimType.AUTO,
            claim_amount=1000.00,
            date_of_loss="2025-06-01",
        )
        assert c1.correlation_id != c2.correlation_id


class TestAuditTrail:
    """Structured audit trail records every state transition."""

    def test_audit_trail_starts_empty(self):
        c = InsuranceClaim(
            policy_number="AUT-123456",
            claimant_name="Audit",
            claim_type=ClaimType.AUTO,
            claim_amount=3000.00,
            date_of_loss="2025-06-01",
        )
        assert c.audit_trail == []

    def test_record_audit_event_appends(self):
        c = InsuranceClaim(
            policy_number="HLT-999999",
            claimant_name="AuditTest",
            claim_type=ClaimType.HEALTH,
            claim_amount=5000.00,
            date_of_loss="2025-06-01",
        )
        c.record_audit_event("ingestion", "received")
        c.record_audit_event("validation", "passed", "no errors")

        assert len(c.audit_trail) == 2
        assert c.audit_trail[0]["stage"] == "ingestion"
        assert c.audit_trail[0]["status"] == "received"
        assert c.audit_trail[0]["correlation_id"] == c.correlation_id
        assert c.audit_trail[1]["detail"] == "no errors"
        assert "timestamp" in c.audit_trail[0]

    def test_audit_trail_preserved_in_kafka_dict(self):
        c = InsuranceClaim(
            policy_number="PRP-111111",
            claimant_name="SerAudit",
            claim_type=ClaimType.PROPERTY,
            claim_amount=10000.00,
            date_of_loss="2025-06-01",
        )
        c.record_audit_event("ingestion", "received")
        d = c.to_kafka_dict()
        assert len(d["audit_trail"]) == 1

        restored = InsuranceClaim.from_kafka_dict(d)
        assert len(restored.audit_trail) == 1
        assert restored.audit_trail[0]["stage"] == "ingestion"


class TestHealthDegradation:
    """Health check returns three-level degradation model."""

    def test_overall_status_returns_valid_level(self):
        from src.observability.health import PipelineHealthMonitor
        monitor = PipelineHealthMonitor()
        # No checks run yet → unknown
        assert monitor.get_overall_status() == "unknown"

    def test_all_healthy_returns_healthy(self):
        from src.observability.health import PipelineHealthMonitor
        monitor = PipelineHealthMonitor()
        for name in monitor.components:
            monitor.update_component(name, "healthy", latency_ms=10)
        assert monitor.get_overall_status() == "healthy"

    def test_critical_component_down_returns_unhealthy(self):
        from src.observability.health import PipelineHealthMonitor
        monitor = PipelineHealthMonitor()
        for name in monitor.components:
            monitor.update_component(name, "healthy", latency_ms=10)
        # Down a critical component
        monitor.update_component("kafka_broker", "down", latency_ms=0, message="unreachable")
        assert monitor.get_overall_status() == "unhealthy"

    def test_non_critical_down_returns_degraded(self):
        from src.observability.health import PipelineHealthMonitor
        monitor = PipelineHealthMonitor()
        for name in monitor.components:
            monitor.update_component(name, "healthy", latency_ms=10)
        monitor.update_component("jaeger_tracing", "down", latency_ms=0, message="timeout")
        assert monitor.get_overall_status() == "degraded"

    def test_health_report_includes_degradation_level(self):
        from src.observability.health import PipelineHealthMonitor
        monitor = PipelineHealthMonitor()
        for name in monitor.components:
            monitor.update_component(name, "healthy", latency_ms=10)
        # Monkey-patch run_all_checks to avoid real network calls
        monitor.run_all_checks = lambda: monitor.components
        report = monitor.get_health_report()
        assert "degradation_level" in report
        assert report["degradation_level"] == 0  # healthy

    def test_report_marks_critical_components(self):
        from src.observability.health import PipelineHealthMonitor
        monitor = PipelineHealthMonitor()
        for name in monitor.components:
            monitor.update_component(name, "healthy", latency_ms=10)
        monitor.run_all_checks = lambda: monitor.components
        report = monitor.get_health_report()
        assert report["components"]["kafka_broker"]["critical"] is True
        assert report["components"]["jaeger_tracing"]["critical"] is False


class TestRedisCache:
    """Redis cache gracefully degrades when Redis is unavailable."""

    def test_unavailable_cache_returns_none(self):
        from src.observability.cache import RedisCache
        cache = RedisCache(host="nonexistent-host-12345", port=9999)
        assert cache.available is False
        assert cache.get("ns", "key") is None
        assert cache.exists("ns", "key") is False
        assert cache.check_idempotency("somekey") is False

    def test_feature_flag_default(self):
        from src.observability.cache import RedisCache
        cache = RedisCache(host="nonexistent-host-12345", port=9999)
        assert cache.get_feature_flag("some_flag", default=True) is True
        assert cache.get_feature_flag("some_flag", default=False) is False

    def test_stats_when_unavailable(self):
        from src.observability.cache import RedisCache
        cache = RedisCache(host="nonexistent-host-12345", port=9999)
        stats = cache.get_stats()
        assert stats["available"] is False


class TestExponentialBackoff:
    """Exponential backoff retries transient failures."""

    def test_succeeds_on_first_try(self):
        from src.processors.stream_processor import _exponential_backoff_call
        call_count = {"n": 0}
        def fn():
            call_count["n"] += 1
            return "ok"
        result = _exponential_backoff_call(fn)
        assert result == "ok"
        assert call_count["n"] == 1

    def test_retries_then_succeeds(self):
        from src.processors.stream_processor import _exponential_backoff_call
        call_count = {"n": 0}
        def fn():
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise ConnectionError("transient")
            return "recovered"
        result = _exponential_backoff_call(fn, max_retries=4)
        assert result == "recovered"
        assert call_count["n"] == 3

    def test_circuit_breaker_open_not_retried(self):
        from src.processors.stream_processor import _exponential_backoff_call
        from src.observability.circuit_breaker import CircuitBreakerOpen
        call_count = {"n": 0}
        def fn():
            call_count["n"] += 1
            raise CircuitBreakerOpen("test")
        with pytest.raises(CircuitBreakerOpen):
            _exponential_backoff_call(fn, max_retries=4)
        assert call_count["n"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])