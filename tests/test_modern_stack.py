"""
Unit tests for 2026 Modern Tech Stack Enhancements
Covering OpenLineage RunEvents, DuckDB Vectorized Analytics Engine, and Declarative Data Quality Scorecards.
"""

import pytest
from src.lineage.openlineage import OpenLineageEmitter
from src.analytics.duckdb_engine import DuckDBAnalyticsEngine
from src.processors.claims_validator import ClaimsValidator
from src.models.claims import InsuranceClaim, ClaimType


class TestOpenLineageEmitter:
    def test_create_run_event_structure(self):
        emitter = OpenLineageEmitter(namespace="test.insurance.pipeline")
        event = emitter.create_run_event(
            job_name="test_job",
            event_type="COMPLETE",
            inputs=[{"name": "kafka.raw-claims"}],
            outputs=[{"name": "postgres.processed_claims"}],
            data_quality_metrics={"quality_score": 98.5}
        )

        assert event["eventType"] == "COMPLETE"
        assert event["job"]["name"] == "test_job"
        assert event["job"]["namespace"] == "test.insurance.pipeline"
        assert len(event["inputs"]) == 1
        assert len(event["outputs"]) == 1
        assert event["run"]["facets"]["dataQualityMetrics"]["quality_score"] == 98.5
        assert "schemaURL" in event


class TestDuckDBAnalyticsEngine:
    def test_register_and_query_claims(self):
        engine = DuckDBAnalyticsEngine()
        claims = [
            {"claim_id": "CLM-1", "policy_number": "POL-1", "claim_amount": 1000.0, "claim_type": "auto", "fraud_score": 0.1, "is_fraud_flag": False},
            {"claim_id": "CLM-2", "policy_number": "POL-2", "claim_amount": 5000.0, "claim_type": "property", "fraud_score": 0.9, "is_fraud_flag": True},
        ]
        engine.register_claims(claims)

        results = engine.execute_query("SELECT COUNT(*) as cnt FROM claims")
        assert len(results) == 1
        assert results[0]["cnt"] == 2

    def test_summary_statistics(self):
        engine = DuckDBAnalyticsEngine()
        claims = [
            {"claim_id": "CLM-1", "policy_number": "POL-1", "claim_amount": 2000.0, "claim_type": "auto", "fraud_score": 0.2, "is_fraud_flag": False},
            {"claim_id": "CLM-2", "policy_number": "POL-2", "claim_amount": 8000.0, "claim_type": "auto", "fraud_score": 0.8, "is_fraud_flag": True},
        ]
        engine.register_claims(claims)

        stats = engine.get_summary_statistics()
        assert stats["total_claims"] == 2
        assert stats["avg_claim_amount"] == 5000.0
        assert stats["max_claim_amount"] == 8000.0
        assert stats["total_fraud_flagged"] == 1

    def test_distribution_query(self):
        engine = DuckDBAnalyticsEngine()
        claims = [
            {"claim_id": "CLM-1", "policy_number": "POL-1", "claim_amount": 1000.0, "claim_type": "auto"},
            {"claim_id": "CLM-2", "policy_number": "POL-2", "claim_amount": 3000.0, "claim_type": "auto"},
            {"claim_id": "CLM-3", "policy_number": "POL-3", "claim_amount": 4000.0, "claim_type": "health"},
        ]
        engine.register_claims(claims)

        dist = engine.get_claims_by_type_distribution()
        assert len(dist) == 2
        assert dist[0]["claim_type"] == "auto"
        assert dist[0]["count"] == 2


class TestDeclarativeDataExpectations:
    def test_evaluate_data_expectations_all_pass(self):
        validator = ClaimsValidator()
        claims = [
            InsuranceClaim(
                claim_id="CLM-001",
                policy_number="ABC-123456",
                claimant_name="John Smith",
                claim_type=ClaimType.AUTO,
                claim_amount=1500.0,
                date_of_loss="2025-01-15"
            )
        ]
        scorecard = validator.evaluate_data_expectations(claims)
        assert scorecard["total_claims"] == 1
        assert scorecard["overall_quality_score"] == 100.0
        assert len(scorecard["expectations"]) == 3
        assert all(exp["passed"] for exp in scorecard["expectations"])

    def test_evaluate_data_expectations_with_violations(self):
        validator = ClaimsValidator()
        claims = [
            InsuranceClaim(
                claim_id="CLM-001",
                policy_number="INVALID_POL",
                claimant_name="John Smith",
                claim_type=ClaimType.AUTO,
                claim_amount=1500.0,
                date_of_loss="2025-01-15"
            )
        ]
        scorecard = validator.evaluate_data_expectations(claims)
        assert scorecard["total_claims"] == 1
        assert scorecard["overall_quality_score"] < 100.0
        policy_exp = next(e for e in scorecard["expectations"] if e["name"] == "expect_policy_number_format_valid")
        assert policy_exp["passed"] is False
