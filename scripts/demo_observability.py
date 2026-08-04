#!/usr/bin/env python3
"""
Interactive Terminal Demo for the 2026 Data Platform Engineering Project
Demonstrates:
1. OpenLineage 1.0 Metadata Protocol Events
2. DuckDB Vectorized OLAP SQL Analytics Engine
3. Declarative Data Quality Scorecard Engine
4. PII Field Masking & Security Output
"""

import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.lineage.openlineage import OpenLineageEmitter
from src.analytics.duckdb_engine import DuckDBAnalyticsEngine
from src.processors.claims_validator import ClaimsValidator
from src.models.claims import InsuranceClaim, ClaimType
from src.observability.pii_masking import mask_claim_for_logging

def print_header(title):
    print("\n" + "=" * 65)
    print(f" 🚀 {title}")
    print("=" * 65)

def main():
    # -------------------------------------------------------------
    # 1. OpenLineage Standard Metadata Protocol
    # -------------------------------------------------------------
    print_header("1. OpenLineage 1.0 Standard Event Stream (Linux Foundation)")
    emitter = OpenLineageEmitter(namespace="insurance.claims.pipeline")
    event = emitter.create_run_event(
        job_name="kafka_to_postgres_enrichment",
        event_type="COMPLETE",
        inputs=[{"name": "kafka.raw-claims", "namespace": "kafka://localhost:9092"}],
        outputs=[{"name": "postgres.processed_claims", "namespace": "postgresql://localhost:5432/insurance_lineage"}],
        data_quality_metrics={"total_claims": 50, "quality_score": 100.0, "passed_expectations": 3}
    )
    print(json.dumps(event, indent=2))

    # -------------------------------------------------------------
    # 2. DuckDB Vectorized OLAP Lakehouse Analytics Engine
    # -------------------------------------------------------------
    print_header("2. DuckDB Embedded Vectorized OLAP SQL Engine")
    engine = DuckDBAnalyticsEngine()
    sample_claims = [
        {"claim_id": "CLM-A101", "policy_number": "ABC-123456", "claim_amount": 3500.0, "claim_type": "auto", "fraud_score": 0.08, "is_fraud_flag": False},
        {"claim_id": "CLM-P202", "policy_number": "XYZ-987654", "claim_amount": 18500.0, "claim_type": "property", "fraud_score": 0.92, "is_fraud_flag": True},
        {"claim_id": "CLM-H303", "policy_number": "DEF-555111", "claim_amount": 4200.0, "claim_type": "health", "fraud_score": 0.15, "is_fraud_flag": False},
        {"claim_id": "CLM-A104", "policy_number": "GHI-222333", "claim_amount": 6100.0, "claim_type": "auto", "fraud_score": 0.81, "is_fraud_flag": True},
    ]
    engine.register_claims(sample_claims)
    
    print("\n📊 Executing Vectorized OLAP Aggregation Query:")
    sql = """
    SELECT 
        claim_type,
        COUNT(*) as total_claims,
        ROUND(AVG(claim_amount), 2) as avg_amount_usd,
        ROUND(MAX(claim_amount), 2) as max_amount_usd,
        SUM(CASE WHEN is_fraud_flag = true THEN 1 ELSE 0 END) as fraud_cases
    FROM claims
    GROUP BY claim_type
    ORDER BY total_claims DESC
    """
    print(f"SQL: {sql.strip()}\n")
    results = engine.execute_query(sql)
    print(json.dumps(results, indent=2))

    # -------------------------------------------------------------
    # 3. Declarative Data Quality Expectations Scorecard
    # -------------------------------------------------------------
    print_header("3. Declarative Data Quality Expectations Scorecard")
    validator = ClaimsValidator()
    claims_batch = [
        InsuranceClaim(
            claim_id="CLM-DQ-01",
            policy_number="ABC-123456",
            claimant_name="Alice Johnson",
            claim_type=ClaimType.AUTO,
            claim_amount=2450.00,
            date_of_loss="2025-01-15"
        ),
        InsuranceClaim(
            claim_id="CLM-DQ-02",
            policy_number="XYZ-987654",
            claimant_name="Bob Smith",
            claim_type=ClaimType.PROPERTY,
            claim_amount=12000.00,
            date_of_loss="2025-01-18"
        )
    ]
    scorecard = validator.evaluate_data_expectations(claims_batch)
    print(json.dumps(scorecard, indent=2))

    # -------------------------------------------------------------
    # 4. PII Security Masking & Compliance Output
    # -------------------------------------------------------------
    print_header("4. PII Security & Compliance Masking")
    raw_payload = {
        "claimant_name": "Alice Johnson",
        "policy_number": "ABC-123456",
        "vehicle_vin": "1HGCR2F83HA000000"
    }
    masked_payload = mask_claim_for_logging(raw_payload)
    print("Raw Payload (PII exposed):   ", raw_payload)
    print("Masked Payload (Logs safe):  ", masked_payload)
    print("=" * 65 + "\n")

if __name__ == "__main__":
    main()
