"""
Phase 3 Enterprise Data Platform Test Suite.

Verifies end-to-end functionality across all 9 Phase 3 workstreams:
  1. Multi-LOB Domain Engine (Auto, Health, Property, Commercial, Cyber)
  2. CQRS + Event Sourcing Architecture
  3. Advanced ML Fraud Detection Pipeline
  4. Enterprise Data Governance & Compliance
  5. Intelligent Claims Adjudication Engine
  6. Enterprise Observability Mesh
  7. Data Lakehouse Architecture
  8. Enterprise Integration Layer
  9. Chaos Engineering & Performance Testing Framework
"""

import pytest
from src.models.claims import InsuranceClaim, ClaimType, ClaimStatus, RiskLevel

# 1. Multi-LOB Domain Engine Tests
from src.domain.lob_registry import LOBRegistry

def test_lob_registry_auto_discovery():
    registry = LOBRegistry.instance()
    lobs = registry.list_lobs()
    codes = [l["lob_code"] for l in lobs]
    assert "AUTO" in codes
    assert "HEALTH" in codes
    assert "PROPERTY" in codes
    assert "COMMERCIAL" in codes
    assert "CYBER" in codes

def test_auto_lob_validation_and_enrichment():
    registry = LOBRegistry.instance()
    lob = registry.get_lob(ClaimType.AUTO)
    claim = InsuranceClaim(
        claim_id="CLM-TEST-AUTO",
        policy_number="POL-AUTO-1",
        claimant_name="Alice Smith",
        claim_type=ClaimType.AUTO,
        claim_amount=15000.0,
        date_of_loss="2026-08-01",
        date_filed="2026-08-01",
        description="Collision damage front bumper",
        vehicle_vin="1G1JC524417100000",
    )
    is_valid, errors = lob.validate_claim(claim)
    assert is_valid
    enrichment = lob.enrich_claim(claim)
    assert "vehicle_make" in enrichment
    assert "actual_cash_value" in enrichment

# 2. CQRS + Event Sourcing Tests
from src.cqrs.event_store import EventStore, EventType
from src.cqrs.command_handler import ClaimCommandHandler, SubmitClaimCommand
from src.cqrs.saga_orchestrator import ClaimLifecycleSagaOrchestrator, SagaStatus

def test_cqrs_saga_orchestration():
    store = EventStore()
    handler = ClaimCommandHandler(store)
    orchestrator = ClaimLifecycleSagaOrchestrator(handler)
    cmd = SubmitClaimCommand(
        claim_id="CLM-SAGA-100",
        policy_number="POL-100",
        claimant_id="C100",
        claimant_name="Bob Jones",
        claim_type=ClaimType.AUTO,
        claim_amount=3500.0,
        date_of_loss="2026-08-01",
        date_filed="2026-08-02",
        description="Side door scratch in parking lot",
        vehicle_vin="1G1JC524417100000",
    )
    saga = orchestrator.start_saga(cmd)
    assert saga.status == SagaStatus.COMPLETED
    events = store.get_events("CLM-SAGA-100")
    assert len(events) == 6
    assert events[0].event_type == EventType.CLAIM_SUBMITTED
    assert events[-1].event_type == EventType.PAYMENT_AUTHORIZED

# 3. Advanced ML Fraud Detection Pipeline Tests
from src.ml.scoring_service import FraudScoringService
from src.ml.drift_detector import DriftDetector

def test_ml_fraud_scoring_and_drift():
    service = FraudScoringService()
    claim = InsuranceClaim(
        claim_id="CLM-ML-TEST",
        policy_number="POL-ML-1",
        claimant_name="Charlie Brown",
        claim_type=ClaimType.AUTO,
        claim_amount=25000.0,
        date_of_loss="2026-08-01",
        date_filed="2026-08-01",
        description="Major vehicle collision",
        vehicle_vin="1G1JC524417100000",
    )
    res = service.score_claim(claim)
    assert 0.0 <= res.combined_score <= 1.0
    assert len(res.explainability) > 0

    detector = DriftDetector()
    psi = detector.calculate_psi([1.0, 2.0, 3.0], [1.1, 2.1, 3.1])
    assert psi < 0.10

# 4. Enterprise Data Governance & Compliance Tests
from src.governance.data_catalog import DataCatalog
from src.governance.access_control import AccessControlEngine, UserRole
from src.governance.privacy_engine import PrivacyEngine
from src.governance.audit_engine import AuditEngine
from src.models.claims import DataClass

def test_governance_rbac_privacy_and_audit():
    catalog = DataCatalog.instance()
    assert catalog.get_asset("kafka.raw.claims") is not None

    rbac = AccessControlEngine()
    masked = rbac.filter_and_mask_record(
        {"claimant_name": "John Doe", "claim_amount": 5000.0},
        UserRole.FRAUD_ANALYST,
        {"claimant_name": DataClass.PII, "claim_amount": DataClass.SENSITIVE},
    )
    assert masked["claimant_name"] != "John Doe"

    privacy = PrivacyEngine()
    anon = privacy.execute_right_to_erasure({"claim_id": "CLM-1", "claimant_name": "Jane Doe"})
    assert anon["gdpr_anonymized"] is True

    audit = AuditEngine()
    audit.log_event("admin", "platform_admin", "READ", "CLM-1", {"test": True})
    valid, _ = audit.verify_integrity()
    assert valid is True

# 5. Intelligent Claims Adjudication Engine Tests
from src.adjudication.workflow_engine import AdjudicationWorkflowEngine

def test_adjudication_workflow():
    engine = AdjudicationWorkflowEngine()
    claim = InsuranceClaim(
        claim_id="CLM-ADJ-TEST",
        policy_number="POL-ADJ-1",
        claimant_name="David Miller",
        claim_type=ClaimType.PROPERTY,
        claim_amount=12000.0,
        date_of_loss="2026-08-01",
        date_filed="2026-08-02",
        description="Property roof windstorm damage",
        property_address="456 Elm St, Dallas TX 75201",
    )
    adj_result = engine.adjudicate(claim)
    assert adj_result["decision"] == "approved"
    assert adj_result["approved_amount"] > 0.0

# 6. Enterprise Observability Mesh Tests
from src.observability.sli_slo_manager import SLISLOManager
from src.observability.synthetic_monitoring import SyntheticMonitoringService
from src.observability.runbook_automation import RunbookAutomationEngine

def test_observability_mesh():
    slo = SLISLOManager.instance()
    slo.record_event("validation_latency_p95", True)
    assert len(slo.get_slo_report()) > 0

    synthetic = SyntheticMonitoringService()
    test_res = synthetic.run_canary_test()
    assert test_res.success is True

    runbook = RunbookAutomationEngine()
    rb_res = runbook.execute_runbook("CircuitBreakerTripped", "postgres")
    assert rb_res.success is True

# 7. Data Lake Architecture Tests
from src.lakehouse.duckdb_warehouse import DuckDBWarehouse
from src.lakehouse.iceberg_catalog import IcebergCatalogManager
from src.lakehouse.data_quality_framework import DataQualityFramework

def test_lakehouse_olap_and_dq():
    dw = DuckDBWarehouse()
    loss_ratios = dw.loss_ratio_by_lob()
    assert len(loss_ratios) > 0

    iceberg = IcebergCatalogManager.instance()
    compaction = iceberg.compact_table("curated.fct_claims")
    assert compaction["compacted_files_count"] == 4

    dq = DataQualityFramework(dw)
    suite_res = dq.run_suite()
    assert all(r.passed for r in suite_res)

# 8. Enterprise Integration Layer Tests
from src.integrations.webhook_engine import WebhookEngine
from src.integrations.cdc_pipeline import CDCPipeline
from src.integrations.adapters.external_adapters import ExternalAdapters

def test_integration_layer():
    wh = WebhookEngine()
    wh.register_subscription("s1", "https://example.com/webhook", ["*"], "secret")
    deliveries = wh.dispatch_event("ClaimValidated", {"claim_id": "CLM-1"})
    assert len(deliveries) == 1

    cdc = CDCPipeline()
    cdc_rec = cdc.process_wal_change("c", "claims", None, {"id": 1})
    assert cdc_rec.op == "c"

    adapters = ExternalAdapters()
    clue = adapters.query_clue_report("John Doe", "123 Main St")
    assert clue["status"] == "clear"

# 9. Chaos Engineering Tests
from src.chaos.game_day_scenarios import GameDayScenarios

def test_chaos_game_day_scenarios():
    gd = GameDayScenarios()
    scen1 = gd.scenario_kafka_broker_outage()
    scen2 = gd.scenario_db_failover()
    assert scen1["validated"] is True
    assert scen2["validated"] is True
