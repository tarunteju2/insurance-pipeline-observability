"""
Unit & Integration Tests for Phase 4 Advanced Enterprise Complexity Features
Covering WindowProcessor stateful aggregations, Iceberg metadata snapshots, RBAC column masking, and ChaosInjector fault injection.
"""

import pytest
from src.analytics.window_processor import WindowProcessor
from src.analytics.iceberg_engine import IcebergMetadataEngine
from src.security.governance import DataGovernanceEngine, SecurityRole
from scripts.chaos_injection import ChaosInjector


class TestWindowProcessor:
    def test_sliding_window_aggregations(self):
        proc = WindowProcessor(sliding_window_sec=60)
        proc.add_event({"claim_id": "C1", "policy_number": "POL-1", "claim_amount": 1000.0})
        proc.add_event({"claim_id": "C2", "policy_number": "POL-1", "claim_amount": 3000.0})
        
        stats = proc.get_sliding_window_stats()
        assert stats["claim_count"] == 2
        assert stats["total_amount_usd"] == 4000.0
        assert stats["avg_amount_usd"] == 2000.0

    def test_velocity_anomaly_detection(self):
        proc = WindowProcessor(sliding_window_sec=60)
        for i in range(4):
            proc.add_event({"claim_id": f"C{i}", "policy_number": "SUSPICIOUS_POL", "claim_amount": 500.0})
            
        stats = proc.get_sliding_window_stats()
        assert "SUSPICIOUS_POL" in stats["velocity_anomalies"]


class TestIcebergMetadataEngine:
    def test_snapshot_commit_and_history(self):
        engine = IcebergMetadataEngine(table_name="test.db.claims")
        initial_count = len(engine.get_snapshot_history())
        
        new_snap = engine.commit_snapshot(records_added=150)
        assert len(engine.get_snapshot_history()) == initial_count + 1
        assert new_snap["summary"]["added_records"] == 150

    def test_time_travel_query(self):
        engine = IcebergMetadataEngine()
        history = engine.get_snapshot_history()
        target_snap_id = history[0]["snapshot_id"]
        
        res = engine.query_time_travel(snapshot_id=target_snap_id)
        assert res["resolved_snapshot_id"] == target_snap_id
        assert res["active_records_at_snapshot"] == 5000


class TestDataGovernanceEngine:
    def test_public_role_masks_pii(self):
        gov = DataGovernanceEngine()
        raw = {
            "claimant_name": "Alice Smith",
            "policy_number": "ABC-123456",
            "vehicle_vin": "1HGCR2F83HA000000",
            "processing_metadata": {"secret": "value"}
        }
        masked = gov.apply_column_masking(raw, SecurityRole.PUBLIC)
        assert masked["claimant_name"] == "A.S."
        assert masked["policy_number"] == "ABC-***456"
        assert masked["vehicle_vin"] == "REDACTED"
        assert "processing_metadata" not in masked

    def test_executive_role_preserves_financials(self):
        gov = DataGovernanceEngine()
        raw = {
            "claimant_name": "Bob Marley",
            "claim_amount": 50000.00,
            "vehicle_vin": "1HGCR2F83HA000000"
        }
        masked = gov.apply_column_masking(raw, SecurityRole.EXECUTIVE)
        assert masked["claim_amount"] == 50000.00
        assert masked["claimant_name"] == "B.M."


class TestChaosInjector:
    def test_chaos_dry_run_suite(self):
        injector = ChaosInjector()
        # Should complete dry run without unhandled exception
        injector.run_dry_run_suite()
