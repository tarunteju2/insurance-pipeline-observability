"""
CQRS Materialized Projections.

Builds and updates read-optimized view models by subscribing to domain events
from the EventStore. Supports real-time query models.
"""

from __future__ import annotations

import structlog
from typing import Any, Dict, List, Optional
from collections import defaultdict

from src.cqrs.event_store import EventStore, ClaimEvent, EventType

logger = structlog.get_logger(__name__)


class ClaimCurrentStateProjection:
    """Materialized view projection holding the latest state of each claim aggregate."""

    def __init__(self, event_store: Optional[EventStore] = None):
        self._claims: Dict[str, Dict[str, Any]] = {}
        if event_store:
            event_store.register_listener(self.on_event)

    def on_event(self, event: ClaimEvent) -> None:
        claim_id = event.aggregate_id
        current = self._claims.setdefault(claim_id, {"claim_id": claim_id, "version": 0})
        current.update(event.data)
        current["version"] = event.aggregate_version
        current["last_updated_at"] = event.timestamp

    def get_claim(self, claim_id: str) -> Optional[Dict[str, Any]]:
        return self._claims.get(claim_id)

    def list_claims(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        if not status:
            return list(self._claims.values())
        return [c for c in self._claims.values() if c.get("status") == status]


class FraudDashboardProjection:
    """Read projection summarizing real-time fraud scoring statistics."""

    def __init__(self, event_store: Optional[EventStore] = None):
        self.total_scored = 0
        self.flagged_count = 0
        self.by_risk_level: Dict[str, int] = defaultdict(int)
        self.by_claim_type: Dict[str, List[float]] = defaultdict(list)
        if event_store:
            event_store.register_listener(self.on_event)

    def on_event(self, event: ClaimEvent) -> None:
        if event.event_type == EventType.FRAUD_SCORE_ASSIGNED:
            self.total_scored += 1
            risk = event.data.get("risk_level", "low")
            self.by_risk_level[risk] += 1

            claim_type = event.data.get("claim_type", "generic")
            score = event.data.get("combined_fraud_score", 0.0)
            self.by_claim_type[claim_type].append(score)

        elif event.event_type == EventType.SIU_REFERRAL_CREATED:
            self.flagged_count += 1

    def get_summary(self) -> Dict[str, Any]:
        avg_scores = {}
        for ctype, scores in self.by_claim_type.items():
            avg_scores[ctype] = round(sum(scores) / max(len(scores), 1), 3)

        return {
            "total_claims_scored": self.total_scored,
            "total_siu_referrals": self.flagged_count,
            "risk_level_counts": dict(self.by_risk_level),
            "avg_fraud_score_by_type": avg_scores,
        }


class FinancialSummaryProjection:
    """Read projection tracking financial exposure, reserves, and authorized payments."""

    def __init__(self, event_store: Optional[EventStore] = None):
        self.total_submitted_amount = 0.0
        self.total_authorized_payments = 0.0
        self.total_approved_amount = 0.0
        self.paid_claims_count = 0
        if event_store:
            event_store.register_listener(self.on_event)

    def on_event(self, event: ClaimEvent) -> None:
        if event.event_type == EventType.CLAIM_SUBMITTED:
            self.total_submitted_amount += event.data.get("claim_amount", 0.0)

        elif event.event_type == EventType.CLAIM_ADJUDICATED:
            if event.data.get("decision") == "paid":
                self.paid_claims_count += 1
                self.total_approved_amount += event.data.get("approved_amount", 0.0)

        elif event.event_type == EventType.PAYMENT_AUTHORIZED:
            self.total_authorized_payments += event.data.get("payment_amount", 0.0)

    def get_summary(self) -> Dict[str, Any]:
        return {
            "total_submitted_amount": round(self.total_submitted_amount, 2),
            "total_approved_amount": round(self.total_approved_amount, 2),
            "total_authorized_payments": round(self.total_authorized_payments, 2),
            "paid_claims_count": self.paid_claims_count,
            "pending_exposure": round(self.total_submitted_amount - self.total_approved_amount, 2),
        }
