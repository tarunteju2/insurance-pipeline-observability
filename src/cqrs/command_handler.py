"""
CQRS Command Handlers.

Encapsulates command definitions and state-mutating command handlers.
Commands produce domain events appended to the EventStore.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from src.cqrs.event_store import EventStore, ClaimEvent, EventType
from src.domain.lob_registry import LOBRegistry
from src.models.claims import InsuranceClaim, ClaimType, ClaimStatus

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SubmitClaimCommand:
    claim_id: str
    policy_number: str
    claimant_id: str
    claimant_name: str
    claim_type: ClaimType
    claim_amount: float
    date_of_loss: str
    date_filed: str
    description: str
    property_address: Optional[str] = None
    vehicle_vin: Optional[str] = None
    diagnosis_code: Optional[str] = None
    provider_name: Optional[str] = None


@dataclass(frozen=True)
class ValidateClaimCommand:
    claim_id: str


@dataclass(frozen=True)
class ScoreFraudCommand:
    claim_id: str
    heuristic_score: float
    ml_score: float
    risk_level: str


@dataclass(frozen=True)
class EnrichClaimCommand:
    claim_id: str


@dataclass(frozen=True)
class AdjudicateClaimCommand:
    claim_id: str
    approved_amount: float
    decision: str  # paid, denied, partial
    reason: str


@dataclass(frozen=True)
class AuthorizePaymentCommand:
    claim_id: str
    payment_amount: float
    payee_id: str
    payment_method: str = "electronic_transfer"


@dataclass(frozen=True)
class EscalateToSIUCommand:
    claim_id: str
    reason: str
    risk_score: float


@dataclass(frozen=True)
class InitiateSubrogationCommand:
    claim_id: str
    third_party_carrier: str
    target_recovery_amount: float


@dataclass(frozen=True)
class CloseClaimCommand:
    claim_id: str
    closure_reason: str


@dataclass(frozen=True)
class ReopenClaimCommand:
    claim_id: str
    reopen_reason: str


# ---------------------------------------------------------------------------
# Command Handler Dispatcher
# ---------------------------------------------------------------------------

class ClaimCommandHandler:
    """Handles incoming state-mutating commands by emitting aggregate domain events."""

    def __init__(self, event_store: Optional[EventStore] = None):
        self.event_store = event_store or EventStore.instance()
        self.lob_registry = LOBRegistry.instance()

    def handle_submit_claim(self, cmd: SubmitClaimCommand) -> ClaimEvent:
        events = self.event_store.get_events(cmd.claim_id)
        if events:
            raise ValueError(f"Claim aggregate {cmd.claim_id} already exists.")

        claim_data = {
            "claim_id": cmd.claim_id,
            "policy_number": cmd.policy_number,
            "claimant_id": cmd.claimant_id,
            "claimant_name": cmd.claimant_name,
            "claim_type": cmd.claim_type.value,
            "claim_amount": cmd.claim_amount,
            "date_of_loss": cmd.date_of_loss,
            "date_filed": cmd.date_filed,
            "description": cmd.description,
            "property_address": cmd.property_address,
            "vehicle_vin": cmd.vehicle_vin,
            "diagnosis_code": cmd.diagnosis_code,
            "provider_name": cmd.provider_name,
            "status": ClaimStatus.SUBMITTED.value,
        }

        evt = ClaimEvent.create(
            aggregate_id=cmd.claim_id,
            event_type=EventType.CLAIM_SUBMITTED,
            version=1,
            data=claim_data,
        )
        self.event_store.append(evt)
        return evt

    def handle_validate_claim(self, cmd: ValidateClaimCommand) -> ClaimEvent:
        events = self.event_store.get_events(cmd.claim_id)
        if not events:
            raise ValueError(f"Claim {cmd.claim_id} not found.")

        current_version = events[-1].aggregate_version
        claim_dict = self._rebuild_claim_dict(events)
        claim = InsuranceClaim(**claim_dict)

        lob = self.lob_registry.get_lob(claim.claim_type)
        is_valid, errors = lob.validate_claim(claim)

        event_type = EventType.CLAIM_VALIDATED if is_valid else EventType.CLAIM_VALIDATION_FAILED
        data = {
            "is_valid": is_valid,
            "errors": errors,
            "status": ClaimStatus.VALIDATED.value if is_valid else ClaimStatus.VALIDATION_FAILED.value,
        }

        evt = ClaimEvent.create(
            aggregate_id=cmd.claim_id,
            event_type=event_type,
            version=current_version + 1,
            data=data,
        )
        self.event_store.append(evt)
        return evt

    def handle_score_fraud(self, cmd: ScoreFraudCommand) -> ClaimEvent:
        events = self.event_store.get_events(cmd.claim_id)
        if not events:
            raise ValueError(f"Claim {cmd.claim_id} not found.")

        current_version = events[-1].aggregate_version
        combined_score = round(0.4 * cmd.heuristic_score + 0.6 * cmd.ml_score, 3)

        data = {
            "heuristic_score": cmd.heuristic_score,
            "ml_score": cmd.ml_score,
            "combined_fraud_score": combined_score,
            "risk_level": cmd.risk_level,
            "status": ClaimStatus.SCORED.value,
        }

        evt = ClaimEvent.create(
            aggregate_id=cmd.claim_id,
            event_type=EventType.FRAUD_SCORE_ASSIGNED,
            version=current_version + 1,
            data=data,
        )
        self.event_store.append(evt)
        return evt

    def handle_enrich_claim(self, cmd: EnrichClaimCommand) -> ClaimEvent:
        events = self.event_store.get_events(cmd.claim_id)
        if not events:
            raise ValueError(f"Claim {cmd.claim_id} not found.")

        current_version = events[-1].aggregate_version
        claim_dict = self._rebuild_claim_dict(events)
        claim = InsuranceClaim(**claim_dict)

        lob = self.lob_registry.get_lob(claim.claim_type)
        enrichment_data = lob.enrich_claim(claim)

        data = {
            "enrichment_data": enrichment_data,
            "status": ClaimStatus.ENRICHED.value,
        }

        evt = ClaimEvent.create(
            aggregate_id=cmd.claim_id,
            event_type=EventType.CLAIM_ENRICHED,
            version=current_version + 1,
            data=data,
        )
        self.event_store.append(evt)
        return evt

    def handle_adjudicate_claim(self, cmd: AdjudicateClaimCommand) -> ClaimEvent:
        events = self.event_store.get_events(cmd.claim_id)
        if not events:
            raise ValueError(f"Claim {cmd.claim_id} not found.")

        current_version = events[-1].aggregate_version
        data = {
            "approved_amount": cmd.approved_amount,
            "decision": cmd.decision,
            "reason": cmd.reason,
            "status": ClaimStatus.COMPLETED.value if cmd.decision == "paid" else ClaimStatus.REJECTED.value,
        }

        evt = ClaimEvent.create(
            aggregate_id=cmd.claim_id,
            event_type=EventType.CLAIM_ADJUDICATED,
            version=current_version + 1,
            data=data,
        )
        self.event_store.append(evt)
        return evt

    def handle_authorize_payment(self, cmd: AuthorizePaymentCommand) -> ClaimEvent:
        events = self.event_store.get_events(cmd.claim_id)
        if not events:
            raise ValueError(f"Claim {cmd.claim_id} not found.")

        current_version = events[-1].aggregate_version
        data = {
            "payment_amount": cmd.payment_amount,
            "payee_id": cmd.payee_id,
            "payment_method": cmd.payment_method,
            "status": "payment_authorized",
        }

        evt = ClaimEvent.create(
            aggregate_id=cmd.claim_id,
            event_type=EventType.PAYMENT_AUTHORIZED,
            version=current_version + 1,
            data=data,
        )
        self.event_store.append(evt)
        return evt

    def handle_escalate_siu(self, cmd: EscalateToSIUCommand) -> ClaimEvent:
        events = self.event_store.get_events(cmd.claim_id)
        if not events:
            raise ValueError(f"Claim {cmd.claim_id} not found.")

        current_version = events[-1].aggregate_version
        data = {
            "reason": cmd.reason,
            "risk_score": cmd.risk_score,
            "status": ClaimStatus.FLAGGED_FRAUD.value,
        }

        evt = ClaimEvent.create(
            aggregate_id=cmd.claim_id,
            event_type=EventType.SIU_REFERRAL_CREATED,
            version=current_version + 1,
            data=data,
        )
        self.event_store.append(evt)
        return evt

    def _rebuild_claim_dict(self, events: list[ClaimEvent]) -> Dict[str, Any]:
        """Reconstruct aggregate state dictionary from event history."""
        state: Dict[str, Any] = {}
        for evt in events:
            state.update(evt.data)
        return state
