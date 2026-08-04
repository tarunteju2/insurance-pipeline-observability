"""
Saga Orchestrator for Multi-Step Claims Workflows.

Manages long-running distributed processes (sagas) across validation, fraud scoring,
enrichment, adjudication, and payment authorization with compensation logic.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from src.cqrs.command_handler import (
    ClaimCommandHandler, SubmitClaimCommand, ValidateClaimCommand,
    ScoreFraudCommand, EnrichClaimCommand, AdjudicateClaimCommand,
    AuthorizePaymentCommand, EscalateToSIUCommand
)

logger = structlog.get_logger(__name__)


class SagaStatus(str, Enum):
    STARTED = "started"
    VALIDATING = "validating"
    SCORING = "scoring"
    ENRICHING = "enriching"
    ADJUDICATING = "adjudicating"
    PAYING = "paying"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATED = "compensated"


@dataclass
class ClaimLifecycleSagaState:
    saga_id: str
    claim_id: str
    status: SagaStatus = SagaStatus.STARTED
    current_step: str = "submit"
    history: List[Dict[str, Any]] = field(default_factory=list)
    error_message: Optional[str] = None


class ClaimLifecycleSagaOrchestrator:
    """
    Saga Orchestrator executing the end-to-end claim lifecycle pipeline.

    Coordinates multi-step workflow with compensating actions on step failure.
    """

    def __init__(self, command_handler: Optional[ClaimCommandHandler] = None):
        self.command_handler = command_handler or ClaimCommandHandler()
        self.active_sagas: Dict[str, ClaimLifecycleSagaState] = {}

    def start_saga(self, submit_cmd: SubmitClaimCommand) -> ClaimLifecycleSagaState:
        saga_id = f"saga_{submit_cmd.claim_id}"
        saga = ClaimLifecycleSagaState(saga_id=saga_id, claim_id=submit_cmd.claim_id)
        self.active_sagas[saga_id] = saga

        logger.info("Starting Claim Lifecycle Saga", saga_id=saga_id, claim_id=submit_cmd.claim_id)

        try:
            # Step 1: Submit Claim
            self.command_handler.handle_submit_claim(submit_cmd)
            saga.history.append({"step": "submit", "status": "success"})

            # Step 2: Validate
            saga.status = SagaStatus.VALIDATING
            val_evt = self.command_handler.handle_validate_claim(ValidateClaimCommand(claim_id=submit_cmd.claim_id))
            if not val_evt.data.get("is_valid", False):
                self._compensate(saga, "Validation failed: " + str(val_evt.data.get("errors")))
                return saga
            saga.history.append({"step": "validate", "status": "success"})

            # Step 3: Score Fraud
            saga.status = SagaStatus.SCORING
            # Default initial scoring call
            score_evt = self.command_handler.handle_score_fraud(
                ScoreFraudCommand(
                    claim_id=submit_cmd.claim_id,
                    heuristic_score=0.1,
                    ml_score=0.15,
                    risk_level="low",
                )
            )
            combined_score = score_evt.data.get("combined_fraud_score", 0.0)
            if combined_score > 0.7:
                self.command_handler.handle_escalate_siu(
                    EscalateToSIUCommand(
                        claim_id=submit_cmd.claim_id,
                        reason="Combined fraud score exceeded 0.7 threshold",
                        risk_score=combined_score,
                    )
                )
                saga.status = SagaStatus.FAILED
                saga.error_message = "Escalated to SIU due to high fraud risk"
                return saga
            saga.history.append({"step": "score_fraud", "status": "success"})

            # Step 4: Enrich
            saga.status = SagaStatus.ENRICHING
            self.command_handler.handle_enrich_claim(EnrichClaimCommand(claim_id=submit_cmd.claim_id))
            saga.history.append({"step": "enrich", "status": "success"})

            # Step 5: Adjudicate
            saga.status = SagaStatus.ADJUDICATING
            adj_evt = self.command_handler.handle_adjudicate_claim(
                AdjudicateClaimCommand(
                    claim_id=submit_cmd.claim_id,
                    approved_amount=submit_cmd.claim_amount * 0.9,
                    decision="paid",
                    reason="Standard automated adjudication approval",
                )
            )
            saga.history.append({"step": "adjudicate", "status": "success"})

            # Step 6: Authorize Payment
            saga.status = SagaStatus.PAYING
            self.command_handler.handle_authorize_payment(
                AuthorizePaymentCommand(
                    claim_id=submit_cmd.claim_id,
                    payment_amount=adj_evt.data["approved_amount"],
                    payee_id=submit_cmd.claimant_id,
                )
            )
            saga.history.append({"step": "payment_authorized", "status": "success"})

            saga.status = SagaStatus.COMPLETED
            logger.info("Claim Lifecycle Saga Completed", saga_id=saga_id, claim_id=submit_cmd.claim_id)

        except Exception as exc:
            logger.error("Saga step execution failed", saga_id=saga_id, error=str(exc))
            self._compensate(saga, str(exc))

        return saga

    def _compensate(self, saga: ClaimLifecycleSagaState, reason: str) -> None:
        """Executes compensation workflow for failed saga."""
        saga.status = SagaStatus.COMPENSATED
        saga.error_message = reason
        saga.history.append({"step": "compensation", "reason": reason, "status": "executed"})
        logger.warning("Saga compensated", saga_id=saga.saga_id, reason=reason)
