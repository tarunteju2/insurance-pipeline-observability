"""
Claims Validation Processor
Validates insurance claim data integrity, required fields, and business rules.
"""

import re
import time
from datetime import datetime, date
from typing import Tuple, List

import structlog

from src.models.claims import InsuranceClaim, ClaimStatus, ClaimType
from src.observability.tracing import get_tracer, PipelineSpanContext
from src.observability.metrics import (
    CLAIMS_PROCESSED_TOTAL, CLAIMS_PROCESSING_DURATION, PIPELINE_ERROR_TOTAL
)
from src.lineage.tracker import lineage_tracker

logger = structlog.get_logger(__name__)


class ClaimsValidator:
    """Validates insurance claims against business rules."""

    REQUIRED_FIELDS = ['claim_id', 'policy_number', 'claimant_name', 'claim_type', 'claim_amount']
    # Policy number format: ABC-123456 (3 letters, dash, 6 digits)
    POLICY_PATTERN = re.compile(r'^[A-Z]{3}-\d{6}$')
    # Maximum claim amounts vary by type (in dollars)
    MAX_CLAIM_AMOUNTS = {
        ClaimType.AUTO: 500_000,
        ClaimType.HEALTH: 2_000_000,
        ClaimType.PROPERTY: 5_000_000,
        ClaimType.LIFE: 10_000_000,
        ClaimType.LIABILITY: 1_000_000,
        ClaimType.WORKERS_COMP: 500_000,
    }

    def __init__(self):
        self.tracer = get_tracer("claims-validator")

    def validate(self, claim: InsuranceClaim) -> Tuple[bool, InsuranceClaim]:
        """Validate a claim and return (is_valid, updated_claim)."""
        span_ctx = PipelineSpanContext(
            self.tracer, "validate_claim",
            claim_id=claim.claim_id,
            claim_type=claim.claim_type.value,
            stage="validation"
        )
        start = time.time()

        with span_ctx as (span, ctx):
            errors = []

            # Make sure all required fields are present and not empty
            for field_name in self.REQUIRED_FIELDS:
                val = getattr(claim, field_name, None)
                if val is None or (isinstance(val, str) and not val.strip()):
                    errors.append(f"Missing required field: {field_name}")

            # Check policy number matches expected format
            if claim.policy_number and not self.POLICY_PATTERN.match(claim.policy_number):
                errors.append(f"Invalid policy number format: {claim.policy_number}")

            # Validate the claim amount is reasonable
            if claim.claim_amount <= 0:
                errors.append(f"Claim amount must be positive: {claim.claim_amount}")
            else:
                max_amount = self.MAX_CLAIM_AMOUNTS.get(claim.claim_type, 1_000_000)
                if claim.claim_amount > max_amount:
                    errors.append(
                        f"Claim amount ${claim.claim_amount:,.2f} exceeds max "
                        f"${max_amount:,.2f} for {claim.claim_type.value}"
                    )

            # Validate dates are logical
            if claim.date_of_loss:
                try:
                    loss_date = date.fromisoformat(claim.date_of_loss)
                    if loss_date > date.today():
                        errors.append("Date of loss cannot be in the future")
                    if (date.today() - loss_date).days > 365:
                        errors.append("Claim filed more than 1 year after loss")
                except ValueError:
                    errors.append(f"Invalid date_of_loss format: {claim.date_of_loss}")

            # 5. Type-specific validations
            if claim.claim_type == ClaimType.HEALTH:
                if not claim.provider_name:
                    errors.append("Health claims require a provider name")
            elif claim.claim_type == ClaimType.AUTO:
                if claim.claim_amount > 100_000 and not claim.vehicle_vin:
                    errors.append("High-value auto claims require vehicle VIN")

            # Update claim
            latency_ms = (time.time() - start) * 1000

            is_valid = len(errors) == 0
            claim.validation_errors = errors
            claim.status = ClaimStatus.VALIDATED if is_valid else ClaimStatus.VALIDATION_FAILED
            claim.processing_metadata['validated_at'] = datetime.utcnow().isoformat()
            claim.processing_metadata['validation_latency_ms'] = round(latency_ms, 2)

            # Record metrics
            CLAIMS_PROCESSED_TOTAL.labels(
                stage="validation",
                status="success" if is_valid else "failed",
                claim_type=claim.claim_type.value
            ).inc()
            CLAIMS_PROCESSING_DURATION.labels(
                stage="validation",
                claim_type=claim.claim_type.value
            ).observe(latency_ms / 1000)

            if not is_valid:
                PIPELINE_ERROR_TOTAL.labels(
                    stage="validation",
                    error_type="validation_failure"
                ).inc()

            # Record lineage
            target = "tx_fraud_detection" if is_valid else "sink_dlq"
            source = "tx_validation" if is_valid else "tx_validation"
            lineage_source = "src_claims_ingestion"
            try:
                lineage_tracker.record_event(
                    source_node_id=lineage_source,
                    target_node_id="tx_validation",
                    claim_id=claim.claim_id,
                    latency_ms=latency_ms,
                    status="success" if is_valid else "failure",
                    error_message="; ".join(errors) if errors else None,
                )
            except Exception as e:
                logger.warning("Lineage recording failed", error=str(e))

            span.set_attribute("validation.is_valid", is_valid)
            span.set_attribute("validation.error_count", len(errors))

            logger.info("Claim validated",
                        claim_id=claim.claim_id,
                        valid=is_valid,
                        errors=len(errors),
                        latency_ms=round(latency_ms, 2))

            return is_valid, claim