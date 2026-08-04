"""
Claims Validation Processor
Validates insurance claim data integrity, required fields, and business rules.

Severity model
--------------
Each error code is classified CRITICAL / HIGH / MEDIUM / LOW via
VALIDATION_SEVERITY_MAP in src/models/claims.py.

Stop-the-line rule: if ANY CRITICAL error is found, validation fails immediately
and the claim is routed to the DLQ without continuing further rule checks.
This prevents wasting fraud-detection and enrichment compute on garbage data.
"""

import re
import time
from datetime import datetime, date, timezone
from typing import Tuple, List

import structlog

from src.models.claims import (
    InsuranceClaim, ClaimStatus, ClaimType,
    ValidationSeverity, VALIDATION_SEVERITY_MAP
)
from src.observability.pii_masking import mask_claim_for_logging
from src.observability.tracing import get_tracer, PipelineSpanContext
from src.observability.metrics import (
    CLAIMS_PROCESSED_TOTAL, CLAIMS_PROCESSING_DURATION, PIPELINE_ERROR_TOTAL,
    VALIDATION_ERROR_CODES_TOTAL, DQ_CRITICAL_FAILURES_TOTAL
)
from src.lineage.tracker import lineage_tracker

logger = structlog.get_logger(__name__)


class ClaimsValidator:
    """Validates insurance claims against business rules."""

    REQUIRED_FIELDS = ['claim_id', 'policy_number', 'claimant_name', 'claim_type', 'claim_amount']
    # Policy number format: ABC-123456 (3 letters, dash, 6 digits)
    POLICY_PATTERN = re.compile(r'^[A-Z]{3}-\d{6}$')
    CLAIMANT_NAME_PATTERN = re.compile(r"^[A-Za-z\-\'\s]{2,100}$")
    VIN_PATTERN = re.compile(r'^[A-HJ-NPR-Z0-9]{17}$')
    DIAGNOSIS_CODE_PATTERN = re.compile(r'^[A-Z][0-9]{1,2}(\.[0-9A-Z]{1,4})?$', re.IGNORECASE)
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

    @staticmethod
    def _record_error(error_list: List[dict], code: str, field: str, message: str):
        severity = VALIDATION_SEVERITY_MAP.get(code, ValidationSeverity.MEDIUM)
        error_list.append({
            "code": code,
            "field": field,
            "message": message,
            "severity": severity.value,
        })

    @staticmethod
    def _has_critical_error(error_details: List[dict]) -> bool:
        """Return True if any recorded error is CRITICAL (stop-the-line)."""
        return any(e.get("severity") == ValidationSeverity.CRITICAL.value for e in error_details)

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
            error_details = []

            # Make sure all required fields are present and not empty
            for field_name in self.REQUIRED_FIELDS:
                val = getattr(claim, field_name, None)
                if val is None or (isinstance(val, str) and not val.strip()):
                    self._record_error(
                        error_details,
                        code="REQUIRED_FIELD_MISSING",
                        field=field_name,
                        message=f"Missing required field: {field_name}"
                    )

            # ── STOP-THE-LINE ────────────────────────────────────────────────
            # If any required field is missing we cannot safely run further
            # rules (risk of AttributeError / misleading secondary errors).
            # Reject immediately; no point burning compute on the rest.
            if self._has_critical_error(error_details):
                return self._finalize(claim, error_details, span, start)
            # ─────────────────────────────────────────────────────────────────

            # Normalize common text fields
            claim.policy_number = (claim.policy_number or "").strip().upper()
            claim.claimant_name = (claim.claimant_name or "").strip()
            claim.description = (claim.description or "").strip()

            # Validate claimant name quality
            if claim.claimant_name and not self.CLAIMANT_NAME_PATTERN.match(claim.claimant_name):
                self._record_error(
                    error_details,
                    code="INVALID_CLAIMANT_NAME",
                    field="claimant_name",
                    message="Claimant name must be 2-100 characters and contain only letters, spaces, apostrophes, or hyphens"
                )

            # Check policy number matches expected format
            if claim.policy_number and not self.POLICY_PATTERN.match(claim.policy_number):
                self._record_error(
                    error_details,
                    code="INVALID_POLICY_FORMAT",
                    field="policy_number",
                    message=f"Invalid policy number format: {claim.policy_number}"
                )

            # Validate the claim amount is reasonable
            if claim.claim_amount <= 0:
                self._record_error(
                    error_details,
                    code="INVALID_CLAIM_AMOUNT",
                    field="claim_amount",
                    message=f"Claim amount must be positive: {claim.claim_amount}"
                )

            # ── STOP-THE-LINE ────────────────────────────────────────────────
            # Amount violations are CRITICAL — reject before further rules.
            if self._has_critical_error(error_details):
                return self._finalize(claim, error_details, span, start)
            # ─────────────────────────────────────────────────────────────────
            else:
                max_amount = self.MAX_CLAIM_AMOUNTS.get(claim.claim_type, 1_000_000)
                if claim.claim_amount > max_amount:
                    self._record_error(
                        error_details,
                        code="CLAIM_AMOUNT_EXCEEDS_LIMIT",
                        field="claim_amount",
                        message=(
                            f"Claim amount ${claim.claim_amount:,.2f} exceeds max "
                            f"${max_amount:,.2f} for {claim.claim_type.value}"
                        )
                    )

            # Require meaningful description for higher value claims
            if claim.claim_amount >= 50_000 and len(claim.description) < 20:
                self._record_error(
                    error_details,
                    code="INSUFFICIENT_DESCRIPTION",
                    field="description",
                    message="Claims >= $50,000 require a description of at least 20 characters"
                )

            # Validate dates are logical
            loss_date = None
            if claim.date_of_loss:
                try:
                    loss_date = date.fromisoformat(claim.date_of_loss)
                    if loss_date > date.today():
                        self._record_error(
                            error_details,
                            code="LOSS_DATE_IN_FUTURE",
                            field="date_of_loss",
                            message="Date of loss cannot be in the future"
                        )
                    if (date.today() - loss_date).days > 365:
                        self._record_error(
                            error_details,
                            code="LOSS_DATE_TOO_OLD",
                            field="date_of_loss",
                            message="Claim filed more than 1 year after loss"
                        )
                except ValueError:
                    self._record_error(
                        error_details,
                        code="INVALID_LOSS_DATE_FORMAT",
                        field="date_of_loss",
                        message=f"Invalid date_of_loss format: {claim.date_of_loss}"
                    )

            if claim.date_filed:
                try:
                    filed_date = date.fromisoformat(claim.date_filed)
                    if filed_date > date.today():
                        self._record_error(
                            error_details,
                            code="FILED_DATE_IN_FUTURE",
                            field="date_filed",
                            message="Date filed cannot be in the future"
                        )
                    if loss_date and filed_date < loss_date:
                        self._record_error(
                            error_details,
                            code="FILED_BEFORE_LOSS",
                            field="date_filed",
                            message="Date filed cannot be earlier than date of loss"
                        )
                except ValueError:
                    self._record_error(
                        error_details,
                        code="INVALID_FILED_DATE_FORMAT",
                        field="date_filed",
                        message=f"Invalid date_filed format: {claim.date_filed}"
                    )

            # 5. Type-specific validations
            if claim.claim_type == ClaimType.HEALTH:
                if not claim.provider_name:
                    self._record_error(
                        error_details,
                        code="PROVIDER_REQUIRED",
                        field="provider_name",
                        message="Health claims require a provider name"
                    )
                if claim.diagnosis_code and not self.DIAGNOSIS_CODE_PATTERN.match(claim.diagnosis_code):
                    self._record_error(
                        error_details,
                        code="INVALID_DIAGNOSIS_CODE",
                        field="diagnosis_code",
                        message=f"Invalid diagnosis code format: {claim.diagnosis_code}"
                    )
            elif claim.claim_type == ClaimType.AUTO:
                if claim.vehicle_vin:
                    claim.vehicle_vin = claim.vehicle_vin.strip().upper()
                    if not self.VIN_PATTERN.match(claim.vehicle_vin):
                        self._record_error(
                            error_details,
                            code="INVALID_VIN_FORMAT",
                            field="vehicle_vin",
                            message="Vehicle VIN must be a valid 17-character VIN"
                        )
                if claim.claim_amount > 100_000 and not claim.vehicle_vin:
                    self._record_error(
                        error_details,
                        code="VIN_REQUIRED_HIGH_VALUE_AUTO",
                        field="vehicle_vin",
                        message="High-value auto claims require vehicle VIN"
                    )

            errors = [entry["message"] for entry in error_details]

            # After date checks, do a final CRITICAL stop-the-line sweep
            if self._has_critical_error(error_details):
                return self._finalize(claim, error_details, span, start)

            return self._finalize(claim, error_details, span, start)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _finalize(self, claim: InsuranceClaim, error_details: list, span, start: float):
        """Commit final validation state onto the claim and emit metrics/lineage."""
        errors = [entry["message"] for entry in error_details]
        latency_ms = (time.time() - start) * 1000

        is_valid = len(errors) == 0
        has_critical = self._has_critical_error(error_details)

        claim.validation_errors = errors
        claim.status = ClaimStatus.VALIDATED if is_valid else ClaimStatus.VALIDATION_FAILED
        claim.processing_metadata['validated_at'] = datetime.utcnow().isoformat()
        claim.processing_metadata['validation_latency_ms'] = round(latency_ms, 2)
        claim.processing_metadata['validation_error_details'] = error_details
        claim.processing_metadata['validation_error_codes'] = [e['code'] for e in error_details]
        claim.processing_metadata['validation_error_count'] = len(error_details)
        claim.processing_metadata['has_critical_validation_error'] = has_critical
        claim.processing_metadata['validation_severity_summary'] = {
            sev: sum(1 for e in error_details if e.get('severity') == sev)
            for sev in ('critical', 'high', 'medium', 'low')
        }

        # Metrics
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
            for detail in error_details:
                VALIDATION_ERROR_CODES_TOTAL.labels(
                    error_code=detail["code"],
                    field=detail["field"],
                    claim_type=claim.claim_type.value
                ).inc()
                if detail.get("severity") == "critical":
                    DQ_CRITICAL_FAILURES_TOTAL.labels(
                        error_code=detail["code"],
                        claim_type=claim.claim_type.value
                    ).inc()

        # Lineage
        try:
            lineage_tracker.record_event(
                source_node_id="src_claims_ingestion",
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
        span.set_attribute("validation.has_critical", has_critical)

        masked = mask_claim_for_logging({"claim_id": claim.claim_id,
                                         "claimant_name": claim.claimant_name,
                                         "policy_number": claim.policy_number})
        logger.info("Claim validated",
                    **masked,
                    valid=is_valid,
                    errors=len(errors),
                    has_critical=has_critical,
                    latency_ms=round(latency_ms, 2))

        return is_valid, claim

    def evaluate_data_expectations(self, claims: List[InsuranceClaim]) -> dict:
        """
        Evaluates declarative Data Quality Expectations over a batch of claims
        and returns an OpenLineage-style Data Quality Scorecard.
        """
        if not claims:
            return {
                "total_claims": 0,
                "overall_quality_score": 100.0,
                "expectations": []
            }

        total_claims = len(claims)

        # 1. Expectation: claim_amount > 0
        positive_amounts = sum(1 for c in claims if c.claim_amount is not None and c.claim_amount > 0)
        exp_amount = {
            "name": "expect_claim_amount_to_be_positive",
            "passed": positive_amounts == total_claims,
            "pass_rate": round((positive_amounts / total_claims) * 100, 2),
            "observed_value": f"{positive_amounts}/{total_claims} positive",
        }

        # 2. Expectation: policy_number matches format
        valid_policies = sum(1 for c in claims if c.policy_number and self.POLICY_PATTERN.match(c.policy_number))
        exp_policy = {
            "name": "expect_policy_number_format_valid",
            "passed": valid_policies == total_claims,
            "pass_rate": round((valid_policies / total_claims) * 100, 2),
            "observed_value": f"{valid_policies}/{total_claims} valid format",
        }

        # 3. Expectation: non-null claimant_name
        non_null_names = sum(1 for c in claims if c.claimant_name and c.claimant_name.strip())
        exp_names = {
            "name": "expect_claimant_name_not_null",
            "passed": non_null_names == total_claims,
            "pass_rate": round((non_null_names / total_claims) * 100, 2),
            "observed_value": f"{non_null_names}/{total_claims} non-null",
        }

        expectations = [exp_amount, exp_policy, exp_names]
        avg_score = round(sum(e["pass_rate"] for e in expectations) / len(expectations), 2)

        return {
            "total_claims": total_claims,
            "overall_quality_score": avg_score,
            "expectations": expectations,
            "evaluated_at": datetime.now(timezone.utc).isoformat()
        }