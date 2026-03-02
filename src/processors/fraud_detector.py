"""
Fraud Detection Processor
Scores insurance claims for potential fraud using rule-based engine.
"""

import time
import math
from datetime import datetime, date
from typing import Tuple

import structlog

from src.models.claims import InsuranceClaim, ClaimStatus, ClaimType, RiskLevel
from src.observability.tracing import get_tracer, PipelineSpanContext
from src.observability.metrics import (
    CLAIMS_PROCESSED_TOTAL, CLAIMS_PROCESSING_DURATION,
    FRAUD_SCORE_DISTRIBUTION, FRAUD_FLAGGED_TOTAL
)
from src.lineage.tracker import lineage_tracker

logger = structlog.get_logger(__name__)


class FraudDetector:
    """
    Rule-based fraud detection engine for insurance claims.
    Generates a fraud score between 0.0 (clean) and 1.0 (highly suspicious).
    """

    # How much each fraud indicator contributes to the overall score
    RULES = {
        "high_amount": 0.15,
        "new_policy": 0.20,
        "weekend_filing": 0.05,
        "round_amount": 0.10,
        "excessive_amount_for_type": 0.20,
        "late_filing": 0.10,
        "missing_details": 0.10,
        "high_frequency_claimant": 0.10,
    }

    # At what amount does a claim start looking suspicious based on type
    SUSPICIOUS_THRESHOLDS = {
        ClaimType.AUTO: 30_000,
        ClaimType.HEALTH: 50_000,
        ClaimType.PROPERTY: 100_000,
        ClaimType.LIFE: 500_000,
        ClaimType.LIABILITY: 75_000,
        ClaimType.WORKERS_COMP: 50_000,
    }

    def __init__(self):
        self.tracer = get_tracer("fraud-detector")

    def score_claim(self, claim: InsuranceClaim) -> Tuple[float, InsuranceClaim]:
        """Score a claim for fraud risk. Returns (fraud_score, updated_claim)."""
        span_ctx = PipelineSpanContext(
            self.tracer, "score_fraud",
            claim_id=claim.claim_id,
            claim_type=claim.claim_type.value,
            stage="fraud_detection"
        )
        start = time.time()

        with span_ctx as (span, ctx):
            scores = {}
            triggered_rules = []

            # Check if claim amount exceeds normal range for this type
            threshold = self.SUSPICIOUS_THRESHOLDS.get(claim.claim_type, 50_000)
            if claim.claim_amount > threshold:
                ratio = min(claim.claim_amount / threshold, 3.0) / 3.0
                scores["high_amount"] = ratio * self.RULES["high_amount"]
                triggered_rules.append("high_amount")

            # Suspiciously round amounts suggest fabrication
            if claim.claim_amount >= 1000 and claim.claim_amount % 1000 == 0:
                scores["round_amount"] = self.RULES["round_amount"]
                triggered_rules.append("round_amount")

            # Rule 3: Weekend filing
            try:
                filed_date = date.fromisoformat(claim.date_filed)
                if filed_date.weekday() >= 5:
                    scores["weekend_filing"] = self.RULES["weekend_filing"]
                    triggered_rules.append("weekend_filing")
            except (ValueError, TypeError):
                pass

            # Rule 4: Late filing (> 30 days after loss)
            try:
                loss_date = date.fromisoformat(claim.date_of_loss)
                filed = date.fromisoformat(claim.date_filed)
                days_gap = (filed - loss_date).days
                if days_gap > 30:
                    scores["late_filing"] = min(days_gap / 180, 1.0) * self.RULES["late_filing"]
                    triggered_rules.append("late_filing")
            except (ValueError, TypeError):
                pass

            # Rule 5: Excessive amount relative to type norms
            excessive_thresholds = {
                ClaimType.AUTO: 100_000,
                ClaimType.HEALTH: 200_000,
                ClaimType.PROPERTY: 300_000,
            }
            excessive_limit = excessive_thresholds.get(claim.claim_type, 200_000)
            if claim.claim_amount > excessive_limit:
                ratio = min(claim.claim_amount / excessive_limit, 5.0) / 5.0
                scores["excessive_amount_for_type"] = ratio * self.RULES["excessive_amount_for_type"]
                triggered_rules.append("excessive_amount_for_type")

            # Rule 6: Missing critical details
            missing_count = 0
            if not claim.description or len(claim.description) < 10:
                missing_count += 1
            if claim.claim_type == ClaimType.HEALTH and not claim.diagnosis_code:
                missing_count += 1
            if claim.claim_type == ClaimType.AUTO and not claim.vehicle_vin:
                missing_count += 1
            if missing_count > 0:
                scores["missing_details"] = min(missing_count / 3, 1.0) * self.RULES["missing_details"]
                triggered_rules.append("missing_details")

            # Calculate final fraud score
            fraud_score = min(sum(scores.values()), 1.0)
            # Apply sigmoid-like smoothing
            fraud_score = 1.0 / (1.0 + math.exp(-10 * (fraud_score - 0.3)))
            fraud_score = round(fraud_score, 4)

            # Determine risk level
            if fraud_score >= 0.8:
                risk_level = RiskLevel.CRITICAL
            elif fraud_score >= 0.6:
                risk_level = RiskLevel.HIGH
            elif fraud_score >= 0.3:
                risk_level = RiskLevel.MEDIUM
            else:
                risk_level = RiskLevel.LOW

            latency_ms = (time.time() - start) * 1000

            # Update claim
            claim.fraud_score = fraud_score
            claim.risk_level = risk_level
            claim.status = ClaimStatus.SCORED
            claim.processing_metadata['fraud_scored_at'] = datetime.utcnow().isoformat()
            claim.processing_metadata['fraud_rules_triggered'] = triggered_rules
            claim.processing_metadata['fraud_scoring_latency_ms'] = round(latency_ms, 2)

            if fraud_score >= 0.7:
                claim.status = ClaimStatus.FLAGGED_FRAUD

            # Record metrics
            CLAIMS_PROCESSED_TOTAL.labels(
                stage="fraud_detection",
                status="flagged" if fraud_score >= 0.7 else "passed",
                claim_type=claim.claim_type.value
            ).inc()
            CLAIMS_PROCESSING_DURATION.labels(
                stage="fraud_detection",
                claim_type=claim.claim_type.value
            ).observe(latency_ms / 1000)
            FRAUD_SCORE_DISTRIBUTION.labels(
                claim_type=claim.claim_type.value
            ).observe(fraud_score)
            if fraud_score >= 0.6:
                FRAUD_FLAGGED_TOTAL.labels(
                    claim_type=claim.claim_type.value,
                    risk_level=risk_level.value
                ).inc()

            # Record lineage
            try:
                lineage_tracker.record_event(
                    source_node_id="tx_validation",
                    target_node_id="tx_fraud_detection",
                    claim_id=claim.claim_id,
                    latency_ms=latency_ms,
                    status="success",
                    metadata={"fraud_score": fraud_score, "risk_level": risk_level.value}
                )
            except Exception as e:
                logger.warning("Lineage recording failed", error=str(e))

            span.set_attribute("fraud.score", fraud_score)
            span.set_attribute("fraud.risk_level", risk_level.value)
            span.set_attribute("fraud.rules_triggered", len(triggered_rules))

            logger.info("Claim fraud scored",
                        claim_id=claim.claim_id,
                        fraud_score=fraud_score,
                        risk_level=risk_level.value,
                        rules=triggered_rules)

            return fraud_score, claim