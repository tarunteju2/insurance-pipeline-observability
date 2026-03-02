"""
Claims Enrichment Processor
Enriches claims with policy details, risk ratings, and geographic data.
"""

import time
import random
import hashlib
from datetime import datetime

import structlog

from src.models.claims import InsuranceClaim, ClaimStatus, ClaimType
from src.observability.tracing import get_tracer, PipelineSpanContext
from src.observability.metrics import (
    CLAIMS_PROCESSED_TOTAL, CLAIMS_PROCESSING_DURATION
)
from src.lineage.tracker import lineage_tracker

logger = structlog.get_logger(__name__)


# Mock policy data for demonstration
POLICY_DATABASE = {
    "premium_tier": ["Bronze", "Silver", "Gold", "Platinum"],
    "policy_status": ["active", "active", "active", "lapsed", "pending_renewal"],
    "deductible_amounts": [250, 500, 1000, 2500, 5000],
    "coverage_limits": [50000, 100000, 250000, 500000, 1000000],
}

# Risk ratings by geographic location (higher = riskier)
STATE_RISK_RATINGS = {
    "FL": 0.85, "CA": 0.75, "TX": 0.70, "NY": 0.65, "LA": 0.80,
    "OK": 0.60, "KS": 0.55, "IL": 0.60, "OH": 0.50, "PA": 0.55,
    "GA": 0.65, "NC": 0.55, "VA": 0.50, "WA": 0.45, "OR": 0.40,
}


class ClaimsEnricher:
    """Enriches insurance claims with additional business context."""

    def __init__(self):
        self.tracer = get_tracer("claims-enricher")

    def enrich(self, claim: InsuranceClaim) -> InsuranceClaim:
        """Enrich a claim with policy and contextual data."""
        span_ctx = PipelineSpanContext(
            self.tracer, "enrich_claim",
            claim_id=claim.claim_id,
            claim_type=claim.claim_type.value,
            stage="enrichment"
        )
        start = time.time()

        with span_ctx as (span, ctx):
            enrichment = {}

            # Lookup the policy details to see coverage and history
            enrichment['policy'] = self._lookup_policy(claim.policy_number)

            # Get historical claims for this claimant
            enrichment['claimant_history'] = self._get_claimant_history(claim.claimant_name)

            # Determine regional risk factors
            enrichment['geo_risk'] = self._get_geo_risk(claim)

            # Analyze the claim based on its category
            enrichment['category_analysis'] = self._analyze_category(claim)

            # Recommend which adjuster should handle this
            enrichment['adjuster_recommendation'] = self._recommend_adjuster(claim)

            # Estimate how much to reserve for this claim
            enrichment['reserve_estimate'] = self._estimate_reserve(claim)

            latency_ms = (time.time() - start) * 1000

            # Update claim
            claim.enrichment_data = enrichment
            claim.status = ClaimStatus.ENRICHED
            claim.processing_metadata['enriched_at'] = datetime.utcnow().isoformat()
            claim.processing_metadata['enrichment_latency_ms'] = round(latency_ms, 2)

            # Record metrics
            CLAIMS_PROCESSED_TOTAL.labels(
                stage="enrichment",
                status="success",
                claim_type=claim.claim_type.value
            ).inc()
            CLAIMS_PROCESSING_DURATION.labels(
                stage="enrichment",
                claim_type=claim.claim_type.value
            ).observe(latency_ms / 1000)

            # Record lineage
            try:
                lineage_tracker.record_event(
                    source_node_id="tx_fraud_detection",
                    target_node_id="tx_enrichment",
                    claim_id=claim.claim_id,
                    latency_ms=latency_ms,
                    status="success",
                    metadata={"enrichment_fields": list(enrichment.keys())}
                )
            except Exception as e:
                logger.warning("Lineage recording failed", error=str(e))

            span.set_attribute("enrichment.fields_added", len(enrichment))

            logger.info("Claim enriched",
                        claim_id=claim.claim_id,
                        fields=list(enrichment.keys()),
                        latency_ms=round(latency_ms, 2))

            return claim

    def _lookup_policy(self, policy_number: str) -> dict:
        """Simulate policy database lookup."""
        seed = int(hashlib.md5(policy_number.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)
        return {
            "policy_number": policy_number,
            "premium_tier": rng.choice(POLICY_DATABASE["premium_tier"]),
            "policy_status": rng.choice(POLICY_DATABASE["policy_status"]),
            "deductible": rng.choice(POLICY_DATABASE["deductible_amounts"]),
            "coverage_limit": rng.choice(POLICY_DATABASE["coverage_limits"]),
            "policy_start_date": "2023-01-15",
            "renewal_date": "2025-01-15",
            "premium_amount": round(rng.uniform(500, 5000), 2),
        }

    def _get_claimant_history(self, claimant_name: str) -> dict:
        """Simulate claimant history lookup."""
        seed = int(hashlib.md5(claimant_name.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)
        prior_claims = rng.randint(0, 5)
        return {
            "prior_claims_count": prior_claims,
            "prior_claims_total_amount": round(rng.uniform(0, prior_claims * 15000), 2),
            "customer_since": f"{rng.randint(2015, 2023)}-{rng.randint(1,12):02d}-01",
            "credit_score_range": rng.choice(["poor", "fair", "good", "excellent"]),
            "loyalty_tier": "Gold" if prior_claims == 0 else "Silver" if prior_claims < 3 else "Bronze",
        }

    def _get_geo_risk(self, claim: InsuranceClaim) -> dict:
        """Calculate geographic risk rating."""
        state = random.choice(list(STATE_RISK_RATINGS.keys()))
        risk_score = STATE_RISK_RATINGS.get(state, 0.5)
        return {
            "state": state,
            "risk_score": risk_score,
            "catastrophe_zone": risk_score > 0.7,
            "flood_zone": state in ["FL", "LA", "TX"],
            "hurricane_zone": state in ["FL", "LA", "TX", "NC", "GA"],
        }

    def _analyze_category(self, claim: InsuranceClaim) -> dict:
        """Analyze claim category-specific metrics."""
        analysis = {"claim_type": claim.claim_type.value}
        if claim.claim_type == ClaimType.HEALTH:
            analysis["diagnosis_category"] = "acute" if claim.diagnosis_code else "general"
            analysis["network_status"] = random.choice(["in_network", "out_of_network"])
        elif claim.claim_type == ClaimType.AUTO:
            analysis["damage_severity"] = "major" if claim.claim_amount > 10000 else "minor"
            analysis["total_loss_candidate"] = claim.claim_amount > 30000
        elif claim.claim_type == ClaimType.PROPERTY:
            analysis["damage_type"] = random.choice(["weather", "fire", "water", "theft"])
            analysis["structural_damage"] = claim.claim_amount > 50000
        return analysis

    def _recommend_adjuster(self, claim: InsuranceClaim) -> dict:
        """Recommend adjuster based on claim characteristics."""
        if claim.fraud_score >= 0.7:
            team = "Special Investigation Unit (SIU)"
            priority = "urgent"
        elif claim.claim_amount > 100000:
            team = "Senior Claims Adjusters"
            priority = "high"
        elif claim.claim_amount > 25000:
            team = "Standard Claims Team"
            priority = "medium"
        else:
            team = "Fast Track Processing"
            priority = "low"

        return {
            "assigned_team": team,
            "priority": priority,
            "estimated_resolution_days": {"urgent": 5, "high": 14, "medium": 30, "low": 7}.get(priority, 30),
        }

    def _estimate_reserve(self, claim: InsuranceClaim) -> dict:
        """Estimate claim reserve amount."""
        base_reserve = claim.claim_amount
        fraud_adjustment = 1.0 + (claim.fraud_score * 0.5)
        type_multipliers = {
            ClaimType.HEALTH: 1.15,
            ClaimType.AUTO: 1.10,
            ClaimType.PROPERTY: 1.20,
            ClaimType.LIFE: 1.05,
            ClaimType.LIABILITY: 1.30,
            ClaimType.WORKERS_COMP: 1.25,
        }
        multiplier = type_multipliers.get(claim.claim_type, 1.1)
        estimated_reserve = round(base_reserve * multiplier * fraud_adjustment, 2)

        return {
            "initial_reserve": round(base_reserve, 2),
            "adjusted_reserve": estimated_reserve,
            "adjustment_factor": round(multiplier * fraud_adjustment, 4),
            "confidence": "high" if claim.fraud_score < 0.3 else "medium" if claim.fraud_score < 0.7 else "low",
        }