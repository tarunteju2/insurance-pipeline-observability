"""
Auto Adjuster Assignment Engine.

Handles intelligent assignment of claims adjusters based on:
  - Coverage zone / geographic region
  - Adjuster workload balancing
  - Specialization matching (total-loss, glass, collision, theft)
  - Field inspection scheduling
  - Adjuster capacity and availability
"""

import hashlib
import structlog
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)


@dataclass
class Adjuster:
    """Represents a claims adjuster in the network."""
    adjuster_id: str
    name: str
    region: str
    specializations: List[str]
    max_active_claims: int = 40
    active_claim_count: int = 0
    average_cycle_days: float = 12.0
    satisfaction_score: float = 4.5
    license_states: List[str] = field(default_factory=list)
    certified_total_loss: bool = False
    certified_glass: bool = False
    certified_heavy_equipment: bool = False
    available: bool = True

    @property
    def utilization_pct(self) -> float:
        return round(
            (self.active_claim_count / max(self.max_active_claims, 1)) * 100, 1
        )

    @property
    def capacity_remaining(self) -> int:
        return max(0, self.max_active_claims - self.active_claim_count)


# Simulated adjuster pool
_ADJUSTER_POOL: List[Adjuster] = [
    Adjuster(
        adjuster_id="ADJ-001", name="Sarah Chen", region="WEST",
        specializations=["collision", "total_loss"],
        active_claim_count=28, license_states=["CA", "NV", "AZ", "OR", "WA"],
        certified_total_loss=True,
    ),
    Adjuster(
        adjuster_id="ADJ-002", name="Marcus Johnson", region="WEST",
        specializations=["glass", "comprehensive"],
        active_claim_count=22, license_states=["CA", "NV", "AZ"],
        certified_glass=True,
    ),
    Adjuster(
        adjuster_id="ADJ-003", name="Patricia Rodriguez", region="SOUTH",
        specializations=["collision", "theft", "total_loss"],
        active_claim_count=35, license_states=["TX", "LA", "OK", "AR"],
        certified_total_loss=True,
    ),
    Adjuster(
        adjuster_id="ADJ-004", name="David Kim", region="SOUTH",
        specializations=["collision", "comprehensive"],
        active_claim_count=18, license_states=["TX", "FL", "GA"],
    ),
    Adjuster(
        adjuster_id="ADJ-005", name="Emily Watson", region="NORTHEAST",
        specializations=["collision", "total_loss", "heavy_equipment"],
        active_claim_count=31, license_states=["NY", "NJ", "CT", "MA", "PA"],
        certified_total_loss=True, certified_heavy_equipment=True,
    ),
    Adjuster(
        adjuster_id="ADJ-006", name="James Thompson", region="NORTHEAST",
        specializations=["glass", "comprehensive"],
        active_claim_count=15, license_states=["NY", "NJ", "CT"],
        certified_glass=True,
    ),
    Adjuster(
        adjuster_id="ADJ-007", name="Maria Garcia", region="MIDWEST",
        specializations=["collision", "comprehensive", "theft"],
        active_claim_count=25, license_states=["IL", "OH", "MI", "IN", "WI"],
    ),
    Adjuster(
        adjuster_id="ADJ-008", name="Robert Williams", region="MIDWEST",
        specializations=["total_loss", "heavy_equipment"],
        active_claim_count=20, license_states=["IL", "OH", "MI"],
        certified_total_loss=True, certified_heavy_equipment=True,
    ),
    Adjuster(
        adjuster_id="ADJ-009", name="Lisa Anderson", region="SOUTHEAST",
        specializations=["collision", "comprehensive", "glass"],
        active_claim_count=30, license_states=["FL", "GA", "SC", "NC"],
        certified_glass=True,
    ),
    Adjuster(
        adjuster_id="ADJ-010", name="Michael Brown", region="SOUTHEAST",
        specializations=["collision", "total_loss"],
        active_claim_count=12, license_states=["FL", "GA", "AL"],
        certified_total_loss=True,
    ),
]

# State-to-region mapping
_STATE_TO_REGION = {
    "CA": "WEST", "NV": "WEST", "AZ": "WEST", "OR": "WEST",
    "WA": "WEST", "CO": "WEST", "UT": "WEST", "NM": "WEST",
    "TX": "SOUTH", "LA": "SOUTH", "OK": "SOUTH", "AR": "SOUTH",
    "MS": "SOUTH", "TN": "SOUTH",
    "NY": "NORTHEAST", "NJ": "NORTHEAST", "CT": "NORTHEAST",
    "MA": "NORTHEAST", "PA": "NORTHEAST", "VT": "NORTHEAST",
    "NH": "NORTHEAST", "ME": "NORTHEAST", "RI": "NORTHEAST",
    "IL": "MIDWEST", "OH": "MIDWEST", "MI": "MIDWEST",
    "IN": "MIDWEST", "WI": "MIDWEST", "MN": "MIDWEST",
    "IA": "MIDWEST", "MO": "MIDWEST",
    "FL": "SOUTHEAST", "GA": "SOUTHEAST", "SC": "SOUTHEAST",
    "NC": "SOUTHEAST", "VA": "SOUTHEAST", "AL": "SOUTHEAST",
}


class AutoAdjusterAssignment:
    """
    Intelligent adjuster assignment engine for auto claims.

    Considers region, specialization, workload, and availability
    to select the optimal adjuster for each claim.
    """

    def __init__(self):
        self._adjusters = list(_ADJUSTER_POOL)

    def assign(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """
        Assign the best-fit adjuster for the given claim.

        Returns assignment metadata including adjuster info, scheduling,
        and field inspection requirements.
        """
        claim_type = self._classify_claim_type(claim)
        region = self._determine_region(claim)
        requires_field = self._requires_field_inspection(claim)
        requires_total_loss_cert = claim.claim_amount > 50_000

        # Score each adjuster
        scored = []
        for adj in self._adjusters:
            if not adj.available or adj.capacity_remaining <= 0:
                continue
            score = self._score_adjuster(
                adj, region, claim_type, requires_total_loss_cert
            )
            scored.append((score, adj))

        scored.sort(key=lambda x: x[0], reverse=True)

        if not scored:
            return self._overflow_assignment(claim, region)

        best_score, best_adj = scored[0]

        # Schedule field inspection if required
        inspection = None
        if requires_field:
            inspection = self._schedule_inspection(claim, best_adj)

        return {
            "adjuster_assigned": True,
            "adjuster": {
                "adjuster_id": best_adj.adjuster_id,
                "name": best_adj.name,
                "region": best_adj.region,
                "specializations": best_adj.specializations,
                "utilization_pct": best_adj.utilization_pct,
                "average_cycle_days": best_adj.average_cycle_days,
                "satisfaction_score": best_adj.satisfaction_score,
            },
            "assignment_score": round(best_score, 3),
            "claim_classification": claim_type,
            "requires_field_inspection": requires_field,
            "field_inspection": inspection,
            "sla_target_days": self._get_sla_target(claim),
            "assigned_at": datetime.utcnow().isoformat(),
        }

    def _classify_claim_type(self, claim: InsuranceClaim) -> str:
        """Classify the auto claim into a sub-category."""
        desc = (claim.description or "").lower()
        if any(kw in desc for kw in ("theft", "stolen", "burglary")):
            return "theft"
        if any(kw in desc for kw in ("windshield", "glass", "window")):
            return "glass"
        if any(kw in desc for kw in ("hail", "flood", "fire", "tree", "weather")):
            return "comprehensive"
        if any(kw in desc for kw in ("total", "totaled", "write-off")):
            return "total_loss"
        return "collision"

    def _determine_region(self, claim: InsuranceClaim) -> str:
        """Determine the geographic region for adjuster assignment."""
        meta = claim.enrichment_data or {}
        state = meta.get("loss_state", "")
        return _STATE_TO_REGION.get(state, "MIDWEST")

    def _requires_field_inspection(self, claim: InsuranceClaim) -> bool:
        """Determine if a field inspection is required."""
        if claim.claim_amount > 25_000:
            return True
        desc = (claim.description or "").lower()
        if any(kw in desc for kw in ("total", "fire", "flood")):
            return True
        return False

    def _score_adjuster(
        self,
        adj: Adjuster,
        region: str,
        claim_type: str,
        requires_total_loss_cert: bool,
    ) -> float:
        """Score an adjuster's fit for this claim (higher = better)."""
        score = 0.0

        # Region match (highest weight)
        if adj.region == region:
            score += 40.0

        # Specialization match
        if claim_type in adj.specializations:
            score += 30.0

        # Total-loss certification
        if requires_total_loss_cert and adj.certified_total_loss:
            score += 15.0

        # Glass certification
        if claim_type == "glass" and adj.certified_glass:
            score += 15.0

        # Workload balance (prefer less utilized adjusters)
        utilization_penalty = adj.utilization_pct * 0.3
        score -= utilization_penalty

        # Satisfaction score bonus
        score += adj.satisfaction_score * 2.0

        # Cycle time bonus (faster adjusters preferred)
        if adj.average_cycle_days < 10:
            score += 5.0

        return score

    def _schedule_inspection(
        self, claim: InsuranceClaim, adjuster: Adjuster
    ) -> Dict[str, Any]:
        """Schedule a field inspection for the claim."""
        # Use deterministic scheduling based on claim ID
        seed = int(hashlib.md5(claim.claim_id.encode()).hexdigest()[:4], 16)
        days_out = 2 + (seed % 5)  # 2-6 business days out
        inspection_date = date.today() + timedelta(days=days_out)

        # Skip weekends
        while inspection_date.weekday() >= 5:
            inspection_date += timedelta(days=1)

        return {
            "inspection_date": inspection_date.isoformat(),
            "inspection_type": "field",
            "estimated_duration_hours": 1.5 if claim.claim_amount > 50_000 else 1.0,
            "inspector_id": adjuster.adjuster_id,
            "inspector_name": adjuster.name,
            "status": "scheduled",
        }

    def _get_sla_target(self, claim: InsuranceClaim) -> int:
        """Get SLA target in days based on claim severity."""
        if claim.claim_amount > 100_000:
            return 30
        elif claim.claim_amount > 50_000:
            return 21
        elif claim.claim_amount > 10_000:
            return 14
        return 10

    def _overflow_assignment(
        self, claim: InsuranceClaim, region: str
    ) -> Dict[str, Any]:
        """Handle case when no adjusters have capacity."""
        return {
            "adjuster_assigned": False,
            "overflow_reason": "All adjusters at capacity",
            "overflow_region": region,
            "escalation_required": True,
            "priority_queue_position": None,
            "estimated_assignment_date": (
                date.today() + timedelta(days=2)
            ).isoformat(),
        }
