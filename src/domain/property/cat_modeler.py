"""
Catastrophe Modeling Engine.

Provides catastrophe event management, geographic claim clustering,
aggregate reserve estimation, and reinsurance treaty tracking.
"""

import structlog
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import hashlib
import random
import math

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)


@dataclass
class CatastropheEvent:
    """Represents a declared catastrophe event."""
    cat_id: str
    name: str
    event_type: str  # hurricane, tornado, wildfire, earthquake, flood, winter_storm
    start_date: date
    end_date: date
    affected_states: List[str]
    affected_counties: List[str] = field(default_factory=list)
    pcs_serial: Optional[str] = None
    estimated_industry_loss: float = 0.0  # billions
    company_loss_estimate: float = 0.0
    claims_count: int = 0
    open_claims: int = 0
    closed_claims: int = 0
    avg_paid_per_claim: float = 0.0
    reinsurance_applicable: bool = False
    status: str = "active"  # active, monitoring, closed


@dataclass
class ReinsuranceTreaty:
    """Reinsurance treaty configuration."""
    treaty_id: str
    treaty_type: str  # quota_share, excess_of_loss, catastrophe_xl
    retention: float  # Company retention amount
    limit: float  # Maximum reinsurance coverage
    rate_on_line: float  # Premium rate
    reinstatements: int = 1
    aggregate_deductible: float = 0.0
    effective_date: str = ""
    expiration_date: str = ""


# Active reinsurance treaties
_TREATIES: List[ReinsuranceTreaty] = [
    ReinsuranceTreaty(
        treaty_id="RE-CAT-001",
        treaty_type="catastrophe_xl",
        retention=10_000_000.0,
        limit=100_000_000.0,
        rate_on_line=0.08,
        reinstatements=2,
        aggregate_deductible=5_000_000.0,
        effective_date="2026-01-01",
        expiration_date="2026-12-31",
    ),
    ReinsuranceTreaty(
        treaty_id="RE-QS-001",
        treaty_type="quota_share",
        retention=0.75,  # Company retains 75%
        limit=0.25,  # Ceded 25%
        rate_on_line=0.30,  # Ceding commission
        effective_date="2026-01-01",
        expiration_date="2026-12-31",
    ),
]


class CatastropheModeler:
    """
    Catastrophe event management and aggregate loss estimation.

    Provides:
      - Event declaration and tracking
      - Geographic claim clustering
      - Aggregate reserve estimation
      - Reinsurance treaty applicability
      - Loss development projections
    """

    def __init__(self):
        self._events: Dict[str, CatastropheEvent] = {}
        self._claim_clusters: Dict[str, List[str]] = {}  # cat_id -> [claim_ids]
        self._treaties = _TREATIES

    def analyze_claim(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """
        Analyze a property claim for catastrophe event association.

        Returns enrichment data with CAT analysis results.
        """
        cat_event = self._find_cat_event(claim)
        geo_cluster = self._geographic_cluster(claim)
        reserve_impact = self._estimate_reserve_impact(claim, cat_event)
        reinsurance = self._check_reinsurance(claim, cat_event)
        development = self._project_loss_development(cat_event)

        return {
            "catastrophe_analysis": {
                "cat_event": {
                    "cat_id": cat_event.cat_id if cat_event else None,
                    "name": cat_event.name if cat_event else None,
                    "event_type": cat_event.event_type if cat_event else None,
                    "status": cat_event.status if cat_event else None,
                } if cat_event else None,
                "geographic_cluster": geo_cluster,
                "reserve_impact": reserve_impact,
                "reinsurance_recovery": reinsurance,
                "loss_development_projection": development,
                "is_catastrophe_claim": cat_event is not None,
            },
        }

    def _find_cat_event(
        self, claim: InsuranceClaim
    ) -> Optional[CatastropheEvent]:
        """Find matching catastrophe event for the claim."""
        meta = claim.enrichment_data or {}
        state = meta.get("loss_state")
        cat_id = meta.get("cat_event_id")

        if cat_id and cat_id in self._events:
            return self._events[cat_id]

        try:
            loss_date = date.fromisoformat(claim.date_of_loss)
        except (ValueError, TypeError):
            return None

        # Check against known events
        from src.domain.property.validator import _CATASTROPHE_EVENTS
        for cat_data in _CATASTROPHE_EVENTS:
            cat_start = date.fromisoformat(cat_data["start_date"])
            cat_end = date.fromisoformat(cat_data["end_date"])
            if (
                state in cat_data.get("affected_states", [])
                and cat_start <= loss_date <= cat_end + timedelta(days=14)
            ):
                # Create/cache event
                event = CatastropheEvent(
                    cat_id=cat_data["cat_id"],
                    name=cat_data["name"],
                    event_type=cat_data["type"],
                    start_date=cat_start,
                    end_date=cat_end,
                    affected_states=cat_data["affected_states"],
                    pcs_serial=cat_data.get("pcs_serial"),
                    estimated_industry_loss=cat_data.get(
                        "estimated_insured_loss_billions", 0
                    ),
                )
                self._events[event.cat_id] = event
                return event

        return None

    def _geographic_cluster(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Cluster claims geographically for CAT analysis."""
        addr = claim.property_address or ""
        seed = int(hashlib.md5(addr.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)

        # Simulated lat/lon from address hash
        lat = 25.0 + (seed % 2500) / 100.0  # 25-50 N latitude
        lon = -125.0 + ((seed >> 12) % 5500) / 100.0  # -125 to -70 W longitude

        # Determine density cluster
        claims_in_radius = rng.randint(0, 200)
        cluster_id = f"GEO-{int(lat)}-{int(abs(lon))}"

        density_tier = "low"
        if claims_in_radius > 100:
            density_tier = "critical"
        elif claims_in_radius > 50:
            density_tier = "high"
        elif claims_in_radius > 20:
            density_tier = "moderate"

        return {
            "latitude": round(lat, 4),
            "longitude": round(lon, 4),
            "cluster_id": cluster_id,
            "claims_in_10mi_radius": claims_in_radius,
            "density_tier": density_tier,
        }

    def _estimate_reserve_impact(
        self, claim: InsuranceClaim, cat_event: Optional[CatastropheEvent]
    ) -> Dict[str, Any]:
        """Estimate aggregate reserve impact of this claim."""
        if not cat_event:
            return {
                "initial_reserve": claim.claim_amount * 0.55,
                "development_factor": 1.0,
                "ultimate_reserve": claim.claim_amount * 0.55,
                "cat_load": 0.0,
            }

        # CAT claims have higher development factors
        cat_dev_factors = {
            "hurricane": 1.45,
            "wildfire": 1.55,
            "earthquake": 1.60,
            "tornado": 1.30,
            "flood": 1.40,
            "winter_storm": 1.25,
            "severe_convective_storm": 1.35,
        }
        dev_factor = cat_dev_factors.get(cat_event.event_type, 1.35)
        initial = claim.claim_amount * 0.55
        ultimate = initial * dev_factor

        return {
            "initial_reserve": round(initial, 2),
            "development_factor": dev_factor,
            "ultimate_reserve": round(ultimate, 2),
            "cat_load": round(ultimate - initial, 2),
            "cat_event_type": cat_event.event_type,
        }

    def _check_reinsurance(
        self, claim: InsuranceClaim, cat_event: Optional[CatastropheEvent]
    ) -> Dict[str, Any]:
        """Check reinsurance treaty applicability."""
        if not cat_event:
            return {"reinsurance_applicable": False, "treaties": []}

        applicable_treaties = []
        for treaty in self._treaties:
            if treaty.treaty_type == "catastrophe_xl":
                applicable_treaties.append({
                    "treaty_id": treaty.treaty_id,
                    "type": treaty.treaty_type,
                    "retention": treaty.retention,
                    "limit": treaty.limit,
                    "reinstatements_remaining": treaty.reinstatements,
                })
            elif treaty.treaty_type == "quota_share":
                ceded_amount = claim.claim_amount * (1 - treaty.retention)
                applicable_treaties.append({
                    "treaty_id": treaty.treaty_id,
                    "type": treaty.treaty_type,
                    "ceded_amount": round(ceded_amount, 2),
                    "retained_amount": round(
                        claim.claim_amount * treaty.retention, 2
                    ),
                    "cession_pct": round((1 - treaty.retention) * 100, 1),
                })

        return {
            "reinsurance_applicable": len(applicable_treaties) > 0,
            "treaties": applicable_treaties,
        }

    def _project_loss_development(
        self, cat_event: Optional[CatastropheEvent]
    ) -> Optional[Dict[str, Any]]:
        """Project loss development for the CAT event."""
        if not cat_event:
            return None

        # Simulated development pattern
        months = [1, 3, 6, 12, 18, 24, 36]
        development_pcts = {
            "hurricane": [0.20, 0.45, 0.65, 0.82, 0.92, 0.97, 1.00],
            "wildfire": [0.15, 0.35, 0.55, 0.75, 0.88, 0.95, 1.00],
            "earthquake": [0.10, 0.30, 0.50, 0.70, 0.85, 0.93, 1.00],
            "tornado": [0.30, 0.55, 0.75, 0.90, 0.96, 0.99, 1.00],
            "flood": [0.18, 0.40, 0.60, 0.78, 0.90, 0.96, 1.00],
            "severe_convective_storm": [0.25, 0.50, 0.70, 0.85, 0.94, 0.98, 1.00],
            "winter_storm": [0.35, 0.60, 0.80, 0.92, 0.97, 0.99, 1.00],
        }

        pcts = development_pcts.get(cat_event.event_type, development_pcts["tornado"])

        return {
            "development_pattern": [
                {"month": m, "cumulative_pct": round(p, 3)}
                for m, p in zip(months, pcts)
            ],
            "expected_closure_months": months[
                next(i for i, p in enumerate(pcts) if p >= 0.95)
            ],
        }
