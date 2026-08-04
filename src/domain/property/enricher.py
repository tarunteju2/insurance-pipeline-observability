"""
Property Claims Enricher.

Enriches property claims with external data:
  - FEMA flood zone determination
  - Weather event correlation (NOAA-style)
  - Replacement cost estimation
  - Building characteristics lookup
  - Geographic risk scoring
  - Contractor network matching
  - Contents inventory valuation
"""

import hashlib
import random
import structlog
from datetime import date
from typing import Any, Dict

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)

# Building type categories
_BUILDING_TYPES = {
    "single_family": {"risk_factor": 1.0, "avg_sqft": 2_200, "avg_cost_per_sqft": 175},
    "townhouse": {"risk_factor": 0.9, "avg_sqft": 1_800, "avg_cost_per_sqft": 165},
    "condo": {"risk_factor": 0.7, "avg_sqft": 1_200, "avg_cost_per_sqft": 190},
    "multi_family": {"risk_factor": 1.2, "avg_sqft": 3_500, "avg_cost_per_sqft": 155},
    "manufactured": {"risk_factor": 1.5, "avg_sqft": 1_400, "avg_cost_per_sqft": 85},
    "commercial": {"risk_factor": 1.3, "avg_sqft": 5_000, "avg_cost_per_sqft": 200},
}

# Construction type risk modifiers
_CONSTRUCTION_TYPES = {
    "frame": {"risk_modifier": 1.20, "fire_resistance": "low"},
    "masonry": {"risk_modifier": 0.95, "fire_resistance": "medium"},
    "steel_frame": {"risk_modifier": 0.85, "fire_resistance": "high"},
    "fire_resistive": {"risk_modifier": 0.75, "fire_resistance": "very_high"},
    "mixed": {"risk_modifier": 1.05, "fire_resistance": "medium"},
}

# Geographic risk scores by state
_STATE_RISK_SCORES = {
    "FL": 0.85, "TX": 0.75, "CA": 0.80, "LA": 0.82,
    "OK": 0.72, "KS": 0.68, "NC": 0.65, "SC": 0.67,
    "GA": 0.60, "AL": 0.62, "MS": 0.70, "NY": 0.45,
    "IL": 0.50, "OH": 0.48, "MI": 0.52, "PA": 0.42,
    "NJ": 0.55, "CT": 0.40, "MA": 0.38, "VA": 0.45,
    "CO": 0.55, "AZ": 0.50, "NV": 0.45, "OR": 0.40,
    "WA": 0.42, "MN": 0.48, "WI": 0.46, "IN": 0.50,
}

# Contractor network
_CONTRACTOR_NETWORK = {
    "SOUTHEAST": [
        {"id": "CTR-SE-001", "name": "Gulf Restoration Services", "rating": 4.8, "cat_response": True},
        {"id": "CTR-SE-002", "name": "Coastal Rebuild Partners", "rating": 4.6, "cat_response": True},
    ],
    "NORTHEAST": [
        {"id": "CTR-NE-001", "name": "Nor'easter Recovery Group", "rating": 4.7, "cat_response": True},
        {"id": "CTR-NE-002", "name": "Atlantic Construction Services", "rating": 4.5, "cat_response": False},
    ],
    "MIDWEST": [
        {"id": "CTR-MW-001", "name": "Heartland Restoration", "rating": 4.6, "cat_response": True},
        {"id": "CTR-MW-002", "name": "Prairie Build Contractors", "rating": 4.4, "cat_response": False},
    ],
    "WEST": [
        {"id": "CTR-WE-001", "name": "Pacific Restoration Group", "rating": 4.8, "cat_response": True},
        {"id": "CTR-WE-002", "name": "Mountain View Builders", "rating": 4.5, "cat_response": False},
    ],
    "SOUTH": [
        {"id": "CTR-SO-001", "name": "Dixie Restoration Services", "rating": 4.7, "cat_response": True},
        {"id": "CTR-SO-002", "name": "Sun Belt Construction", "rating": 4.4, "cat_response": False},
    ],
}


class PropertyClaimEnricher:
    """Enriches property claims with building, weather, and geographic data."""

    def enrich(self, claim: InsuranceClaim) -> Dict[str, Any]:
        enrichment: Dict[str, Any] = {}

        # 1. Building characteristics
        enrichment.update(self._lookup_building(claim))

        # 2. FEMA flood zone
        enrichment.update(self._lookup_flood_zone(claim))

        # 3. Weather event correlation
        enrichment.update(self._correlate_weather(claim))

        # 4. Replacement cost estimation
        enrichment.update(self._estimate_replacement_cost(claim, enrichment))

        # 5. Geographic risk scoring
        enrichment.update(self._geographic_risk(claim))

        # 6. Contractor network
        enrichment.update(self._match_contractors(claim))

        # 7. Contents valuation
        enrichment.update(self._estimate_contents(claim, enrichment))

        logger.debug(
            "Property claim enriched",
            claim_id=claim.claim_id,
            flood_zone=enrichment.get("fema_flood_zone"),
            replacement_cost=enrichment.get("replacement_cost_estimate"),
            geo_risk=enrichment.get("geographic_risk_score"),
        )

        return enrichment

    def _lookup_building(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Look up building characteristics."""
        addr = claim.property_address or ""
        seed = int(hashlib.md5(addr.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)

        building_type = rng.choice(list(_BUILDING_TYPES.keys()))
        construction = rng.choice(list(_CONSTRUCTION_TYPES.keys()))
        bt = _BUILDING_TYPES[building_type]
        ct = _CONSTRUCTION_TYPES[construction]

        year_built = rng.randint(1960, 2024)
        sqft = int(rng.gauss(bt["avg_sqft"], bt["avg_sqft"] * 0.3))
        sqft = max(600, sqft)
        stories = rng.choice([1, 1, 2, 2, 2, 3]) if building_type != "condo" else 1
        roof_age = rng.randint(0, 25)
        roof_type = rng.choice(["asphalt_shingle", "tile", "metal", "slate", "flat"])

        return {
            "building_characteristics": {
                "type": building_type,
                "construction": construction,
                "year_built": year_built,
                "square_footage": sqft,
                "stories": stories,
                "roof_type": roof_type,
                "roof_age_years": roof_age,
                "fire_resistance": ct["fire_resistance"],
                "has_sprinklers": rng.random() > 0.6,
                "has_security_system": rng.random() > 0.5,
                "has_backup_generator": rng.random() > 0.85,
                "foundation_type": rng.choice(["slab", "crawlspace", "basement", "pier"]),
            },
            "building_risk_factor": round(bt["risk_factor"] * ct["risk_modifier"], 3),
        }

    def _lookup_flood_zone(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Determine FEMA flood zone for the property."""
        addr = claim.property_address or ""
        seed = int(hashlib.md5(addr.encode()).hexdigest()[:6], 16)
        rng = random.Random(seed)

        zones = list(_FLOOD_ZONES.keys())
        weights = [5, 5, 2, 2, 3, 3, 10, 5, 30, 25, 10]
        zone = rng.choices(zones, weights=weights[:len(zones)])[0]
        zone_info = _FLOOD_ZONES[zone]

        return {
            "fema_flood_zone": zone,
            "flood_zone_risk": zone_info["risk"],
            "flood_zone_description": zone_info["description"],
            "flood_insurance_required": zone_info["risk"] == "high",
            "flood_map_panel": f"FIRM-{rng.randint(1000, 9999)}-{rng.randint(100, 999)}",
        }

    def _correlate_weather(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Correlate claim with weather events."""
        meta = claim.enrichment_data or {}
        state = meta.get("loss_state")

        try:
            loss_date = date.fromisoformat(claim.date_of_loss)
        except (ValueError, TypeError):
            return {"weather_correlation": None}

        # Check against known CAT events
        from src.domain.property.validator import _CATASTROPHE_EVENTS
        for cat in _CATASTROPHE_EVENTS:
            cat_start = date.fromisoformat(cat["start_date"])
            cat_end = date.fromisoformat(cat["end_date"])
            if (
                state in cat.get("affected_states", [])
                and cat_start <= loss_date <= cat_end
            ):
                return {
                    "weather_correlation": {
                        "cat_event_id": cat["cat_id"],
                        "event_name": cat["name"],
                        "event_type": cat["type"],
                        "pcs_serial": cat["pcs_serial"],
                        "confidence": 0.95,
                    },
                    "cat_event_id": cat["cat_id"],
                    "is_catastrophe_claim": True,
                }

        return {
            "weather_correlation": None,
            "is_catastrophe_claim": False,
        }

    def _estimate_replacement_cost(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Estimate dwelling replacement cost."""
        building = current.get("building_characteristics", {})
        sqft = building.get("square_footage", 2_000)
        construction = building.get("construction", "frame")
        year_built = building.get("year_built", 1990)

        # Base cost per sqft (adjusted for construction type)
        bt_name = building.get("type", "single_family")
        base_cost = _BUILDING_TYPES.get(bt_name, {}).get("avg_cost_per_sqft", 175)

        # Construction quality modifier
        quality_mod = _CONSTRUCTION_TYPES.get(construction, {}).get("risk_modifier", 1.0)
        # Older buildings cost more to bring to code
        age = max(0, date.today().year - year_built)
        code_upgrade_mod = 1.0 + min(age * 0.005, 0.25)

        replacement_cost = round(sqft * base_cost * quality_mod * code_upgrade_mod, 2)
        depreciation = round(replacement_cost * min(age * 0.01, 0.40), 2)
        acv = round(replacement_cost - depreciation, 2)

        return {
            "replacement_cost_estimate": replacement_cost,
            "actual_cash_value_estimate": acv,
            "depreciation_amount": depreciation,
            "cost_per_sqft": round(base_cost * quality_mod * code_upgrade_mod, 2),
            "code_upgrade_factor": round(code_upgrade_mod, 3),
        }

    def _geographic_risk(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Calculate geographic risk score."""
        meta = claim.enrichment_data or {}
        state = meta.get("loss_state", "")
        risk_score = _STATE_RISK_SCORES.get(state, 0.50)

        # Adjust for flood zone
        flood_risk = meta.get("flood_zone_risk", "low")
        if flood_risk == "high":
            risk_score = min(1.0, risk_score + 0.15)
        elif flood_risk == "moderate":
            risk_score = min(1.0, risk_score + 0.05)

        risk_tier = "low"
        if risk_score >= 0.75:
            risk_tier = "critical"
        elif risk_score >= 0.60:
            risk_tier = "high"
        elif risk_score >= 0.45:
            risk_tier = "moderate"

        return {
            "geographic_risk_score": round(risk_score, 3),
            "geographic_risk_tier": risk_tier,
            "loss_state": state,
        }

    def _match_contractors(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Match preferred contractors in the loss area."""
        meta = claim.enrichment_data or {}
        state = meta.get("loss_state", "")

        # Map state to region
        region_map = {
            "FL": "SOUTHEAST", "GA": "SOUTHEAST", "SC": "SOUTHEAST",
            "NC": "SOUTHEAST", "VA": "SOUTHEAST", "AL": "SOUTHEAST",
            "NY": "NORTHEAST", "NJ": "NORTHEAST", "CT": "NORTHEAST",
            "MA": "NORTHEAST", "PA": "NORTHEAST",
            "IL": "MIDWEST", "OH": "MIDWEST", "MI": "MIDWEST",
            "IN": "MIDWEST", "WI": "MIDWEST",
            "CA": "WEST", "OR": "WEST", "WA": "WEST",
            "CO": "WEST", "AZ": "WEST",
            "TX": "SOUTH", "LA": "SOUTH", "OK": "SOUTH",
        }
        region = region_map.get(state, "MIDWEST")
        contractors = _CONTRACTOR_NETWORK.get(region, [])

        return {
            "preferred_contractors": contractors,
            "contractor_region": region,
        }

    def _estimate_contents(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Estimate personal property / contents value."""
        building = current.get("building_characteristics", {})
        sqft = building.get("square_footage", 2_000)

        # Industry standard: contents ≈ 50-75% of dwelling replacement cost
        replacement = current.get("replacement_cost_estimate", sqft * 175)
        contents_estimate = round(replacement * 0.60, 2)

        return {
            "contents_coverage_estimate": contents_estimate,
            "contents_to_dwelling_ratio": 0.60,
        }
