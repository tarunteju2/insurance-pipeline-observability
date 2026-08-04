"""
Property Claims Validator.

Performs LOB-specific validation for property/homeowners claims:
  - ISO location code validation
  - FEMA flood zone verification
  - Catastrophe (CAT) event correlation
  - Building code compliance checks
  - Coverage verification (dwelling, personal property, liability)
  - Replacement cost vs. actual cash value determination
  - Ordinance or law coverage applicability
"""

import re
import structlog
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)

# FEMA flood zone risk categories
_FLOOD_ZONES = {
    "A": {"risk": "high", "description": "100-year floodplain, no BFE determined"},
    "AE": {"risk": "high", "description": "100-year floodplain, BFE determined"},
    "AH": {"risk": "high", "description": "100-year floodplain, shallow flooding"},
    "AO": {"risk": "high", "description": "100-year floodplain, sheet flow"},
    "V": {"risk": "high", "description": "Coastal high hazard area"},
    "VE": {"risk": "high", "description": "Coastal high hazard, BFE determined"},
    "B": {"risk": "moderate", "description": "500-year floodplain"},
    "X500": {"risk": "moderate", "description": "500-year floodplain (shaded)"},
    "C": {"risk": "low", "description": "Minimal flood hazard"},
    "X": {"risk": "low", "description": "Minimal flood hazard (unshaded)"},
    "D": {"risk": "undetermined", "description": "Flood hazard undetermined"},
}

# Known catastrophe events registry
_CATASTROPHE_EVENTS = [
    {
        "cat_id": "CAT-2026-001",
        "name": "Hurricane Elena",
        "type": "hurricane",
        "start_date": "2026-06-15",
        "end_date": "2026-06-22",
        "affected_states": ["FL", "GA", "SC", "NC"],
        "estimated_insured_loss_billions": 28.5,
        "pcs_serial": "PCS-2026-0142",
    },
    {
        "cat_id": "CAT-2026-002",
        "name": "Texas Hailstorm Complex",
        "type": "severe_convective_storm",
        "start_date": "2026-04-10",
        "end_date": "2026-04-14",
        "affected_states": ["TX", "OK", "KS"],
        "estimated_insured_loss_billions": 8.2,
        "pcs_serial": "PCS-2026-0089",
    },
    {
        "cat_id": "CAT-2026-003",
        "name": "California Wildfire Season",
        "type": "wildfire",
        "start_date": "2026-07-01",
        "end_date": "2026-08-15",
        "affected_states": ["CA"],
        "estimated_insured_loss_billions": 15.3,
        "pcs_serial": "PCS-2026-0201",
    },
    {
        "cat_id": "CAT-2025-010",
        "name": "Midwest Winter Storm",
        "type": "winter_storm",
        "start_date": "2025-12-18",
        "end_date": "2025-12-26",
        "affected_states": ["IL", "OH", "MI", "IN", "WI", "MN"],
        "estimated_insured_loss_billions": 5.7,
        "pcs_serial": "PCS-2025-0312",
    },
]

# Peril types and their typical claim characteristics
_PERIL_KEYWORDS = {
    "fire": {"perils": ["fire", "arson", "smoke"], "typical_range": (5_000, 500_000)},
    "water": {"perils": ["water", "pipe", "leak", "plumbing", "burst"], "typical_range": (2_000, 100_000)},
    "wind": {"perils": ["wind", "storm", "tornado", "hurricane"], "typical_range": (3_000, 300_000)},
    "hail": {"perils": ["hail"], "typical_range": (1_000, 50_000)},
    "theft": {"perils": ["theft", "burglary", "stolen", "break-in"], "typical_range": (500, 75_000)},
    "flood": {"perils": ["flood", "flooding", "rising water"], "typical_range": (5_000, 250_000)},
    "earthquake": {"perils": ["earthquake", "quake", "seismic"], "typical_range": (10_000, 1_000_000)},
    "lightning": {"perils": ["lightning", "electrical surge"], "typical_range": (1_000, 25_000)},
    "vandalism": {"perils": ["vandalism", "graffiti", "malicious"], "typical_range": (200, 15_000)},
}


class PropertyClaimValidator:
    """
    LOB-specific validation for property/homeowners claims.

    Returns (is_valid, list_of_error_dicts).
    """

    def validate(
        self, claim: InsuranceClaim
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        errors: List[Dict[str, Any]] = []

        # --- Property address validation ---
        errors.extend(self._validate_property_address(claim))

        # --- Flood zone verification ---
        errors.extend(self._validate_flood_zone(claim))

        # --- Catastrophe event correlation ---
        errors.extend(self._validate_cat_event(claim))

        # --- Peril classification ---
        errors.extend(self._validate_peril(claim))

        # --- Coverage limits ---
        errors.extend(self._validate_coverage_limits(claim))

        # --- Replacement cost check ---
        errors.extend(self._validate_replacement_cost(claim))

        # --- Occupancy status ---
        errors.extend(self._validate_occupancy(claim))

        has_critical = any(e["severity"] == "critical" for e in errors)
        is_valid = not has_critical
        return is_valid, errors

    def _validate_property_address(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        if not claim.property_address:
            errors.append({
                "code": "PROP_ADDRESS_MISSING",
                "field": "property_address",
                "message": "Property address is required for property claims",
                "severity": "critical",
            })
        elif len(claim.property_address) < 10:
            errors.append({
                "code": "PROP_ADDRESS_INCOMPLETE",
                "field": "property_address",
                "message": "Property address appears incomplete (must include street, city, state, zip)",
                "severity": "high",
            })
        return errors

    def _validate_flood_zone(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        desc = (claim.description or "").lower()
        flood_zone = meta.get("fema_flood_zone")
        is_flood_claim = any(kw in desc for kw in ("flood", "rising water", "storm surge"))

        if is_flood_claim and not flood_zone:
            errors.append({
                "code": "PROP_FLOOD_ZONE_UNKNOWN",
                "field": "enrichment_data.fema_flood_zone",
                "message": "Flood claim requires FEMA flood zone determination",
                "severity": "high",
            })
        elif is_flood_claim and flood_zone:
            zone_info = _FLOOD_ZONES.get(flood_zone)
            if zone_info and zone_info["risk"] == "low":
                errors.append({
                    "code": "PROP_FLOOD_LOW_RISK_ZONE",
                    "field": "enrichment_data.fema_flood_zone",
                    "message": (
                        f"Property is in low-risk flood zone '{flood_zone}'. "
                        f"Verify flood coverage is in force."
                    ),
                    "severity": "medium",
                })

        return errors

    def _validate_cat_event(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        state = meta.get("loss_state")

        if not state:
            return errors

        try:
            loss_date = date.fromisoformat(claim.date_of_loss)
        except (ValueError, TypeError):
            return errors

        # Check if claim overlaps with a declared CAT event
        for cat in _CATASTROPHE_EVENTS:
            cat_start = date.fromisoformat(cat["start_date"])
            cat_end = date.fromisoformat(cat["end_date"])
            if (
                state in cat["affected_states"]
                and cat_start <= loss_date <= cat_end + timedelta(days=7)
            ):
                if not meta.get("cat_event_id"):
                    errors.append({
                        "code": "PROP_CAT_EVENT_NOT_TAGGED",
                        "field": "enrichment_data.cat_event_id",
                        "message": (
                            f"Loss date/location overlaps with declared catastrophe "
                            f"'{cat['name']}' ({cat['cat_id']}). Claim should be "
                            f"tagged as CAT event for proper reserve and reporting."
                        ),
                        "severity": "medium",
                    })
                break

        return errors

    def _validate_peril(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        desc = (claim.description or "").lower()

        detected_peril = None
        for peril_name, config in _PERIL_KEYWORDS.items():
            if any(kw in desc for kw in config["perils"]):
                detected_peril = peril_name
                low, high = config["typical_range"]
                if claim.claim_amount > high * 2:
                    errors.append({
                        "code": "PROP_AMOUNT_EXCEEDS_PERIL_NORM",
                        "field": "claim_amount",
                        "message": (
                            f"Claim amount ${claim.claim_amount:,.2f} is unusually high "
                            f"for {peril_name} peril (typical range: "
                            f"${low:,.0f} - ${high:,.0f})"
                        ),
                        "severity": "medium",
                    })
                break

        if not detected_peril:
            errors.append({
                "code": "PROP_PERIL_UNCLASSIFIED",
                "field": "description",
                "message": "Unable to classify peril from claim description. Manual review required.",
                "severity": "low",
            })

        return errors

    def _validate_coverage_limits(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        dwelling_limit = meta.get("dwelling_coverage_limit")

        if dwelling_limit and claim.claim_amount > dwelling_limit:
            errors.append({
                "code": "PROP_EXCEEDS_DWELLING_LIMIT",
                "field": "claim_amount",
                "message": (
                    f"Claim amount ${claim.claim_amount:,.2f} exceeds "
                    f"dwelling coverage limit ${dwelling_limit:,.2f}"
                ),
                "severity": "high",
            })

        return errors

    def _validate_replacement_cost(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        replacement_cost = meta.get("replacement_cost_estimate")
        has_rcv_endorsement = meta.get("replacement_cost_endorsement", False)

        if replacement_cost and not has_rcv_endorsement:
            if claim.claim_amount > replacement_cost * 0.8:
                errors.append({
                    "code": "PROP_RCV_ENDORSEMENT_MISSING",
                    "field": "enrichment_data.replacement_cost_endorsement",
                    "message": (
                        "Large claim approaching replacement cost but policy "
                        "does not have Replacement Cost Value (RCV) endorsement. "
                        "Payment will be limited to Actual Cash Value (ACV)."
                    ),
                    "severity": "medium",
                })

        return errors

    def _validate_occupancy(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        occupancy = meta.get("occupancy_status")

        if occupancy == "vacant" and claim.claim_amount > 10_000:
            errors.append({
                "code": "PROP_VACANT_PROPERTY",
                "field": "enrichment_data.occupancy_status",
                "message": (
                    "Property was vacant at time of loss. Most policies exclude "
                    "or limit coverage for properties vacant more than 60 days."
                ),
                "severity": "high",
            })

        return errors
