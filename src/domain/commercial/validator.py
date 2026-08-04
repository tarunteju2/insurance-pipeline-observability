"""
Commercial Claims Validator.

Validates commercial lines claims:
  - GL/PL policy structure verification
  - SIC/NAICS business classification
  - Umbrella/excess layer validation
  - Workers' compensation jurisdiction checks
  - Experience modification rating (EMR) verification
  - Litigation hold detection
"""

import re
import structlog
from datetime import date
from typing import Any, Dict, List, Tuple

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)

# NAICS code format: 2-6 digits
_NAICS_PATTERN = re.compile(r'^\d{2,6}$')

# NAICS sector ranges (2-digit)
_NAICS_SECTORS = {
    "11": "Agriculture, Forestry", "21": "Mining, Oil & Gas",
    "22": "Utilities", "23": "Construction",
    "31": "Manufacturing", "32": "Manufacturing",
    "33": "Manufacturing", "42": "Wholesale Trade",
    "44": "Retail Trade", "45": "Retail Trade",
    "48": "Transportation", "49": "Warehousing",
    "51": "Information", "52": "Finance & Insurance",
    "53": "Real Estate", "54": "Professional Services",
    "55": "Management", "56": "Administrative Services",
    "61": "Educational Services", "62": "Health Care",
    "71": "Arts & Entertainment", "72": "Accommodation & Food",
    "81": "Other Services", "92": "Public Administration",
}

# High-risk NAICS sectors for liability
_HIGH_RISK_NAICS = {"23", "31", "32", "33", "21", "48", "49"}

# Workers' comp class codes and rates (per $100 of payroll)
_WC_CLASS_CODES = {
    "5183": {"description": "Plumbing", "rate_per_100": 4.52, "hazard_group": "D"},
    "5190": {"description": "Electrical", "rate_per_100": 3.89, "hazard_group": "C"},
    "5403": {"description": "Carpentry", "rate_per_100": 8.72, "hazard_group": "E"},
    "5606": {"description": "Construction - General", "rate_per_100": 12.45, "hazard_group": "F"},
    "8810": {"description": "Clerical Office", "rate_per_100": 0.18, "hazard_group": "A"},
    "8742": {"description": "Sales Outside", "rate_per_100": 0.42, "hazard_group": "A"},
    "8380": {"description": "Automobile Dealerships", "rate_per_100": 2.15, "hazard_group": "C"},
    "9079": {"description": "Restaurant", "rate_per_100": 1.85, "hazard_group": "B"},
    "7219": {"description": "Trucking", "rate_per_100": 9.83, "hazard_group": "E"},
    "2003": {"description": "Manufacturing - Bakery", "rate_per_100": 3.22, "hazard_group": "C"},
}


class CommercialClaimValidator:
    """LOB-specific validation for commercial insurance claims."""

    def validate(
        self, claim: InsuranceClaim
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        errors: List[Dict[str, Any]] = []

        errors.extend(self._validate_business_classification(claim))
        errors.extend(self._validate_policy_structure(claim))
        errors.extend(self._validate_workers_comp(claim))
        errors.extend(self._validate_experience_mod(claim))
        errors.extend(self._validate_litigation_status(claim))
        errors.extend(self._validate_aggregate_limits(claim))

        has_critical = any(e["severity"] == "critical" for e in errors)
        return not has_critical, errors

    def _validate_business_classification(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        naics = meta.get("naics_code")

        if naics:
            if not _NAICS_PATTERN.match(str(naics)):
                errors.append({
                    "code": "COMM_NAICS_INVALID_FORMAT",
                    "field": "enrichment_data.naics_code",
                    "message": f"NAICS code '{naics}' is not valid (expected 2-6 digits)",
                    "severity": "medium",
                })
            else:
                sector = str(naics)[:2]
                if sector not in _NAICS_SECTORS:
                    errors.append({
                        "code": "COMM_NAICS_UNKNOWN_SECTOR",
                        "field": "enrichment_data.naics_code",
                        "message": f"NAICS sector '{sector}' not recognized",
                        "severity": "low",
                    })
                elif sector in _HIGH_RISK_NAICS and claim.claim_amount > 100_000:
                    errors.append({
                        "code": "COMM_HIGH_RISK_INDUSTRY",
                        "field": "enrichment_data.naics_code",
                        "message": (
                            f"High-risk industry ({_NAICS_SECTORS.get(sector)}) "
                            f"claim exceeding $100K requires senior adjuster review"
                        ),
                        "severity": "medium",
                    })

        return errors

    def _validate_policy_structure(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}

        occurrence_limit = meta.get("occurrence_limit")
        aggregate_limit = meta.get("aggregate_limit")

        if occurrence_limit and claim.claim_amount > occurrence_limit:
            errors.append({
                "code": "COMM_EXCEEDS_OCCURRENCE_LIMIT",
                "field": "claim_amount",
                "message": (
                    f"Claim ${claim.claim_amount:,.2f} exceeds per-occurrence "
                    f"limit ${occurrence_limit:,.2f}"
                ),
                "severity": "high",
            })

        # Check for excess/umbrella layer trigger
        umbrella_attachment = meta.get("umbrella_attachment_point")
        if umbrella_attachment and claim.claim_amount > umbrella_attachment:
            errors.append({
                "code": "COMM_UMBRELLA_LAYER_TRIGGERED",
                "field": "claim_amount",
                "message": (
                    f"Claim pierces primary layer. Umbrella/excess coverage "
                    f"attachment point: ${umbrella_attachment:,.2f}"
                ),
                "severity": "medium",
            })

        return errors

    def _validate_workers_comp(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        from src.models.claims import ClaimType
        if claim.claim_type != ClaimType.WORKERS_COMP:
            return errors

        meta = claim.enrichment_data or {}
        class_code = meta.get("wc_class_code")

        if class_code and str(class_code) in _WC_CLASS_CODES:
            wc_info = _WC_CLASS_CODES[str(class_code)]
            if wc_info["hazard_group"] in ("E", "F") and claim.claim_amount > 75_000:
                errors.append({
                    "code": "COMM_WC_HIGH_HAZARD_CLAIM",
                    "field": "enrichment_data.wc_class_code",
                    "message": (
                        f"High-hazard class ({wc_info['description']}, "
                        f"hazard group {wc_info['hazard_group']}) "
                        f"with large claim requires loss control review"
                    ),
                    "severity": "medium",
                })

        # Jurisdiction check
        wc_state = meta.get("wc_jurisdiction_state")
        monopolistic_states = {"OH", "WA", "WY", "ND"}
        if wc_state in monopolistic_states:
            errors.append({
                "code": "COMM_WC_MONOPOLISTIC_STATE",
                "field": "enrichment_data.wc_jurisdiction_state",
                "message": (
                    f"Workers' comp in monopolistic state fund state ({wc_state}). "
                    f"Verify coverage through state fund."
                ),
                "severity": "medium",
            })

        return errors

    def _validate_experience_mod(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        emr = meta.get("experience_modification_rate")

        if emr is not None:
            if emr > 1.50:
                errors.append({
                    "code": "COMM_EMR_VERY_HIGH",
                    "field": "enrichment_data.experience_modification_rate",
                    "message": (
                        f"Experience mod rate {emr:.2f} significantly above "
                        f"industry average (1.00). Verify risk acceptability."
                    ),
                    "severity": "high",
                })
            elif emr > 1.25:
                errors.append({
                    "code": "COMM_EMR_ELEVATED",
                    "field": "enrichment_data.experience_modification_rate",
                    "message": (
                        f"Elevated experience mod rate {emr:.2f} indicates "
                        f"adverse loss history"
                    ),
                    "severity": "low",
                })

        return errors

    def _validate_litigation_status(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}

        if meta.get("litigation_pending"):
            errors.append({
                "code": "COMM_LITIGATION_PENDING",
                "field": "enrichment_data.litigation_pending",
                "message": "Active litigation on this claim. Route to legal department.",
                "severity": "high",
            })

        if meta.get("regulatory_action_pending"):
            errors.append({
                "code": "COMM_REGULATORY_ACTION",
                "field": "enrichment_data.regulatory_action_pending",
                "message": "Pending regulatory action. Flag for compliance review.",
                "severity": "high",
            })

        return errors

    def _validate_aggregate_limits(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}

        aggregate_limit = meta.get("aggregate_limit")
        aggregate_used = meta.get("aggregate_used", 0)

        if aggregate_limit and aggregate_used:
            remaining = aggregate_limit - aggregate_used
            if claim.claim_amount > remaining:
                errors.append({
                    "code": "COMM_AGGREGATE_EXHAUSTED",
                    "field": "claim_amount",
                    "message": (
                        f"Claim ${claim.claim_amount:,.2f} exceeds remaining "
                        f"aggregate limit (${remaining:,.2f} of "
                        f"${aggregate_limit:,.2f} remaining)"
                    ),
                    "severity": "critical",
                })
            elif claim.claim_amount > remaining * 0.5:
                errors.append({
                    "code": "COMM_AGGREGATE_WARNING",
                    "field": "claim_amount",
                    "message": (
                        f"This claim will consume >{50}% of remaining "
                        f"aggregate limit"
                    ),
                    "severity": "medium",
                })

        return errors
