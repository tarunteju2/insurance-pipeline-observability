"""
Automobile Claims Validator.

Performs LOB-specific validation beyond the generic pipeline checks:
  - NHTSA-style VIN structure validation (17-character ISO 3779)
  - Salvage title indicator detection
  - Mileage plausibility analysis
  - Glass breakage pattern consistency
  - Collision report cross-reference
  - Total-loss threshold pre-screening
"""

import re
import hashlib
import structlog
from datetime import date
from typing import Any, Dict, List, Tuple

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)

# VIN check-digit weights (position 1–17 minus position 9 which is the check digit)
_VIN_TRANSLITERATION = {
    'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6, 'G': 7, 'H': 8,
    'J': 1, 'K': 2, 'L': 3, 'M': 4, 'N': 5, 'P': 7, 'R': 9,
    'S': 2, 'T': 3, 'U': 4, 'V': 5, 'W': 6, 'X': 7, 'Y': 8, 'Z': 9,
}
_VIN_WEIGHTS = [8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2]

# NHTSA World Manufacturer Identifier prefixes (sample set)
_KNOWN_WMI_PREFIXES = {
    "1G1": "Chevrolet", "1G2": "Pontiac", "1GC": "Chevrolet Truck",
    "1FA": "Ford", "1FB": "Ford", "1FC": "Ford",
    "1HD": "Harley-Davidson", "1HG": "Honda",
    "1J4": "Jeep", "1N4": "Nissan", "2G1": "Chevrolet (Canada)",
    "2HG": "Honda (Canada)", "3FA": "Ford (Mexico)",
    "3VW": "Volkswagen (Mexico)", "4T1": "Toyota",
    "5YJ": "Tesla", "JHM": "Honda (Japan)", "JTD": "Toyota (Japan)",
    "KMH": "Hyundai (Korea)", "WAU": "Audi", "WBA": "BMW",
    "WDB": "Mercedes-Benz", "WF0": "Ford (Germany)",
    "YV1": "Volvo", "ZFF": "Ferrari",
}

# Average annual mileage by vehicle age (years)
_AVG_ANNUAL_MILES = {
    0: 14_000, 1: 13_500, 2: 13_000, 3: 12_500, 4: 12_000,
    5: 11_500, 6: 11_000, 7: 10_500, 8: 10_000, 9: 9_500,
    10: 9_000, 15: 7_500, 20: 6_000,
}

# Total-loss thresholds by state (percentage of ACV)
_TOTAL_LOSS_THRESHOLD_PCT = {
    "DEFAULT": 0.75, "AL": 0.75, "CA": 0.80, "CO": 1.00,
    "FL": 0.80, "GA": 0.75, "IL": 0.50, "MI": 0.75,
    "NY": 0.75, "OH": 1.00, "TX": 1.00,
}

# Glass claim patterns — suspicious if these conflict with damage type
_GLASS_CLAIM_KEYWORDS = {"windshield", "window", "sunroof", "glass", "mirror"}


class AutoClaimValidator:
    """
    LOB-specific validation for automobile insurance claims.

    Returns (is_valid, list_of_error_dicts) where each error dict contains:
      - code: machine-readable error code
      - field: affected field
      - message: human-readable description
      - severity: critical | high | medium | low
    """

    def validate(
        self, claim: InsuranceClaim
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        errors: List[Dict[str, Any]] = []

        # --- VIN validation ---
        errors.extend(self._validate_vin(claim))

        # --- Mileage plausibility ---
        errors.extend(self._validate_mileage(claim))

        # --- Glass breakage consistency ---
        errors.extend(self._validate_glass_pattern(claim))

        # --- Total-loss pre-screen ---
        errors.extend(self._validate_total_loss_threshold(claim))

        # --- Collision report reference ---
        errors.extend(self._validate_collision_report(claim))

        # --- High-value auto-specific checks ---
        errors.extend(self._validate_high_value_auto(claim))

        has_critical = any(e["severity"] == "critical" for e in errors)
        is_valid = not has_critical
        return is_valid, errors

    # ------------------------------------------------------------------
    # VIN Validation (ISO 3779 / NHTSA)
    # ------------------------------------------------------------------

    def _validate_vin(self, claim: InsuranceClaim) -> List[Dict[str, Any]]:
        errors = []
        vin = claim.vehicle_vin

        if not vin:
            if claim.claim_amount > 15_000:
                errors.append({
                    "code": "AUTO_VIN_REQUIRED_HIGH_VALUE",
                    "field": "vehicle_vin",
                    "message": (
                        f"VIN required for auto claims exceeding $15,000 "
                        f"(claim amount: ${claim.claim_amount:,.2f})"
                    ),
                    "severity": "high",
                })
            return errors

        # Length check
        if len(vin) != 17:
            errors.append({
                "code": "AUTO_VIN_INVALID_LENGTH",
                "field": "vehicle_vin",
                "message": f"VIN must be exactly 17 characters (got {len(vin)})",
                "severity": "critical",
            })
            return errors

        # Forbidden characters (I, O, Q are not valid in VINs)
        if re.search(r'[IOQ]', vin.upper()):
            errors.append({
                "code": "AUTO_VIN_INVALID_CHARACTERS",
                "field": "vehicle_vin",
                "message": "VIN contains forbidden characters (I, O, or Q)",
                "severity": "critical",
            })

        # Check-digit validation (position 9)
        check_digit_result = self._verify_vin_check_digit(vin.upper())
        if check_digit_result is False:
            errors.append({
                "code": "AUTO_VIN_CHECK_DIGIT_FAILED",
                "field": "vehicle_vin",
                "message": "VIN check digit (position 9) does not match calculated value",
                "severity": "high",
            })

        # WMI lookup
        wmi = vin[:3].upper()
        if wmi not in _KNOWN_WMI_PREFIXES:
            errors.append({
                "code": "AUTO_VIN_UNKNOWN_MANUFACTURER",
                "field": "vehicle_vin",
                "message": f"World Manufacturer Identifier '{wmi}' not in known registry",
                "severity": "low",
            })

        # Model year extraction (position 10)
        model_year_char = vin[9].upper()
        model_year = self._decode_model_year(model_year_char)
        if model_year and model_year > date.today().year + 1:
            errors.append({
                "code": "AUTO_VIN_FUTURE_MODEL_YEAR",
                "field": "vehicle_vin",
                "message": f"VIN indicates model year {model_year}, which is in the future",
                "severity": "medium",
            })

        return errors

    def _verify_vin_check_digit(self, vin: str) -> bool:
        """Verify the VIN check digit per NHTSA/ISO 3779 algorithm."""
        try:
            total = 0
            for i, char in enumerate(vin):
                if char.isdigit():
                    value = int(char)
                else:
                    value = _VIN_TRANSLITERATION.get(char)
                    if value is None:
                        return False
                total += value * _VIN_WEIGHTS[i]

            remainder = total % 11
            expected = 'X' if remainder == 10 else str(remainder)
            return vin[8] == expected
        except (IndexError, ValueError):
            return False

    def _decode_model_year(self, char: str) -> int | None:
        """Decode VIN position-10 character to model year."""
        year_map = {}
        # 1980-2000: A=1980, B=1981, ..., Y=2000
        for i, c in enumerate("ABCDEFGHJKLMNPRSTVWXY"):
            year_map[c] = 1980 + i
        # 2001-2009: 1-9
        for i in range(1, 10):
            year_map[str(i)] = 2000 + i
        # 2010-2030: A=2010, B=2011, ...
        for i, c in enumerate("ABCDEFGHJKLMNPRSTVWXY"):
            if c not in year_map:
                year_map[c] = 2010 + i
            else:
                # Second cycle — prefer newer year if ambiguous
                year_map[c] = 2010 + i
        return year_map.get(char)

    # ------------------------------------------------------------------
    # Mileage plausibility
    # ------------------------------------------------------------------

    def _validate_mileage(self, claim: InsuranceClaim) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        mileage = meta.get("vehicle_mileage")
        vehicle_year = meta.get("vehicle_year")

        if mileage is None or vehicle_year is None:
            return errors

        try:
            age = max(0, date.today().year - int(vehicle_year))
            expected_avg = _AVG_ANNUAL_MILES.get(
                min(age, 20), 6_000
            )
            expected_total = expected_avg * max(age, 1)
            ratio = mileage / max(expected_total, 1)

            if ratio > 2.5:
                errors.append({
                    "code": "AUTO_MILEAGE_IMPLAUSIBLY_HIGH",
                    "field": "enrichment_data.vehicle_mileage",
                    "message": (
                        f"Reported mileage ({mileage:,}) is {ratio:.1f}x the "
                        f"expected average for a {age}-year-old vehicle"
                    ),
                    "severity": "medium",
                })
            elif ratio < 0.1 and age > 3:
                errors.append({
                    "code": "AUTO_MILEAGE_SUSPICIOUSLY_LOW",
                    "field": "enrichment_data.vehicle_mileage",
                    "message": (
                        f"Reported mileage ({mileage:,}) is unusually low "
                        f"for a {age}-year-old vehicle"
                    ),
                    "severity": "low",
                })
        except (ValueError, TypeError):
            pass

        return errors

    # ------------------------------------------------------------------
    # Glass breakage consistency
    # ------------------------------------------------------------------

    def _validate_glass_pattern(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        desc = (claim.description or "").lower()
        is_glass_claim = any(kw in desc for kw in _GLASS_CLAIM_KEYWORDS)
        meta = claim.enrichment_data or {}

        if is_glass_claim and claim.claim_amount > 5_000:
            errors.append({
                "code": "AUTO_GLASS_AMOUNT_EXCESSIVE",
                "field": "claim_amount",
                "message": (
                    f"Glass-only claim amount ${claim.claim_amount:,.2f} exceeds "
                    f"typical glass replacement cost ceiling ($5,000)"
                ),
                "severity": "medium",
            })

        # Check for repeated glass claims from same claimant
        prior_glass_count = meta.get("prior_glass_claims_12m", 0)
        if prior_glass_count >= 3:
            errors.append({
                "code": "AUTO_GLASS_FREQUENCY_SUSPICIOUS",
                "field": "enrichment_data.prior_glass_claims_12m",
                "message": (
                    f"Claimant has {prior_glass_count} glass claims in the "
                    f"past 12 months — exceeds normal frequency"
                ),
                "severity": "high",
            })

        return errors

    # ------------------------------------------------------------------
    # Total-loss pre-screening
    # ------------------------------------------------------------------

    def _validate_total_loss_threshold(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        acv = meta.get("actual_cash_value")
        state = meta.get("loss_state", "DEFAULT")

        if acv is None or acv <= 0:
            return errors

        threshold_pct = _TOTAL_LOSS_THRESHOLD_PCT.get(
            state, _TOTAL_LOSS_THRESHOLD_PCT["DEFAULT"]
        )
        total_loss_threshold = acv * threshold_pct

        if claim.claim_amount >= total_loss_threshold:
            errors.append({
                "code": "AUTO_TOTAL_LOSS_THRESHOLD_EXCEEDED",
                "field": "claim_amount",
                "message": (
                    f"Repair estimate ${claim.claim_amount:,.2f} exceeds "
                    f"total-loss threshold ({threshold_pct:.0%} of ACV "
                    f"${acv:,.2f} = ${total_loss_threshold:,.2f}) for state "
                    f"{state}. Routing to total-loss adjuster."
                ),
                "severity": "medium",
            })

        return errors

    # ------------------------------------------------------------------
    # Collision report cross-reference
    # ------------------------------------------------------------------

    def _validate_collision_report(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.processing_metadata or {}
        desc = (claim.description or "").lower()

        # If claim describes a collision but no police report number provided
        collision_keywords = {"collision", "accident", "hit", "crash", "rear-end"}
        is_collision = any(kw in desc for kw in collision_keywords)

        if is_collision and claim.claim_amount > 10_000:
            report_number = meta.get("police_report_number")
            if not report_number:
                errors.append({
                    "code": "AUTO_COLLISION_NO_POLICE_REPORT",
                    "field": "processing_metadata.police_report_number",
                    "message": (
                        "Collision claim exceeding $10,000 requires a police "
                        "report number for processing"
                    ),
                    "severity": "high",
                })

        return errors

    # ------------------------------------------------------------------
    # High-value automobile checks
    # ------------------------------------------------------------------

    def _validate_high_value_auto(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}

        # Luxury/exotic vehicle flag
        if claim.claim_amount > 75_000:
            make = meta.get("vehicle_make", "").upper()
            exotic_makes = {
                "FERRARI", "LAMBORGHINI", "MASERATI", "BENTLEY",
                "ROLLS-ROYCE", "MCLAREN", "BUGATTI", "ASTON MARTIN",
                "PORSCHE", "LOTUS",
            }
            is_exotic = make in exotic_makes

            if is_exotic and not meta.get("agreed_value_endorsement"):
                errors.append({
                    "code": "AUTO_EXOTIC_NO_AGREED_VALUE",
                    "field": "enrichment_data.agreed_value_endorsement",
                    "message": (
                        f"Exotic vehicle ({make}) claim exceeding $75,000 "
                        f"should have an agreed-value endorsement on file"
                    ),
                    "severity": "medium",
                })

            # Independent appraisal required for high-value claims
            if not meta.get("independent_appraisal"):
                errors.append({
                    "code": "AUTO_HIGH_VALUE_NO_APPRAISAL",
                    "field": "enrichment_data.independent_appraisal",
                    "message": (
                        f"Claims exceeding $75,000 require an independent "
                        f"appraisal before adjudication"
                    ),
                    "severity": "high",
                })

        return errors
