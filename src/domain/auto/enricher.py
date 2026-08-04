"""
Automobile Claims Enricher.

Enriches auto claims with external data lookups:
  - Vehicle identification (make, model, year from VIN decode)
  - CARFAX-style vehicle history lookup
  - Repair shop network matching by coverage zone
  - Actual Cash Value (ACV) estimation
  - Total-loss threshold determination
  - Rental car authorization
  - Salvage value estimation
"""

import hashlib
import random
import structlog
from datetime import date, timedelta
from typing import Any, Dict

from src.models.claims import InsuranceClaim
from src.domain.auto.validator import _TOTAL_LOSS_THRESHOLD_PCT

logger = structlog.get_logger(__name__)

# CARFAX-style vehicle history database
_VEHICLE_MAKES = {
    "1G1": ("Chevrolet", "Malibu"), "1FA": ("Ford", "Mustang"),
    "1HG": ("Honda", "Civic"), "4T1": ("Toyota", "Camry"),
    "5YJ": ("Tesla", "Model 3"), "WBA": ("BMW", "3 Series"),
    "WDB": ("Mercedes-Benz", "C-Class"), "WAU": ("Audi", "A4"),
    "JHM": ("Honda", "Accord"), "KMH": ("Hyundai", "Sonata"),
    "3VW": ("Volkswagen", "Jetta"), "2HG": ("Honda", "Civic"),
    "1N4": ("Nissan", "Altima"), "YV1": ("Volvo", "S60"),
    "JTD": ("Toyota", "Corolla"), "ZFF": ("Ferrari", "F8"),
}

# ACV depreciation curve by vehicle age (pct of MSRP retained)
_DEPRECIATION_CURVE = {
    0: 1.00, 1: 0.80, 2: 0.70, 3: 0.62, 4: 0.55,
    5: 0.48, 6: 0.42, 7: 0.37, 8: 0.32, 9: 0.28,
    10: 0.25, 12: 0.20, 15: 0.14, 20: 0.08,
}

# Average MSRP by manufacturer tier
_TIER_MSRP = {
    "economy": 25_000, "midrange": 38_000, "premium": 55_000,
    "luxury": 85_000, "exotic": 200_000,
}

_MAKE_TIER = {
    "Chevrolet": "economy", "Ford": "economy", "Hyundai": "economy",
    "Nissan": "economy", "Volkswagen": "economy",
    "Honda": "midrange", "Toyota": "midrange", "Volvo": "midrange",
    "Audi": "premium", "BMW": "premium", "Mercedes-Benz": "premium",
    "Tesla": "premium",
    "Ferrari": "exotic", "Lamborghini": "exotic", "Porsche": "luxury",
}

# Repair network — preferred shops by state
_PREFERRED_SHOPS = {
    "CA": [
        {"shop_id": "CRN-CA-001", "name": "Pacific Coast Collision", "rating": 4.8, "certified": True},
        {"shop_id": "CRN-CA-002", "name": "Golden State Auto Body", "rating": 4.6, "certified": True},
        {"shop_id": "CRN-CA-003", "name": "Bay Area Collision Center", "rating": 4.5, "certified": True},
    ],
    "TX": [
        {"shop_id": "CRN-TX-001", "name": "Lone Star Auto Repair", "rating": 4.7, "certified": True},
        {"shop_id": "CRN-TX-002", "name": "Texas Pride Collision", "rating": 4.5, "certified": True},
    ],
    "NY": [
        {"shop_id": "CRN-NY-001", "name": "Empire State Auto Body", "rating": 4.6, "certified": True},
        {"shop_id": "CRN-NY-002", "name": "Metro Collision Specialists", "rating": 4.4, "certified": True},
    ],
    "FL": [
        {"shop_id": "CRN-FL-001", "name": "Sunshine Collision Center", "rating": 4.7, "certified": True},
        {"shop_id": "CRN-FL-002", "name": "Gulf Coast Auto Body", "rating": 4.5, "certified": True},
    ],
    "DEFAULT": [
        {"shop_id": "CRN-DFL-001", "name": "National Auto Network Partner", "rating": 4.3, "certified": True},
    ],
}


class AutoClaimEnricher:
    """
    Enriches automobile claims with vehicle data, repair network info,
    ACV estimation, and total-loss determination.
    """

    def enrich(self, claim: InsuranceClaim) -> Dict[str, Any]:
        enrichment: Dict[str, Any] = {}

        # 1. VIN decode — extract make, model, year
        enrichment.update(self._decode_vin(claim))

        # 2. Vehicle history report (CARFAX-style)
        enrichment.update(self._vehicle_history(claim, enrichment))

        # 3. ACV estimation
        enrichment.update(self._estimate_acv(enrichment))

        # 4. Repair network lookup
        enrichment.update(self._lookup_repair_network(claim))

        # 5. Total-loss determination
        enrichment.update(self._total_loss_check(claim, enrichment))

        # 6. Rental car authorization
        enrichment.update(self._rental_authorization(claim, enrichment))

        # 7. Salvage value estimation
        enrichment.update(self._estimate_salvage_value(enrichment))

        logger.debug(
            "Auto claim enriched",
            claim_id=claim.claim_id,
            make=enrichment.get("vehicle_make"),
            model=enrichment.get("vehicle_model"),
            acv=enrichment.get("actual_cash_value"),
            total_loss=enrichment.get("is_total_loss"),
        )

        return enrichment

    def _decode_vin(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Decode VIN to extract manufacturer, model, and model year."""
        vin = claim.vehicle_vin
        if not vin or len(vin) < 10:
            return {
                "vehicle_make": "Unknown",
                "vehicle_model": "Unknown",
                "vehicle_year": date.today().year - 3,
                "vin_decoded": False,
            }

        wmi = vin[:3].upper()
        make_model = _VEHICLE_MAKES.get(wmi, ("Unknown", "Unknown"))

        # Decode model year from position 10
        year_char = vin[9].upper()
        year_map = {}
        for i, c in enumerate("ABCDEFGHJKLMNPRSTVWXY"):
            year_map[c] = 2010 + i
        for i in range(1, 10):
            year_map[str(i)] = 2000 + i
        model_year = year_map.get(year_char, date.today().year - 5)

        # Deterministic "assembly plant" from position 11
        plant_codes = {
            "1": "USA", "2": "Canada", "3": "Mexico",
            "A": "South Africa", "J": "Japan", "K": "Korea",
            "S": "UK", "W": "Germany", "Z": "Italy",
        }
        assembly_country = plant_codes.get(vin[10].upper(), "Unknown")

        return {
            "vehicle_make": make_model[0],
            "vehicle_model": make_model[1],
            "vehicle_year": model_year,
            "vehicle_wmi": wmi,
            "assembly_country": assembly_country,
            "vin_decoded": True,
        }

    def _vehicle_history(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Vehicle history report (CARFAX-style).

        Uses deterministic hashing of VIN for reproducible results.
        """
        vin = claim.vehicle_vin or ""
        seed = int(hashlib.md5(vin.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)

        year = current.get("vehicle_year", date.today().year - 5)
        age = max(0, date.today().year - year)

        owner_count = max(1, rng.randint(1, min(age + 1, 6)))
        accident_count = rng.choices([0, 1, 2, 3], weights=[50, 30, 15, 5])[0]
        has_salvage_title = rng.random() < 0.04  # 4% of vehicles
        has_flood_damage = rng.random() < 0.02  # 2%
        has_odometer_rollback = rng.random() < 0.03  # 3%
        mileage = int(rng.gauss(12_000, 3_000) * max(age, 1))
        mileage = max(1_000, mileage)

        service_record_count = rng.randint(age, age * 4)
        recall_count = rng.randint(0, 3)
        recalls_completed = rng.randint(0, recall_count)

        return {
            "vehicle_history": {
                "owner_count": owner_count,
                "prior_accident_count": accident_count,
                "salvage_title": has_salvage_title,
                "flood_damage_reported": has_flood_damage,
                "odometer_rollback_detected": has_odometer_rollback,
                "service_records": service_record_count,
                "open_recalls": recall_count - recalls_completed,
                "last_inspection_date": (
                    date.today() - timedelta(days=rng.randint(30, 365))
                ).isoformat(),
            },
            "vehicle_mileage": mileage,
        }

    def _estimate_acv(self, current: Dict[str, Any]) -> Dict[str, Any]:
        """Estimate Actual Cash Value based on make, year, and condition."""
        make = current.get("vehicle_make", "Unknown")
        year = current.get("vehicle_year", date.today().year - 5)
        age = max(0, date.today().year - year)

        tier = _MAKE_TIER.get(make, "midrange")
        base_msrp = _TIER_MSRP.get(tier, 35_000)

        # Apply depreciation
        dep_pct = 0.08  # fallback
        for threshold_age in sorted(_DEPRECIATION_CURVE.keys(), reverse=True):
            if age >= threshold_age:
                dep_pct = _DEPRECIATION_CURVE[threshold_age]
                break

        acv = round(base_msrp * dep_pct, 2)

        # Condition adjustments
        history = current.get("vehicle_history", {})
        if history.get("salvage_title"):
            acv *= 0.50
        if history.get("flood_damage_reported"):
            acv *= 0.60
        if history.get("prior_accident_count", 0) > 2:
            acv *= 0.85

        return {
            "actual_cash_value": round(acv, 2),
            "acv_method": "market_depreciation",
            "vehicle_tier": tier,
            "base_msrp": base_msrp,
            "depreciation_pct": round(dep_pct, 4),
        }

    def _lookup_repair_network(
        self, claim: InsuranceClaim
    ) -> Dict[str, Any]:
        """Find preferred repair shops in the claimant's area."""
        meta = claim.enrichment_data or {}
        state = meta.get("loss_state", "DEFAULT")
        shops = _PREFERRED_SHOPS.get(state, _PREFERRED_SHOPS["DEFAULT"])

        return {
            "preferred_repair_shops": shops,
            "repair_network_size": len(shops),
            "repair_estimate_requested": claim.claim_amount > 2_500,
        }

    def _total_loss_check(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Determine if the vehicle should be declared a total loss."""
        acv = current.get("actual_cash_value", 0)
        if acv <= 0:
            return {"is_total_loss": False, "total_loss_reason": None}

        meta = claim.enrichment_data or {}
        state = meta.get("loss_state", "DEFAULT")
        threshold_pct = _TOTAL_LOSS_THRESHOLD_PCT.get(
            state, _TOTAL_LOSS_THRESHOLD_PCT["DEFAULT"]
        )
        threshold_amount = acv * threshold_pct

        is_total_loss = claim.claim_amount >= threshold_amount
        reason = None
        if is_total_loss:
            reason = (
                f"Repair estimate ${claim.claim_amount:,.2f} exceeds "
                f"{threshold_pct:.0%} of ACV ${acv:,.2f} "
                f"(threshold: ${threshold_amount:,.2f}, state: {state})"
            )

        return {
            "is_total_loss": is_total_loss,
            "total_loss_reason": reason,
            "total_loss_threshold_pct": threshold_pct,
            "total_loss_threshold_amount": round(threshold_amount, 2),
        }

    def _rental_authorization(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Authorize rental car coverage based on claim status."""
        acv = current.get("actual_cash_value", 0)
        is_total_loss = current.get("is_total_loss", False)

        # Rental authorization logic
        rental_days = 0
        daily_rate = 0.0

        if is_total_loss:
            rental_days = 14  # 14 days to find replacement vehicle
            daily_rate = 45.0
        elif claim.claim_amount > 5_000:
            rental_days = min(int(claim.claim_amount / 1_000), 30)
            daily_rate = 40.0
        elif claim.claim_amount > 1_000:
            rental_days = 5
            daily_rate = 35.0

        return {
            "rental_authorized": rental_days > 0,
            "rental_days_authorized": rental_days,
            "rental_daily_rate": daily_rate,
            "rental_total_authorized": round(rental_days * daily_rate, 2),
        }

    def _estimate_salvage_value(
        self, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Estimate salvage value if the vehicle is declared a total loss."""
        if not current.get("is_total_loss"):
            return {"salvage_value": None, "salvage_pct": None}

        acv = current.get("actual_cash_value", 0)
        tier = current.get("vehicle_tier", "midrange")

        # Salvage percentages by tier
        salvage_pcts = {
            "economy": 0.18, "midrange": 0.20, "premium": 0.22,
            "luxury": 0.25, "exotic": 0.30,
        }
        pct = salvage_pcts.get(tier, 0.20)

        # Reduce salvage if flood/fire damage
        history = current.get("vehicle_history", {})
        if history.get("flood_damage_reported"):
            pct *= 0.50

        salvage = round(acv * pct, 2)
        return {
            "salvage_value": salvage,
            "salvage_pct": round(pct, 4),
            "net_payout": round(acv - salvage, 2),
        }
