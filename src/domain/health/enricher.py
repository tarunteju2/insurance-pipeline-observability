"""
Health Claims Enricher.

Enriches health claims with external data lookups:
  - Provider NPI lookup and verification
  - Explanation of Benefits (EOB) generation
  - Coordination of Benefits (COB) determination
  - Network status determination (in/out-of-network)
  - Allowed amount calculation
  - Patient responsibility estimation
  - Place of service mapping
"""

import hashlib
import random
import structlog
from datetime import date
from typing import Any, Dict

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)

# Provider NPI database registry
_PROVIDER_DB = {
    "1234567890": {
        "name": "Dr. Jennifer Morrison",
        "specialty": "Internal Medicine",
        "facility": "Metro Health Medical Center",
        "network_status": "in_network",
        "tier": "preferred",
        "accepting_new_patients": True,
        "board_certified": True,
        "npi_status": "active",
    },
    "2345678901": {
        "name": "Dr. Robert Stevens",
        "specialty": "Orthopedic Surgery",
        "facility": "Regional Orthopedic Institute",
        "network_status": "in_network",
        "tier": "standard",
        "accepting_new_patients": True,
        "board_certified": True,
        "npi_status": "active",
    },
    "3456789012": {
        "name": "Dr. Lisa Chang",
        "specialty": "Cardiology",
        "facility": "Heart Care Specialists",
        "network_status": "out_of_network",
        "tier": "none",
        "accepting_new_patients": False,
        "board_certified": True,
        "npi_status": "active",
    },
    "4567890123": {
        "name": "Dr. Ahmad Patel",
        "specialty": "Radiology",
        "facility": "Advanced Imaging Center",
        "network_status": "in_network",
        "tier": "preferred",
        "accepting_new_patients": True,
        "board_certified": True,
        "npi_status": "active",
    },
}

# Fee schedule — allowed amounts by CPT category
_FEE_SCHEDULE = {
    "evaluation_management": {"in_network": 250.0, "out_of_network": 350.0},
    "surgery": {"in_network": 8_000.0, "out_of_network": 12_000.0},
    "radiology": {"in_network": 1_500.0, "out_of_network": 2_500.0},
    "pathology_lab": {"in_network": 400.0, "out_of_network": 600.0},
    "medicine": {"in_network": 2_000.0, "out_of_network": 3_000.0},
    "anesthesia": {"in_network": 3_000.0, "out_of_network": 4_500.0},
}

# Place of service codes (CMS standard)
_PLACE_OF_SERVICE = {
    "11": "Office",
    "21": "Inpatient Hospital",
    "22": "Outpatient Hospital",
    "23": "Emergency Room",
    "24": "Ambulatory Surgical Center",
    "31": "Skilled Nursing Facility",
    "41": "Ambulance — Land",
    "50": "Federally Qualified Health Center",
    "65": "End-Stage Renal Disease Facility",
    "81": "Independent Laboratory",
}

# Plan benefit structures
_BENEFIT_STRUCTURES = {
    "PPO_Standard": {
        "deductible_individual": 1_500.0,
        "deductible_family": 3_000.0,
        "coinsurance_in_network": 0.20,
        "coinsurance_out_of_network": 0.40,
        "copay_office_visit": 30.0,
        "copay_specialist": 50.0,
        "copay_emergency": 250.0,
        "out_of_pocket_max_individual": 7_500.0,
        "out_of_pocket_max_family": 15_000.0,
    },
    "HMO_Basic": {
        "deductible_individual": 2_500.0,
        "deductible_family": 5_000.0,
        "coinsurance_in_network": 0.25,
        "coinsurance_out_of_network": 0.50,
        "copay_office_visit": 25.0,
        "copay_specialist": 45.0,
        "copay_emergency": 300.0,
        "out_of_pocket_max_individual": 8_500.0,
        "out_of_pocket_max_family": 17_000.0,
    },
    "HDHP_HSA": {
        "deductible_individual": 3_000.0,
        "deductible_family": 6_000.0,
        "coinsurance_in_network": 0.20,
        "coinsurance_out_of_network": 0.40,
        "copay_office_visit": 0.0,  # No copay until deductible met
        "copay_specialist": 0.0,
        "copay_emergency": 0.0,
        "out_of_pocket_max_individual": 6_900.0,
        "out_of_pocket_max_family": 13_800.0,
    },
}


class HealthClaimEnricher:
    """
    Enriches health claims with provider data, benefit calculations,
    and Explanation of Benefits (EOB) generation.
    """

    def enrich(self, claim: InsuranceClaim) -> Dict[str, Any]:
        enrichment: Dict[str, Any] = {}

        # 1. Provider NPI lookup
        enrichment.update(self._lookup_provider(claim))

        # 2. Network status determination
        enrichment.update(self._determine_network_status(claim, enrichment))

        # 3. Allowed amount calculation
        enrichment.update(self._calculate_allowed_amount(claim, enrichment))

        # 4. Benefit calculation (patient responsibility)
        enrichment.update(self._calculate_patient_responsibility(claim, enrichment))

        # 5. Place of service mapping
        enrichment.update(self._map_place_of_service(claim))

        # 6. Generate EOB summary
        enrichment.update(self._generate_eob_summary(claim, enrichment))

        # 7. COB determination
        enrichment.update(self._determine_cob(claim))

        logger.debug(
            "Health claim enriched",
            claim_id=claim.claim_id,
            network_status=enrichment.get("network_status"),
            allowed_amount=enrichment.get("allowed_amount"),
            patient_responsibility=enrichment.get("patient_responsibility"),
        )

        return enrichment

    def _lookup_provider(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Look up provider details by NPI."""
        meta = claim.enrichment_data or {}
        npi = meta.get("provider_npi")

        if npi and str(npi) in _PROVIDER_DB:
            provider = _PROVIDER_DB[str(npi)]
            return {
                "provider_details": provider,
                "provider_verified": True,
                "provider_npi": npi,
            }

        # Generate deterministic provider data from provider_name
        seed = int(
            hashlib.md5((claim.provider_name or "unknown").encode()).hexdigest()[:8],
            16,
        )
        rng = random.Random(seed)

        specialties = [
            "Internal Medicine", "Family Medicine", "Orthopedic Surgery",
            "Cardiology", "Radiology", "Emergency Medicine",
            "General Surgery", "Neurology", "Oncology", "Dermatology",
        ]
        facilities = [
            "Community Medical Center", "Regional Health System",
            "University Hospital", "Specialty Clinic Associates",
            "Outpatient Surgery Center", "Urgent Care Network",
        ]

        network = rng.choice(["in_network", "in_network", "in_network", "out_of_network"])

        return {
            "provider_details": {
                "name": claim.provider_name or "Unknown Provider",
                "specialty": rng.choice(specialties),
                "facility": rng.choice(facilities),
                "network_status": network,
                "tier": "preferred" if network == "in_network" else "none",
                "accepting_new_patients": rng.random() > 0.2,
                "board_certified": rng.random() > 0.1,
                "npi_status": "active",
            },
            "provider_verified": False,
            "provider_npi": npi,
        }

    def _determine_network_status(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Determine in-network or out-of-network status."""
        provider = current.get("provider_details", {})
        status = provider.get("network_status", "unknown")

        return {
            "network_status": status,
            "network_tier": provider.get("tier", "none"),
            "balance_billing_protected": status == "in_network",
        }

    def _calculate_allowed_amount(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate the plan's allowed amount for the claim."""
        network = current.get("network_status", "out_of_network")
        billed_amount = claim.claim_amount

        # Determine CPT category for fee schedule lookup
        meta = claim.enrichment_data or {}
        cpt_codes = meta.get("cpt_codes", [])

        if cpt_codes:
            first_cpt = str(cpt_codes[0]).split("-")[0]
            category = self._categorize_cpt(first_cpt)
        else:
            category = "medicine"

        fee = _FEE_SCHEDULE.get(category, _FEE_SCHEDULE["medicine"])
        schedule_amount = fee.get(network, fee["out_of_network"])

        # Allowed amount is the lesser of billed and fee schedule
        allowed = min(billed_amount, schedule_amount * max(1, len(cpt_codes)))

        # Provider write-off (in-network contracted rate)
        write_off = max(0, billed_amount - allowed)

        return {
            "billed_amount": billed_amount,
            "allowed_amount": round(allowed, 2),
            "provider_write_off": round(write_off, 2),
            "fee_schedule_category": category,
        }

    def _calculate_patient_responsibility(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate patient out-of-pocket responsibility."""
        allowed = current.get("allowed_amount", claim.claim_amount)
        network = current.get("network_status", "out_of_network")

        # Use PPO_Standard as default plan
        plan = _BENEFIT_STRUCTURES["PPO_Standard"]

        # Policy deductible status
        seed = int(
            hashlib.md5(claim.policy_number.encode()).hexdigest()[:8], 16
        )
        rng = random.Random(seed)
        deductible_met = rng.random() > 0.4
        deductible_remaining = 0.0 if deductible_met else plan["deductible_individual"] * rng.uniform(0.2, 1.0)

        # Apply deductible
        deductible_applied = min(deductible_remaining, allowed)
        after_deductible = allowed - deductible_applied

        # Apply coinsurance
        coinsurance_key = f"coinsurance_{network.replace('-', '_')}"
        coinsurance_rate = plan.get(coinsurance_key, 0.20)
        coinsurance_amount = after_deductible * coinsurance_rate

        # Patient responsibility
        patient_responsibility = round(deductible_applied + coinsurance_amount, 2)

        # Cap at out-of-pocket maximum
        oop_max = plan["out_of_pocket_max_individual"]
        patient_responsibility = min(patient_responsibility, oop_max)

        # Plan payment
        plan_payment = round(allowed - patient_responsibility, 2)

        return {
            "deductible_met": deductible_met,
            "deductible_applied": round(deductible_applied, 2),
            "coinsurance_rate": coinsurance_rate,
            "coinsurance_amount": round(coinsurance_amount, 2),
            "patient_responsibility": patient_responsibility,
            "plan_payment": max(0, plan_payment),
            "out_of_pocket_max": oop_max,
            "plan_type": "PPO_Standard",
        }

    def _map_place_of_service(
        self, claim: InsuranceClaim
    ) -> Dict[str, Any]:
        """Map the place of service code to description."""
        meta = claim.enrichment_data or {}
        pos_code = meta.get("place_of_service_code", "11")
        pos_description = _PLACE_OF_SERVICE.get(
            str(pos_code), "Unknown Facility Type"
        )

        return {
            "place_of_service_code": pos_code,
            "place_of_service_description": pos_description,
        }

    def _generate_eob_summary(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate an Explanation of Benefits summary."""
        return {
            "eob_summary": {
                "claim_id": claim.claim_id,
                "service_date": claim.date_of_loss,
                "provider": current.get("provider_details", {}).get("name", "Unknown"),
                "diagnosis": claim.diagnosis_code,
                "billed_amount": current.get("billed_amount", claim.claim_amount),
                "allowed_amount": current.get("allowed_amount"),
                "provider_write_off": current.get("provider_write_off"),
                "deductible_applied": current.get("deductible_applied"),
                "coinsurance_amount": current.get("coinsurance_amount"),
                "plan_payment": current.get("plan_payment"),
                "patient_responsibility": current.get("patient_responsibility"),
                "network_status": current.get("network_status"),
                "explanation": self._generate_eob_explanation(current),
            },
        }

    def _generate_eob_explanation(self, current: Dict[str, Any]) -> str:
        """Generate human-readable EOB explanation text."""
        network = current.get("network_status", "unknown")
        allowed = current.get("allowed_amount", 0)
        billed = current.get("billed_amount", 0)
        write_off = current.get("provider_write_off", 0)

        parts = []
        if write_off > 0:
            parts.append(
                f"Your {network.replace('_', '-')} provider has a contracted rate. "
                f"${write_off:,.2f} was written off as a network discount."
            )
        parts.append(
            f"The allowed amount for this service is ${allowed:,.2f}."
        )
        deductible = current.get("deductible_applied", 0)
        if deductible > 0:
            parts.append(
                f"${deductible:,.2f} was applied to your annual deductible."
            )
        coinsurance = current.get("coinsurance_amount", 0)
        if coinsurance > 0:
            rate = current.get("coinsurance_rate", 0.20)
            parts.append(
                f"Your coinsurance ({rate:.0%}) is ${coinsurance:,.2f}."
            )

        return " ".join(parts)

    def _determine_cob(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Determine Coordination of Benefits status."""
        meta = claim.enrichment_data or {}
        has_other = meta.get("other_insurance_coverage", False)

        if not has_other:
            return {
                "cob_applicable": False,
                "cob_determination_complete": True,
                "other_insurance_coverage": False,
            }

        # Simulate COB determination
        seed = int(
            hashlib.md5(claim.claim_id.encode()).hexdigest()[:8], 16
        )
        rng = random.Random(seed)
        is_primary = rng.random() > 0.4

        return {
            "cob_applicable": True,
            "cob_determination_complete": True,
            "other_insurance_coverage": True,
            "cob_order": "primary" if is_primary else "secondary",
            "cob_other_carrier": {
                "carrier_name": "United Health Group",
                "policy_number": f"UHG-{rng.randint(100000, 999999)}",
                "coverage_type": "group",
            },
            "cob_payment_reduction": 0.0 if is_primary else claim.claim_amount * 0.60,
        }

    @staticmethod
    def _categorize_cpt(cpt_str: str) -> str:
        """Categorize a CPT code into a fee schedule category."""
        if not cpt_str.isdigit():
            return "medicine"
        cpt_num = int(cpt_str)
        for category, (low, high) in {
            "evaluation_management": (99201, 99499),
            "anesthesia": (100, 1999),
            "surgery": (10021, 69990),
            "radiology": (70010, 79999),
            "pathology_lab": (80047, 89398),
            "medicine": (90281, 99199),
        }.items():
            if low <= cpt_num <= high:
                return category
        return "medicine"
