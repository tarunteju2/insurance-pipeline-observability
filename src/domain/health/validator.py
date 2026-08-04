"""
Health Claims Validator.

Performs LOB-specific validation for medical/health claims:
  - ICD-10-CM diagnosis code validation (format + known code set)
  - CPT/HCPCS procedure code validation
  - Prior authorization verification
  - Network in/out-of-network determination
  - Medical necessity plausibility check
  - Coordination of Benefits (COB) flag detection
  - Duplicate service detection (same provider + same date + same CPT)
"""

import re
import structlog
from datetime import date, datetime
from typing import Any, Dict, List, Tuple

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)

# ICD-10-CM format: letter + 2 digits + optional decimal + up to 4 alphanumeric
_ICD10_PATTERN = re.compile(
    r'^[A-TV-Z]\d{2}(\.\d{1,4})?$', re.IGNORECASE
)

# CPT code format: 5 digits (sometimes with modifier suffix e.g. 99213-25)
_CPT_PATTERN = re.compile(r'^\d{5}(-\d{1,2})?$')

# HCPCS Level II: letter + 4 digits
_HCPCS_PATTERN = re.compile(r'^[A-V]\d{4}$', re.IGNORECASE)

# Common ICD-10 category ranges (simplified validation)
_ICD10_VALID_PREFIXES = {
    "A", "B",  # Infectious diseases
    "C", "D",  # Neoplasms, blood disorders
    "E",       # Endocrine/metabolic
    "F",       # Mental disorders
    "G",       # Nervous system
    "H",       # Eye / Ear
    "I",       # Circulatory
    "J",       # Respiratory
    "K",       # Digestive
    "L",       # Skin
    "M",       # Musculoskeletal
    "N",       # Genitourinary
    "O",       # Pregnancy
    "P",       # Perinatal
    "Q",       # Congenital
    "R",       # Symptoms/signs
    "S", "T",  # Injury/poisoning
    "V", "W", "X", "Y",  # External causes
    "Z",       # Factors influencing health
}

# Common CPT code ranges by category
_CPT_RANGES = {
    "evaluation_management": (99201, 99499),
    "anesthesia": (100, 1999),
    "surgery": (10021, 69990),
    "radiology": (70010, 79999),
    "pathology_lab": (80047, 89398),
    "medicine": (90281, 99199),
}

# Procedures requiring prior authorization
_PRIOR_AUTH_REQUIRED_CPT = {
    "27447",  # Total knee replacement
    "27130",  # Total hip replacement
    "43239",  # Upper GI endoscopy with biopsy
    "43644",  # Gastric bypass
    "49560",  # Hernia repair
    "63030",  # Laminectomy
    "29881",  # Knee arthroscopy
    "70553",  # MRI brain with/without contrast
    "72148",  # MRI lumbar spine
    "77401",  # Radiation treatment delivery
    "96413",  # Chemotherapy infusion
    "96365",  # IV infusion therapy
}

# Maximum reasonable charges by CPT category (simplified)
_MAX_CHARGE_BY_CATEGORY = {
    "evaluation_management": 500.0,
    "anesthesia": 5_000.0,
    "surgery": 150_000.0,
    "radiology": 8_000.0,
    "pathology_lab": 3_000.0,
    "medicine": 10_000.0,
}

# NPI (National Provider Identifier) format: 10 digits starting with 1 or 2
_NPI_PATTERN = re.compile(r'^[12]\d{9}$')


class HealthClaimValidator:
    """
    LOB-specific validation for health/medical insurance claims.

    Returns (is_valid, list_of_error_dicts).
    """

    def validate(
        self, claim: InsuranceClaim
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        errors: List[Dict[str, Any]] = []

        # --- ICD-10 diagnosis code validation ---
        errors.extend(self._validate_diagnosis_code(claim))

        # --- CPT/HCPCS procedure code validation ---
        errors.extend(self._validate_procedure_codes(claim))

        # --- Prior authorization check ---
        errors.extend(self._validate_prior_auth(claim))

        # --- Provider NPI validation ---
        errors.extend(self._validate_provider_npi(claim))

        # --- Network status check ---
        errors.extend(self._validate_network_status(claim))

        # --- Medical necessity plausibility ---
        errors.extend(self._validate_medical_necessity(claim))

        # --- Duplicate service detection ---
        errors.extend(self._validate_duplicate_services(claim))

        # --- Timely filing check ---
        errors.extend(self._validate_timely_filing(claim))

        # --- Coordination of Benefits ---
        errors.extend(self._validate_cob(claim))

        has_critical = any(e["severity"] == "critical" for e in errors)
        is_valid = not has_critical
        return is_valid, errors

    # ------------------------------------------------------------------
    # ICD-10 Diagnosis Code
    # ------------------------------------------------------------------

    def _validate_diagnosis_code(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        code = claim.diagnosis_code

        if not code:
            errors.append({
                "code": "HEALTH_DIAGNOSIS_CODE_MISSING",
                "field": "diagnosis_code",
                "message": "ICD-10 diagnosis code is required for health claims",
                "severity": "critical",
            })
            return errors

        # Format validation
        code_clean = code.strip().upper()
        if not _ICD10_PATTERN.match(code_clean):
            errors.append({
                "code": "HEALTH_DIAGNOSIS_CODE_INVALID_FORMAT",
                "field": "diagnosis_code",
                "message": (
                    f"Diagnosis code '{code}' does not match ICD-10-CM format "
                    f"(expected: letter + 2 digits + optional decimal + up to 4 chars)"
                ),
                "severity": "critical",
            })
            return errors

        # Category prefix validation
        prefix = code_clean[0]
        if prefix not in _ICD10_VALID_PREFIXES:
            errors.append({
                "code": "HEALTH_DIAGNOSIS_CODE_UNKNOWN_CATEGORY",
                "field": "diagnosis_code",
                "message": f"ICD-10 category prefix '{prefix}' is not a valid category",
                "severity": "high",
            })

        # Specificity check (ICD-10 generally requires 4+ characters for billing)
        base_code = code_clean.replace(".", "")
        if len(base_code) < 4:
            errors.append({
                "code": "HEALTH_DIAGNOSIS_CODE_INSUFFICIENT_SPECIFICITY",
                "field": "diagnosis_code",
                "message": (
                    f"Diagnosis code '{code}' lacks sufficient specificity for "
                    f"billing. ICD-10 codes typically require 4-7 characters."
                ),
                "severity": "medium",
            })

        return errors

    # ------------------------------------------------------------------
    # CPT/HCPCS Procedure Code
    # ------------------------------------------------------------------

    def _validate_procedure_codes(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        cpt_codes = meta.get("cpt_codes", [])

        if not cpt_codes:
            # Not all claims have procedure codes at submission time
            return errors

        for cpt in cpt_codes:
            cpt_str = str(cpt).strip()

            # Check CPT format
            if not _CPT_PATTERN.match(cpt_str) and not _HCPCS_PATTERN.match(cpt_str):
                errors.append({
                    "code": "HEALTH_PROCEDURE_CODE_INVALID",
                    "field": "enrichment_data.cpt_codes",
                    "message": f"Procedure code '{cpt_str}' is not a valid CPT or HCPCS-II format",
                    "severity": "high",
                })
                continue

            # Check for E/M code with surgery — potential unbundling
            base_cpt = cpt_str.split("-")[0]
            if base_cpt.isdigit():
                cpt_num = int(base_cpt)
                is_em = 99201 <= cpt_num <= 99499
                has_surgery = any(
                    10021 <= int(c.split("-")[0]) <= 69990
                    for c in cpt_codes
                    if c.split("-")[0].isdigit()
                    and c != cpt_str
                )
                if is_em and has_surgery:
                    # Check for modifier -25 (significant, separately identifiable E/M)
                    if "-25" not in cpt_str:
                        errors.append({
                            "code": "HEALTH_UNBUNDLING_POTENTIAL",
                            "field": "enrichment_data.cpt_codes",
                            "message": (
                                f"E/M code {cpt_str} billed with surgical procedure "
                                f"without modifier -25. Potential unbundling issue."
                            ),
                            "severity": "medium",
                        })

        return errors

    # ------------------------------------------------------------------
    # Prior Authorization
    # ------------------------------------------------------------------

    def _validate_prior_auth(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        cpt_codes = meta.get("cpt_codes", [])
        has_prior_auth = meta.get("prior_authorization_number") is not None

        for cpt in cpt_codes:
            base_cpt = str(cpt).split("-")[0]
            if base_cpt in _PRIOR_AUTH_REQUIRED_CPT and not has_prior_auth:
                errors.append({
                    "code": "HEALTH_PRIOR_AUTH_REQUIRED",
                    "field": "enrichment_data.prior_authorization_number",
                    "message": (
                        f"CPT code {base_cpt} requires prior authorization. "
                        f"No authorization number found on claim."
                    ),
                    "severity": "high",
                })
                break  # One error is sufficient

        # Verify prior auth is not expired
        auth_expiry = meta.get("prior_authorization_expiry")
        if auth_expiry:
            try:
                expiry_date = date.fromisoformat(auth_expiry)
                service_date = date.fromisoformat(claim.date_of_loss)
                if service_date > expiry_date:
                    errors.append({
                        "code": "HEALTH_PRIOR_AUTH_EXPIRED",
                        "field": "enrichment_data.prior_authorization_expiry",
                        "message": (
                            f"Prior authorization expired on {auth_expiry}, "
                            f"but service date is {claim.date_of_loss}"
                        ),
                        "severity": "high",
                    })
            except (ValueError, TypeError):
                pass

        return errors

    # ------------------------------------------------------------------
    # Provider NPI
    # ------------------------------------------------------------------

    def _validate_provider_npi(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        npi = meta.get("provider_npi")

        if not npi and claim.provider_name:
            errors.append({
                "code": "HEALTH_PROVIDER_NPI_MISSING",
                "field": "enrichment_data.provider_npi",
                "message": "Provider NPI (National Provider Identifier) is required for health claims",
                "severity": "medium",
            })
        elif npi and not _NPI_PATTERN.match(str(npi)):
            errors.append({
                "code": "HEALTH_PROVIDER_NPI_INVALID",
                "field": "enrichment_data.provider_npi",
                "message": f"NPI '{npi}' is not a valid 10-digit NPI format",
                "severity": "high",
            })

        return errors

    # ------------------------------------------------------------------
    # Network Status
    # ------------------------------------------------------------------

    def _validate_network_status(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        network_status = meta.get("network_status")

        if network_status == "out_of_network" and claim.claim_amount > 10_000:
            errors.append({
                "code": "HEALTH_OUT_OF_NETWORK_HIGH_VALUE",
                "field": "enrichment_data.network_status",
                "message": (
                    f"Out-of-network claim exceeding $10,000 requires "
                    f"medical director review before adjudication"
                ),
                "severity": "medium",
            })

        return errors

    # ------------------------------------------------------------------
    # Medical Necessity
    # ------------------------------------------------------------------

    def _validate_medical_necessity(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []

        # High-cost claims without diagnosis specificity
        if claim.claim_amount > 50_000 and claim.diagnosis_code:
            base_code = claim.diagnosis_code.replace(".", "")
            if len(base_code) <= 3:
                errors.append({
                    "code": "HEALTH_MEDICAL_NECESSITY_REVIEW",
                    "field": "diagnosis_code",
                    "message": (
                        f"High-value claim (${claim.claim_amount:,.2f}) with "
                        f"non-specific diagnosis code requires medical "
                        f"necessity review"
                    ),
                    "severity": "medium",
                })

        return errors

    # ------------------------------------------------------------------
    # Duplicate Service Detection
    # ------------------------------------------------------------------

    def _validate_duplicate_services(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        prior_services = meta.get("prior_services_same_date", [])

        if prior_services:
            cpt_codes = set(meta.get("cpt_codes", []))
            prior_cpts = set(str(s.get("cpt_code", "")) for s in prior_services)
            overlap = cpt_codes & prior_cpts
            if overlap:
                errors.append({
                    "code": "HEALTH_DUPLICATE_SERVICE_DETECTED",
                    "field": "enrichment_data.prior_services_same_date",
                    "message": (
                        f"Duplicate service codes detected for same date of "
                        f"service: {', '.join(overlap)}. May be duplicate billing."
                    ),
                    "severity": "high",
                })

        return errors

    # ------------------------------------------------------------------
    # Timely Filing
    # ------------------------------------------------------------------

    def _validate_timely_filing(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []

        try:
            service_date = date.fromisoformat(claim.date_of_loss)
            filed_date = date.fromisoformat(claim.date_filed)
            days_gap = (filed_date - service_date).days

            # Most health plans require filing within 90-365 days
            if days_gap > 365:
                errors.append({
                    "code": "HEALTH_TIMELY_FILING_EXCEEDED",
                    "field": "date_filed",
                    "message": (
                        f"Claim filed {days_gap} days after service date. "
                        f"Most plans require filing within 365 days."
                    ),
                    "severity": "critical",
                })
            elif days_gap > 180:
                errors.append({
                    "code": "HEALTH_TIMELY_FILING_WARNING",
                    "field": "date_filed",
                    "message": (
                        f"Claim filed {days_gap} days after service date. "
                        f"Approaching timely filing deadline."
                    ),
                    "severity": "medium",
                })
        except (ValueError, TypeError):
            pass

        return errors

    # ------------------------------------------------------------------
    # Coordination of Benefits
    # ------------------------------------------------------------------

    def _validate_cob(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}

        has_other_coverage = meta.get("other_insurance_coverage", False)
        cob_completed = meta.get("cob_determination_complete", False)

        if has_other_coverage and not cob_completed:
            errors.append({
                "code": "HEALTH_COB_INCOMPLETE",
                "field": "enrichment_data.cob_determination_complete",
                "message": (
                    "Claimant has other insurance coverage but Coordination "
                    "of Benefits determination has not been completed"
                ),
                "severity": "high",
            })

        return errors
