"""
Medical Utilization Review Engine.

Performs clinical utilization review for health claims:
  - Medical necessity scoring
  - Length-of-stay appropriateness (inpatient)
  - Level-of-care determination
  - Peer review routing
  - Clinical criteria matching (InterQual/Milliman-style)
  - Pre-certification tracking
"""

import hashlib
import random
import structlog
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)


@dataclass
class ClinicalCriteria:
    """Represents a clinical appropriateness criterion."""
    criterion_id: str
    description: str
    diagnosis_codes: List[str]      # ICD-10 codes this applies to
    required_documentation: List[str]
    max_los_days: Optional[int] = None  # Max length of stay (inpatient)
    level_of_care: str = "outpatient"
    requires_peer_review_above: Optional[float] = None  # Amount threshold


# Clinical criteria database (simplified InterQual/Milliman-style)
_CLINICAL_CRITERIA: List[ClinicalCriteria] = [
    ClinicalCriteria(
        criterion_id="CC-ORTHO-001",
        description="Total Joint Replacement — Knee/Hip",
        diagnosis_codes=["M17", "M16", "M87"],
        required_documentation=[
            "conservative_treatment_failure",
            "imaging_within_90_days",
            "functional_assessment",
            "bmi_documentation",
        ],
        max_los_days=3,
        level_of_care="inpatient",
        requires_peer_review_above=75_000.0,
    ),
    ClinicalCriteria(
        criterion_id="CC-CARDIO-001",
        description="Cardiac Catheterization / Stent Placement",
        diagnosis_codes=["I25", "I21", "I20"],
        required_documentation=[
            "stress_test_results",
            "ecg_findings",
            "cardiac_enzyme_levels",
            "prior_medical_management",
        ],
        max_los_days=2,
        level_of_care="inpatient",
        requires_peer_review_above=50_000.0,
    ),
    ClinicalCriteria(
        criterion_id="CC-SPINE-001",
        description="Lumbar Spine Surgery / Laminectomy",
        diagnosis_codes=["M51", "M54", "G55"],
        required_documentation=[
            "mri_lumbar_within_6_months",
            "failed_conservative_treatment_6_weeks",
            "neurological_exam",
            "pain_management_documentation",
        ],
        max_los_days=2,
        level_of_care="inpatient",
        requires_peer_review_above=60_000.0,
    ),
    ClinicalCriteria(
        criterion_id="CC-ONCO-001",
        description="Chemotherapy / Immunotherapy Treatment",
        diagnosis_codes=["C00", "C34", "C50", "C61", "C18"],
        required_documentation=[
            "pathology_report",
            "staging_documentation",
            "treatment_protocol",
            "lab_values_within_7_days",
        ],
        max_los_days=None,  # Typically outpatient
        level_of_care="outpatient",
        requires_peer_review_above=100_000.0,
    ),
    ClinicalCriteria(
        criterion_id="CC-ER-001",
        description="Emergency Room Visit — Non-Emergent Screening",
        diagnosis_codes=["R10", "R51", "R05", "J06", "J02"],
        required_documentation=[
            "presenting_symptoms",
            "vitals_on_arrival",
        ],
        max_los_days=None,
        level_of_care="emergency",
        requires_peer_review_above=None,
    ),
    ClinicalCriteria(
        criterion_id="CC-MH-001",
        description="Inpatient Behavioral Health / Psychiatric",
        diagnosis_codes=["F32", "F33", "F31", "F20", "F41"],
        required_documentation=[
            "psychiatric_evaluation",
            "suicide_risk_assessment",
            "treatment_plan",
            "prior_outpatient_treatment_history",
        ],
        max_los_days=7,
        level_of_care="inpatient",
        requires_peer_review_above=25_000.0,
    ),
    ClinicalCriteria(
        criterion_id="CC-IMG-001",
        description="Advanced Imaging — MRI/CT/PET",
        diagnosis_codes=["M54", "G43", "R51", "S72", "C34"],
        required_documentation=[
            "clinical_indication",
            "prior_imaging_results",
            "failed_conservative_treatment",
        ],
        max_los_days=None,
        level_of_care="outpatient",
        requires_peer_review_above=5_000.0,
    ),
]


class MedicalUtilizationReview:
    """
    Performs utilization review for health claims using clinical criteria
    matching, medical necessity scoring, and peer review routing.
    """

    def __init__(self):
        self._criteria = _CLINICAL_CRITERIA

    def review(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """
        Perform utilization review on a health claim.

        Returns a review result dict with:
          - medical_necessity_score (0-1)
          - level_of_care_appropriate (bool)
          - peer_review_required (bool)
          - matching_criteria (list)
          - documentation_gaps (list)
          - recommended_action (str)
        """
        matching = self._match_clinical_criteria(claim)
        necessity_score = self._score_medical_necessity(claim, matching)
        doc_gaps = self._identify_documentation_gaps(claim, matching)
        level_appropriate = self._assess_level_of_care(claim, matching)
        peer_review = self._needs_peer_review(claim, matching)
        los_check = self._check_length_of_stay(claim, matching)
        action = self._recommend_action(
            necessity_score, doc_gaps, level_appropriate, peer_review
        )

        result = {
            "utilization_review": {
                "medical_necessity_score": round(necessity_score, 3),
                "level_of_care_appropriate": level_appropriate,
                "peer_review_required": peer_review,
                "matching_criteria": [
                    {"id": c.criterion_id, "description": c.description}
                    for c in matching
                ],
                "documentation_gaps": doc_gaps,
                "length_of_stay_check": los_check,
                "recommended_action": action,
                "review_status": "completed",
                "reviewed_by": "automated_ur_engine",
            },
        }

        logger.debug(
            "Utilization review completed",
            claim_id=claim.claim_id,
            necessity_score=round(necessity_score, 3),
            action=action,
            peer_review=peer_review,
        )

        return result

    def _match_clinical_criteria(
        self, claim: InsuranceClaim
    ) -> List[ClinicalCriteria]:
        """Find clinical criteria matching the claim's diagnosis code."""
        dx = claim.diagnosis_code or ""
        dx_base = dx.replace(".", "").upper()[:3]

        matched = []
        for criteria in self._criteria:
            for dx_code in criteria.diagnosis_codes:
                if dx_base.startswith(dx_code.replace(".", "").upper()[:3]):
                    matched.append(criteria)
                    break

        return matched

    def _score_medical_necessity(
        self, claim: InsuranceClaim, matching: List[ClinicalCriteria]
    ) -> float:
        """
        Score medical necessity from 0 (not medically necessary) to 1 (clearly necessary).

        Factors:
          - Matching clinical criteria (+0.3)
          - Diagnosis specificity (+0.2)
          - Amount reasonableness (+0.2)
          - Documentation completeness (+0.3)
        """
        score = 0.0

        # Criteria match
        if matching:
            score += 0.30

        # Diagnosis specificity
        dx = (claim.diagnosis_code or "").replace(".", "")
        if len(dx) >= 5:
            score += 0.20
        elif len(dx) >= 4:
            score += 0.15
        elif len(dx) >= 3:
            score += 0.10

        # Amount reasonableness (based on diagnosis-claim amount correlation)
        if matching:
            # Use first matching criterion's peer review threshold as proxy
            threshold = matching[0].requires_peer_review_above
            if threshold and claim.claim_amount <= threshold:
                score += 0.20
            elif threshold:
                score += 0.10
        else:
            if claim.claim_amount <= 5_000:
                score += 0.20
            elif claim.claim_amount <= 25_000:
                score += 0.15

        # Supporting documentation presence
        meta = claim.enrichment_data or {}
        doc_count = len(meta.get("supporting_documents", []))
        if doc_count >= 3:
            score += 0.30
        elif doc_count >= 1:
            score += 0.15

        return min(score, 1.0)

    def _identify_documentation_gaps(
        self, claim: InsuranceClaim, matching: List[ClinicalCriteria]
    ) -> List[Dict[str, str]]:
        """Identify missing documentation per clinical criteria."""
        gaps = []
        meta = claim.enrichment_data or {}
        provided_docs = set(meta.get("supporting_documents", []))

        for criteria in matching:
            for req in criteria.required_documentation:
                if req not in provided_docs:
                    gaps.append({
                        "criterion_id": criteria.criterion_id,
                        "required_document": req,
                        "description": req.replace("_", " ").title(),
                    })

        return gaps

    def _assess_level_of_care(
        self, claim: InsuranceClaim, matching: List[ClinicalCriteria]
    ) -> bool:
        """Assess whether the level of care is appropriate for the diagnosis."""
        if not matching:
            return True  # No criteria to check against

        meta = claim.enrichment_data or {}
        claimed_pos = meta.get("place_of_service_code", "11")

        # Map POS to level of care
        pos_to_level = {
            "11": "outpatient",
            "21": "inpatient",
            "22": "outpatient",
            "23": "emergency",
            "24": "outpatient",
        }
        claimed_level = pos_to_level.get(str(claimed_pos), "outpatient")

        for criteria in matching:
            if criteria.level_of_care == claimed_level:
                return True

        # If none match, flag as potentially inappropriate
        return False

    def _needs_peer_review(
        self, claim: InsuranceClaim, matching: List[ClinicalCriteria]
    ) -> bool:
        """Determine if peer review is required."""
        for criteria in matching:
            threshold = criteria.requires_peer_review_above
            if threshold and claim.claim_amount > threshold:
                return True

        # Default: peer review for claims > $100K
        if claim.claim_amount > 100_000:
            return True

        return False

    def _check_length_of_stay(
        self, claim: InsuranceClaim, matching: List[ClinicalCriteria]
    ) -> Optional[Dict[str, Any]]:
        """Check length of stay against clinical criteria."""
        meta = claim.enrichment_data or {}
        admission_date = meta.get("admission_date")
        discharge_date = meta.get("discharge_date")

        if not admission_date or not discharge_date:
            return None

        try:
            admit = date.fromisoformat(admission_date)
            discharge = date.fromisoformat(discharge_date)
            actual_los = (discharge - admit).days
        except (ValueError, TypeError):
            return None

        for criteria in matching:
            if criteria.max_los_days is not None:
                is_appropriate = actual_los <= criteria.max_los_days
                return {
                    "actual_los_days": actual_los,
                    "max_los_days": criteria.max_los_days,
                    "appropriate": is_appropriate,
                    "criterion_id": criteria.criterion_id,
                    "excess_days": max(0, actual_los - criteria.max_los_days),
                }

        return {
            "actual_los_days": actual_los,
            "max_los_days": None,
            "appropriate": True,
            "criterion_id": None,
            "excess_days": 0,
        }

    def _recommend_action(
        self,
        necessity_score: float,
        doc_gaps: List[Dict[str, str]],
        level_appropriate: bool,
        peer_review: bool,
    ) -> str:
        """Recommend an action based on the review findings."""
        if necessity_score >= 0.75 and not doc_gaps and level_appropriate:
            return "approve"

        if necessity_score < 0.30:
            return "deny"

        if peer_review:
            return "refer_to_medical_director"

        if doc_gaps:
            return "request_additional_documentation"

        if not level_appropriate:
            return "refer_to_level_of_care_review"

        if necessity_score >= 0.50:
            return "approve_with_conditions"

        return "pend_for_review"
