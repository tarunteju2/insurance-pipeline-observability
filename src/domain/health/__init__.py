"""
Health Insurance Line of Business.

Handles medical/health claims with ICD-10/CPT validation,
provider NPI lookup, utilization review, and coordination of benefits.
"""

from src.domain.health.validator import HealthClaimValidator
from src.domain.health.enricher import HealthClaimEnricher
from src.domain.health.medical_review import MedicalUtilizationReview
from src.domain.health.lob import HealthLineOfBusiness


def register(registry) -> None:
    """Register the Health LOB with the central registry."""
    lob = HealthLineOfBusiness()
    registry.register(lob)


__all__ = [
    "HealthClaimValidator",
    "HealthClaimEnricher",
    "MedicalUtilizationReview",
    "HealthLineOfBusiness",
    "register",
]
