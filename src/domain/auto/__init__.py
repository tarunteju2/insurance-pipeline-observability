"""
Automobile Insurance Line of Business.

Handles personal and commercial auto claims with NHTSA VIN decoding,
salvage title detection, total-loss threshold calculations, and
auto-adjuster assignment by coverage zone.
"""

from src.domain.auto.validator import AutoClaimValidator
from src.domain.auto.enricher import AutoClaimEnricher
from src.domain.auto.adjuster import AutoAdjusterAssignment
from src.domain.auto.lob import AutoLineOfBusiness


def register(registry) -> None:
    """Register the Auto LOB with the central registry."""
    lob = AutoLineOfBusiness()
    registry.register(lob)


__all__ = [
    "AutoClaimValidator",
    "AutoClaimEnricher",
    "AutoAdjusterAssignment",
    "AutoLineOfBusiness",
    "register",
]
