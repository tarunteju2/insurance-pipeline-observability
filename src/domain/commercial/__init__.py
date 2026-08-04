"""
Commercial Insurance Line of Business.

Handles general liability (GL), professional liability (PL),
commercial property, and umbrella/excess claims.
"""

from src.domain.commercial.validator import CommercialClaimValidator
from src.domain.commercial.enricher import CommercialClaimEnricher
from src.domain.commercial.lob import CommercialLineOfBusiness


def register(registry) -> None:
    """Register the Commercial LOB with the central registry."""
    lob = CommercialLineOfBusiness()
    registry.register(lob)


__all__ = [
    "CommercialClaimValidator",
    "CommercialClaimEnricher",
    "CommercialLineOfBusiness",
    "register",
]
