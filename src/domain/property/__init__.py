"""
Property Insurance Line of Business.

Handles homeowners, renters, and commercial property claims with
catastrophe event correlation, FEMA flood zone lookup, replacement
cost estimation, and geographic clustering.
"""

from src.domain.property.validator import PropertyClaimValidator
from src.domain.property.enricher import PropertyClaimEnricher
from src.domain.property.cat_modeler import CatastropheModeler
from src.domain.property.lob import PropertyLineOfBusiness


def register(registry) -> None:
    """Register the Property LOB with the central registry."""
    lob = PropertyLineOfBusiness()
    registry.register(lob)


__all__ = [
    "PropertyClaimValidator",
    "PropertyClaimEnricher",
    "CatastropheModeler",
    "PropertyLineOfBusiness",
    "register",
]
