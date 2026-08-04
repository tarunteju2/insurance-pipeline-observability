"""
Cyber Insurance Line of Business.

Handles cyber liability, data breach, ransomware, social engineering,
and business interruption claims with NIST framework mapping and
threat intelligence correlation.
"""

from src.domain.cyber.validator import CyberClaimValidator
from src.domain.cyber.enricher import CyberClaimEnricher
from src.domain.cyber.lob import CyberLineOfBusiness


def register(registry) -> None:
    """Register the Cyber LOB with the central registry."""
    lob = CyberLineOfBusiness()
    registry.register(lob)


__all__ = [
    "CyberClaimValidator",
    "CyberClaimEnricher",
    "CyberLineOfBusiness",
    "register",
]
