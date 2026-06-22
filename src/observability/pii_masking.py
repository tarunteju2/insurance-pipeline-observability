"""
PII masking utilities for safe logging and external data exposure.

All PII and SENSITIVE fields must be masked before being written to logs,
metrics labels, or any external sink that is not the authoritative data store.

Rules:
  - claimant_name : keep first initial + last name initial (e.g. "J. D.")
  - vehicle_vin   : show last 4 chars only  (e.g. "***-4352")
  - policy_number : show type prefix + last 3 digits (e.g. "AUT-***456")
  - diagnosis_code: expose as-is (not directly identifying on its own)
  - property_address: show city/state only if parseable, else full mask
  - claim_amount  : safe to log (financial aggregate, not PII)
"""

import re
from typing import Optional


def mask_name(name: Optional[str]) -> str:
    """Return initials only: 'John Michael Doe' → 'J.M.D.'"""
    if not name:
        return "***"
    parts = name.strip().split()
    return ".".join(p[0].upper() for p in parts if p) + "."


def mask_vin(vin: Optional[str]) -> str:
    """Mask all but last 4 chars of a VIN."""
    if not vin:
        return "***"
    vin = vin.strip()
    return f"{'*' * (len(vin) - 4)}{vin[-4:]}" if len(vin) >= 4 else "***"


def mask_policy_number(policy_number: Optional[str]) -> str:
    """Show type prefix and last 3 digits: 'AUT-123456' → 'AUT-***456'"""
    if not policy_number:
        return "***"
    match = re.match(r'^([A-Z]{2,4})-(\d+)$', policy_number.strip().upper())
    if match:
        prefix, digits = match.group(1), match.group(2)
        return f"{prefix}-***{digits[-3:]}"
    return "***"


def mask_address(address: Optional[str]) -> str:
    """Return only the last comma-separated segment (typically city, state)."""
    if not address:
        return "***"
    parts = [p.strip() for p in address.split(",") if p.strip()]
    if len(parts) >= 2:
        return ", ".join(parts[-2:])
    return "***"


def mask_claim_for_logging(claim_dict: dict) -> dict:
    """
    Return a copy of a claim dict with all PII/SENSITIVE fields masked.
    Safe to pass to structlog or any logger.
    """
    masked = claim_dict.copy()
    if "claimant_name" in masked:
        masked["claimant_name"] = mask_name(masked.get("claimant_name"))
    if "vehicle_vin" in masked:
        masked["vehicle_vin"] = mask_vin(masked.get("vehicle_vin"))
    if "policy_number" in masked:
        masked["policy_number"] = mask_policy_number(masked.get("policy_number"))
    if "property_address" in masked:
        masked["property_address"] = mask_address(masked.get("property_address"))
    # Diagnosis codes are medical but not directly identifying on their own — keep them
    return masked
