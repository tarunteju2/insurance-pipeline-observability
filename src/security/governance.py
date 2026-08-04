"""
Role-Based Access Control (RBAC) & Dynamic Column-Level Governance Engine
Applies column masking, PII obfuscation, and field redaction based on caller security role.
"""

from enum import Enum
from typing import Any, Dict, List, Optional
from src.observability.pii_masking import mask_name, mask_policy_number, mask_vin


class SecurityRole(str, Enum):
    EXECUTIVE = "executive"
    ADJUSTER = "adjuster"
    AUDITOR = "auditor"
    PUBLIC = "public"


class DataGovernanceEngine:
    """
    Applies dynamic fine-grained column masking and security policies per SecurityRole.
    """

    def apply_column_masking(self, claim_data: Dict[str, Any], role: SecurityRole) -> Dict[str, Any]:
        """
        Returns a role-masked copy of the claim dataset according to governance policies.
        """
        masked = dict(claim_data)

        if role == SecurityRole.EXECUTIVE:
            # Executive: Full financial numbers, masked PII
            if "claimant_name" in masked:
                masked["claimant_name"] = mask_name(masked["claimant_name"])
            if "vehicle_vin" in masked and masked["vehicle_vin"]:
                masked["vehicle_vin"] = mask_vin(masked["vehicle_vin"])

        elif role == SecurityRole.ADJUSTER:
            # Adjuster: Needs policy and claimant name, but vehicle VIN masked
            if "vehicle_vin" in masked and masked["vehicle_vin"]:
                masked["vehicle_vin"] = mask_vin(masked["vehicle_vin"])

        elif role == SecurityRole.AUDITOR:
            # Auditor: Full audit log visibility, masked PII
            if "claimant_name" in masked:
                masked["claimant_name"] = mask_name(masked["claimant_name"])
            if "policy_number" in masked:
                masked["policy_number"] = mask_policy_number(masked["policy_number"])
            if "vehicle_vin" in masked and masked["vehicle_vin"]:
                masked["vehicle_vin"] = mask_vin(masked["vehicle_vin"])

        elif role == SecurityRole.PUBLIC:
            # Public API: Heavy redaction
            if "claimant_name" in masked:
                masked["claimant_name"] = mask_name(masked["claimant_name"])
            if "policy_number" in masked:
                masked["policy_number"] = mask_policy_number(masked["policy_number"])
            if "vehicle_vin" in masked and masked["vehicle_vin"]:
                masked["vehicle_vin"] = "REDACTED"
            if "property_address" in masked and masked["property_address"]:
                masked["property_address"] = "REDACTED"
            # Hide internal processing details
            masked.pop("processing_metadata", None)
            masked.pop("audit_trail", None)

        return masked

    def apply_batch_masking(self, claims: List[Dict[str, Any]], role: SecurityRole) -> List[Dict[str, Any]]:
        """
        Applies dynamic column masking over a batch of claim dictionaries.
        """
        return [self.apply_column_masking(c, role) for c in claims]
