"""
Column-Level Role-Based Access Control (RBAC) & Data Masking Engine.

Enforces role-based permissions and applies dynamic masking strategies (hash, redaction, partial)
for PII and sensitive fields based on user roles.
"""

from __future__ import annotations

import hashlib
import structlog
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from src.models.claims import DataClass

logger = structlog.get_logger(__name__)


class UserRole(str, Enum):
    CLAIMS_ADJUSTER = "claims_adjuster"
    FRAUD_ANALYST = "fraud_analyst"
    SIU_INVESTIGATOR = "siu_investigator"
    ACTUARIAL_ANALYST = "actuarial_analyst"
    COMPLIANCE_OFFICER = "compliance_officer"
    DATA_ENGINEER = "data_engineer"
    PLATFORM_ADMIN = "platform_admin"


# Role permission matrix for viewing unmasked DataClasses
_ALLOWED_UNMASKED_CLASSES: Dict[UserRole, Set[DataClass]] = {
    UserRole.CLAIMS_ADJUSTER: {DataClass.PUBLIC, DataClass.INTERNAL, DataClass.SENSITIVE, DataClass.PII},
    UserRole.SIU_INVESTIGATOR: {DataClass.PUBLIC, DataClass.INTERNAL, DataClass.SENSITIVE, DataClass.PII},
    UserRole.COMPLIANCE_OFFICER: {DataClass.PUBLIC, DataClass.INTERNAL, DataClass.SENSITIVE, DataClass.PII},
    UserRole.FRAUD_ANALYST: {DataClass.PUBLIC, DataClass.INTERNAL, DataClass.SENSITIVE},  # PII masked
    UserRole.ACTUARIAL_ANALYST: {DataClass.PUBLIC, DataClass.INTERNAL, DataClass.SENSITIVE}, # PII masked
    UserRole.DATA_ENGINEER: {DataClass.PUBLIC, DataClass.INTERNAL},                         # PII & Sensitive masked
    UserRole.PLATFORM_ADMIN: {DataClass.PUBLIC, DataClass.INTERNAL, DataClass.SENSITIVE, DataClass.PII},
}


class AccessControlEngine:
    """RBAC and dynamic column-level data masking engine."""

    def filter_and_mask_record(
        self,
        record: Dict[str, Any],
        role: UserRole,
        field_classifications: Dict[str, DataClass],
    ) -> Dict[str, Any]:
        """Apply dynamic masking rules to a data record based on user role."""
        allowed_classes = _ALLOWED_UNMASKED_CLASSES.get(role, {DataClass.PUBLIC})
        masked_record = dict(record)

        for field_name, value in record.items():
            classification = field_classifications.get(field_name, DataClass.PUBLIC)

            if classification not in allowed_classes and value is not None:
                masked_record[field_name] = self.mask_value(str(value), classification)

        return masked_record

    @staticmethod
    def mask_value(val: str, classification: DataClass) -> str:
        """Apply masking transformation (hash, partial mask, or REDACTED)."""
        if classification == DataClass.PII:
            if len(val) > 4:
                return val[0] + "*" * (len(val) - 4) + val[-3:]
            return "***REDACTED***"
        elif classification == DataClass.SENSITIVE:
            return f"[MASKED_HASH_{hashlib.md5(val.encode()).hexdigest()[:8]}]"
        return "[REDACTED]"
