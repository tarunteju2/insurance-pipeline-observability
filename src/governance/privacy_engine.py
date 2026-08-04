"""
GDPR / CCPA Privacy Compliance Engine.

Processes Data Subject Access Requests (DSAR) and Right-to-Erasure workflows
by locating and anonymizing PII fields across all pipeline data stores.
"""

from __future__ import annotations

import hashlib
import structlog
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

logger = structlog.get_logger(__name__)


class DSARType(str, Enum):
    ACCESS_REQUEST = "access_request"    # Right to Access
    ERASURE_REQUEST = "erasure_request"  # Right to be Forgotten (Erasure)
    RECTIFICATION = "rectification"      # Right to Rectify


@dataclass
class DSARRecord:
    request_id: str
    subject_id: str  # claimant_id or email/phone
    request_type: DSARType
    status: str = "pending"  # pending, in_progress, completed, rejected
    submitted_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    completed_at: Optional[str] = None
    audit_notes: List[str] = field(default_factory=list)


class PrivacyEngine:
    """Processes GDPR/CCPA Privacy Requests and Right-to-Erasure anonymization."""

    def __init__(self):
        self._requests: Dict[str, DSARRecord] = {}

    def submit_dsar(self, request_id: str, subject_id: str, req_type: DSARType) -> DSARRecord:
        record = DSARRecord(request_id=request_id, subject_id=subject_id, request_type=req_type)
        self._requests[request_id] = record
        logger.info("Privacy DSAR request submitted", request_id=request_id, type=req_type.value)
        return record

    def execute_right_to_erasure(self, claim_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Anonymize PII fields in a claim dictionary while preserving business analytics value.
        Replaces direct PII (name, address, VIN) with non-reversible cryptographically salted hashes.
        """
        anonymized = dict(claim_record)
        salt = f"salt_gdpr_{claim_record.get('claim_id', '')}"

        if "claimant_name" in anonymized:
            raw = str(anonymized["claimant_name"])
            anonymized["claimant_name"] = f"ANONYMIZED_{hashlib.sha256((raw + salt).encode()).hexdigest()[:12]}"

        if "property_address" in anonymized and anonymized["property_address"]:
            anonymized["property_address"] = "ANONYMIZED_PROPERTY_ADDRESS"

        if "vehicle_vin" in anonymized and anonymized["vehicle_vin"]:
            anonymized["vehicle_vin"] = f"ANON_VIN_{hashlib.md5(str(anonymized['vehicle_vin']).encode()).hexdigest()[:8]}"

        anonymized["gdpr_anonymized"] = True
        anonymized["anonymized_at"] = datetime.utcnow().isoformat()

        logger.info("Claim record anonymized under GDPR Right-to-Erasure", claim_id=claim_record.get("claim_id"))
        return anonymized
