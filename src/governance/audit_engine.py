"""
Tamper-Evident Audit Trail Engine.

Maintains a cryptographically linked hash-chain audit log tracking data access,
mutations, PII exports, and administrative actions for SOX/DOI compliance.
"""

from __future__ import annotations

import hashlib
import json
import structlog
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class AuditEntry:
    entry_id: int
    timestamp: str
    actor: str
    role: str
    action: str  # READ, WRITE, EXPORT, MASK, ANONYMIZE, PURGE
    resource_id: str
    details: Dict[str, Any]
    prev_hash: str
    current_hash: str

    @classmethod
    def create(
        cls,
        entry_id: int,
        actor: str,
        role: str,
        action: str,
        resource_id: str,
        details: Dict[str, Any],
        prev_hash: str = "GENESIS_HASH",
    ) -> AuditEntry:
        timestamp = datetime.utcnow().isoformat()
        raw_payload = f"{entry_id}|{timestamp}|{actor}|{role}|{action}|{resource_id}|{json.dumps(details, sort_keys=True)}|{prev_hash}"
        curr_hash = hashlib.sha256(raw_payload.encode()).hexdigest()

        return cls(
            entry_id=entry_id,
            timestamp=timestamp,
            actor=actor,
            role=role,
            action=action,
            resource_id=resource_id,
            details=details,
            prev_hash=prev_hash,
            current_hash=curr_hash,
        )


class AuditEngine:
    """Tamper-evident audit log with hash-chain verification."""

    _instance: Optional[AuditEngine] = None

    def __init__(self):
        self._chain: List[AuditEntry] = []

    @classmethod
    def instance(cls) -> AuditEngine:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def log_event(self, actor: str, role: str, action: str, resource_id: str, details: Dict[str, Any]) -> AuditEntry:
        """Log a new audit event and append to cryptographic hash chain."""
        entry_id = len(self._chain) + 1
        prev_hash = self._chain[-1].current_hash if self._chain else "GENESIS_HASH"

        entry = AuditEntry.create(
            entry_id=entry_id,
            actor=actor,
            role=role,
            action=action,
            resource_id=resource_id,
            details=details,
            prev_hash=prev_hash,
        )
        self._chain.append(entry)
        logger.debug("Audit event logged", action=action, resource_id=resource_id, hash=entry.current_hash[:8])
        return entry

    def verify_integrity(self) -> Tuple[bool, Optional[int]]:
        """
        Verify cryptographic integrity of the entire audit hash chain.
        Returns (is_valid, tampered_entry_id).
        """
        prev_hash = "GENESIS_HASH"
        for entry in self._chain:
            if entry.prev_hash != prev_hash:
                logger.error("Audit chain broken! Link mismatch", entry_id=entry.entry_id)
                return False, entry.entry_id

            raw_payload = f"{entry.entry_id}|{entry.timestamp}|{entry.actor}|{entry.role}|{entry.action}|{entry.resource_id}|{json.dumps(entry.details, sort_keys=True)}|{entry.prev_hash}"
            expected_hash = hashlib.sha256(raw_payload.encode()).hexdigest()

            if entry.current_hash != expected_hash:
                logger.error("Audit entry content tampered!", entry_id=entry.entry_id)
                return False, entry.entry_id

            prev_hash = entry.current_hash

        return True, None

    def get_logs(self, resource_id: Optional[str] = None) -> List[AuditEntry]:
        if not resource_id:
            return list(self._chain)
        return [e for e in self._chain if e.resource_id == resource_id]
