"""
Data Retention Policy Engine.

Manages data retention lifecycle (Hot -> Warm -> Cold -> Archive -> Purge),
legal holds, and regulatory compliance storage rules per line of business.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

logger = structlog.get_logger(__name__)


class RetentionTier(str, Enum):
    HOT = "hot"          # Active processing DB (0-90 days)
    WARM = "warm"        # Analytics warehouse (90-365 days)
    COLD = "cold"        # Iceberg/S3 standard (1-7 years)
    ARCHIVE = "archive"  # Glacier/Deep Archive (7-10+ years)
    PURGED = "purged"    # Permanently deleted


@dataclass
class RetentionPolicy:
    lob_code: str
    hot_days: int = 90
    warm_days: int = 365
    cold_days: int = 2555  # 7 years
    archive_days: int = 3650  # 10 years
    auto_purge_enabled: bool = True


class RetentionEngine:
    """Enforces storage tier migration policies and legal holds on claim data."""

    def __init__(self):
        self._policies: Dict[str, RetentionPolicy] = {
            "AUTO": RetentionPolicy("AUTO", hot_days=90, cold_days=2555),
            "HEALTH": RetentionPolicy("HEALTH", hot_days=180, cold_days=3650),
            "PROPERTY": RetentionPolicy("PROPERTY", hot_days=90, cold_days=2555),
            "COMMERCIAL": RetentionPolicy("COMMERCIAL", hot_days=180, cold_days=3650),
            "CYBER": RetentionPolicy("CYBER", hot_days=180, cold_days=3650),
        }
        self._legal_holds: Dict[str, str] = {}  # claim_id -> legal_reason

    def place_legal_hold(self, claim_id: str, reason: str) -> None:
        """Place legal hold on a claim aggregate to suspend purge/archive."""
        self._legal_holds[claim_id] = reason
        logger.warning("Legal hold placed on claim", claim_id=claim_id, reason=reason)

    def remove_legal_hold(self, claim_id: str) -> None:
        """Remove legal hold from a claim."""
        self._legal_holds.pop(claim_id, None)
        logger.info("Legal hold removed", claim_id=claim_id)

    def evaluate_retention_tier(self, claim_id: str, lob_code: str, date_created: str) -> RetentionTier:
        """Determine target retention tier for a claim aggregate."""
        if claim_id in self._legal_holds:
            # Legal hold keeps data in accessible cold tier, never purges
            return RetentionTier.COLD

        policy = self._policies.get(lob_code, RetentionPolicy(lob_code))
        try:
            created_d = date.fromisoformat(date_created[:10])
            age_days = (date.today() - created_d).days
        except Exception:
            return RetentionTier.HOT

        if age_days <= policy.hot_days:
            return RetentionTier.HOT
        elif age_days <= policy.warm_days:
            return RetentionTier.WARM
        elif age_days <= policy.cold_days:
            return RetentionTier.COLD
        elif age_days <= policy.archive_days:
            return RetentionTier.ARCHIVE
        else:
            return RetentionTier.PURGED if policy.auto_purge_enabled else RetentionTier.ARCHIVE
