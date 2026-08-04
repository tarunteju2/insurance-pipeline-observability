"""
Structured Log Aggregator & Loki Exemplar Correlator.

Formats JSON logs with correlation IDs, trace IDs, and Loki-compatible labels.
"""

from __future__ import annotations

import json
import structlog
from datetime import datetime
from typing import Any, Dict, Optional

logger = structlog.get_logger(__name__)


class StructuredLogAggregator:
    """Formatter and log shipper preparing trace-correlated JSON logs for Loki."""

    @staticmethod
    def format_log_entry(
        level: str,
        message: str,
        correlation_id: str,
        trace_id: Optional[str] = None,
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": level.upper(),
            "message": message,
            "correlation_id": correlation_id,
            "trace_id": trace_id or "0000000000000000",
            "service": "insurance-claims-observability",
            "environment": "production",
        }
        if extra_fields:
            entry.update(extra_fields)
        return entry
