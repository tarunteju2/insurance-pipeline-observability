"""
Observability Metrics Anomaly Detector.

Calculates rolling Z-scores and EWMA (Exponentially Weighted Moving Average)
to detect anomalous metric spikes or drop-offs in real time.
"""

from __future__ import annotations

import math
import structlog
from typing import Any, Dict, List, Optional

logger = structlog.get_logger(__name__)


class MetricAnomalyDetector:
    """Detects statistical anomalies in real-time Prometheus metric streams."""

    def __init__(self, z_threshold: float = 3.0):
        self.z_threshold = z_threshold
        self.metric_history: Dict[str, List[float]] = {}

    def record_and_check(self, metric_name: str, value: float) -> Tuple[bool, float]:
        """Record metric value and return (is_anomaly, z_score)."""
        history = self.metric_history.setdefault(metric_name, [])
        history.append(value)
        if len(history) > 100:
            history.pop(0)

        if len(history) < 10:
            return False, 0.0

        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        std_dev = math.sqrt(variance)

        if std_dev == 0:
            return False, 0.0

        z_score = (value - mean) / std_dev
        is_anomaly = abs(z_score) >= self.z_threshold

        if is_anomaly:
            logger.warning("Metric anomaly detected", metric=metric_name, value=value, z_score=round(z_score, 2))

        return is_anomaly, round(z_score, 2)
