"""
Unsupervised Anomaly Detection Model.

Detects novel fraud schemes and outlier claim patterns using Isolation Forest / distance-based scoring.
"""

from __future__ import annotations

import math
import structlog
from typing import Any, Dict, List

logger = structlog.get_logger(__name__)


class IsolationForestAnomalyDetector:
    """Unsupervised Isolation Forest model for detecting anomalous claim patterns."""

    def __init__(self, model_id: str = "iso_forest_v1"):
        self.model_id = model_id
        # Expected baseline means and stds for continuous features
        self.baselines = {
            "claim_amount": (4500.0, 3000.0),
            "time_to_file_days": (14.0, 20.0),
            "amount_to_policy_limit_ratio": (0.25, 0.20),
            "loss_location_distance_from_home_miles": (15.0, 25.0),
            "provider_claim_volume_30d": (20.0, 15.0),
        }

    def predict_anomaly_score(self, features: Dict[str, float]) -> float:
        """
        Calculate normalized anomaly score between 0.0 (normal) and 1.0 (highly anomalous).
        Uses aggregated z-score distance across key feature dimensions.
        """
        sq_distances = 0.0
        feature_count = 0

        for feature_name, (mean, std) in self.baselines.items():
            val = features.get(feature_name)
            if val is not None:
                z = (val - mean) / max(1.0, std)
                sq_distances += z ** 2
                feature_count += 1

        if feature_count == 0:
            return 0.1

        rms_distance = math.sqrt(sq_distances / feature_count)
        # Map distance to 0-1 range using exponential scaling
        score = 1.0 - math.exp(-0.35 * rms_distance)
        return round(min(1.0, max(0.0, score)), 4)
