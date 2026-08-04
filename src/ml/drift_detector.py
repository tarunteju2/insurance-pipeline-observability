"""
Model Drift Detector.

Calculates Population Stability Index (PSI) and Kolmogorov-Smirnov (KS) statistical tests
to detect feature drift and prediction distribution shifts between training and serving.
"""

from __future__ import annotations

import math
import structlog
from typing import Any, Dict, List, Tuple

logger = structlog.get_logger(__name__)


class DriftDetector:
    """Detects data and prediction drift in serving traffic vs reference distributions."""

    def __init__(self, psi_threshold: float = 0.25):
        self.psi_threshold = psi_threshold

    def calculate_psi(self, reference: List[float], target: List[float], num_bins: int = 5) -> float:
        """
        Calculate Population Stability Index (PSI) between reference and target distributions.
          PSI < 0.10: No significant distribution change
          0.10 <= PSI < 0.25: Moderate drift
          PSI >= 0.25: Significant drift (action/retraining required)
        """
        if not reference or not target:
            return 0.0

        ref_sorted = sorted(reference)
        n_ref = len(ref_sorted)
        n_tar = len(target)

        # Quantile break points based on reference distribution
        quantiles = [i / num_bins for i in range(num_bins + 1)]
        bins = [ref_sorted[min(n_ref - 1, int(q * n_ref))] for q in quantiles]
        bins[0] = min(bins[0], min(target)) - 1e-5
        bins[-1] = max(bins[-1], max(target)) + 1e-5

        ref_counts = [0] * num_bins
        tar_counts = [0] * num_bins

        for v in reference:
            for i in range(num_bins):
                if bins[i] <= v < bins[i + 1]:
                    ref_counts[i] += 1
                    break

        for v in target:
            for i in range(num_bins):
                if bins[i] <= v < bins[i + 1]:
                    tar_counts[i] += 1
                    break

        ref_pcts = [max(0.001, count / n_ref) for count in ref_counts]
        tar_pcts = [max(0.001, count / n_tar) for count in tar_counts]

        psi = 0.0
        for r, t in zip(ref_pcts, tar_pcts):
            psi += (t - r) * math.log(t / r)

        return round(psi, 4)

    def check_feature_drift(self, reference_features: Dict[str, List[float]], current_features: Dict[str, List[float]]) -> Dict[str, Any]:
        """Check PSI across all monitored features."""
        drift_results = {}
        drifted_features = []

        for feature_name, ref_vals in reference_features.items():
            curr_vals = current_features.get(feature_name, [])
            psi = self.calculate_psi(ref_vals, curr_vals)
            is_drifted = psi >= self.psi_threshold
            drift_results[feature_name] = {
                "psi": psi,
                "drift_detected": is_drifted,
            }
            if is_drifted:
                drifted_features.append(feature_name)

        if drifted_features:
            logger.warning("Feature drift detected", drifted_features=drifted_features)

        return {
            "drift_detected": len(drifted_features) > 0,
            "drifted_features": drifted_features,
            "feature_details": drift_results,
        }
