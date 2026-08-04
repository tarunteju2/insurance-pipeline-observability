"""
Online and Offline Feature Store for ML Fraud Pipeline.

Provides low-latency online feature retrieval for scoring inference
and offline feature storage for model training datasets.
"""

from __future__ import annotations

import structlog
from typing import Any, Dict, List, Optional
from datetime import datetime

from src.ml.feature_engineering import FeatureEngineer
from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)


class FeatureStore:
    """Central store managing online (inference) and offline (training) feature vectors."""

    _instance: Optional[FeatureStore] = None

    def __init__(self):
        self.engineer = FeatureEngineer()
        self._online_store: Dict[str, Dict[str, float]] = {}
        self._entity_profiles: Dict[str, Dict[str, Any]] = {}
        self._offline_dataset: List[Dict[str, Any]] = []

    @classmethod
    def instance(cls) -> FeatureStore:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def get_online_features(self, claim: InsuranceClaim, context: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
        """Compute or retrieve online feature vector for claim inference."""
        entity_ctx = self._build_context(claim, context)
        features = self.engineer.extract_features(claim, entity_ctx)

        # Cache feature vector in online store
        self._online_store[claim.claim_id] = features
        logger.debug("Online features extracted", claim_id=claim.claim_id, feature_count=len(features))
        return features

    def update_entity_profile(self, entity_id: str, profile_data: Dict[str, Any]) -> None:
        """Update claimant, policy, or provider profile metadata."""
        profile = self._entity_profiles.setdefault(entity_id, {})
        profile.update(profile_data)
        profile["last_updated"] = datetime.utcnow().isoformat()

    def record_offline_feature_vector(self, claim_id: str, features: Dict[str, float], label: int) -> None:
        """Save feature vector and ground-truth label to offline dataset for retraining."""
        record = {
            "claim_id": claim_id,
            "timestamp": datetime.utcnow().isoformat(),
            "label": label,  # 1 = fraud, 0 = non-fraud
            **features,
        }
        self._offline_dataset.append(record)

    def get_training_dataset(self) -> List[Dict[str, Any]]:
        """Retrieve offline feature dataset for ML model training."""
        return list(self._offline_dataset)

    def _build_context(self, claim: InsuranceClaim, context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        ctx = context or {}

        # Merge cached entity profiles if available
        claimant_id = getattr(claim, "claimant_id", claim.claimant_name)
        claimant_prof = self._entity_profiles.get(claimant_id, {})
        policy_prof = self._entity_profiles.get(claim.policy_number, {})

        if "claimant_history" not in ctx and claimant_prof:
            ctx["claimant_history"] = claimant_prof
        if "policy_info" not in ctx and policy_prof:
            ctx["policy_info"] = policy_prof

        return ctx
