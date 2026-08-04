"""
Model Registry for ML Fraud Detection Pipeline.

Manages model versions, deployment stages (staging -> production -> archived),
A/B test traffic splitting (champion vs. challenger model), and fallback handling.
"""

from __future__ import annotations

import random
import structlog
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol

logger = structlog.get_logger(__name__)


class ModelStage(str, Enum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    CHALLENGER = "challenger"
    ARCHIVED = "archived"


@dataclass
class ModelMetadata:
    model_id: str
    version: str
    stage: ModelStage
    algorithm: str
    trained_at: str
    feature_set_version: str
    metrics: Dict[str, float]  # precision, recall, f1, auc_roc
    description: str


class MLModelProtocol(Protocol):
    """Protocol for pluggable fraud prediction models."""
    def predict_proba(self, features: Dict[str, float]) -> float:
        ...


class ModelRegistry:
    """Central registry tracking active, challenger, and archived ML models."""

    _instance: Optional[ModelRegistry] = None

    def __init__(self):
        self._models: Dict[str, MLModelProtocol] = {}
        self._metadata: Dict[str, ModelMetadata] = {}
        self._champion_id: Optional[str] = None
        self._challenger_id: Optional[str] = None
        self.ab_test_challenger_pct: float = 0.20  # 20% traffic to challenger

    @classmethod
    def instance(cls) -> ModelRegistry:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def register_model(
        self,
        model_id: str,
        model_instance: MLModelProtocol,
        metadata: ModelMetadata,
    ) -> None:
        self._models[model_id] = model_instance
        self._metadata[model_id] = metadata

        if metadata.stage == ModelStage.PRODUCTION:
            self._champion_id = model_id
        elif metadata.stage == ModelStage.CHALLENGER:
            self._challenger_id = model_id

        logger.info(
            "Model registered",
            model_id=model_id,
            version=metadata.version,
            stage=metadata.stage.value,
            auc_roc=metadata.metrics.get("auc_roc"),
        )

    def select_model_for_inference(self) -> Tuple[MLModelProtocol, ModelMetadata]:
        """Select champion or challenger model based on A/B test traffic split."""
        if self._challenger_id and self._challenger_id in self._models:
            if random.random() < self.ab_test_challenger_pct:
                return self._models[self._challenger_id], self._metadata[self._challenger_id]

        if self._champion_id and self._champion_id in self._models:
            return self._models[self._champion_id], self._metadata[self._champion_id]

        # Fallback to any registered model
        if self._models:
            first_id = next(iter(self._models.keys()))
            return self._models[first_id], self._metadata[first_id]

        raise RuntimeError("No ML models registered in ModelRegistry.")

    def promote_to_champion(self, model_id: str) -> None:
        """Promote a challenger/staging model to production champion."""
        if model_id not in self._models:
            raise KeyError(f"Model {model_id} not found in registry.")

        if self._champion_id and self._champion_id in self._metadata:
            self._metadata[self._champion_id].stage = ModelStage.ARCHIVED

        self._metadata[model_id].stage = ModelStage.PRODUCTION
        self._champion_id = model_id
        if self._challenger_id == model_id:
            self._challenger_id = None

        logger.info("Promoted model to champion", champion_id=model_id)

    def list_models(self) -> List[Dict[str, Any]]:
        return [
            {
                "model_id": meta.model_id,
                "version": meta.version,
                "stage": meta.stage.value,
                "algorithm": meta.algorithm,
                "metrics": meta.metrics,
            }
            for meta in self._metadata.values()
        ]
