"""
FastAPI Router for Admin & Feature Flags API v2.

System configuration, model management, and feature toggle endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter
from typing import Any, Dict, List

from src.ml.model_registry import ModelRegistry

router = APIRouter(prefix="/api/v2/admin", tags=["Admin V2"])


@router.get("/models")
def list_ml_models() -> List[Dict[str, Any]]:
    registry = ModelRegistry.instance()
    return registry.list_models()


@router.get("/feature-flags")
def get_feature_flags() -> Dict[str, bool]:
    return {
        "ENABLE_CQRS_EVENT_SOURCING": True,
        "ENABLE_ML_CHALLENGER_MODEL": True,
        "ENABLE_AUTOMATED_RUNBOOKS": True,
        "ENABLE_DUCKDB_LAKEHOUSE": True,
        "ENABLE_GDPR_AUTO_ANONYMIZATION": True,
    }
