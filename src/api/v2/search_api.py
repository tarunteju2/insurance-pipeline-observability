"""
FastAPI Router for Search API v2.

Faceted and full-text claim search API.
"""

from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Any, Dict, List, Optional

router = APIRouter(prefix="/api/v2/search", tags=["Search V2"])


@router.get("/")
def search_claims(
    q: Optional[str] = Query(None, description="Search query string"),
    claim_type: Optional[str] = Query(None, description="Filter by LOB claim type"),
    min_amount: Optional[float] = Query(None, description="Minimum claim amount"),
    max_amount: Optional[float] = Query(None, description="Maximum claim amount"),
) -> Dict[str, Any]:
    return {
        "query": q,
        "filters": {"claim_type": claim_type, "min_amount": min_amount, "max_amount": max_amount},
        "total_hits": 1,
        "hits": [
            {
                "claim_id": "CLM-V2-0001",
                "claim_type": claim_type or "auto",
                "claim_amount": 4500.0,
                "score": 0.98,
            }
        ],
    }
