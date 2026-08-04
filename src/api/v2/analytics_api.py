"""
FastAPI Router for Analytics API v2.

Provides loss ratios, financial summaries, and actuarial analytics endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter
from typing import Any, Dict, List

from src.lakehouse.duckdb_warehouse import DuckDBWarehouse
from src.cqrs.projections import FinancialSummaryProjection

router = APIRouter(prefix="/api/v2/analytics", tags=["Analytics V2"])


@router.get("/loss-ratios")
def get_loss_ratios() -> List[Dict[str, Any]]:
    dw = DuckDBWarehouse()
    return dw.loss_ratio_by_lob()


@router.get("/financial-summary")
def get_financial_summary() -> Dict[str, Any]:
    proj = FinancialSummaryProjection()
    return proj.get_summary()
