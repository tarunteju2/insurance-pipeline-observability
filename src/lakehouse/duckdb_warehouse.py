"""
DuckDB OLAP Analytical Warehouse Engine.

Executes high-performance analytical queries, actuarial loss triangles,
and insurance KPI calculations over DuckDB and Parquet storage layers.
"""

from __future__ import annotations

import duckdb
import structlog
from typing import Any, Dict, List, Optional

logger = structlog.get_logger(__name__)


class DuckDBWarehouse:
    """In-process DuckDB OLAP Warehouse engine for analytics and loss triangle generation."""

    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def loss_ratio_by_lob(self) -> List[Dict[str, Any]]:
        """Calculate loss ratios by Line of Business."""
        query = """
            SELECT
                claim_type AS lob,
                COUNT(claim_id) AS total_claims,
                SUM(claim_amount) AS total_incurred,
                ROUND(AVG(claim_amount), 2) AS avg_severity,
                ROUND(SUM(approved_amount) / NULLIF(SUM(claim_amount), 0) * 100, 2) AS payout_ratio_pct
            FROM claims_fact
            GROUP BY claim_type
            ORDER BY total_incurred DESC
        """
        rel = self.conn.execute(query)
        columns = [desc[0] for desc in rel.description]
        return [dict(zip(columns, row)) for row in rel.fetchall()]

    def execute_analytical_query(self, query: str) -> List[Dict[str, Any]]:
        rel = self.conn.execute(query)
        columns = [desc[0] for desc in rel.description]
        return [dict(zip(columns, row)) for row in rel.fetchall()]

    def _init_schema(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS claims_fact (
                claim_id VARCHAR,
                policy_number VARCHAR,
                claim_type VARCHAR,
                claim_amount DOUBLE,
                approved_amount DOUBLE,
                fraud_score DOUBLE,
                date_of_loss DATE,
                date_filed DATE
            );
        """)
        # Seed mock rows for analytical queries
        self.conn.execute("""
            INSERT INTO claims_fact VALUES
            ('CLM-001', 'POL-101', 'auto', 5000.0, 4500.0, 0.12, '2026-07-01', '2026-07-02'),
            ('CLM-002', 'POL-102', 'health', 12000.0, 9500.0, 0.25, '2026-07-05', '2026-07-06'),
            ('CLM-003', 'POL-103', 'property', 45000.0, 40000.0, 0.08, '2026-07-10', '2026-07-11'),
            ('CLM-004', 'POL-104', 'auto', 3500.0, 3000.0, 0.15, '2026-07-15', '2026-07-16'),
            ('CLM-005', 'POL-105', 'commercial', 85000.0, 75000.0, 0.35, '2026-07-20', '2026-07-22');
        """)
