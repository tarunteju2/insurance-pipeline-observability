"""
Data Quality Validation Framework for Lakehouse Tables.

Provides Great Expectations-style automated assertion checks for completeness,
validity, non-null constraints, and range bounds on analytical tables.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass
from typing import Any, Dict, List

from src.lakehouse.duckdb_warehouse import DuckDBWarehouse

logger = structlog.get_logger(__name__)


@dataclass
class ExpectationResult:
    expectation_type: str
    column: str
    passed: bool
    details: Dict[str, Any]


class DataQualityFramework:
    """Automated expectation testing engine for lakehouse data assets."""

    def __init__(self, warehouse: Optional[DuckDBWarehouse] = None):
        self.warehouse = warehouse or DuckDBWarehouse()

    def run_suite(self, table_name: str = "claims_fact") -> List[ExpectationResult]:
        results: List[ExpectationResult] = []

        # Expectation 1: Non-null claim_id
        res_null = self.warehouse.execute_analytical_query(
            f"SELECT COUNT(*) AS null_count FROM {table_name} WHERE claim_id IS NULL"
        )
        null_count = res_null[0]["null_count"]
        results.append(ExpectationResult(
            expectation_type="expect_column_values_to_not_be_null",
            column="claim_id",
            passed=null_count == 0,
            details={"null_count": null_count},
        ))

        # Expectation 2: Positive claim_amount
        res_neg = self.warehouse.execute_analytical_query(
            f"SELECT COUNT(*) AS invalid_count FROM {table_name} WHERE claim_amount <= 0"
        )
        invalid_count = res_neg[0]["invalid_count"]
        results.append(ExpectationResult(
            expectation_type="expect_column_values_to_be_greater_than_zero",
            column="claim_amount",
            passed=invalid_count == 0,
            details={"invalid_count": invalid_count},
        ))

        # Expectation 3: Fraud score in [0.0, 1.0] range
        res_range = self.warehouse.execute_analytical_query(
            f"SELECT COUNT(*) AS out_of_bounds FROM {table_name} WHERE fraud_score < 0.0 OR fraud_score > 1.0"
        )
        out_count = res_range[0]["out_of_bounds"]
        results.append(ExpectationResult(
            expectation_type="expect_column_values_to_be_between",
            column="fraud_score",
            passed=out_count == 0,
            details={"out_of_bounds_count": out_count, "range": [0.0, 1.0]},
        ))

        logger.info("Data quality suite completed", table=table_name, total_checks=len(results), passed=sum(1 for r in results if r.passed))
        return results
