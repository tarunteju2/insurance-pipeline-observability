"""
DuckDB Vectorized OLAP Analytics Engine
Provides zero-copy, sub-millisecond analytical query execution over claims datasets and lakehouse storage.
"""

from typing import Any, Dict, List, Optional
import duckdb
import pandas as pd


class DuckDBAnalyticsEngine:
    """
    Embedded DuckDB OLAP engine for querying claim streams, Parquet files, and relational snapshots.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._init_conn()

    def _init_conn(self):
        self.conn = duckdb.connect(self.db_path)

    def register_claims(self, claims: List[Dict[str, Any]], table_name: str = "claims"):
        """
        Registers an in-memory list of claim dictionaries as a DuckDB table for instant SQL querying.
        """
        if not claims:
            # Create empty table schema if no claims provided
            df = pd.DataFrame(
                columns=[
                    "claim_id",
                    "policy_number",
                    "claim_amount",
                    "claim_type",
                    "fraud_score",
                    "is_fraud_flag",
                    "status",
                ]
            )
        else:
            # Clean dicts for dataframe conversion
            cleaned_claims = []
            for c in claims:
                item = {
                    "claim_id": c.get("claim_id"),
                    "policy_number": c.get("policy_number"),
                    "claim_amount": float(c.get("claim_amount", 0.0)),
                    "claim_type": str(c.get("claim_type", "auto")),
                    "fraud_score": float(c.get("fraud_score", 0.0)),
                    "is_fraud_flag": bool(c.get("is_fraud_flag", False)),
                    "status": str(c.get("status", "SUBMITTED")),
                    "date_filed": str(c.get("date_filed", "")),
                }
                cleaned_claims.append(item)
            df = pd.DataFrame(cleaned_claims)

        self.conn.register(table_name, df)

    def execute_query(self, sql_query: str) -> List[Dict[str, Any]]:
        """
        Executes an arbitrary SQL query against registered tables and returns results as a list of dicts.
        """
        try:
            rel = self.conn.sql(sql_query)
            if rel is None:
                return []
            df = rel.df()
            return df.to_dict(orient="records")
        except Exception as e:
            raise ValueError(f"DuckDB SQL Execution Error: {str(e)}")

    def get_summary_statistics(self, table_name: str = "claims") -> Dict[str, Any]:
        """
        Calculates fast vectorized OLAP summary statistics for the dataset.
        """
        query = f"""
        SELECT 
            COUNT(*) as total_claims,
            ROUND(AVG(claim_amount), 2) as avg_claim_amount,
            ROUND(MAX(claim_amount), 2) as max_claim_amount,
            ROUND(AVG(fraud_score), 4) as avg_fraud_score,
            SUM(CASE WHEN is_fraud_flag = true THEN 1 ELSE 0 END) as total_fraud_flagged,
            COUNT(DISTINCT claim_type) as distinct_claim_types
        FROM {table_name}
        """
        results = self.execute_query(query)
        return results[0] if results else {}

    def get_claims_by_type_distribution(self, table_name: str = "claims") -> List[Dict[str, Any]]:
        """
        Calculates distribution of claims grouped by claim type using DuckDB vectorized execution.
        """
        query = f"""
        SELECT 
            claim_type,
            COUNT(*) as count,
            ROUND(AVG(claim_amount), 2) as avg_amount,
            ROUND(AVG(fraud_score), 4) as avg_fraud_score
        FROM {table_name}
        GROUP BY claim_type
        ORDER BY count DESC
        """
        return self.execute_query(query)
