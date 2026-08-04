"""
Data Lakehouse Architecture (Iceberg + DuckDB + dbt).

Provides Apache Iceberg catalog management, in-process DuckDB OLAP analytical warehouse engine,
dbt star-schema model definitions, and Great Expectations-style Data Quality framework.
"""

from src.lakehouse.duckdb_warehouse import DuckDBWarehouse
from src.lakehouse.iceberg_catalog import IcebergCatalogManager

__all__ = ["DuckDBWarehouse", "IcebergCatalogManager"]
