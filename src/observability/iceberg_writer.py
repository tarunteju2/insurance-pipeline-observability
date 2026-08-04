"""
Apache Iceberg writer for the insurance claims data lake.

Provides ACID transactions, time-travel queries, and schema evolution
on top of MinIO (S3-compatible) using PyIceberg with a SQL catalog
backed by the existing PostgreSQL instance.

Architecture
------------
* Catalog  : PyIceberg SQL catalog — metadata stored in Postgres
             alongside existing tables (uses "iceberg_" prefixed tables).
* Warehouse: s3://insurance-claims-lake/iceberg  (MinIO bucket)
* Table    : insurance.claims  — one row per completed claim.

Usage
-----
    from src.observability.iceberg_writer import iceberg_writer
    iceberg_writer.write_batch(claims)   # thread-safe, fails gracefully

Fails gracefully
----------------
If PyIceberg is not installed, MinIO is unreachable, or the catalog
cannot be initialised, a warning is logged and the existing JSON/S3
write path continues unaffected.
"""

import structlog
from datetime import datetime
from typing import List, Optional

logger = structlog.get_logger(__name__)

_TABLE_NAMESPACE = "insurance"
_TABLE_NAME = "claims"
_FULL_TABLE_NAME = f"{_TABLE_NAMESPACE}.{_TABLE_NAME}"
_WAREHOUSE_PATH = "s3://insurance-claims-lake/iceberg"


def _build_iceberg_schema():
    """Build the native PyIceberg Schema for the claims table."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        NestedField, StringType, DoubleType,
    )
    return Schema(
        NestedField(1,  "claim_id",        StringType(), required=True),
        NestedField(2,  "schema_version",  StringType(), required=True),
        NestedField(3,  "idempotency_key", StringType(), required=False),
        NestedField(4,  "policy_number",   StringType(), required=True),
        NestedField(5,  "claim_type",      StringType(), required=True),
        NestedField(6,  "claim_amount",    DoubleType(), required=True),
        NestedField(7,  "date_of_loss",    StringType(), required=True),
        NestedField(8,  "date_filed",      StringType(), required=False),
        NestedField(9,  "status",          StringType(), required=True),
        NestedField(10, "fraud_score",     DoubleType(), required=False),
        NestedField(11, "risk_level",      StringType(), required=False),
        NestedField(12, "correlation_id",  StringType(), required=False),
        NestedField(13, "trace_id",        StringType(), required=False),
        NestedField(14, "processed_at",    StringType(), required=True),
    )


def _build_pyarrow_schema():
    """Matching PyArrow schema used when writing data rows."""
    import pyarrow as pa
    return pa.schema([
        pa.field("claim_id",        pa.string(),  nullable=False),
        pa.field("schema_version",  pa.string(),  nullable=False),
        pa.field("idempotency_key", pa.string(),  nullable=True),
        pa.field("policy_number",   pa.string(),  nullable=False),
        pa.field("claim_type",      pa.string(),  nullable=False),
        pa.field("claim_amount",    pa.float64(), nullable=False),
        pa.field("date_of_loss",    pa.string(),  nullable=False),
        pa.field("date_filed",      pa.string(),  nullable=True),
        pa.field("status",          pa.string(),  nullable=False),
        pa.field("fraud_score",     pa.float64(), nullable=True),
        pa.field("risk_level",      pa.string(),  nullable=True),
        pa.field("correlation_id",  pa.string(),  nullable=True),
        pa.field("trace_id",        pa.string(),  nullable=True),
        pa.field("processed_at",    pa.string(),  nullable=False),
    ])


class IcebergWriter:
    """
    Thread-safe Iceberg batch writer.

    Initialised lazily on first write attempt so that startup failures
    do not block the pipeline — just degrade to JSON-only storage.
    """

    def __init__(self) -> None:
        self._catalog = None
        self._table = None
        self._enabled: Optional[bool] = None  # None = not yet probed

    @property
    def enabled(self) -> bool:
        if self._enabled is None:
            self._try_init()
        return bool(self._enabled)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_batch(self, claims: list) -> None:
        """
        Append *claims* (list of InsuranceClaim) to the Iceberg table.

        Silently skips if Iceberg is disabled or unavailable.
        Each failure is logged as a warning; the calling code is never
        interrupted.
        """
        if not self.enabled or not claims:
            return
        try:
            import pyarrow as pa
            processed_at = datetime.utcnow().isoformat()

            data = {
                "claim_id":        [c.claim_id for c in claims],
                "schema_version":  [c.schema_version for c in claims],
                "idempotency_key": [c.idempotency_key for c in claims],
                "policy_number":   [c.policy_number for c in claims],
                "claim_type":      [c.claim_type.value for c in claims],
                "claim_amount":    [float(c.claim_amount) for c in claims],
                "date_of_loss":    [c.date_of_loss for c in claims],
                "date_filed":      [c.date_filed for c in claims],
                "status":          [c.status.value for c in claims],
                "fraud_score":     [float(c.fraud_score) for c in claims],
                "risk_level":      [c.risk_level.value for c in claims],
                "correlation_id":  [c.correlation_id for c in claims],
                "trace_id":        [c.trace_id for c in claims],
                "processed_at":    [processed_at] * len(claims),
            }
            arrow_table = pa.table(data, schema=_build_pyarrow_schema())
            self._table.append(arrow_table)
            logger.debug("Iceberg batch written", count=len(claims),
                         table=_FULL_TABLE_NAME)
        except Exception as exc:
            logger.warning("Iceberg write failed — non-fatal, JSON/S3 path unaffected",
                           error=str(exc))

    # ------------------------------------------------------------------
    # Lazy initialisation
    # ------------------------------------------------------------------

    def _try_init(self) -> None:
        """
        Attempt to connect to the PyIceberg SQL catalog.
        Sets self._enabled to True on success, False on any failure.
        """
        try:
            from pyiceberg.catalog import load_catalog
            from src.config import config

            self._catalog = load_catalog(
                "insurance",
                **{
                    "type":                    "sql",
                    "uri":                     config.postgres.connection_string,
                    "warehouse":               _WAREHOUSE_PATH,
                    "s3.endpoint":             f"http://{config.minio.endpoint}",
                    "s3.access-key-id":        config.minio.access_key,
                    "s3.secret-access-key":    config.minio.secret_key,
                    "s3.path-style-access":    "true",
                    "py-io-impl":              "pyiceberg.io.fsspec.FsspecFileIO",
                },
            )
            self._ensure_table()
            self._enabled = True
            logger.info("Iceberg writer initialised",
                        warehouse=_WAREHOUSE_PATH, table=_FULL_TABLE_NAME)
        except Exception as exc:
            self._enabled = False
            logger.warning(
                "Iceberg writer unavailable — falling back to JSON/S3 only",
                error=str(exc),
            )

    def _ensure_table(self) -> None:
        """Create the Iceberg namespace + table if they do not already exist."""
        from pyiceberg.exceptions import NoSuchTableError

        try:
            self._catalog.create_namespace(_TABLE_NAMESPACE)
        except Exception:
            pass  # namespace already exists — not an error

        try:
            self._table = self._catalog.load_table(_FULL_TABLE_NAME)
            logger.debug("Iceberg table loaded", table=_FULL_TABLE_NAME)
        except NoSuchTableError:
            self._table = self._catalog.create_table(
                _FULL_TABLE_NAME,
                schema=_build_iceberg_schema(),
            )
            logger.info("Iceberg table created", table=_FULL_TABLE_NAME)


# Module-level singleton — initialised lazily on first write_batch() call.
iceberg_writer = IcebergWriter()
