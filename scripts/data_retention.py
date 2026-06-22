#!/usr/bin/env python3
"""
Data Retention Policy Engine
=============================
Automatically purges stale records from PostgreSQL and sets lifecycle
rules on MinIO buckets.

Usage
-----
  # Dry-run (show what would be deleted)
  python scripts/data_retention.py --dry-run

  # Apply retention policies
  python scripts/data_retention.py --apply

  # Custom retention periods
  python scripts/data_retention.py --apply --claims-days 180 --health-days 30

Environment
-----------
Set RETENTION_CLAIMS_DAYS, RETENTION_LINEAGE_DAYS, RETENTION_HEALTH_DAYS
to override defaults via env vars.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import structlog
from sqlalchemy import text

from src.config import config
from src.lineage.models import get_session

logger = structlog.get_logger("data-retention")

# Default retention periods (days)
_DEFAULT_CLAIMS_DAYS = int(os.getenv("RETENTION_CLAIMS_DAYS", "365"))
_DEFAULT_LINEAGE_DAYS = int(os.getenv("RETENTION_LINEAGE_DAYS", "90"))
_DEFAULT_HEALTH_DAYS = int(os.getenv("RETENTION_HEALTH_DAYS", "30"))
_DEFAULT_IDEM_DAYS = int(os.getenv("RETENTION_IDEM_DAYS", "30"))


def _purge_table(session, table: str, timestamp_col: str, cutoff: datetime, dry_run: bool) -> int:
    """Delete rows older than cutoff. Returns count of rows affected."""
    count_query = text(f"SELECT COUNT(*) FROM {table} WHERE {timestamp_col} < :cutoff")
    row_count = session.execute(count_query, {"cutoff": cutoff}).scalar()

    if dry_run:
        logger.info(f"[DRY RUN] Would delete {row_count} rows from {table}",
                     cutoff=cutoff.isoformat())
        return row_count

    if row_count == 0:
        logger.info(f"No rows to purge from {table}")
        return 0

    delete_query = text(f"DELETE FROM {table} WHERE {timestamp_col} < :cutoff")
    session.execute(delete_query, {"cutoff": cutoff})
    session.commit()
    logger.info(f"Purged {row_count} rows from {table}", cutoff=cutoff.isoformat())
    return row_count


def apply_postgres_retention(
    claims_days: int = _DEFAULT_CLAIMS_DAYS,
    lineage_days: int = _DEFAULT_LINEAGE_DAYS,
    health_days: int = _DEFAULT_HEALTH_DAYS,
    idem_days: int = _DEFAULT_IDEM_DAYS,
    dry_run: bool = True,
):
    """Purge old rows from all pipeline tables."""
    session = get_session()
    now = datetime.utcnow()
    summary = {}
    try:
        summary["processed_claims"] = _purge_table(
            session, "processed_claims", "created_at",
            now - timedelta(days=claims_days), dry_run,
        )
        summary["lineage_events"] = _purge_table(
            session, "lineage_events", "created_at",
            now - timedelta(days=lineage_days), dry_run,
        )
        summary["pipeline_health_snapshots"] = _purge_table(
            session, "pipeline_health_snapshots", "snapshot_at",
            now - timedelta(days=health_days), dry_run,
        )
        summary["claim_idempotency_records"] = _purge_table(
            session, "claim_idempotency_records", "first_seen_at",
            now - timedelta(days=idem_days), dry_run,
        )
    except Exception as e:
        session.rollback()
        logger.error("Retention failed", error=str(e))
        raise
    finally:
        session.close()
    return summary


def apply_minio_lifecycle(dry_run: bool = True):
    """Set lifecycle rules on the MinIO insurance-claims-lake bucket."""
    import boto3
    from botocore.client import Config as BotoConfig

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{config.minio.endpoint}",
        aws_access_key_id=config.minio.access_key,
        aws_secret_access_key=config.minio.secret_key,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )

    lifecycle_rules = [
        {
            "ID": "purge-rejected-90d",
            "Status": "Enabled",
            "Filter": {"Prefix": "rejected/"},
            "Expiration": {"Days": 90},
        },
        {
            "ID": "purge-validated-180d",
            "Status": "Enabled",
            "Filter": {"Prefix": "validated/"},
            "Expiration": {"Days": 180},
        },
        {
            "ID": "purge-scored-180d",
            "Status": "Enabled",
            "Filter": {"Prefix": "scored/"},
            "Expiration": {"Days": 180},
        },
        {
            "ID": "purge-enriched-365d",
            "Status": "Enabled",
            "Filter": {"Prefix": "enriched/"},
            "Expiration": {"Days": 365},
        },
    ]

    if dry_run:
        print("\n[DRY RUN] Would apply lifecycle rules to MinIO:")
        for rule in lifecycle_rules:
            print(f"  • {rule['ID']}: {rule['Filter']['Prefix']} → expire after {rule['Expiration']['Days']}d")
        return lifecycle_rules

    s3.put_bucket_lifecycle_configuration(
        Bucket=config.minio.bucket,
        LifecycleConfiguration={"Rules": lifecycle_rules},
    )
    logger.info("MinIO lifecycle rules applied", bucket=config.minio.bucket,
                 rules=len(lifecycle_rules))
    return lifecycle_rules


def main():
    parser = argparse.ArgumentParser(description="Insurance Pipeline — Data Retention Policy")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Show what would be deleted")
    parser.add_argument("--apply", action="store_true", help="Actually apply retention (overrides --dry-run)")
    parser.add_argument("--claims-days", type=int, default=_DEFAULT_CLAIMS_DAYS)
    parser.add_argument("--lineage-days", type=int, default=_DEFAULT_LINEAGE_DAYS)
    parser.add_argument("--health-days", type=int, default=_DEFAULT_HEALTH_DAYS)
    args = parser.parse_args()

    dry_run = not args.apply
    print(f"\n{'='*60}")
    print(f"  Data Retention Policy {'(DRY RUN)' if dry_run else '(APPLYING)'}")
    print(f"  Claims: {args.claims_days}d | Lineage: {args.lineage_days}d | Health: {args.health_days}d")
    print(f"{'='*60}\n")

    pg_summary = apply_postgres_retention(
        claims_days=args.claims_days,
        lineage_days=args.lineage_days,
        health_days=args.health_days,
        dry_run=dry_run,
    )
    apply_minio_lifecycle(dry_run=dry_run)

    total = sum(pg_summary.values())
    print(f"\n{'='*60}")
    print(f"  Total rows {'that would be' if dry_run else ''} purged: {total}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
