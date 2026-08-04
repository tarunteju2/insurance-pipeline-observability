"""
Data Catalog and Metadata Governance.

Registers data assets, field-level PII/PHI/PCI classifications, quality scores,
and upstream/downstream lineage dependencies.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.models.claims import DataClass

logger = structlog.get_logger(__name__)


@dataclass
class ColumnMetadata:
    name: str
    data_type: str
    classification: DataClass
    description: str
    is_nullable: bool = True
    masking_strategy: Optional[str] = None  # hash, redactor, partial_mask


@dataclass
class AssetMetadata:
    asset_id: str
    name: str
    owner: str
    layer: str  # raw, curated, analytics, lakehouse
    columns: List[ColumnMetadata]
    upstream_dependencies: List[str] = field(default_factory=list)
    downstream_dependencies: List[str] = field(default_factory=list)
    quality_score: float = 1.0


class DataCatalog:
    """Central Data Catalog tracking data assets and governance metadata."""

    _instance: Optional[DataCatalog] = None

    def __init__(self):
        self._assets: Dict[str, AssetMetadata] = {}
        self._init_default_catalog()

    @classmethod
    def instance(cls) -> DataCatalog:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def register_asset(self, asset: AssetMetadata) -> None:
        self._assets[asset.asset_id] = asset
        logger.info("Registered data asset in catalog", asset_id=asset.asset_id, column_count=len(asset.columns))

    def get_asset(self, asset_id: str) -> Optional[AssetMetadata]:
        return self._assets.get(asset_id)

    def list_pii_columns(self, asset_id: str) -> List[ColumnMetadata]:
        asset = self.get_asset(asset_id)
        if not asset:
            return []
        return [col for col in asset.columns if col.classification in (DataClass.PII, DataClass.SENSITIVE)]

    def _init_default_catalog(self) -> None:
        raw_claims = AssetMetadata(
            asset_id="kafka.raw.claims",
            name="Kafka Raw Claims Ingestion Stream",
            owner="Data Engineering",
            layer="raw",
            columns=[
                ColumnMetadata("claim_id", "string", DataClass.PUBLIC, "Unique claim ID"),
                ColumnMetadata("claimant_name", "string", DataClass.PII, "Full legal name of claimant", masking_strategy="partial_mask"),
                ColumnMetadata("vehicle_vin", "string", DataClass.PII, "17-char VIN", masking_strategy="hash"),
                ColumnMetadata("property_address", "string", DataClass.PII, "Property loss address", masking_strategy="partial_mask"),
                ColumnMetadata("claim_amount", "float", DataClass.SENSITIVE, "Claim monetary value"),
            ],
            downstream_dependencies=["db.curated.claims_fact", "lakehouse.iceberg.claims"],
            quality_score=0.98,
        )
        self.register_asset(raw_claims)
