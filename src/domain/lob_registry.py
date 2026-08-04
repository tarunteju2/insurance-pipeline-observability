"""
LOB Registry — central dispatch for line-of-business-specific processing.

Each LOB is a self-contained processing unit with its own:
  - Validation ruleset (regulatory + business rules)
  - Fraud model configuration (weights, thresholds, ML model ID)
  - Enrichment data sources
  - SLA targets (latency, error rate)
  - Reserve estimation parameters
  - Regulatory reporting requirements

The registry is the single entry point for the stream processor to route
claims to the correct domain pipeline without if/else branching.
"""

from __future__ import annotations

import importlib
import structlog
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, Tuple, Type

from src.models.claims import InsuranceClaim, ClaimType

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# SLA + regulatory configuration per LOB
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SLATargets:
    """Service-level agreement targets for a line of business."""
    validation_latency_p95_ms: float = 200.0
    fraud_scoring_latency_p95_ms: float = 500.0
    enrichment_latency_p95_ms: float = 1000.0
    end_to_end_latency_p95_ms: float = 3000.0
    max_error_rate: float = 0.02
    max_dlq_rate: float = 0.01


@dataclass(frozen=True)
class RegulatoryConfig:
    """Regulatory reporting configuration for a line of business."""
    jurisdiction: str = "US-ALL"
    reporting_frequency: str = "quarterly"
    doi_filing_required: bool = True
    naic_annual_statement_line: Optional[str] = None
    statutory_reserve_method: str = "case_basis"
    prompt_pay_days: int = 30  # days to pay after proof of loss
    acknowledgment_days: int = 15  # days to acknowledge receipt
    investigation_days: int = 45  # days to complete investigation


@dataclass(frozen=True)
class FraudModelConfig:
    """Fraud detection configuration for a line of business."""
    heuristic_weight: float = 0.30
    ml_model_weight: float = 0.40
    anomaly_weight: float = 0.15
    network_weight: float = 0.15
    auto_approve_threshold: float = 0.15
    manual_review_threshold: float = 0.45
    siu_referral_threshold: float = 0.75
    model_id: Optional[str] = None


@dataclass(frozen=True)
class ReserveConfig:
    """Reserve estimation parameters for a line of business."""
    initial_reserve_method: str = "average_paid"
    development_factor_months: List[int] = field(
        default_factory=lambda: [3, 6, 12, 18, 24, 36, 48, 60]
    )
    loss_development_factors: Dict[int, float] = field(
        default_factory=lambda: {
            3: 1.80, 6: 1.55, 12: 1.30, 18: 1.18,
            24: 1.10, 36: 1.05, 48: 1.02, 60: 1.00,
        }
    )
    ibnr_method: str = "bornhuetter_ferguson"
    catastrophe_load_pct: float = 0.05


# ---------------------------------------------------------------------------
# LOB base classes (strategy pattern)
# ---------------------------------------------------------------------------

class LOBValidator(Protocol):
    """Protocol for LOB-specific validation logic."""
    def validate(self, claim: InsuranceClaim) -> Tuple[bool, List[Dict[str, Any]]]:
        """Return (is_valid, list_of_error_dicts)."""
        ...


class LOBEnricher(Protocol):
    """Protocol for LOB-specific data enrichment."""
    def enrich(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Return dict of enrichment data to merge into claim."""
        ...


class LOBFraudScorer(Protocol):
    """Protocol for LOB-specific fraud indicators."""
    def score(self, claim: InsuranceClaim) -> Tuple[float, List[str]]:
        """Return (additional_score_delta, list_of_triggered_rules)."""
        ...


class LineOfBusiness:
    """
    Encapsulates all LOB-specific processing logic and configuration.

    Subclass per LOB (Auto, Health, Property, etc.) and register
    with LOBRegistry.
    """

    # Override in subclass
    lob_code: str = "GENERIC"
    lob_name: str = "Generic Insurance"
    claim_types: List[ClaimType] = []
    sla: SLATargets = SLATargets()
    regulatory: RegulatoryConfig = RegulatoryConfig()
    fraud_config: FraudModelConfig = FraudModelConfig()
    reserve_config: ReserveConfig = ReserveConfig()

    def __init__(self):
        self._validator: Optional[LOBValidator] = None
        self._enricher: Optional[LOBEnricher] = None
        self._fraud_scorer: Optional[LOBFraudScorer] = None

    @property
    def validator(self) -> Optional[LOBValidator]:
        return self._validator

    @property
    def enricher(self) -> Optional[LOBEnricher]:
        return self._enricher

    @property
    def fraud_scorer(self) -> Optional[LOBFraudScorer]:
        return self._fraud_scorer

    def validate_claim(
        self, claim: InsuranceClaim
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """Run LOB-specific validation rules on top of generic validation."""
        if self._validator is None:
            return True, []
        return self._validator.validate(claim)

    def enrich_claim(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Run LOB-specific enrichment and return data to merge."""
        if self._enricher is None:
            return {}
        return self._enricher.enrich(claim)

    def score_fraud(
        self, claim: InsuranceClaim
    ) -> Tuple[float, List[str]]:
        """Return (score_delta, triggered_rules) from LOB-specific fraud rules."""
        if self._fraud_scorer is None:
            return 0.0, []
        return self._fraud_scorer.score(claim)

    def estimate_initial_reserve(self, claim: InsuranceClaim) -> float:
        """Estimate initial case reserve based on LOB reserve config."""
        # Default: percentage of claim amount based on LOB averages
        reserve_pcts = {
            "AUTO": 0.65, "HEALTH": 0.75, "PROPERTY": 0.55,
            "COMMERCIAL": 0.60, "CYBER": 0.70, "LIFE": 1.00,
            "LIABILITY": 0.50, "WORKERS_COMP": 0.80,
        }
        pct = reserve_pcts.get(self.lob_code, 0.65)
        base = claim.claim_amount * pct

        # Apply development factor for month 3 as initial uplift
        ldf = self.reserve_config.loss_development_factors.get(3, 1.80)
        cat_load = 1.0 + self.reserve_config.catastrophe_load_pct
        return round(base * ldf * cat_load, 2)


# ---------------------------------------------------------------------------
# LOB Registry (singleton)
# ---------------------------------------------------------------------------

class LOBRegistry:
    """
    Central registry mapping ClaimType → LineOfBusiness instance.

    Usage:
        registry = LOBRegistry.instance()
        lob = registry.get_lob(ClaimType.AUTO)
        is_valid, errors = lob.validate_claim(claim)
    """

    _instance: Optional[LOBRegistry] = None

    def __init__(self):
        self._lob_map: Dict[ClaimType, LineOfBusiness] = {}
        self._lob_by_code: Dict[str, LineOfBusiness] = {}
        self._default = LineOfBusiness()
        self._auto_discover()

    @classmethod
    def instance(cls) -> LOBRegistry:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def register(self, lob: LineOfBusiness) -> None:
        """Register a LOB instance for its associated claim types."""
        for ct in lob.claim_types:
            self._lob_map[ct] = lob
        self._lob_by_code[lob.lob_code] = lob
        logger.info(
            "LOB registered",
            lob_code=lob.lob_code,
            lob_name=lob.lob_name,
            claim_types=[ct.value for ct in lob.claim_types],
        )

    def get_lob(self, claim_type: ClaimType) -> LineOfBusiness:
        """Get the LOB processor for a given claim type."""
        return self._lob_map.get(claim_type, self._default)

    def get_lob_by_code(self, code: str) -> LineOfBusiness:
        """Get a LOB processor by its code (e.g., 'AUTO')."""
        return self._lob_by_code.get(code, self._default)

    def list_lobs(self) -> List[Dict[str, Any]]:
        """List all registered LOBs with metadata."""
        results = []
        for code, lob in sorted(self._lob_by_code.items()):
            results.append({
                "lob_code": lob.lob_code,
                "lob_name": lob.lob_name,
                "claim_types": [ct.value for ct in lob.claim_types],
                "sla": {
                    "validation_p95_ms": lob.sla.validation_latency_p95_ms,
                    "fraud_p95_ms": lob.sla.fraud_scoring_latency_p95_ms,
                    "e2e_p95_ms": lob.sla.end_to_end_latency_p95_ms,
                    "max_error_rate": lob.sla.max_error_rate,
                },
                "regulatory": {
                    "jurisdiction": lob.regulatory.jurisdiction,
                    "prompt_pay_days": lob.regulatory.prompt_pay_days,
                    "naic_line": lob.regulatory.naic_annual_statement_line,
                },
                "fraud_config": {
                    "siu_threshold": lob.fraud_config.siu_referral_threshold,
                    "auto_approve_threshold": lob.fraud_config.auto_approve_threshold,
                },
            })
        return results

    def _auto_discover(self) -> None:
        """
        Auto-discover and register LOB modules from the domain package.

        Expects each LOB subpackage to expose a `register(registry)` function.
        """
        lob_modules = [
            "src.domain.auto",
            "src.domain.health",
            "src.domain.property",
            "src.domain.commercial",
            "src.domain.cyber",
        ]
        for mod_path in lob_modules:
            try:
                mod = importlib.import_module(mod_path)
                if hasattr(mod, "register"):
                    mod.register(self)
            except ImportError:
                logger.debug("LOB module not found — skipping", module=mod_path)
            except Exception as exc:
                logger.warning(
                    "LOB module registration failed",
                    module=mod_path,
                    error=str(exc),
                )
