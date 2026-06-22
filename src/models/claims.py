"""
Insurance Claims data models with Pydantic validation.
"""

import hashlib
import uuid
from datetime import date, datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, field_validator

# Current schema version — bump whenever the claim structure changes
SCHEMA_VERSION = "v1"


class ClaimType(str, Enum):
    AUTO = "auto"
    HEALTH = "health"
    PROPERTY = "property"
    LIFE = "life"
    LIABILITY = "liability"
    WORKERS_COMP = "workers_comp"


class ClaimStatus(str, Enum):
    SUBMITTED = "submitted"
    VALIDATING = "validating"
    VALIDATED = "validated"
    VALIDATION_FAILED = "validation_failed"
    SCORING = "scoring"
    SCORED = "scored"
    ENRICHING = "enriching"
    ENRICHED = "enriched"
    COMPLETED = "completed"
    REJECTED = "rejected"
    FLAGGED_FRAUD = "flagged_fraud"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ValidationSeverity(str, Enum):
    """Severity classification for individual validation rule violations."""
    CRITICAL = "critical"   # Stop-the-line; claim must be rejected immediately
    HIGH = "high"           # Claim likely fraudulent or legally problematic
    MEDIUM = "medium"       # Data quality issue; claim can proceed with a warning
    LOW = "low"             # Minor formatting issue


class DataClass(str, Enum):
    """Data classification for GDPR/compliance purposes."""
    PII = "pii"             # Directly identifies a person (name, SSN, VIN, address)
    SENSITIVE = "sensitive" # Financially or medically sensitive but not direct PII
    INTERNAL = "internal"   # Internal business data, not for external exposure
    PUBLIC = "public"       # Non-sensitive, safe to log or expose externally


# Maps each validation error code to its severity level.
# CRITICAL codes trigger immediate rejection (stop-the-line).
VALIDATION_SEVERITY_MAP: dict[str, ValidationSeverity] = {
    "REQUIRED_FIELD_MISSING":        ValidationSeverity.CRITICAL,
    "INVALID_CLAIM_AMOUNT":          ValidationSeverity.CRITICAL,
    "CLAIM_AMOUNT_EXCEEDS_LIMIT":    ValidationSeverity.CRITICAL,
    "LOSS_DATE_IN_FUTURE":           ValidationSeverity.CRITICAL,
    "FILED_BEFORE_LOSS":             ValidationSeverity.CRITICAL,
    "INVALID_POLICY_FORMAT":         ValidationSeverity.HIGH,
    "VIN_REQUIRED_HIGH_VALUE_AUTO":  ValidationSeverity.HIGH,
    "PROVIDER_REQUIRED":             ValidationSeverity.HIGH,
    "INVALID_VIN_FORMAT":            ValidationSeverity.MEDIUM,
    "INVALID_DIAGNOSIS_CODE":        ValidationSeverity.MEDIUM,
    "INVALID_CLAIMANT_NAME":         ValidationSeverity.MEDIUM,
    "INSUFFICIENT_DESCRIPTION":      ValidationSeverity.MEDIUM,
    "LOSS_DATE_TOO_OLD":             ValidationSeverity.MEDIUM,
    "INVALID_LOSS_DATE_FORMAT":      ValidationSeverity.LOW,
    "INVALID_FILED_DATE_FORMAT":     ValidationSeverity.LOW,
    "FILED_DATE_IN_FUTURE":          ValidationSeverity.LOW,
}


def _compute_idempotency_key(policy_number: str, date_of_loss: str, claim_amount: float) -> str:
    """Deterministic key used to detect duplicate claim submissions."""
    raw = f"{policy_number}|{date_of_loss}|{claim_amount:.2f}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


class InsuranceClaim(BaseModel):
    # --- identity ---
    claim_id: str = Field(default_factory=lambda: f"CLM-{uuid.uuid4().hex[:12].upper()}")
    schema_version: str = Field(default=SCHEMA_VERSION)
    # Computed at construction; used downstream for dedup detection
    idempotency_key: Optional[str] = None
    # End-to-end correlation ID — links every log, trace span, Kafka header, and DB row
    correlation_id: str = Field(default_factory=lambda: uuid.uuid4().hex)

    # --- PII fields (DataClass.PII) ---
    claimant_name: str          # PII: direct identifier
    vehicle_vin: Optional[str] = None      # PII: links to a person's vehicle
    property_address: Optional[str] = None # PII: physical location

    # --- SENSITIVE fields ---
    policy_number: str          # SENSITIVE: ties to financial contract
    claim_amount: float = Field(gt=0)      # SENSITIVE
    diagnosis_code: Optional[str] = None   # SENSITIVE: medical data

    # --- INTERNAL fields ---
    claim_type: ClaimType
    date_of_loss: str
    date_filed: str = Field(default_factory=lambda: date.today().isoformat())
    description: str = ""
    status: ClaimStatus = ClaimStatus.SUBMITTED
    provider_name: Optional[str] = None

    # Processing fields
    fraud_score: float = 0.0
    risk_level: RiskLevel = RiskLevel.LOW
    validation_errors: list = Field(default_factory=list)
    enrichment_data: dict = Field(default_factory=dict)
    processing_metadata: dict = Field(default_factory=dict)

    # Tracing
    trace_id: Optional[str] = None
    span_id: Optional[str] = None

    # Audit trail — append-only list recording every state transition
    audit_trail: list = Field(default_factory=list)

    def record_audit_event(self, stage: str, status: str, detail: str = ""):
        """Append an immutable audit entry with timestamp."""
        self.audit_trail.append({
            "stage": stage,
            "status": status,
            "detail": detail,
            "timestamp": datetime.utcnow().isoformat(),
            "correlation_id": self.correlation_id,
        })

    def model_post_init(self, __context) -> None:
        if self.idempotency_key is None:
            object.__setattr__(
                self,
                'idempotency_key',
                _compute_idempotency_key(self.policy_number, self.date_of_loss, self.claim_amount)
            )

    @field_validator('claim_amount')
    @classmethod
    def validate_amount(cls, v):
        if v > 10_000_000:
            raise ValueError('Claim amount exceeds maximum threshold of $10M')
        return round(v, 2)

    def to_kafka_dict(self) -> dict:
        data = self.model_dump()
        data['timestamp'] = datetime.utcnow().isoformat()
        data['schema_version'] = self.schema_version
        return data

    @classmethod
    def from_kafka_dict(cls, data: dict) -> 'InsuranceClaim':
        data.pop('timestamp', None)
        return cls(**data)


class ClaimProcessingResult(BaseModel):
    claim_id: str
    stage: str
    status: str
    latency_ms: float
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    errors: list = Field(default_factory=list)
    metadata: dict = Field(default_factory=dict)


class PipelineMetric(BaseModel):
    metric_name: str
    value: float
    labels: dict = Field(default_factory=dict)
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())