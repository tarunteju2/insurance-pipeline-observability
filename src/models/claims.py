"""
Insurance Claims data models with Pydantic validation.
"""

import uuid
from datetime import date, datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, field_validator


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


class InsuranceClaim(BaseModel):
    claim_id: str = Field(default_factory=lambda: f"CLM-{uuid.uuid4().hex[:12].upper()}")
    policy_number: str
    claimant_name: str
    claim_type: ClaimType
    claim_amount: float = Field(gt=0)
    date_of_loss: str
    date_filed: str = Field(default_factory=lambda: date.today().isoformat())
    description: str = ""
    status: ClaimStatus = ClaimStatus.SUBMITTED

    # These fields are populated based on the type of claim
    provider_name: Optional[str] = None  # Used for health insurance claims
    diagnosis_code: Optional[str] = None  # Health specific
    vehicle_vin: Optional[str] = None  # Auto insurance specific
    property_address: Optional[str] = None  # Property insurance specific

    # Processing fields
    fraud_score: float = 0.0
    risk_level: RiskLevel = RiskLevel.LOW
    validation_errors: list = Field(default_factory=list)
    enrichment_data: dict = Field(default_factory=dict)
    processing_metadata: dict = Field(default_factory=dict)

    # Tracing
    trace_id: Optional[str] = None
    span_id: Optional[str] = None

    @field_validator('claim_amount')
    @classmethod
    def validate_amount(cls, v):
        if v > 10_000_000:
            raise ValueError('Claim amount exceeds maximum threshold of $10M')
        return round(v, 2)

    def to_kafka_dict(self) -> dict:
        data = self.model_dump()
        data['timestamp'] = datetime.utcnow().isoformat()
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