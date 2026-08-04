"""
Central configuration for Insurance Pipeline Observability Platform.
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Detect active environment (dev / staging / prod) and load the matching file
# ---------------------------------------------------------------------------
_ENV = os.getenv("APP_ENV", "dev").lower()
_env_file = os.path.join(os.path.dirname(__file__), "..", "config", f"{_ENV}.env")
if os.path.exists(_env_file):
    load_dotenv(_env_file, override=False)


@dataclass
class KafkaConfig:
    # Kafka broker connection details
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    # Topic names for each stage of claim processing
    topics: dict = field(default_factory=lambda: {
        "raw": "insurance.claims.raw",
        "validated": "insurance.claims.validated",
        "scored": "insurance.claims.scored",
        "enriched": "insurance.claims.enriched",
        "dlq": "insurance.claims.dlq",
        "lineage": "insurance.pipeline.lineage",
        "metrics": "insurance.pipeline.metrics",
    })
    consumer_group: str = "insurance-pipeline-group"
    auto_offset_reset: str = "earliest"


@dataclass
class PostgresConfig:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "insurance_lineage")
    user: str = os.getenv("POSTGRES_USER", "pipeline_admin")
    password: str = os.getenv("POSTGRES_PASSWORD", "securepass123")

    @property
    def connection_string(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class MinIOConfig:
    endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    bucket: str = os.getenv("MINIO_BUCKET", "insurance-claims-lake")
    use_ssl: bool = os.getenv("MINIO_USE_SSL", "false").lower() == "true"


@dataclass
class JaegerConfig:
    endpoint: str = os.getenv("JAEGER_ENDPOINT", "http://localhost:4317")
    service_name: str = "insurance-claims-pipeline"


@dataclass
class ObservabilityConfig:
    metrics_port: int = int(os.getenv("PROMETHEUS_PORT", "8000"))
    api_host: str = os.getenv("API_HOST", "0.0.0.0")
    api_port: int = int(os.getenv("API_PORT", "8080"))


@dataclass
class SLOConfig:
    """
    Service Level Objectives per pipeline stage.
    Prometheus recording rules evaluate these continually.
    Breach = page-worthy incident.
    """
    # Latency: P95 must be under these ms thresholds
    validation_latency_p95_ms: float = float(os.getenv("SLO_VALIDATION_LATENCY_P95_MS", "500"))
    fraud_latency_p95_ms: float = float(os.getenv("SLO_FRAUD_LATENCY_P95_MS", "1000"))
    enrichment_latency_p95_ms: float = float(os.getenv("SLO_ENRICHMENT_LATENCY_P95_MS", "2000"))
    pipeline_latency_p95_ms: float = float(os.getenv("SLO_PIPELINE_LATENCY_P95_MS", "5000"))

    # Error rate: must stay below this fraction per stage
    max_error_rate: float = float(os.getenv("SLO_MAX_ERROR_RATE", "0.05"))        # 5%
    max_dlq_rate: float = float(os.getenv("SLO_MAX_DLQ_RATE", "0.02"))            # 2%

    # Throughput: minimum claims/sec the pipeline must sustain
    min_throughput_per_sec: float = float(os.getenv("SLO_MIN_THROUGHPUT_PER_SEC", "1.0"))

    # Lineage: % of claims that must have full lineage coverage
    min_lineage_coverage_pct: float = float(os.getenv("SLO_MIN_LINEAGE_COVERAGE_PCT", "80.0"))

    # Data quality: minimum acceptable DQ scorecard scores (0–1)
    min_dq_completeness: float = float(os.getenv("SLO_MIN_DQ_COMPLETENESS", "0.95"))
    min_dq_validity: float = float(os.getenv("SLO_MIN_DQ_VALIDITY", "0.90"))
    min_dq_timeliness: float = float(os.getenv("SLO_MIN_DQ_TIMELINESS", "0.95"))


@dataclass
class PipelineConfig:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    minio: MinIOConfig = field(default_factory=MinIOConfig)
    jaeger: JaegerConfig = field(default_factory=JaegerConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)
    slo: SLOConfig = field(default_factory=SLOConfig)
    batch_size: int = int(os.getenv("CLAIMS_BATCH_SIZE", "100"))
    processing_interval: int = int(os.getenv("PROCESSING_INTERVAL_SECONDS", "5"))
    environment: str = _ENV


# Initialize config once at startup for use throughout the app
config = PipelineConfig()