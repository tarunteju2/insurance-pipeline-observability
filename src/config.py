"""
Central configuration for Insurance Pipeline Observability Platform.
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


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
    use_ssl: bool = False


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
class PipelineConfig:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    minio: MinIOConfig = field(default_factory=MinIOConfig)
    jaeger: JaegerConfig = field(default_factory=JaegerConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)
    batch_size: int = int(os.getenv("CLAIMS_BATCH_SIZE", "100"))
    processing_interval: int = int(os.getenv("PROCESSING_INTERVAL_SECONDS", "5"))


# Initialize config once at startup for use throughout the app
config = PipelineConfig()