"""
Metrics collected from the insurance claims pipeline.

Tracks claims as they flow through each stage, fraud scores,
errors, latency, component health, and more.
"""

from prometheus_client import (
    Counter, Histogram, Gauge, Summary, Info,
    CollectorRegistry, generate_latest, start_http_server,
    CONTENT_TYPE_LATEST
)
import structlog

logger = structlog.get_logger(__name__)

# Custom registry to avoid conflicts with other prometheus instances in the process
REGISTRY = CollectorRegistry()

# Track claims as they move through the pipeline

CLAIMS_RECEIVED_TOTAL = Counter(
    'insurance_claims_received_total',
    'Total number of claims that came in',
    ['claim_type', 'source'],
    registry=REGISTRY
)

CLAIMS_PROCESSED_TOTAL = Counter(
    'insurance_claims_processed_total',
    'Total claims through each pipeline stage',
    ['stage', 'status', 'claim_type'],
    registry=REGISTRY
)

CLAIMS_PROCESSING_DURATION = Histogram(
    'insurance_claims_processing_duration_seconds',
    'How long claims take at each processing stage',
    ['stage', 'claim_type'],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
    registry=REGISTRY
)

CLAIMS_AMOUNT_DISTRIBUTION = Histogram(
    'insurance_claims_amount_dollars',
    'Histogram of claim amounts',
    ['claim_type'],
    buckets=[100, 500, 1000, 5000, 10000, 25000, 50000, 100000, 500000, 1000000],
    registry=REGISTRY
)

# Monitor fraud detection performance

FRAUD_SCORE_DISTRIBUTION = Histogram(
    'insurance_fraud_score_distribution',
    'Distribution of fraud scores',
    ['claim_type'],
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
    registry=REGISTRY
)

FRAUD_FLAGGED_TOTAL = Counter(
    'insurance_fraud_flagged_total',
    'Total claims flagged for potential fraud',
    ['claim_type', 'risk_level'],
    registry=REGISTRY
)

# Monitor overall pipeline performance and health

PIPELINE_STAGE_LATENCY = Summary(
    'insurance_pipeline_stage_latency_ms',
    'Pipeline stage latency in milliseconds',
    ['stage'],
    registry=REGISTRY
)

PIPELINE_THROUGHPUT = Gauge(
    'insurance_pipeline_throughput_per_second',
    'Current pipeline throughput (claims/second)',
    ['stage'],
    registry=REGISTRY
)

PIPELINE_QUEUE_DEPTH = Gauge(
    'insurance_pipeline_queue_depth',
    'Number of claims waiting in each stage queue',
    ['stage', 'topic'],
    registry=REGISTRY
)

PIPELINE_ERROR_TOTAL = Counter(
    'insurance_pipeline_errors_total',
    'Total pipeline errors',
    ['stage', 'error_type'],
    registry=REGISTRY
)

VALIDATION_ERROR_CODES_TOTAL = Counter(
    'insurance_validation_error_codes_total',
    'Total validation errors by code',
    ['error_code', 'field', 'claim_type'],
    registry=REGISTRY
)

PIPELINE_COMPONENT_STATUS = Gauge(
    'insurance_pipeline_component_status',
    'Pipeline component health (1=healthy, 0=down)',
    ['component'],
    registry=REGISTRY
)

# ==================== KAFKA METRICS ====================

KAFKA_MESSAGES_PRODUCED = Counter(
    'insurance_kafka_messages_produced_total',
    'Total Kafka messages produced',
    ['topic'],
    registry=REGISTRY
)

KAFKA_MESSAGES_CONSUMED = Counter(
    'insurance_kafka_messages_consumed_total',
    'Total Kafka messages consumed',
    ['topic', 'consumer_group'],
    registry=REGISTRY
)

KAFKA_CONSUMER_LAG = Gauge(
    'insurance_kafka_consumer_lag',
    'Kafka consumer lag',
    ['topic', 'partition'],
    registry=REGISTRY
)

# ==================== LINEAGE METRICS ====================

LINEAGE_EVENTS_RECORDED = Counter(
    'insurance_lineage_events_recorded_total',
    'Total lineage events recorded',
    ['edge_type'],
    registry=REGISTRY
)

LINEAGE_COVERAGE_PERCENT = Gauge(
    'insurance_lineage_coverage_percent',
    'Percentage of pipeline with lineage tracking',
    registry=REGISTRY
)

# ==================== S3/MINIO METRICS ====================

S3_OBJECTS_WRITTEN = Counter(
    'insurance_s3_objects_written_total',
    'Total objects written to S3',
    ['bucket', 'prefix'],
    registry=REGISTRY
)

S3_BYTES_WRITTEN = Counter(
    'insurance_s3_bytes_written_total',
    'Total bytes written to S3',
    ['bucket', 'prefix'],
    registry=REGISTRY
)

# ==================== PIPELINE INFO ====================

PIPELINE_INFO = Info(
    'insurance_pipeline',
    'Insurance pipeline information',
    registry=REGISTRY
)
PIPELINE_INFO.info({
    'version': '1.0.0',
    'environment': 'development',
    'pipeline_name': 'insurance-claims-observability'
})


def start_metrics_server(port: int = 8000):
    """Start Prometheus metrics HTTP server."""
    start_http_server(port, registry=REGISTRY)
    logger.info("Prometheus metrics server started", port=port)


def get_metrics_output() -> bytes:
    """Get current metrics in Prometheus format."""
    return generate_latest(REGISTRY)