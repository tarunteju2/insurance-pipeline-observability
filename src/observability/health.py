"""
Health checks for pipeline components.

Monitors Kafka, Postgres, MinIO, Jaeger, and other services.
Tracks latency and marks components as healthy, degraded, or down.
"""

import time
from datetime import datetime, timedelta
from typing import Dict, Optional
from dataclasses import dataclass, field

import structlog

from src.config import config
from src.observability.metrics import PIPELINE_COMPONENT_STATUS

logger = structlog.get_logger(__name__)


@dataclass
class ComponentHealth:
    name: str
    # Status can be: healthy, degraded, down, or unknown
    status: str = "unknown"
    last_heartbeat: Optional[datetime] = None
    latency_ms: float = 0.0
    error_rate: float = 0.0
    throughput: float = 0.0
    message: str = ""
    metadata: dict = field(default_factory=dict)


class PipelineHealthMonitor:
    """Monitors health of all pipeline components."""

    def __init__(self):
        self.components: Dict[str, ComponentHealth] = {}
        self._init_components()

    def _init_components(self):
        """Set up monitoring for all key pipeline components."""
        component_names = [
            "kafka_broker", "claims_producer", "claims_validator",
            "fraud_detector", "claims_enricher", "postgres_db",
            "minio_s3", "jaeger_tracing", "prometheus_metrics",
            "lineage_tracker"
        ]
        for name in component_names:
            self.components[name] = ComponentHealth(name=name)

    def update_component(self, name: str, status: str, latency_ms: float = 0,
                         error_rate: float = 0, throughput: float = 0,
                         message: str = "", **kwargs):
        """Update health status of a component."""
        if name not in self.components:
            self.components[name] = ComponentHealth(name=name)

        comp = self.components[name]
        comp.status = status
        comp.last_heartbeat = datetime.utcnow()
        comp.latency_ms = latency_ms
        comp.error_rate = error_rate
        comp.throughput = throughput
        comp.message = message
        comp.metadata.update(kwargs)

        # Update the metric gauge to reflect health status (1.0 = healthy, 0.5 = degraded, 0.0 = down)
        status_value = 1.0 if status == "healthy" else (0.5 if status == "degraded" else 0.0)
        PIPELINE_COMPONENT_STATUS.labels(component=name).set(status_value)

    def check_kafka(self) -> ComponentHealth:
        """Hit the Kafka broker and make sure it's responding."""
        start = time.time()
        try:
            from confluent_kafka.admin import AdminClient
            admin = AdminClient({'bootstrap.servers': config.kafka.bootstrap_servers})
            cluster_meta = admin.list_topics(timeout=5)
            latency = (time.time() - start) * 1000
            topic_count = len(cluster_meta.topics)
            self.update_component(
                "kafka_broker", "healthy", latency_ms=latency,
                message=f"{topic_count} topics available"
            )
        except Exception as e:
            latency = (time.time() - start) * 1000
            self.update_component("kafka_broker", "down", latency_ms=latency, message=str(e))
        return self.components["kafka_broker"]

    def check_postgres(self) -> ComponentHealth:
        """Check if PostgreSQL is up and responding to connections."""
        start = time.time()
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=config.postgres.host,
                port=config.postgres.port,
                database=config.postgres.database,
                user=config.postgres.user,
                password=config.postgres.password,
                connect_timeout=5
            )
            conn.close()
            latency = (time.time() - start) * 1000
            self.update_component("postgres_db", "healthy", latency_ms=latency)
        except Exception as e:
            latency = (time.time() - start) * 1000
            self.update_component("postgres_db", "down", latency_ms=latency, message=str(e))
        return self.components["postgres_db"]

    def check_minio(self) -> ComponentHealth:
        """Test MinIO S3 connectivity."""
        start = time.time()
        try:
            import boto3
            from botocore.client import Config as BotoConfig
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{config.minio.endpoint}",
                aws_access_key_id=config.minio.access_key,
                aws_secret_access_key=config.minio.secret_key,
                config=BotoConfig(signature_version='s3v4'),
                region_name='us-east-1'
            )
            s3.head_bucket(Bucket=config.minio.bucket)
            latency = (time.time() - start) * 1000
            self.update_component("minio_s3", "healthy", latency_ms=latency)
        except Exception as e:
            latency = (time.time() - start) * 1000
            self.update_component("minio_s3", "down", latency_ms=latency, message=str(e))
        return self.components["minio_s3"]

    def check_jaeger(self) -> ComponentHealth:
        """Check if Jaeger tracing is running."""
        start = time.time()
        try:
            import httpx
            # Try to reach Jaeger API - first try localhost, then Docker network
            try:
                resp = httpx.get("http://localhost:16686/api/services", timeout=2)
            except (httpx.ConnectError, httpx.TimeoutException):
                # If localhost fails, Jaeger is likely running in Docker
                resp = httpx.get("http://jaeger:16686/api/services", timeout=2)
            
            latency = (time.time() - start) * 1000
            status = "healthy" if resp.status_code == 200 else "degraded"
            self.update_component("jaeger_tracing", status, latency_ms=latency)
        except Exception as e:
            latency = (time.time() - start) * 1000
            self.update_component("jaeger_tracing", "down", latency_ms=latency, message=str(e))
        return self.components["jaeger_tracing"]

    def run_all_checks(self) -> Dict[str, ComponentHealth]:
        """Run all health checks and return the results."""
        self.check_kafka()
        self.check_postgres()
        self.check_minio()
        self.check_jaeger()
        return self.components

    def get_overall_status(self) -> str:
        """
        Determine if the pipeline is healthy overall.
        
        Returns "healthy" if all components are ok,
        "degraded" if something is down but not critical,
        "unknown" if nothing has been checked yet.
        """
        statuses = [c.status for c in self.components.values() if c.last_heartbeat]
        if not statuses:
            return "unknown"
        if all(s == "healthy" for s in statuses):
            return "healthy"
        if any(s == "down" for s in statuses):
            return "degraded"
        return "degraded"

    def get_health_report(self) -> dict:
        """Generate a full health report for all components."""
        self.run_all_checks()
        return {
            "overall_status": self.get_overall_status(),
            "timestamp": datetime.utcnow().isoformat(),
            "components": {
                name: {
                    "status": comp.status,
                    "latency_ms": round(comp.latency_ms, 2),
                    "last_heartbeat": comp.last_heartbeat.isoformat() if comp.last_heartbeat else None,
                    "error_rate": comp.error_rate,
                    "throughput": comp.throughput,
                    "message": comp.message,
                }
                for name, comp in self.components.items()
            }
        }


# Global health monitor singleton
health_monitor = PipelineHealthMonitor()