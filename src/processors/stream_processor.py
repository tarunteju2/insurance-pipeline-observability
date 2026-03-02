"""
Main stream processor - orchestrates Kafka consumption and processing pipeline.
"""

import json
import time
from datetime import datetime
from typing import Optional

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import boto3
from botocore.client import Config as BotoConfig
import structlog

from src.config import config
from src.models.claims import InsuranceClaim, ClaimStatus
from src.processors.claims_validator import ClaimsValidator
from src.processors.fraud_detector import FraudDetector
from src.processors.claims_enricher import ClaimsEnricher
from src.observability.tracing import get_tracer, init_tracing
from src.observability.metrics import (
    KAFKA_MESSAGES_CONSUMED, KAFKA_MESSAGES_PRODUCED,
    PIPELINE_THROUGHPUT, S3_OBJECTS_WRITTEN, S3_BYTES_WRITTEN,
    CLAIMS_PROCESSED_TOTAL, start_metrics_server
)
from src.lineage.tracker import lineage_tracker
from src.lineage.models import ProcessedClaim, get_session

logger = structlog.get_logger(__name__)


class InsuranceClaimsStreamProcessor:
    """Main orchestrator for the insurance claims processing pipeline."""

    def __init__(self):
        self.validator = ClaimsValidator()
        self.fraud_detector = FraudDetector()
        self.enricher = ClaimsEnricher()
        self.tracer = get_tracer("stream-processor")

        # Subscribe to raw claims from Kafka
        self.consumer = Consumer({
            'bootstrap.servers': config.kafka.bootstrap_servers,
            'group.id': config.kafka.consumer_group,
            'auto.offset.reset': config.kafka.auto_offset_reset,
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
        })

        # Produce processed claims to intermediate topics
        self.producer = Producer({
            'bootstrap.servers': config.kafka.bootstrap_servers,
            'client.id': 'stream-processor',
        })

        # Connect to MinIO for long-term storage
        self.s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{config.minio.endpoint}",
            aws_access_key_id=config.minio.access_key,
            aws_secret_access_key=config.minio.secret_key,
            config=BotoConfig(signature_version='s3v4'),
            region_name='us-east-1'
        )

        self._running = False
        self._processed_count = 0
        self._start_time = None

    def _produce_to_topic(self, topic: str, claim: InsuranceClaim):
        """Send a processed claim to a specific Kafka topic."""
        value = json.dumps(claim.to_kafka_dict()).encode('utf-8')
        self.producer.produce(
            topic=topic,
            key=claim.claim_id.encode('utf-8'),
            value=value,
        )
        self.producer.poll(0)
        KAFKA_MESSAGES_PRODUCED.labels(topic=topic).inc()

    def _store_to_s3(self, claim: InsuranceClaim, prefix: str):
        """Store processed claim to S3/MinIO."""
        try:
            key = f"{prefix}/{datetime.utcnow().strftime('%Y/%m/%d')}/{claim.claim_id}.json"
            body = json.dumps(claim.to_kafka_dict(), indent=2)
            self.s3_client.put_object(
                Bucket=config.minio.bucket,
                Key=key,
                Body=body.encode('utf-8'),
                ContentType='application/json'
            )
            S3_OBJECTS_WRITTEN.labels(bucket=config.minio.bucket, prefix=prefix).inc()
            S3_BYTES_WRITTEN.labels(bucket=config.minio.bucket, prefix=prefix).inc(len(body))
            logger.debug("Stored to S3", key=key)
        except Exception as e:
            logger.error("S3 storage failed", error=str(e), claim_id=claim.claim_id)

    def _store_to_postgres(self, claim: InsuranceClaim):
        """Store final processed claim to PostgreSQL."""
        session = get_session()
        try:
            processed = ProcessedClaim(
                claim_id=claim.claim_id,
                policy_number=claim.policy_number,
                claimant_name=claim.claimant_name,
                claim_type=claim.claim_type.value,
                claim_amount=claim.claim_amount,
                date_of_loss=claim.date_of_loss,
                date_filed=claim.date_filed,
                description=claim.description,
                status=claim.status.value,
                provider_name=claim.provider_name,
                diagnosis_code=claim.diagnosis_code,
                vehicle_vin=claim.vehicle_vin,
                property_address=claim.property_address,
                fraud_score=claim.fraud_score,
                risk_level=claim.risk_level.value,
                validation_errors=claim.validation_errors,
                enrichment_data=claim.enrichment_data,
                processing_metadata=claim.processing_metadata,
                trace_id=claim.trace_id,
            )

            existing = session.query(ProcessedClaim).filter_by(claim_id=claim.claim_id).first()
            if existing:
                for key, value in processed.__dict__.items():
                    if not key.startswith('_'):
                        setattr(existing, key, value)
            else:
                session.add(processed)

            session.commit()
            logger.debug("Stored to PostgreSQL", claim_id=claim.claim_id)

            # Record lineage for storage
            lineage_tracker.record_event(
                source_node_id="tx_enrichment",
                target_node_id="sink_postgres",
                claim_id=claim.claim_id,
                latency_ms=0,
                status="success"
            )
        except Exception as e:
            session.rollback()
            logger.error("PostgreSQL storage failed", error=str(e))
        finally:
            session.close()

    def process_claim(self, raw_data: dict) -> Optional[InsuranceClaim]:
        """Process a single claim through the entire pipeline."""
        pipeline_start = time.time()

        with self.tracer.start_as_current_span("process_claim_pipeline") as span:
            try:
                # Deserialize
                claim = InsuranceClaim.from_kafka_dict(raw_data)
                span.set_attribute("claim.id", claim.claim_id)
                span.set_attribute("claim.type", claim.claim_type.value)
                span.set_attribute("claim.amount", claim.claim_amount)

                # Stage 1: Validation
                is_valid, claim = self.validator.validate(claim)
                if not is_valid:
                    self._produce_to_topic(config.kafka.topics["dlq"], claim)
                    self._store_to_s3(claim, "rejected")
                    span.set_attribute("pipeline.result", "rejected_validation")
                    return claim

                self._produce_to_topic(config.kafka.topics["validated"], claim)
                self._store_to_s3(claim, "validated")

                # Stage 2: Fraud Detection
                fraud_score, claim = self.fraud_detector.score_claim(claim)
                self._produce_to_topic(config.kafka.topics["scored"], claim)
                self._store_to_s3(claim, "scored")

                # Stage 3: Enrichment
                claim = self.enricher.enrich(claim)
                self._produce_to_topic(config.kafka.topics["enriched"], claim)
                self._store_to_s3(claim, "enriched")

                # Record final S3 lineage
                try:
                    lineage_tracker.record_event(
                        source_node_id="tx_enrichment",
                        target_node_id="sink_s3_datalake",
                        claim_id=claim.claim_id,
                        latency_ms=(time.time() - pipeline_start) * 1000,
                        status="success"
                    )
                except Exception:
                    pass

                # Stage 4: Final Storage
                claim.status = ClaimStatus.COMPLETED
                claim.processing_metadata['completed_at'] = datetime.utcnow().isoformat()
                claim.processing_metadata['total_pipeline_latency_ms'] = round(
                    (time.time() - pipeline_start) * 1000, 2
                )
                self._store_to_postgres(claim)

                CLAIMS_PROCESSED_TOTAL.labels(
                    stage="completed",
                    status="success",
                    claim_type=claim.claim_type.value
                ).inc()

                span.set_attribute("pipeline.result", "completed")
                span.set_attribute("pipeline.total_latency_ms",
                                   claim.processing_metadata['total_pipeline_latency_ms'])

                self._processed_count += 1
                return claim

            except Exception as e:
                logger.error("Pipeline processing failed", error=str(e))
                span.set_attribute("pipeline.result", "error")
                span.record_exception(e)
                return None

    def run(self, max_messages: int = None, timeout_seconds: int = None):
        """Run the stream processor, consuming from Kafka."""
        self.consumer.subscribe([config.kafka.topics["raw"]])
        self._running = True
        self._start_time = time.time()
        processed = 0

        logger.info("Stream processor started",
                     topic=config.kafka.topics["raw"],
                     max_messages=max_messages)

        try:
            while self._running:
                if max_messages and processed >= max_messages:
                    break
                if timeout_seconds and (time.time() - self._start_time) > timeout_seconds:
                    break

                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer error", error=msg.error())
                    continue

                KAFKA_MESSAGES_CONSUMED.labels(
                    topic=msg.topic(),
                    consumer_group=config.kafka.consumer_group
                ).inc()

                try:
                    raw_data = json.loads(msg.value().decode('utf-8'))
                    result = self.process_claim(raw_data)
                    processed += 1

                    # Update throughput metric
                    elapsed = time.time() - self._start_time
                    if elapsed > 0:
                        PIPELINE_THROUGHPUT.labels(stage="overall").set(processed / elapsed)

                    if processed % 10 == 0:
                        logger.info(f"Processed {processed} claims",
                                    throughput=f"{processed/elapsed:.2f}/s")

                except json.JSONDecodeError as e:
                    logger.error("Failed to decode message", error=str(e))
                except Exception as e:
                    logger.error("Failed to process message", error=str(e))

        except KeyboardInterrupt:
            logger.info("Stream processor interrupted")
        finally:
            self.consumer.close()
            self.producer.flush(timeout=10)
            self._running = False
            logger.info(f"Stream processor stopped. Total processed: {processed}")

    def stop(self):
        """Gracefully stop the processor."""
        self._running = False


def run_stream_processor(max_messages=None, timeout_seconds=None):
    """Entry point for running the stream processor."""
    init_tracing("insurance-stream-processor")
    start_metrics_server(8002)

    processor = InsuranceClaimsStreamProcessor()
    processor.run(max_messages=max_messages, timeout_seconds=timeout_seconds)


if __name__ == "__main__":
    run_stream_processor(timeout_seconds=300)