"""
Main stream processor - orchestrates Kafka consumption and processing pipeline.

Phase 2 additions
-----------------
- Schema version validation at ingestion (unknown versions → DLQ)
- Idempotency / duplicate detection via ClaimIdempotencyRecord
- Circuit breakers around PostgreSQL and MinIO (via module-level singletons)
- Exponential back-off with jitter for transient downstream failures
- Data Quality (DQ) scorecard computed per batch and published to Prometheus
- Correlation ID propagation through Kafka headers, logs, and traces
- Graceful shutdown with in-flight drain and offset commit
- Batch PostgreSQL writes (configurable flush interval)
- Consumer lag monitoring via committed vs latest offsets
- Structured audit trail per claim state transition
"""

import json
import time
import random
import signal
import threading
from datetime import datetime, date
from typing import Optional, List

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException, TopicPartition
import boto3
from botocore.client import Config as BotoConfig
import structlog

from src.config import config
from src.models.claims import InsuranceClaim, ClaimStatus, SCHEMA_VERSION
from src.processors.claims_validator import ClaimsValidator
from src.processors.fraud_detector import FraudDetector
from src.processors.claims_enricher import ClaimsEnricher
from src.observability.tracing import get_tracer, init_tracing
from src.observability.pii_masking import mask_claim_for_logging
from src.observability.circuit_breaker import postgres_breaker, minio_breaker, CircuitBreakerOpen
from src.observability.metrics import (
    KAFKA_MESSAGES_CONSUMED, KAFKA_MESSAGES_PRODUCED,
    PIPELINE_THROUGHPUT, S3_OBJECTS_WRITTEN, S3_BYTES_WRITTEN,
    CLAIMS_PROCESSED_TOTAL, start_metrics_server, KAFKA_CONSUMER_LAG,
    DUPLICATE_CLAIMS_DETECTED, SCHEMA_VERSION_COUNTER, SCHEMA_VALIDATION_ERRORS,
    DQ_COMPLETENESS, DQ_VALIDITY, DQ_TIMELINESS, DQ_CONSISTENCY, DQ_OVERALL_SCORE,
    CIRCUIT_BREAKER_REJECTED_CALLS
)
from src.lineage.tracker import lineage_tracker
from src.lineage.models import ProcessedClaim, ClaimIdempotencyRecord, get_session

logger = structlog.get_logger(__name__)

# Supported schema versions — any version not in this set is routed to DLQ
SUPPORTED_SCHEMA_VERSIONS = {"v1"}

# Exponential back-off config for transient failures
_BACKOFF_BASE = 0.5      # seconds
_BACKOFF_MAX = 30.0      # seconds cap
_BACKOFF_RETRIES = 4     # attempts before giving up

# Batch flush config
_BATCH_PG_FLUSH_SIZE = int(config.batch_size if hasattr(config, 'batch_size') else 50)
_BATCH_PG_FLUSH_INTERVAL_S = 5  # seconds


def _exponential_backoff_call(fn, *args, max_retries=_BACKOFF_RETRIES, **kwargs):
    """
    Retry *fn* up to max_retries times with exponential back-off + full jitter.
    Raises the last exception if all retries are exhausted.
    """
    last_exc = None
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except CircuitBreakerOpen:
            raise  # never retry a tripped circuit
        except Exception as exc:
            last_exc = exc
            sleep_time = min(_BACKOFF_MAX, _BACKOFF_BASE * (2 ** attempt))
            jitter = random.uniform(0, sleep_time)
            logger.warning("Transient failure — retrying",
                           attempt=attempt + 1, max_retries=max_retries,
                           sleep_s=round(jitter, 2), error=str(exc))
            time.sleep(jitter)
    raise last_exc


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
            'enable.auto.commit': False,   # manual commit for graceful drain
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
        self._draining = False
        self._pg_batch: List[InsuranceClaim] = []
        self._pg_batch_lock = threading.Lock()
        self._last_pg_flush = time.time()

    # ------------------------------------------------------------------ #
    #  Kafka produce with correlation_id header
    # ------------------------------------------------------------------ #
    def _produce_to_topic(self, topic: str, claim: InsuranceClaim):
        """Send a processed claim to a specific Kafka topic with correlation_id header."""
        value = json.dumps(claim.to_kafka_dict()).encode('utf-8')
        headers = [
            ('correlation_id', claim.correlation_id.encode('utf-8')),
            ('schema_version', claim.schema_version.encode('utf-8')),
        ]
        self.producer.produce(
            topic=topic,
            key=claim.claim_id.encode('utf-8'),
            value=value,
            headers=headers,
        )
        self.producer.poll(0)
        KAFKA_MESSAGES_PRODUCED.labels(topic=topic).inc()

    def _store_to_s3(self, claim: InsuranceClaim, prefix: str):
        """Store processed claim to S3/MinIO with circuit breaker + retry."""
        def _do_put():
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

        try:
            minio_breaker.call(_exponential_backoff_call, _do_put)
        except CircuitBreakerOpen:
            CIRCUIT_BREAKER_REJECTED_CALLS.labels(dependency="minio").inc()
            logger.warning("MinIO circuit OPEN — skipping S3 write", claim_id=claim.claim_id)
        except Exception as e:
            logger.error("S3 storage failed after retries", error=str(e), claim_id=claim.claim_id)

    def _store_to_postgres(self, claim: InsuranceClaim):
        """Buffer claim for batch PostgreSQL write; flushes when buffer is full or timer fires."""
        with self._pg_batch_lock:
            self._pg_batch.append(claim)
            if len(self._pg_batch) >= _BATCH_PG_FLUSH_SIZE:
                self._flush_pg_batch()

    def _flush_pg_batch(self):
        """Flush buffered claims to PostgreSQL in a single transaction."""
        with self._pg_batch_lock:
            if not self._pg_batch:
                return
            batch = list(self._pg_batch)
            self._pg_batch.clear()

        def _do_batch_upsert():
            session = get_session()
            try:
                for claim in batch:
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
                logger.debug("Batch PG flush complete", count=len(batch))
                for claim in batch:
                    lineage_tracker.record_event(
                        source_node_id="tx_enrichment",
                        target_node_id="sink_postgres",
                        claim_id=claim.claim_id,
                        latency_ms=0,
                        status="success"
                    )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()

        try:
            postgres_breaker.call(_exponential_backoff_call, _do_batch_upsert)
        except CircuitBreakerOpen:
            CIRCUIT_BREAKER_REJECTED_CALLS.labels(dependency="postgres").inc()
            logger.warning("Postgres circuit OPEN — skipping batch PG write", count=len(batch))
        except Exception as e:
            logger.error("Batch PostgreSQL write failed", error=str(e), count=len(batch))

    def process_claim(self, raw_data: dict) -> Optional[InsuranceClaim]:
        """Process a single claim through the entire pipeline."""
        pipeline_start = time.time()

        with self.tracer.start_as_current_span("process_claim_pipeline") as span:
            try:
                # ── SCHEMA VERSION CHECK ──────────────────────────────────────
                incoming_version = raw_data.get("schema_version", "unknown")
                SCHEMA_VERSION_COUNTER.labels(
                    schema_version=incoming_version,
                    topic=config.kafka.topics["raw"]
                ).inc()
                if incoming_version not in SUPPORTED_SCHEMA_VERSIONS:
                    SCHEMA_VALIDATION_ERRORS.labels(
                        reason="unsupported_schema_version",
                        topic=config.kafka.topics["raw"]
                    ).inc()
                    logger.warning("Unsupported schema version — routing to DLQ",
                                   schema_version=incoming_version,
                                   claim_id=raw_data.get("claim_id", "unknown"))
                    try:
                        claim = InsuranceClaim.from_kafka_dict(raw_data)
                    except Exception:
                        return None
                    claim.processing_metadata['dlq_reason'] = 'unsupported_schema_version'
                    claim.processing_metadata['received_schema_version'] = incoming_version
                    claim.processing_metadata['supported_versions'] = list(SUPPORTED_SCHEMA_VERSIONS)
                    claim.record_audit_event("ingestion", "rejected", "unsupported_schema_version")
                    self._produce_to_topic(config.kafka.topics["dlq"], claim)
                    return claim
                # ─────────────────────────────────────────────────────────────

                claim = InsuranceClaim.from_kafka_dict(raw_data)

                # Bind correlation_id to every log line and trace span for this claim
                bound_log = logger.bind(correlation_id=claim.correlation_id, claim_id=claim.claim_id)
                span.set_attribute("claim.id", claim.claim_id)
                span.set_attribute("claim.type", claim.claim_type.value)
                span.set_attribute("claim.amount", claim.claim_amount)
                span.set_attribute("claim.correlation_id", claim.correlation_id)

                claim.record_audit_event("ingestion", "received")

                # ── IDEMPOTENCY / DUPLICATE CHECK ────────────────────────────
                if claim.idempotency_key:
                    session = get_session()
                    try:
                        existing_idem = session.query(ClaimIdempotencyRecord).filter_by(
                            idempotency_key=claim.idempotency_key
                        ).first()
                        if existing_idem:
                            DUPLICATE_CLAIMS_DETECTED.labels(
                                claim_type=claim.claim_type.value
                            ).inc()
                            existing_idem.processing_count += 1
                            session.commit()
                            bound_log.info("Duplicate claim detected — skipping",
                                        original_claim_id=existing_idem.claim_id,
                                        idempotency_key=claim.idempotency_key)
                            claim.record_audit_event("ingestion", "duplicate_skipped")
                            span.set_attribute("pipeline.result", "duplicate_skipped")
                            return claim
                        session.add(ClaimIdempotencyRecord(
                            idempotency_key=claim.idempotency_key,
                            claim_id=claim.claim_id,
                            policy_number=claim.policy_number,
                        ))
                        session.commit()
                    except Exception as e:
                        bound_log.warning("Idempotency check failed — proceeding", error=str(e))
                    finally:
                        session.close()
                # ─────────────────────────────────────────────────────────────

                # Stage 1: Validation
                is_valid, claim = self.validator.validate(claim)
                claim.record_audit_event("validation", "passed" if is_valid else "failed",
                                         f"errors={claim.validation_errors}")
                if not is_valid:
                    claim.processing_metadata['dlq_reason'] = 'validation_failed'
                    claim.processing_metadata['dlq_at'] = datetime.utcnow().isoformat()
                    claim.processing_metadata['dlq_error_codes'] = claim.processing_metadata.get(
                        'validation_error_codes', []
                    )
                    self._produce_to_topic(config.kafka.topics["dlq"], claim)
                    self._store_to_s3(claim, "rejected")
                    span.set_attribute("pipeline.result", "rejected_validation")
                    return claim

                self._produce_to_topic(config.kafka.topics["validated"], claim)
                self._store_to_s3(claim, "validated")

                # Stage 2: Fraud Detection
                fraud_score, claim = self.fraud_detector.score_claim(claim)
                claim.record_audit_event("fraud_scoring", "completed", f"score={fraud_score:.3f}")
                self._produce_to_topic(config.kafka.topics["scored"], claim)
                self._store_to_s3(claim, "scored")

                # Stage 3: Enrichment
                claim = self.enricher.enrich(claim)
                claim.record_audit_event("enrichment", "completed")
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
                claim.record_audit_event("storage", "completed",
                                         f"latency_ms={claim.processing_metadata['total_pipeline_latency_ms']}")
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
                masked = mask_claim_for_logging({"claim_id": raw_data.get("claim_id", "unknown")})
                logger.error("Pipeline processing failed", error=str(e), **masked)
                span.set_attribute("pipeline.result", "error")
                span.record_exception(e)
                return None

    def _update_consumer_lag(self):
        """Poll committed vs latest offsets and publish consumer lag to Prometheus."""
        try:
            assignment = self.consumer.assignment()
            if not assignment:
                return
            committed = self.consumer.committed(assignment, timeout=5)
            for tp in committed:
                if tp is None or tp.offset < 0:
                    continue
                # Get high watermark (latest offset) for each partition
                lo, hi = self.consumer.get_watermark_offsets(
                    TopicPartition(tp.topic, tp.partition), timeout=5
                )
                lag = max(0, hi - tp.offset)
                KAFKA_CONSUMER_LAG.labels(topic=tp.topic, partition=str(tp.partition)).set(lag)
        except Exception:
            pass  # lag reporting is best-effort

    def run(self, max_messages: int = None, timeout_seconds: int = None):
        """Run the stream processor, consuming from Kafka."""
        self.consumer.subscribe([config.kafka.topics["raw"]])
        self._running = True
        self._start_time = time.time()
        processed = 0

        # DQ scorecard accumulators — reset each batch
        _dq_total = 0
        _dq_valid = 0
        _dq_complete = 0
        _dq_timely = 0
        _dq_consistent = 0

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

                    # ── DQ scorecard accumulation ────────────────────────────
                    if result is not None:
                        _dq_total += 1
                        claim_type = result.claim_type.value if hasattr(result, 'claim_type') else "unknown"

                        # Validity: claim passed all rules
                        if result.status not in (ClaimStatus.VALIDATION_FAILED, ClaimStatus.REJECTED):
                            _dq_valid += 1

                        # Completeness: required fields non-null
                        required = ['policy_number', 'claimant_name', 'claim_type', 'claim_amount', 'date_of_loss']
                        if all(getattr(result, f, None) for f in required):
                            _dq_complete += 1

                        # Timeliness: filed within 30 days of loss
                        try:
                            loss = date.fromisoformat(result.date_of_loss)
                            filed = date.fromisoformat(result.date_filed)
                            if (filed - loss).days <= 30:
                                _dq_timely += 1
                        except (ValueError, TypeError):
                            pass

                        # Consistency: filed >= loss date
                        try:
                            loss = date.fromisoformat(result.date_of_loss)
                            filed = date.fromisoformat(result.date_filed)
                            if filed >= loss:
                                _dq_consistent += 1
                        except (ValueError, TypeError):
                            pass

                    processed += 1

                    # Publish DQ scorecard every 10 claims
                    if processed % 10 == 0 and _dq_total > 0:
                        ct = result.claim_type.value if result else "unknown"
                        completeness = _dq_complete / _dq_total
                        validity = _dq_valid / _dq_total
                        timeliness = _dq_timely / _dq_total
                        consistency = _dq_consistent / _dq_total
                        overall = (completeness * 0.35 + validity * 0.35
                                   + timeliness * 0.15 + consistency * 0.15)
                        DQ_COMPLETENESS.labels(claim_type=ct).set(completeness)
                        DQ_VALIDITY.labels(claim_type=ct).set(validity)
                        DQ_TIMELINESS.labels(claim_type=ct).set(timeliness)
                        DQ_CONSISTENCY.labels(claim_type=ct).set(consistency)
                        DQ_OVERALL_SCORE.labels(claim_type=ct).set(overall)
                        # Reset accumulators for next window
                        _dq_total = _dq_valid = _dq_complete = _dq_timely = _dq_consistent = 0

                    # Update throughput metric
                    elapsed = time.time() - self._start_time
                    if elapsed > 0:
                        PIPELINE_THROUGHPUT.labels(stage="overall").set(processed / elapsed)

                    if processed % 10 == 0:
                        logger.info(f"Processed {processed} claims",
                                    throughput=f"{processed/elapsed:.2f}/s")

                    # Periodic PG batch flush by timer
                    if time.time() - self._last_pg_flush > _BATCH_PG_FLUSH_INTERVAL_S:
                        self._flush_pg_batch()
                        self._last_pg_flush = time.time()

                    # Periodic consumer lag check
                    if processed % 20 == 0:
                        self._update_consumer_lag()

                    # Manual offset commit (graceful drain support)
                    if processed % 10 == 0:
                        try:
                            self.consumer.commit(asynchronous=False)
                        except Exception:
                            pass

                except json.JSONDecodeError as e:
                    logger.error("Failed to decode message", error=str(e))
                except Exception as e:
                    logger.error("Failed to process message", error=str(e))

        except KeyboardInterrupt:
            logger.info("Stream processor interrupted")
        finally:
            # ── GRACEFUL DRAIN ────────────────────────────────────────────
            self._draining = True
            logger.info("Draining in-flight work...")
            self._flush_pg_batch()
            self.producer.flush(timeout=10)
            try:
                self.consumer.commit(asynchronous=False)
            except Exception:
                pass
            self.consumer.close()
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