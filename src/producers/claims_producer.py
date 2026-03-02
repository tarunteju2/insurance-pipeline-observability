"""
Insurance claims producer - generates realistic fake insurance claims
and publishes them to Kafka for processing.
"""

import json
import time
import random
import uuid
from datetime import datetime, timedelta
from typing import List

from faker import Faker
from confluent_kafka import Producer
import structlog

from src.config import config
from src.models.claims import InsuranceClaim, ClaimType, ClaimStatus
from src.observability.tracing import get_tracer, PipelineSpanContext
from src.observability.metrics import (
    CLAIMS_RECEIVED_TOTAL, KAFKA_MESSAGES_PRODUCED,
    CLAIMS_AMOUNT_DISTRIBUTION
)

logger = structlog.get_logger(__name__)
fake = Faker()

# Medical diagnosis codes used in health claims
DIAGNOSIS_CODES = [
    "M54.5", "J06.9", "R10.9", "K21.0", "I10", "E11.9",
    "S62.0", "S82.0", "S52.5", "T14.9", "Z96.1", "M17.1"
]

# Common healthcare providers
HEALTH_PROVIDERS = [
    "St. Mary's Medical Center", "Memorial Hospital", "CityHealth Clinic",
    "Pacific Northwest Medical", "Sunrise Healthcare", "MetroMed Associates",
    "Atlantic General Hospital", "Valley Medical Group"
]

# Realistic descriptions for auto claims
AUTO_DESCRIPTIONS = [
    "Rear-end collision at intersection",
    "Side-swipe in parking lot",
    "Single vehicle accident - hit guardrail",
    "Multi-vehicle pile-up on highway",
    "Fender bender in drive-through",
    "Hail damage to vehicle exterior",
    "Windshield cracked by road debris",
    "Vehicle vandalism in parking garage"
]

# Property claim scenarios
PROPERTY_DESCRIPTIONS = [
    "Water damage from burst pipe",
    "Fire damage in kitchen area",
    "Storm damage to roof shingles",
    "Theft of personal electronics",
    "Tree fell on garage structure",
    "Flooding in basement after heavy rain",
    "Lightning strike damaged electrical system",
    "Vandalism to exterior of property"
]

# Health insurance claim reasons
HEALTH_DESCRIPTIONS = [
    "Emergency room visit for acute condition",
    "Scheduled surgical procedure",
    "Physical therapy sessions after injury",
    "Diagnostic imaging and lab work",
    "Specialist consultation and follow-up",
    "Hospital admission for observation",
    "Prescription medication coverage",
    "Ambulance transport to hospital"
]


class InsuranceClaimsProducer:
    """Produces realistic insurance claims to Kafka topics."""

    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': config.kafka.bootstrap_servers,
            'client.id': 'insurance-claims-producer',
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 500,
        })
        self.tracer = get_tracer("claims-producer")
        self.topic = config.kafka.topics["raw"]

    def _delivery_report(self, err, msg):
        """Kafka delivery callback."""
        if err:
            logger.error("Message delivery failed", error=str(err), topic=msg.topic())
        else:
            KAFKA_MESSAGES_PRODUCED.labels(topic=msg.topic()).inc()

    def generate_claim(self) -> InsuranceClaim:
        """Generate a single realistic insurance claim."""
        claim_type = random.choice(list(ClaimType))
        claim_id = f"CLM-{uuid.uuid4().hex[:12].upper()}"
        policy_prefix = {"auto": "AUT", "health": "HLT", "property": "PRP",
                         "life": "LIF", "liability": "LIA", "workers_comp": "WCM"}
        policy_number = f"{policy_prefix.get(claim_type.value, 'GEN')}-{random.randint(100000, 999999)}"

        # Vary amounts by claim type
        amount_ranges = {
            ClaimType.AUTO: (500, 75000),
            ClaimType.HEALTH: (200, 250000),
            ClaimType.PROPERTY: (1000, 500000),
            ClaimType.LIFE: (10000, 1000000),
            ClaimType.LIABILITY: (5000, 200000),
            ClaimType.WORKERS_COMP: (2000, 150000),
        }
        min_amt, max_amt = amount_ranges.get(claim_type, (100, 100000))
        claim_amount = round(random.uniform(min_amt, max_amt), 2)

        # Occasionally generate suspicious claims (for fraud detection)
        is_suspicious = random.random() < 0.15
        if is_suspicious:
            claim_amount *= random.uniform(2.5, 5.0)
            claim_amount = min(claim_amount, 9_999_999)

        date_of_loss = fake.date_between(start_date='-90d', end_date='today')

        claim = InsuranceClaim(
            claim_id=claim_id,
            policy_number=policy_number,
            claimant_name=fake.name(),
            claim_type=claim_type,
            claim_amount=round(claim_amount, 2),
            date_of_loss=date_of_loss.isoformat(),
            date_filed=datetime.now().strftime("%Y-%m-%d"),
            description=self._get_description(claim_type),
            status=ClaimStatus.SUBMITTED,
            provider_name=random.choice(HEALTH_PROVIDERS) if claim_type == ClaimType.HEALTH else None,
            diagnosis_code=random.choice(DIAGNOSIS_CODES) if claim_type == ClaimType.HEALTH else None,
            vehicle_vin=fake.bothify('???########???##') if claim_type == ClaimType.AUTO else None,
            property_address=fake.address() if claim_type == ClaimType.PROPERTY else None,
        )

        # Inject some invalid claims for testing validation
        if random.random() < 0.05:
            claim.policy_number = ""  # Invalid: empty policy
        if random.random() < 0.03:
            claim.claim_amount = -100  # Invalid: negative amount (will be caught)

        return claim

    def _get_description(self, claim_type: ClaimType) -> str:
        descriptions = {
            ClaimType.AUTO: AUTO_DESCRIPTIONS,
            ClaimType.HEALTH: HEALTH_DESCRIPTIONS,
            ClaimType.PROPERTY: PROPERTY_DESCRIPTIONS,
        }
        desc_list = descriptions.get(claim_type, ["General insurance claim filed"])
        return random.choice(desc_list)

    def produce_claim(self, claim: InsuranceClaim) -> bool:
        """Produce a single claim to Kafka."""
        span_ctx = PipelineSpanContext(
            self.tracer, "produce_claim",
            claim_id=claim.claim_id,
            claim_type=claim.claim_type.value,
            stage="ingestion"
        )
        with span_ctx as (span, ctx):
            try:
                # Add trace context to claim
                span_context = span.get_span_context()
                claim.trace_id = format(span_context.trace_id, '032x')
                claim.processing_metadata['produced_at'] = datetime.utcnow().isoformat()
                claim.processing_metadata['producer_id'] = 'insurance-claims-producer'

                value = json.dumps(claim.to_kafka_dict()).encode('utf-8')

                self.producer.produce(
                    topic=self.topic,
                    key=claim.claim_id.encode('utf-8'),
                    value=value,
                    callback=self._delivery_report
                )
                self.producer.poll(0)

                CLAIMS_RECEIVED_TOTAL.labels(
                    claim_type=claim.claim_type.value,
                    source="producer"
                ).inc()
                CLAIMS_AMOUNT_DISTRIBUTION.labels(
                    claim_type=claim.claim_type.value
                ).observe(claim.claim_amount)

                span.set_attribute("kafka.topic", self.topic)
                span.set_attribute("claim.amount", claim.claim_amount)

                logger.info("Claim produced",
                            claim_id=claim.claim_id,
                            claim_type=claim.claim_type.value,
                            amount=claim.claim_amount)
                return True
            except Exception as e:
                logger.error("Failed to produce claim", error=str(e), claim_id=claim.claim_id)
                return False

    def produce_batch(self, count: int = 10) -> List[str]:
        """Produce a batch of claims."""
        produced_ids = []
        for i in range(count):
            claim = self.generate_claim()
            if self.produce_claim(claim):
                produced_ids.append(claim.claim_id)

        self.producer.flush(timeout=10)
        logger.info(f"Batch produced: {len(produced_ids)}/{count} claims")
        return produced_ids

    def close(self):
        """Flush and close producer."""
        self.producer.flush(timeout=30)
        logger.info("Claims producer closed")


def run_continuous_producer(claims_per_second: float = 2.0, duration_seconds: int = 60):
    """Run the producer continuously for a specified duration."""
    producer = InsuranceClaimsProducer()
    interval = 1.0 / claims_per_second
    end_time = time.time() + duration_seconds
    total = 0

    logger.info("Starting continuous producer",
                rate=claims_per_second, duration=duration_seconds)

    try:
        while time.time() < end_time:
            claim = producer.generate_claim()
            producer.produce_claim(claim)
            total += 1
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Producer interrupted")
    finally:
        producer.close()
        logger.info(f"Producer finished. Total claims produced: {total}")


if __name__ == "__main__":
    from src.observability.tracing import init_tracing
    from src.observability.metrics import start_metrics_server

    init_tracing("claims-producer")
    start_metrics_server(8001)
    run_continuous_producer(claims_per_second=2, duration_seconds=300)