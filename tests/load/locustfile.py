"""
Locust load testing harness for the Insurance Claims Pipeline.

Generates synthetic claims at configurable TPS, measures P50/P95/P99
latency, and verifies SLO compliance.

Usage
-----
  # Start Locust web UI (default http://localhost:8089)
  locust -f tests/load/locustfile.py --host http://localhost:8080

  # Headless run: 50 users, 5 users/sec spawn, run for 2 minutes
  locust -f tests/load/locustfile.py --host http://localhost:8080 \
         --headless -u 50 -r 5 --run-time 2m

  # Target the observability API
  locust -f tests/load/locustfile.py --host http://localhost:8082
"""

import json
import random
import uuid
from datetime import date, timedelta

from locust import HttpUser, task, between, events, tag

# ------------------------------------------------------------------ #
#  Synthetic claim generator (mirrors claims_producer logic)
# ------------------------------------------------------------------ #
CLAIM_TYPES = ["auto", "health", "property", "life", "liability", "workers_comp"]

POLICY_PREFIXES = {
    "auto": "AUT", "health": "HLT", "property": "PRP",
    "life": "LIF", "liability": "LBL", "workers_comp": "WRK",
}

NAMES = [
    "Alice Johnson", "Bob Williams", "Carol Davis", "David Brown",
    "Eve Martinez", "Frank Wilson", "Grace Lee", "Henry Taylor",
]


def _random_claim() -> dict:
    ct = random.choice(CLAIM_TYPES)
    prefix = POLICY_PREFIXES[ct]
    loss_days_ago = random.randint(1, 60)
    loss_date = (date.today() - timedelta(days=loss_days_ago)).isoformat()
    return {
        "claim_id": f"CLM-{uuid.uuid4().hex[:12].upper()}",
        "schema_version": "v1",
        "correlation_id": uuid.uuid4().hex,
        "policy_number": f"{prefix}-{random.randint(100000,999999)}",
        "claimant_name": random.choice(NAMES),
        "claim_type": ct,
        "claim_amount": round(random.uniform(500, 100000), 2),
        "date_of_loss": loss_date,
        "date_filed": date.today().isoformat(),
        "description": f"Load test claim for {ct}",
        "status": "submitted",
        "fraud_score": 0.0,
        "risk_level": "low",
    }


# ------------------------------------------------------------------ #
#  Locust user classes
# ------------------------------------------------------------------ #

class PipelineAPIUser(HttpUser):
    """Simulates a user hitting the observability API endpoints."""
    wait_time = between(0.5, 2)

    @tag("health")
    @task(3)
    def check_health(self):
        self.client.get("/health", name="/health")

    @tag("metrics")
    @task(2)
    def get_metrics(self):
        self.client.get("/metrics", name="/metrics")

    @tag("lineage")
    @task(2)
    def get_lineage_graph(self):
        self.client.get("/lineage/graph", name="/lineage/graph")

    @tag("claims")
    @task(3)
    def get_claims_stats(self):
        self.client.get("/claims/stats", name="/claims/stats")

    @tag("claims")
    @task(2)
    def get_recent_claims(self):
        self.client.get("/claims/recent?limit=20", name="/claims/recent")

    @tag("lineage")
    @task(1)
    def get_lineage_stats(self):
        self.client.get("/lineage/statistics", name="/lineage/statistics")


class ClaimSubmitter(HttpUser):
    """
    Simulates external systems submitting claims.
    This user POSTs new claims to a hypothetical /claims/submit endpoint.
    If the endpoint doesn't exist yet, you can use it to plan capacity.
    """
    wait_time = between(0.2, 1)

    @tag("submit")
    @task
    def submit_claim(self):
        claim = _random_claim()
        self.client.post(
            "/claims/submit",
            json=claim,
            name="/claims/submit",
            headers={"X-Correlation-ID": claim["correlation_id"]},
        )


# ------------------------------------------------------------------ #
#  Event hooks for custom SLO reporting
# ------------------------------------------------------------------ #

@events.quitting.add_listener
def _print_slo_summary(environment, **kwargs):
    """Print SLO compliance stats at the end of a load test."""
    stats = environment.runner.stats
    total = stats.total
    if total.num_requests == 0:
        return

    p95 = total.get_response_time_percentile(0.95) or 0
    p99 = total.get_response_time_percentile(0.99) or 0
    fail_rate = (total.num_failures / total.num_requests) * 100

    print("\n" + "=" * 70)
    print("  SLO COMPLIANCE SUMMARY")
    print("=" * 70)
    print(f"  Total Requests:     {total.num_requests}")
    print(f"  Total Failures:     {total.num_failures} ({fail_rate:.2f}%)")
    print(f"  P50 Latency:        {total.get_response_time_percentile(0.50) or 0:.0f} ms")
    print(f"  P95 Latency:        {p95:.0f} ms   (SLO: <500ms {'✓' if p95 < 500 else '✗'})")
    print(f"  P99 Latency:        {p99:.0f} ms   (SLO: <1000ms {'✓' if p99 < 1000 else '✗'})")
    print(f"  Error Rate:         {fail_rate:.2f}%   (SLO: <5% {'✓' if fail_rate < 5 else '✗'})")
    print(f"  Avg RPS:            {total.total_rps:.1f}")
    print("=" * 70 + "\n")
