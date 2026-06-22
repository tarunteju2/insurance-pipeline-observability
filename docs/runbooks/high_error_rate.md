# Runbook: High Error Rate

**Alert:** `HighErrorRate` | `ErrorRateSLOBreach`
**Severity:** Warning / Critical (>5% = warning, >10% sustained = critical)
**Escalation:** On-call → Data Engineering Lead

---

## Symptoms
- `rate(insurance_pipeline_errors_total[5m]) / rate(insurance_claims_received_total[5m]) > 0.05`
- Grafana "Error Rate" panel in the red zone
- Airflow `generate_summary` task showing high error/reject counts
- Slack/PagerDuty alert firing

---

## Immediate Triage (5 minutes)

```bash
# 1. Identify which stage is generating errors
curl -s "http://localhost:9090/api/v1/query?query=topk(5,rate(insurance_pipeline_errors_total[5m]))" \
  | python3 -m json.tool

# 2. Break down by error type
curl -s "http://localhost:9090/api/v1/query?query=topk(10,insurance_validation_error_codes_total)" \
  | python3 -m json.tool

# 3. Check DLQ for rejection reasons
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic insurance.claims.dlq \
  --max-messages 20 \
  --property print.key=true

# 4. Stream processor logs
docker logs stream-processor --since 15m | grep -i error | tail -30
```

---

## Decision Tree

```
Error rate high?
├── Mostly VALIDATION failures?
│   ├── Critical errors (REQUIRED_FIELD_MISSING, INVALID_CLAIM_AMOUNT)?
│   │   └── → Producer regression. Notify producer team. Check schema_version.
│   └── High/Medium errors (format, date range)?
│       └── → Review recent data source changes. Consider relaxing non-critical rules.
├── Mostly fraud_detection / enrichment stage?
│   └── → Check enricher dependencies (external APIs, DB lookups)
├── Mostly postgres / minio stage?
│   └── → Follow db_pressure.md or check MinIO health
└── Random / scattered errors?
    └── → Likely transient; check if circuit breakers are cycling
```

---

## Common Fixes

### Producer sending bad data
```bash
# Identify the claim_type with the most errors
curl -s "http://localhost:9090/api/v1/query?query=topk(5,insurance_validation_error_codes_total)" \
  | python3 -m json.tool | grep "claim_type"
# Notify the team responsible for that claim type's producer
```

### Enricher external call failing
```bash
# Check enricher logs
docker logs stream-processor --since 15m | grep enricher | grep error
# The minio circuit breaker will protect the rest of the pipeline
# Verify enricher can reach its dependencies
```

---

## Recovery Verification
1. `HighErrorRate` alert resolves (error rate drops below 5%)
2. Grafana "DLQ" panel returns to baseline
3. Completed claims count resumes upward trend

---

## Escalation
- > 15 minutes above 10% → page Data Engineering Lead
- > 30 minutes above 5% with no fix → open incident, notify stakeholders
