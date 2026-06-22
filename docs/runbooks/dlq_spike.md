# Runbook: DLQ Spike

**Alert:** `InvalidClaimsInDLQ` | `CriticalValidationFailuresElevated` | `UnknownSchemaVersionDetected`
**Severity:** Critical / Warning
**Escalation:** On-call → Data Engineering Lead → Producer Team

---

## Symptoms
- DLQ topic `insurance.claims.dlq` growing rapidly
- `insurance_dq_critical_failures_total` counter spiking
- `insurance_schema_validation_errors_total{reason="unsupported_schema_version"}` > 0
- Completed claims count dropping in Grafana while DLQ count rises

---

## Immediate Triage (5 minutes)

```bash
# 1. Sample the DLQ to understand the failure pattern
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic insurance.claims.dlq \
  --from-beginning \
  --max-messages 50 \
  --property print.key=true | python3 -m json.tool | grep -E "dlq_reason|dlq_error_codes|schema_version"

# 2. Check the Prometheus validation error breakdown
curl -s http://localhost:9090/api/v1/query \
  '?query=topk(10,insurance_validation_error_codes_total)' \
  | python3 -m json.tool

# 3. Check critical failures
curl -s http://localhost:9090/api/v1/query \
  '?query=increase(insurance_dq_critical_failures_total[30m])' \
  | python3 -m json.tool
```

---

## Likely Causes

### Cause 1: Schema version mismatch (UnknownSchemaVersionDetected alert)
A producer was deployed with a new `schema_version` that the consumer does not yet support.

```bash
# Check what version is arriving
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic insurance.claims.raw \
  --max-messages 5 | python3 -c "import sys,json;[print(json.loads(l).get('schema_version')) for l in sys.stdin]"

# Fix: Add the new version to SUPPORTED_SCHEMA_VERSIONS in stream_processor.py
# and redeploy the consumer, or rollback the producer.
```

### Cause 2: Required field missing in upstream producer
A code change removed or renamed a field.

```bash
# Cross-reference the top error codes against VALIDATION_SEVERITY_MAP in claims.py
# If REQUIRED_FIELD_MISSING is spiking, the producer dropped a field.
```

### Cause 3: Data quality regression (e.g., policy number format change)
```bash
# Check which field / error code dominates
# Fix the upstream system or add a migration rule in claims_validator.py
```

---

## DLQ Replay (after fix)

Once the root cause is resolved, replay the DLQ claims:

```bash
# Re-publish DLQ messages back to raw topic
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic insurance.claims.dlq \
  --from-beginning \
  | docker exec -i kafka kafka-console-producer.sh \
      --bootstrap-server localhost:9092 \
      --topic insurance.claims.raw
```

---

## Recovery Verification
1. DLQ rate drops back to baseline in Grafana
2. `insurance_dq_critical_failures_total` rate flattens
3. Completed claim count recovers

---

## Escalation
- > 10 minutes with no root cause → escalate to Data Engineering Lead
- Schema version mismatch → immediately notify Producer Team
