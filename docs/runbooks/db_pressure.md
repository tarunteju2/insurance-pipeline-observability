# Runbook: Database Pressure

**Alert:** `DatabaseConnectionFailure` | `CircuitBreakerOpen{dependency="postgres"}`
**Severity:** Critical
**Escalation:** On-call → Platform Lead → DBA

---

## Symptoms
- `insurance_pipeline_component_status{component="postgres_db"}` == 0
- `insurance_circuit_breaker_state{dependency="postgres"}` == 0 (OPEN)
- `insurance_circuit_breaker_trips_total{dependency="postgres"}` incrementing
- Claims completing pipeline but not persisting to database
- Grafana "claims in DB" count stalling

---

## Immediate Triage (5 minutes)

```bash
# 1. Direct connectivity check
docker exec -it postgres psql \
  -U pipeline_admin -d insurance_lineage \
  -c "SELECT count(*) FROM processed_claims;"

# 2. Check connection pool exhaustion
docker exec -it postgres psql \
  -U pipeline_admin -d insurance_lineage \
  -c "SELECT count(*), state FROM pg_stat_activity GROUP BY state;"

# 3. Check DB container status
docker ps | grep postgres
docker logs postgres --tail 50

# 4. Check circuit breaker state via Prometheus
curl -s "http://localhost:9090/api/v1/query?query=insurance_circuit_breaker_state" \
  | python3 -m json.tool
```

---

## Likely Causes and Fixes

### Cause 1: PostgreSQL container crashed
```bash
docker restart postgres
# Wait 10s for startup, then check the circuit breaker — it will auto-recover (HALF_OPEN → CLOSED)
```

### Cause 2: Connection pool exhausted (too many open connections)
```bash
# Check max connections
docker exec -it postgres psql -U pipeline_admin \
  -c "SHOW max_connections;"

# Kill idle connections older than 5 minutes
docker exec -it postgres psql -U pipeline_admin \
  -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
      WHERE state = 'idle' AND query_start < now() - interval '5 minutes';"
```

### Cause 3: Long-running query blocking writes
```bash
docker exec -it postgres psql -U pipeline_admin \
  -c "SELECT pid, now() - query_start AS duration, query, state
      FROM pg_stat_activity
      WHERE state != 'idle'
      ORDER BY duration DESC LIMIT 10;"
# Kill the offending query: SELECT pg_cancel_backend(<pid>);
```

### Cause 4: Disk full
```bash
docker exec -it postgres df -h /var/lib/postgresql/data
# If >90%, clear WAL logs or increase volume
```

---

## Circuit Breaker Auto-Recovery
The `postgres_breaker` circuit breaker will automatically probe PostgreSQL every 30 seconds once 
tripped. No manual intervention needed for the breaker itself — fix the underlying DB issue and 
it will self-heal within one recovery cycle.

Monitor via:
```
insurance_circuit_breaker_state{dependency="postgres"}
```
Value moves: 0 (OPEN) → 0.5 (HALF_OPEN) → 1 (CLOSED)

---

## Recovery Verification
1. `DatabaseConnectionFailure` alert resolves
2. `insurance_circuit_breaker_state{dependency="postgres"}` returns to 1
3. New claims appear in `processed_claims` table

---

## Escalation
- > 5 minutes with DB unreachable → page Platform Lead
- Disk-related → page DBA immediately
