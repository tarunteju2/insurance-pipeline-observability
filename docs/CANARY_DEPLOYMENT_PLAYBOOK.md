# Python 3.11 Canary Deployment Playbook

## Overview
Rolling deployment of Python 3.11 runtime to 10% production traffic for 1-week validation period before full fleet migration.

## Pre-Deployment Checklist

- [ ] PR #1 merged to main
- [ ] All CI checks passing (lint, test on Py39 + Py311, schema-check, Docker build, security scan)
- [ ] Docker images built and pushed: `insurance-pipeline:py311-canary`
- [ ] Alerts configured for canary monitoring (see Monitoring section)
- [ ] Runbooks reviewed (db_pressure, dlq_spike, high_error_rate, kafka_lag)
- [ ] On-call team briefed on canary objectives and SLO targets

## Canary Deployment Steps

### Phase 1: Prepare Canary Environment (T-0)

1. **Build Python 3.11 Docker Image**
   ```bash
   docker build -f Dockerfile -t insurance-pipeline:py311-canary \
     --build-arg PYTHON_VERSION=3.11 .
   docker push <registry>/insurance-pipeline:py311-canary
   ```

2. **Deploy Canary Replicas (10% of Airflow Scheduler/Worker/API pods)**
   - Kubernetes: Update deployment with `image: insurance-pipeline:py311-canary`
   - Deploy 1-2 scheduler replicas, 1-2 worker pods, 1 API replica
   - Verification: `kubectl get pods | grep py311-canary`

3. **Configure Load Balancer for 10% Traffic Split**
   - Ingress/ALB rule: Route 10% of traffic to Py311 backend, 90% to Py39 (stable)
   - DNS weighted: 10% → py311-canary.prod, 90% → py39-stable.prod
   - Verification: Check traffic split ratio in load balancer metrics

### Phase 2: Monitoring (Week 1)

**Watch Prometheus Metrics (every 12 hours):**
```promql
# Error rate (target: <0.1% increase vs baseline)
rate(http_requests_total{status=~"5.."}[5m])

# Latency P95 (target: ±5% vs baseline)
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))

# Airflow DAG success rate (target: ≥99%)
airflow_dag_run_success_rate

# Kafka consumer lag (target: <30s)
kafka_consumer_lag_max

# PostgreSQL query latency P99 (target: <100ms)
pg_query_duration_ms
```

**Daily Alerting Triggers (escalate if triggered):**
- Error rate jumps >0.5% (potential regression)
- Latency P95 increases >10% (performance issue)
- Kafka lag exceeds 1 minute (processing backlog)
- PostgreSQL connection pool exhaustion (resource leak)

### Phase 3: Success Criteria (all must be met)

- ✅ Error rate on Py311 canary ≤ Py39 baseline
- ✅ Latency P95 on Py311 within ±5% of Py39
- ✅ No new exception patterns in logs
- ✅ No alerts triggered for canary replicas
- ✅ Airflow DAG runs complete successfully on Py311 scheduler
- ✅ Data schema validation passes 100% of claims
- ✅ DLQ replay CLI functions correctly with Py311 runtime

### Phase 4: Rollback Criteria (any one triggered)

- ❌ Error rate >0.5% above baseline for >30 minutes
- ❌ Latency P95 >15% slower than baseline
- ❌ Multiple unhandled exceptions in Python runtime
- ❌ Database corruption or data loss detected
- ❌ Airflow DAG failures due to Py311 incompatibility

**Rollback Command:**
```bash
# Scale Py311 canary to 0
kubectl scale deployment airflow-scheduler-canary --replicas=0

# Verify all traffic routed to Py39
kubectl get pods | grep -v canary
```

## Week 1 Monitoring Timeline

| Day | Action | Metric Target |
|-----|--------|--------------|
| 1   | Canary live, hourly metric checks | Error rate <0.1% |
| 2   | Daily summary, alert review | Latency ±3% |
| 3-4 | Baseline confidence period, 12h reviews | DQ compliance 100% |
| 5-6 | Mid-week validation, stress test prep | SLO sustained |
| 7   | Final validation, decision gate | All criteria met → PROCEED |

## Decision Gate (T+7 days)

**Go/No-Go Criteria:**
- ✅ ALL success criteria met → Proceed to full rollout
- ⚠️  SOME criteria at threshold → Extend canary 3 more days
- ❌ ROLLBACK criteria triggered → Revert to Py39, investigate

**Approval Required From:**
1. On-call engineer (metric validation)
2. Platform lead (operational readiness)
3. SRE lead (capacity impact assessment)

## Full Production Rollout (Post-Decision Gate)

Once approved after week 1:

1. **Gradually increase traffic (day 8-9)**
   - 25% → Py311
   - 50% → Py311
   - 75% → Py311
   - 100% → Py311

2. **Update production manifests**
   ```yaml
   # Switch all deployments from :latest to :py311-latest
   image: insurance-pipeline:py311-latest
   ```

3. **Monitor full fleet metrics** (first 24 hours post-100%)
   - Error rate, latency, Kafka lag all within baseline

4. **Announce completion** to stakeholders
   - Summary: -40% vulnerability reduction, 100% test compatibility
   - Benefit: LTS Python version, security compliance

## Monitoring Dashboard Setup

**Grafana Dashboard:** `Python 3.11 Canary Deployment`
```yaml
Panels:
  1. Error Rate (Py311 vs Py39) - red line = threshold
  2. Latency P95/P99 (dual chart) - baseline band
  3. DAG Success Rate (Airflow metrics)
  4. Kafka Consumer Lag (canary consumer group)
  5. PostgreSQL Latency (Py311 connections)
  6. Memory Usage (canary pods vs stable baseline)
  7. CPU Usage (canary pods vs stable baseline)
  8. Active Connections (Postgres pool utilization)
```

## Incident Response

**If metric alert triggered during canary:**

1. **Immediate (T+0-5m):**
   - Page on-call engineer
   - Snapshot current metrics (Prometheus queries to runbook)
   - Check application logs for Py311-specific errors

2. **Triage (T+5-15m):**
   - Compare error signature to known Py39 issues
   - Check for resource exhaustion (memory, CPU, connections)
   - Correlate with code changes (DAG modifications, library updates)

3. **Decision (T+15-30m):**
   - If likely Py311 issue: Rollback canary (no data loss risk)
   - If likely transient: Continue monitoring, escalate if repeats
   - If infrastructure issue: Scale replicas or investigate resource

4. **Postmortem (T+1 day):**
   - Document root cause
   - Add test case to prevent regression
   - Update runbook if new pattern discovered

## Success Handoff to Platform Team

Upon completion, deliver:
- ✅ Py311 production deployment runbook
- ✅ Updated Dockerfile with Python 3.11 base
- ✅ Metrics baseline report (error rate, latency for SLO tuning)
- ✅ Incident log (if any) + resolution steps
- ✅ Py39 deprecation schedule (recommend: 3-month support window)

## Reference

- Phase 3 Uplift Doc: [docs/PROJECT_PHASES_OVERVIEW.md](PROJECT_PHASES_OVERVIEW.md#phase-3-python-311--airflow-uplift)
- Security Scan Results: `requirements-py311.txt` (82 vulns vs 136 baseline)
- Test Coverage Report: `pytest -v --cov=src` (70/70 passing)
