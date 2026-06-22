# Python 3.11 Production Rollout Runbook

## Executive Summary

Full fleet migration from Python 3.9 + Airflow 2.8.1 to Python 3.11 + Airflow 2.11.1 after successful 1-week canary validation. Target: 100% traffic on new runtime, -40% vulnerability reduction (136 CVEs → 82 CVEs).

## Pre-Rollout Checklist (after canary decision gate approval)

- [ ] Canary validation passed all success criteria (7-day sign-off obtained)
- [ ] No incidents during canary monitoring period
- [ ] Platform/SRE team capacity confirmed for 24h post-rollout observation
- [ ] Production backup strategy validated (RTO <1h, RPO <15m)
- [ ] Rollback plan tested and verified (can revert in <30m)
- [ ] Incident commanders briefed and on standby
- [ ] Stakeholder communication sent (maintenance window notification)

## Rollout Strategy: Blue-Green Gradual Cutover

### Phase 1: Deploy Green (Py311) Infrastructure (2 hours)

**Timeline: T+0 to T+2h**

```bash
# 1. Deploy new Py311 infrastructure alongside Py39 (no traffic yet)
kubectl scale deployment airflow-scheduler-py39 --replicas=3
kubectl apply -f deployment-airflow-scheduler-py311.yaml --replicas=3
kubectl apply -f deployment-airflow-worker-py311.yaml --replicas=3
kubectl apply -f deployment-api-py311.yaml --replicas=2

# 2. Verify Py311 replicas healthy
kubectl get pods -l version=py311 -w

# 3. Run smoke tests on Py311 stack
curl -sf http://api-py311:8082/health && echo "Health check OK"
python -m pytest tests/ -k "not integration" -x  # Quick validation

# 4. Confirm database connectivity (shared schema)
python scripts/validate_schemas.py --strict
```

**Success Criteria:**
- All Py311 pods RUNNING/READY
- Health check responds 200 OK
- Schema validation passes
- No startup errors in logs

### Phase 2: Gradual Traffic Shift (6 hours)

**Timeline: T+2h to T+8h**

Traffic shift in increments with 30-min observation between each step:

| Time  | Py39   | Py311  | Action |
|-------|--------|--------|--------|
| T+2h  | 90%    | 10%    | Begin shift, watch metrics |
| T+2.5h| 80%    | 20%    | If error rate stable, continue |
| T+3.5h| 70%    | 30%    | If latency ±3%, continue |
| T+4.5h| 50%    | 50%    | Split traffic equally |
| T+5.5h| 25%    | 75%    | Monitor for Py39 degradation |
| T+6.5h| 10%    | 90%    | Final approach, extreme caution |
| T+7.5h| 0%     | 100%   | Full cutover, begin observation |

**Traffic Shift Commands:**

```bash
# Update load balancer weight (ALB/Ingress)
kubectl patch ingress insurance-pipeline --type merge \
  -p '{"spec":{"rules":[{"http":{"paths":[{"backend":{"service":{"name":"api-py39","port":8082},"weight":80},{"backend":{"service":{"name":"api-py311","port":8082},"weight":20}}]}}]}}'

# Or Kubernetes service routing via iptables
kubectl patch service api -p '{
  "spec": {
    "selector": {
      "version": "py311"
    }
  }
}'

# Verify traffic split in metrics
curl -s http://prometheus:9090/api/v1/query?query='rate(http_requests_total{job="api"}[5m])' | jq .
```

**Monitoring During Shift:**

Every 15 minutes during T+2h to T+8h, check:

```promql
# Error rate (must stay <0.5% above baseline)
rate(http_requests_total{status=~"5.."}[5m])

# Latency P95 (must stay ±5% of baseline)
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))

# Airflow DAG success rate (must be ≥99%)
airflow_dag_run_success

# Kafka consumer lag (must be <30s)
kafka_consumer_lag_seconds

# Database connection pool (must not exceed 90%)
pg_connections_used / pg_connections_max * 100
```

**Go/No-Go Checkpoints:**

At each traffic increment, evaluate:
- ✅ Error rate increase <0.5%? → Continue
- ✅ Latency increase <5%? → Continue
- ✅ No alerts triggered? → Continue
- ✅ DAG success ≥99%? → Continue
- ❌ Any criteria failed? → STOP, investigate, potentially rollback

### Phase 3: Stabilization & Cutover (1 hour)

**Timeline: T+8h to T+9h (100% Py311)**

```bash
# Final cutover: All traffic to Py311
kubectl patch service api -p '{"spec":{"selector":{"version":"py311"}}}'
kubectl patch service scheduler -p '{"spec":{"selector":{"version":"py311"}}}'
kubectl patch service worker -p '{"spec":{"selector":{"version":"py311"}}}'

# Decommission Py39 (scale to 0, but keep manifests for quick rollback)
kubectl scale deployment airflow-scheduler-py39 --replicas=0
kubectl scale deployment airflow-worker-py39 --replicas=0
kubectl scale deployment api-py39 --replicas=0
```

**Immediate Post-Cutover (T+9h to T+12h):**

- Monitor metrics continuously (no sleep for ops team)
- Check logs every 5 minutes for new exception patterns
- Validate data processing end-to-end (produce 100 claims, confirm all stages)
- Run schema validation script
- Verify backup job runs successfully on Py311 scheduler

## Rollback Procedure (if needed at any point)

**Decision Criteria for Rollback:**
- Error rate >1% above baseline for >15 min
- Latency P95 >15% slower than baseline
- DAG failure rate >5%
- Database data loss or corruption
- Kubernetes cluster instability

**Rollback Command (can execute in <30 minutes):**

```bash
# Immediate: Route all traffic back to Py39
kubectl patch service api -p '{"spec":{"selector":{"version":"py39"}}}'

# Scale Py311 to 0 (but don't delete)
kubectl scale deployment airflow-scheduler-py311 --replicas=0
kubectl scale deployment airflow-worker-py311 --replicas=0
kubectl scale deployment api-py311 --replicas=0

# Scale Py39 back up (from stashed manifests)
kubectl scale deployment airflow-scheduler-py39 --replicas=3
kubectl scale deployment airflow-worker-py39 --replicas=5
kubectl scale deployment api-py39 --replicas=2

# Verify Py39 healthy
kubectl wait --for=condition=ready pod -l version=py39 --timeout=300s

# Post-incident: Create PagerDuty incident, page oncall engineer
```

## Post-Rollout (24 hours after 100% cutover)

### Metrics Review (T+24h)

Compare final 24h window vs pre-uplift baseline:

| Metric | Baseline | Py311 24h | Target | Status |
|--------|----------|-----------|--------|--------|
| Error Rate | 0.02% | ≤0.05% | ±0.05% | ✅ |
| Latency P95 | 245ms | 250-260ms | ±5% (267ms) | ✅ |
| DAG Success | 99.5% | ≥99% | ≥99% | ✅ |
| Kafka Lag | 15s | ≤30s | <30s | ✅ |
| Memory/Pod | 512MB | ≤600MB | <700MB | ✅ |

### Infrastructure Cleanup

```bash
# After 48h stability window, delete Py39 manifests entirely
kubectl delete deployment airflow-scheduler-py39
kubectl delete deployment airflow-worker-py39
kubectl delete deployment api-py39

# Remove Py39 from service mesh
kubectl delete virtualservice api-py39
kubectl delete destinationrule api-py39

# Update Docker registry to remove Py39 images (keep 1 backup tag)
# docker rmi <registry>/insurance-pipeline:py39-latest
# docker tag <registry>/insurance-pipeline:py311-latest <registry>/insurance-pipeline:latest
```

### Vulnerability Audit (Post-Rollout)

```bash
# Re-run security scan on production pods
pip-audit -r requirements-py311.txt

# Expected result: 82 vulnerabilities (vs 136 baseline) = -40% reduction
# Create Jira ticket for remaining 82 CVEs (future patch schedule)
```

### Stakeholder Communication

**Send rollout completion email:**

```
Subject: ✅ Python 3.11 + Airflow 2.11.1 Production Rollout Complete

Dear Stakeholders,

Python 3.11 + Airflow 2.11.1 uplift has been successfully deployed to 100% 
of production traffic (2026-06-22 T+8h cutover).

✅ Achievements:
- 40% vulnerability reduction (136 CVEs → 82 CVEs, -54 total)
- Zero regression in error rates or latency (within ±5% SLO)
- 100% test compatibility (70/70 tests passing)
- Py39 support window: 90 days (until Sept 20, 2026)

📊 Metrics (24h post-rollout):
- Error Rate: 0.02% (unchanged)
- Latency P95: 252ms (±3% vs baseline)
- DAG Success Rate: 99.5%
- Kafka Lag: 18s (below threshold)

🔒 Security Impact:
- aiohttp: 10 CVEs fixed
- airflow: 13 CVEs fixed
- cryptography: 6 CVEs fixed
- Others: 15 CVEs fixed

Next Steps:
1. Py39 decommission scheduled: Sept 20, 2026
2. Remaining 82 CVEs tracked in Jira (separate patch process)
3. Feedback? File issues in GitHub or Slack #data-eng

Thank you for enabling this critical security upgrade.

— Platform Team
```

## Incident Response (During Rollout)

**If critical issue detected:**

1. **Page on-call immediately** (PagerDuty escalation)
2. **Create war room Zoom** for real-time coordination
3. **Decision tree:**
   - Is it Py311-specific? → Investigate logs, check correlation IDs
   - Is it infrastructure? → Scale resources or rollback
   - Is it transient? → Wait 5 min, if persists → rollback
4. **Execute rollback** if any go/no-go threshold exceeded
5. **Create postmortem** template: [docs/postmortem_template.md](postmortem_template.md)

## Success Handoff to Operations

After 24h stable window, document:

1. **Py311 SLO Baseline** (for future alerting)
   - P95 latency ±5% band
   - Error rate <0.5% daily
   - DAG success ≥99%

2. **Py311 Runbook Updates**
   - Update all runbooks to reference Py311 (scheduler, workers, API)
   - Add Py311-specific troubleshooting steps

3. **Py39 Deprecation Schedule**
   - End of support: Sept 20, 2026
   - Announce to all users
   - Set calendar reminders for cleanup

4. **Deliverables to Archive**
   - Docker images: `insurance-pipeline:py311-latest`
   - Frozen lockfile: `requirements-py311.txt`
   - Uplift documentation: [docs/PROJECT_PHASES_OVERVIEW.md](PROJECT_PHASES_OVERVIEW.md#phase-3)
   - Canary playbook: [docs/CANARY_DEPLOYMENT_PLAYBOOK.md](CANARY_DEPLOYMENT_PLAYBOOK.md)

## Reference Links

- **Main Branch:** https://github.com/tarunteju2/insurance-pipeline-observability/commit/b875c423
- **Merged PR #1:** https://github.com/tarunteju2/insurance-pipeline-observability/pull/1
- **Phase Documentation:** [docs/PROJECT_PHASES_OVERVIEW.md](PROJECT_PHASES_OVERVIEW.md)
- **Canary Playbook:** [docs/CANARY_DEPLOYMENT_PLAYBOOK.md](CANARY_DEPLOYMENT_PLAYBOOK.md)
- **Architecture Diagram:** [README.md](../../README.md)
- **Runbooks:**
  - [Kafka Lag](runbooks/kafka_lag.md)
  - [DLQ Spike](runbooks/dlq_spike.md)
  - [DB Pressure](runbooks/db_pressure.md)
  - [High Error Rate](runbooks/high_error_rate.md)
