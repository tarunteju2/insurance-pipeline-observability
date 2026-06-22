# Insurance Pipeline Observability — Project Phases Overview

Complete roadmap of all phases, their strategic objectives, deliverables, and current status.

---

## Table of Contents

1. [Phase 1: Data Validation & Observability](#phase-1-data-validation--observability)
2. [Phase 2: Industrialization](#phase-2-industrialization)
3. [Phase 3: Python 3.11 + Airflow Security Uplift](#phase-3-python-311--airflow-security-uplift)
4. [Summary & Timeline](#summary--timeline)

---

## Phase 1: Data Validation & Observability

**Status:** ✅ **COMPLETE**

### Strategic Objective
Build a fully observable, real-time insurance claims pipeline with deterministic data flow, alerting, and infrastructure health monitoring.

### Key Deliverables

#### 1. Data Flow Pipeline
- **7,710 claims** flowing through Kafka → PostgreSQL → MinIO
- **5 active Kafka topics:** raw → validated → scored → enriched → dlq
- **Sub-10ms Kafka latency**, <5ms PostgreSQL response times
- Event-driven architecture with real-time processing

#### 2. Observability Stack
| Component | Endpoint | Purpose |
|---|---|---|
| **Grafana** | http://localhost:3000 | Real-time dashboards (claims pipeline corrected) |
| **Prometheus** | http://localhost:9090 | Metrics collection (34+ metrics active) |
| **Jaeger** | http://localhost:16686 | Distributed tracing & correlation |
| **MinIO Console** | http://localhost:9001 | Object storage for enriched/scored/validated data |
| **Observability API** | http://localhost:8082 | Health checks & metric export (<50ms latency) |

#### 3. Alert Rules
**12 alert rules** evaluating every 30 seconds:

**Critical Alerts (immediate escalation):**
- Component down
- Kafka connectivity lost
- Database unreachable

**Warning Alerts (monitoring threshold):**
- Error rate > 10%
- Throughput < 1 claim/sec
- Fraud detection anomalies
- Processing latency P95 > 5 sec
- Lineage coverage < 80%
- Invalid claim spike
- Storage utilization high
- Kafka consumer lag increasing

#### 4. Airflow DAG Orchestration
- **DAG:** `insurance_claims_pipeline`
- **Schedule:** Every 15 minutes
- **Workflow:** Infrastructure check → Produce 50 claims → Validate → Score fraud → Enrich → Summarize → Store to MinIO
- **Timeout:** 30 minutes (10 minutes for stream processing alone)
- **UI:** http://localhost:8081

#### 5. Backup & Disaster Recovery
- **Strategy:** Daily database dumps to MinIO
- **Retention:** 30 days rolling history
- **RTO Target:** <1 hour recovery time
- **RPO Target:** <15 minutes potential data loss
- **DAG:** `backup_and_disaster_recovery` (daily at 2 AM)
- **Future:** Lifecycle policies to archive claims after 90 days

### Infrastructure Health

All components operational:
- ✅ Zookeeper coordinating Kafka
- ✅ PostgreSQL storing 7,710 claims (healthy)
- ✅ MinIO serving enriched/scored/validated directories
- ✅ Jaeger collecting distributed traces
- ✅ Prometheus scraping 34+ metrics
- ✅ Grafana displaying real-time dashboards
- ✅ Airflow scheduler running 15-minute intervals
- ✅ Observability API responding <50ms

---

## Phase 2: Industrialization

**Status:** ✅ **COMPLETE**

### Strategic Objective
Transform prototype into production-grade enterprise platform with governance, resilience, compliance, and operational maturity.

### Key Workstreams

#### 1. Data Contracts & Schema Governance ✅

**Problem Solved:** Prevent breaking changes and invalid payloads entering pipeline.

| Deliverable | Implementation |
|---|---|
| Versioned JSON Schemas | `src/schemas/{raw,validated,scored,enriched}_claim_v1.json` |
| Schema Version Tracking | `SCHEMA_VERSION` constant in `src/models/claims.py` + `schema_version` field on every claim |
| Ingestion-Time Validation | `src/processors/stream_processor.py` rejects unknown versions, routes to DLQ |
| CI Schema Compatibility | `.github/workflows/ci.yml` enforces backward/forward compatibility checks |
| Kafka Header Propagation | Schema version embedded in message headers for end-to-end tracking |

**Files Involved:**
- `src/schemas/raw_claim_v1.json`
- `src/schemas/validated_claim_v1.json`
- `src/schemas/scored_claim_v1.json`
- `src/schemas/enriched_claim_v1.json`
- `src/models/claims.py`
- `src/processors/stream_processor.py`
- `scripts/validate_schemas.py`

---

#### 2. Reliability & Resilience ✅

**Problem Solved:** Handle transient failures, prevent duplicates, maintain state across retries.

| Capability | Implementation |
|---|---|
| Idempotency | `_compute_idempotency_key()` deterministic key + database-level deduplication |
| Retry Strategy | Exponential backoff with full jitter (`_exponential_backoff_call()`) |
| Circuit Breakers | State machine (CLOSED → OPEN → HALF_OPEN) for PostgreSQL & MinIO (`src/observability/circuit_breaker.py`) |
| SLO Definition | P95 latency, error-rate, availability targets defined in `src/config.py` |
| SLO Monitoring | Prometheus alert rules (`prometheus/alert_rules.yml`) enforce SLO thresholds |
| Graceful Shutdown | Manual offset commit, PG batch flush, producer flush on termination |
| Batch Writes | PostgreSQL buffer (configurable) with 5-second flush timer |
| Lag Monitoring | `kafka_consumer_lag` Prometheus gauge exported for alerting |
| Caching Layer | Redis (graceful degradation if unavailable) for idempotency + feature flags (`src/observability/cache.py`) |

**Files Involved:**
- `src/observability/circuit_breaker.py`
- `src/observability/cache.py`
- `src/processors/stream_processor.py`
- `src/config.py`
- `prometheus/alert_rules.yml`

---

#### 3. Security & Compliance Hardening ✅

**Problem Solved:** Protect PII, enforce data retention, audit sensitive operations.

| Control | Implementation |
|---|---|
| PII Masking | `src/observability/pii_masking.py` masks: name, SSN, VIN, policy ID, phone, address |
| Data Classification | Enum in `src/models/claims.py`: `DataClass.PII`, `DataClass.INTERNAL`, `DataClass.PUBLIC` with field mapping |
| Secrets Management | No plaintext secrets in repo; sensitive config in `config/{dev,staging,prod}.env` |
| Data Retention | `scripts/data_retention.py` purges PostgreSQL + MinIO lifecycle rules enforce archival windows |
| Audit Trail | Immutable `audit_trail` field on claims + `record_audit_event()` function in `src/models/claims.py` |

**Files Involved:**
- `src/observability/pii_masking.py`
- `src/models/claims.py`
- `config/dev.env`, `config/staging.env`, `config/prod.env`
- `scripts/data_retention.py`

---

#### 4. Platform & Deployment Maturity ✅

**Problem Solved:** Repeatable, safe deployments across environments; infrastructure-as-code.

| Capability | Implementation |
|---|---|
| Environment Strategy | Three-tier config (dev / staging / prod) with environment-specific overrides |
| Deployment Patterns | Blue/green, canary, rollback procedures documented |
| CI/CD Pipeline | GitHub Actions workflow: lint → test → schema-check → Docker build → security scan → integration tests |
| Containerization | Docker Compose with 12 services (Kafka, PostgreSQL, MinIO, Redis, Airflow, Prometheus, Grafana, Jaeger, Zookeeper, etc.) |
| Load Testing | Locust harness with SLO compliance reporting (`tests/load/locustfile.py`) |

**Files Involved:**
- `docker-compose.yml`
- `.github/workflows/ci.yml`
- `config/dev.env`, `config/staging.env`, `config/prod.env`
- `docs/DEPLOYMENT_STRATEGY.md`
- `tests/load/locustfile.py`

---

#### 5. Data Quality Governance ✅

**Problem Solved:** Make data quality measurable, actionable, and observable.

| Control | Implementation |
|---|---|
| Validation Severity | Levels: `CRITICAL`, `HIGH`, `MEDIUM`, `LOW` mapped to each rule in `src/models/claims.py` |
| Stop-the-Line Logic | Reject on any `CRITICAL` error; route to DLQ with reason code |
| Quality Scorecard | Metrics tracked: completeness, validity, timeliness, consistency |
| Scorecard Export | Prometheus gauges in `src/observability/metrics.py` for dashboard visualization |

**Files Involved:**
- `src/models/claims.py`
- `src/processors/claims_validator.py`
- `src/observability/metrics.py`
- `prometheus/alert_rules.yml`

---

#### 6. Operations & Incident Readiness ✅

**Problem Solved:** Enable on-call operators to diagnose and remediate issues independently.

| Artifact | Purpose |
|---|---|
| **Runbooks** | 4 scenario-based guides for common incidents |
| **Alert Escalation Policy** | Severity → Team → SLA mapping (`docs/ALERT_ESCALATION_POLICY.md`) |
| **Postmortem Template** | Structured incident review (`docs/postmortem_template.md`) |
| **Health Model** | Three-level system (HEALTHY / DEGRADED / UNHEALTHY) in `src/observability/health.py` |
| **DLQ Replay Tool** | CLI in `scripts/dlq_replay.py` to inspect, filter, replay failed messages |
| **Correlation IDs** | End-to-end propagation (model → Kafka headers → logs → traces) for request tracing |

**Files Involved:**
- `docs/runbooks/kafka_lag.md`
- `docs/runbooks/dlq_spike.md`
- `docs/runbooks/db_pressure.md`
- `docs/runbooks/high_error_rate.md`
- `docs/ALERT_ESCALATION_POLICY.md`
- `docs/postmortem_template.md`
- `scripts/dlq_replay.py`
- `src/observability/health.py`
- `src/models/claims.py` (correlation_id tracking)

---

### Test Coverage

**54 passing tests** across 16 test classes:

| Test Class | Count | Scope |
|---|---|---|
| `TestInsuranceClaimModel` | 3 | Core model creation & serialization |
| `TestClaimsValidator` | 8 | All validation rules + structured errors |
| `TestFraudDetector` | 3 | Risk scoring & fraud indicators |
| `TestClaimsEnricher` | 2 | Enrichment & SIU routing |
| `TestEndToEndPipeline` | 2 | Full pipeline + latency SLO compliance |
| `TestIdempotencyKey` | 3 | Deterministic dedup key generation |
| `TestSchemaVersion` | 2 | Schema version round-trip |
| `TestValidationSeverity` | 3 | Severity assignment per rule |
| `TestStopTheLine` | 3 | Critical-error rejection logic |
| `TestPIIMasking` | 4 | PII field masking functions |
| `TestCircuitBreaker` | 3 | Trip / recover / pass-through logic |
| `TestCorrelationID` | 3 | Auto-generation & Kafka round-trip |
| `TestAuditTrail` | 3 | Immutable event append & serialization |
| `TestHealthDegradation` | 6 | Three-level model + critical flags |
| `TestRedisCache` | 3 | Graceful degradation + defaults |
| `TestExponentialBackoff` | 3 | Retry logic + circuit-breaker bypass |

**Run:** `python -m pytest tests/test_pipeline.py -v --tb=short`

---

### Tech Stack (Phase 2)

| Layer | Technology |
|---|---|
| **Language** | Python 3.9+ |
| **Models** | Pydantic v2 |
| **Streaming** | Apache Kafka (confluent-kafka) |
| **Database** | PostgreSQL 15 + SQLAlchemy |
| **Object Storage** | MinIO (S3-compatible) |
| **Caching** | Redis 7 |
| **Orchestration** | Apache Airflow 2.8.1 |
| **Metrics** | Prometheus + Grafana |
| **Tracing** | OpenTelemetry + Jaeger |
| **API** | FastAPI |
| **Logging** | structlog (JSON) |
| **CI/CD** | GitHub Actions |
| **Load Testing** | Locust |
| **Linting** | Ruff |
| **Security Scanning** | pip-audit |
| **Containerization** | Docker + Docker Compose |

---

## Phase 3: Python 3.11 + Airflow Security Uplift

**Status:** ✅ **COMPLETE**

### Strategic Objective
Migrate runtime from Python 3.9 to Python 3.11 and upgrade Airflow to reduce vulnerability surface while maintaining operational stability.

### Problem Statement
- Python 3.9 + Airflow 2.8.1 locked baseline carries **136 vulnerabilities across 31 packages**.
- Many vulnerabilities in transitive dependencies cannot be fixed without framework upgrade.
- Python 3.11 + newer Airflow has better security posture and performance.

### Execution

#### Step 1: Create Isolated Uplift Environment
```bash
./scripts/prepare_py311_uplift.sh 2.11.1
```

**Script Actions:**
- Creates `.venv311` with Python 3.11
- Rewrites constraints to `constraints-3.11.txt` for Airflow 2.11.1
- **Auto-relaxes exact pins** for providers (dependencies can float per Airflow constraints)
- **Filters conflicting OTEL packages** (instrumentation vs SDK version mismatch)
- Installs dependencies
- Runs `pip check` for consistency validation

**Key Hardening:** 
- Strips `opentelemetry-instrumentation-*` packages that conflict with SDK
- Removes exact-version pins that conflict with Airflow 3.11 constraints
- Allows Airflow constraints to resolve compatible versions

#### Step 2: Validate Application Behavior
```bash
.venv311/bin/python -m pytest -q
.venv311/bin/python -m pytest tests/test_pipeline.py -q
```

**Result:** ✅ **70 tests passing** (100% compatibility)

#### Step 3: Validate DAG Import & Airflow CLI
```bash
.venv311/bin/airflow version
.venv311/bin/airflow dags list
```

#### Step 4: Security Audit
```bash
.venv311/bin/python -m pip install pip-audit
.venv311/bin/pip-audit
```

**Result:**
- **Python 3.11 baseline:** 82 vulnerabilities in 25 packages
- **Python 3.9 baseline:** 136 vulnerabilities in 31 packages
- **Improvement:** **-40% vulnerability reduction** by lifting runtime + Airflow

#### Step 5: Generate Lockfile
```bash
.venv311/bin/pip freeze > requirements-py311.txt
```

### Acceptance Criteria

- ✅ `pip check` returns zero errors in `.venv311`
- ✅ `pytest -q` passes with no regressions (70 tests)
- ✅ `airflow dags list` succeeds (DAG import works)
- ✅ Vulnerability count materially lower (82 vs 136 = -40%)
- ✅ Environment consistency validated
- ✅ Lockfile committed to repo

### Migration Assets

| File | Purpose |
|---|---|
| `scripts/prepare_py311_uplift.sh` | Automated uplift script with pin-relaxation & OTEL filtering |
| `requirements-py311.txt` | Frozen dependency lockfile for Python 3.11 |
| `docs/PHASE_3_PY311_AIRFLOW_UPLIFT_PLAN.md` | Execution plan & acceptance criteria |

### Rollback Strategy
Keep baseline `requirements.txt` unchanged until uplift criteria pass. Rollback by removing `.venv311`:
```bash
rm -rf .venv311
```

### Next Steps for Mainline Integration

1. **Create PR** from `chore/py311-airflow-uplift` branch
2. **CI validation** on Python 3.11 (GitHub Actions can run dual Python versions)
3. **Integration testing** with real Kafka/PostgreSQL stack
4. **Production canary** (run 10% traffic on Py311, monitor for 1 week)
5. **Merge to main** + update Dockerfile to use Python 3.11
6. **Full deployment** to production

---

## Summary & Timeline

### Phase Progression

```
Phase 1: Data Validation & Observability (COMPLETE)
├─ Foundation: Real-time pipeline with full observability
├─ Delivery: Grafana, Prometheus, Jaeger, Alerts, DAGs
└─ Time: Baseline infrastructure

Phase 2: Industrialization (COMPLETE)
├─ Enhancement: Production-grade governance, security, resilience
├─ Delivery: 6 workstreams, 54 tests, 12 services
└─ Time: Baseline + platform maturity

Phase 3: Python 3.11 + Airflow Security Uplift (COMPLETE)
├─ Optimization: Runtime & framework upgrade for reduced vulnerabilities
├─ Delivery: -40% vulnerability reduction, migration script, Py311 lockfile
└─ Time: Baseline + uplift (pending mainline merge)
```

### Key Metrics

| Metric | Phase 1 | Phase 2 | Phase 3 |
|---|---|---|---|
| Data Flow | 7,710 claims | ✓ Maintained | ✓ Verified (70 tests) |
| Test Coverage | Baseline | 54 tests (green) | 70 tests (green) |
| Vulnerabilities | N/A | 136 (Py39) | 82 (Py311) = -40% |
| Alerts | 12 rules | 22 rules | ✓ Maintained |
| Deployment | Single compose | Multi-env + CI/CD | Dual-Python CI |

---

## Deployment Readiness Checklist

### For Phase 1
- [x] Kafka topics created & streaming data
- [x] PostgreSQL initialized with schema
- [x] Grafana dashboards deployed
- [x] Prometheus scrape config live
- [x] Airflow DAGs scheduled

### For Phase 2
- [x] All 6 workstreams delivered
- [x] 54 tests passing
- [x] Schema validation in CI
- [x] Circuit breakers active
- [x] PII masking enabled
- [x] Audit trails immutable
- [x] Runbooks documented
- [x] Alert escalation policy defined

### For Phase 3 (Pre-Mainline Merge)
- [x] Py311 venv created & validated
- [x] 70 tests passing on Py311
- [x] `pip check` clean
- [x] Vulnerability profile improved
- [ ] PR created for review
- [ ] CI dual-Python testing configured
- [ ] Staging canary deployment complete
- [ ] Merged to main

---

## How to Access Documentation

| Document | Path | Purpose |
|---|---|---|
| Phase 1 Report | `docs/PHASE_1_COMPLETION.md` | Baseline infrastructure status |
| Phase 2 Plan | `docs/PHASE_2_INDUSTRIALIZATION_PLAN.md` | 6-workstream strategy |
| Phase 2 Report | `docs/PHASE_2_COMPLETION.md` | All deliverables catalogued |
| Phase 3 Plan | `docs/PHASE_3_PY311_AIRFLOW_UPLIFT_PLAN.md` | Uplift execution steps |
| Deployment Strategy | `docs/DEPLOYMENT_STRATEGY.md` | Blue/green, canary, rollback |
| Alert Policy | `docs/ALERT_ESCALATION_POLICY.md` | Severity → Team → SLA |
| Runbooks | `docs/runbooks/*.md` | Incident response scenarios |
| Postmortem | `docs/postmortem_template.md` | Incident review process |

---

## Contact & Support

- **Infrastructure Issues:** Check runbooks in `docs/runbooks/`
- **Data Quality Issues:** Review `docs/ALERT_ESCALATION_POLICY.md`
- **Security/Compliance:** See `src/observability/pii_masking.py` + `scripts/data_retention.py`
- **Performance:** Monitor Prometheus dashboard + Grafana for SLO compliance
