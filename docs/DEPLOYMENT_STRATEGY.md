# Deployment Strategy

This document covers the release pipeline, rollout approach, rollback procedures, and capacity guidelines for the insurance claims pipeline.

---

## Environment Strategy

Three environments mirror each other in topology:

| Environment | Purpose | Data | Trigger |
|---|---|---|---|
| **dev** | Local development; Docker Compose on laptop | Synthetic (Faker-generated) | Manual |
| **staging** | Pre-production validation; mirrors prod topology | Anonymised subset of prod claims | PR merge to `main` |
| **prod** | Live production | Real claims | Tagged release + manual approval |

Configuration for each environment lives in `config/{dev,staging,prod}.env`.  
Secrets are **never committed** — injected by CI/CD or fetched from a secrets manager at deploy time.

---

## Release Process

```
feature branch ──► PR ──► CI checks ──► merge to main ──► auto-deploy to staging
                                                        └──► manual approval ──► prod release tag
```

### CI Checks (must all pass before merge)
1. `pytest tests/` — unit + integration tests
2. `python scripts/validate_schemas.py` — schema backward-compatibility check
3. Docker build succeeds for `Dockerfile.airflow`
4. Linting / type checks

---

## Deployment Approach (Blue/Green Concept)

Until a full Kubernetes rollout is in place, we simulate blue/green by running two Compose stacks in parallel on the same host.

### Blue/Green Steps

**Deploy new version (Green)**
```bash
# Bring up the new version on alternate ports
APP_ENV=prod docker-compose -p insurance-green -f docker-compose.yml up -d

# Verify Green is healthy
curl http://localhost:8084/health   # Green API port
```

**Switch traffic (flip Nginx/load-balancer to Green)**
```bash
# Example: update upstream in Nginx config and reload
nginx -s reload
```

**Verify + decommission Blue**
```bash
# Watch metrics for 10 minutes post-flip; if healthy:
docker-compose -p insurance-blue down
```

**Rollback (if Green is unhealthy)**
```bash
# Flip Nginx back to Blue — takes < 60 seconds
# Then tear down Green
docker-compose -p insurance-green down
```

---

## Rollback Checklist

Use this checklist any time a release needs to be reverted:

- [ ] Alert firing confirmed as release-related (check Grafana "deploy" annotation)
- [ ] Decision to rollback made by on-call engineer or lead
- [ ] Traffic switched back to previous version (Blue or previous image tag)
- [ ] Confirm health endpoint returns `200` on rolled-back version
- [ ] DLQ drained or replayed if schema-incompatible messages were produced
- [ ] Post-incident ticket opened
- [ ] Rollback reason documented in release notes

---

## Autoscaling Design

Current state: single stream processor container; vertical scaling only.

**Planned horizontal autoscaling trigger:**

| Metric | Scale-out threshold | Scale-in threshold |
|---|---|---|
| `insurance_kafka_consumer_lag > 5000` | +1 processor replica | lag < 500 for 5m |
| `insurance_pipeline_throughput_per_second < 0.5` (lag > 1000) | +1 replica | throughput stable > 2/s |
| CPU > 80% sustained 3m | +1 replica | CPU < 40% for 10m |

**When Kubernetes is adopted:** convert `stream_processor.py` to a `Deployment` with a `HorizontalPodAutoscaler` backed by a KEDA `KafkaTopic` trigger on `insurance.claims.raw`.

---

## Capacity Guidelines

| Component | Current limit | Scale indicator |
|---|---|---|
| Kafka | 3 partitions per topic | Add partitions when lag > 10k sustained 15m |
| PostgreSQL | 100 max connections | Add read replica when avg query time > 50ms |
| MinIO | Single node | Add distributed mode when disk > 70% |
| Stream processor | 1 replica | Scale to 2 when lag > 5k (see autoscaling above) |

---

## Secrets Management (Current → Target)

| Secret | Current storage | Target |
|---|---|---|
| `POSTGRES_PASSWORD` | `.env` file (gitignored) | HashiCorp Vault / AWS Secrets Manager |
| `MINIO_SECRET_KEY` | `.env` file | Vault |
| Kafka SASL credentials | (not yet enabled) | Vault + Kafka ACLs |

**Migration path:**  
1. Add a `src/config_secrets.py` module with a `get_secret(key)` function  
2. Implement Vault backend: `VAULT_ADDR` + `VAULT_TOKEN` from environment  
3. Fall back to env var if Vault is unavailable (dev mode)  
4. Rotate all secrets post-migration

---

## Definition of Done (Deployment Maturity)

- [ ] CI pipeline runs all checks on every PR
- [ ] Staging deploy is fully automated on `main` merge
- [ ] Blue/green or equivalent rollback takes < 2 minutes
- [ ] `config/prod.env` contains zero hard-coded secret values
- [ ] Autoscaling design reviewed and approved by platform team
