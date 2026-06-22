# Alert Escalation Policy

This document defines how alerts are triaged, escalated, and resolved.
All team members with on-call duties must be familiar with this policy.

---

## Severity Definitions

| Severity | Description | Response SLA | Resolution SLA |
|---|---|---|---|
| **Critical** | Pipeline down, data loss possible, SLO breach affecting production | 5 minutes to acknowledge | 30 minutes to resolve or escalate |
| **Warning** | Degraded performance, elevated error rate, approaching SLO thresholds | 15 minutes to acknowledge | 2 hours to resolve |
| **Info** | Informational only; no immediate action required | Review within 24 hours | N/A |

---

## Alert Routing

| Alert | Severity | Primary On-Call | Secondary (Escalation) |
|---|---|---|---|
| `PipelineComponentDown` | Critical | Platform Engineer | Platform Lead |
| `DatabaseConnectionFailure` | Critical | Platform Engineer | DBA |
| `CircuitBreakerOpen` | Critical | Platform Engineer | Platform Lead |
| `KafkaBrokerUnreachable` | Critical | Platform Engineer | Kafka Admin |
| `ErrorRateSLOBreach` (>5%) | Critical | Data Engineer | Engineering Lead |
| `UnknownSchemaVersionDetected` | Critical | Data Engineer | Producer Team Lead |
| `CriticalValidationFailuresElevated` | Critical | Data Engineer | Producer Team Lead |
| `HighErrorRate` (>10%) | Warning | Data Engineer | — |
| `ProcessingLatencyHigh` | Warning | Data Engineer | — |
| `DuplicateClaimsElevated` | Warning | Data Engineer | — |
| `DQValidityScoreLow` | Warning | Data Engineer | — |
| `DQCompletenessCritical` | Critical | Data Engineer | Data Lead |
| `InvalidClaimsInDLQ` | Warning | Data Engineer | — |
| `LineageCoverageLow` | Warning | Data Engineer | — |
| `MinIOStorageAvailable` | Warning | Platform Engineer | — |
| `QueueDepthHigh` | Warning | Platform Engineer | — |

---

## Escalation Ladder

```
Level 1 (0–5 min):   On-call engineer responds, begins triage
Level 2 (5–15 min):  If unresolved → notify Primary Lead (team-specific)
Level 3 (15–30 min): If unresolved → notify Engineering Director, open war-room
Level 4 (30+ min):   If customer-impacting → notify Product + Stakeholders
```

---

## On-Call Expectations

- **Acknowledge** PagerDuty alert within 5 minutes for Critical, 15 minutes for Warning
- **Update** the incident Slack channel (#incidents) with status every 15 minutes during active incidents
- **Open** a postmortem ticket within 24 hours for any Critical incident
- **Hand off** cleanly at shift change: open incidents must be verbally briefed

---

## Runbook Index

| Symptom | Runbook |
|---|---|
| Kafka lag / throughput drop | [kafka_lag.md](runbooks/kafka_lag.md) |
| DLQ spike / schema mismatch | [dlq_spike.md](runbooks/dlq_spike.md) |
| Database unreachable / circuit open | [db_pressure.md](runbooks/db_pressure.md) |
| High error rate | [high_error_rate.md](runbooks/high_error_rate.md) |

---

## Post-Incident Process

For every Critical incident:
1. Create incident ticket immediately when alert fires
2. Fill in [postmortem_template.md](postmortem_template.md) within 24 hours
3. Review in next team sync
4. Track action items to closure in JIRA/GitHub Issues
