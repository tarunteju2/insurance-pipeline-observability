# Post-Incident Review Template

**Incident ID:** INC-XXXX  
**Date:** YYYY-MM-DD  
**Severity:** Critical / Warning  
**Duration:** HH:MM (detected → resolved)  
**Author(s):**  
**Reviewers:**

---

## Summary

_One paragraph: what broke, what was the user/system impact, how it was resolved._

---

## Timeline

| Time (UTC) | Event |
|---|---|
| HH:MM | Alert fired: `<alert_name>` |
| HH:MM | On-call acknowledged |
| HH:MM | Root cause identified |
| HH:MM | Fix applied |
| HH:MM | Incident resolved / alert cleared |

---

## Root Cause

_What was the underlying technical cause? Why did it happen now?_

---

## Impact

| Metric | Value |
|---|---|
| Claims affected | ~ N |
| DLQ messages | N |
| Data loss | Yes / No |
| Downtime (full) | N minutes |
| SLO breach | Yes / No — which SLO? |

---

## What Went Well

- 
- 

---

## What Could Be Better

- 
- 

---

## Action Items

| # | Action | Owner | Due Date | Ticket |
|---|---|---|---|---|
| 1 | | | | |
| 2 | | | | |

---

## Lessons Learned

_What would prevent this from happening again? What monitoring gaps did this expose?_

---

## Linked Resources

- Grafana dashboard snapshot: 
- Prometheus query used in triage: 
- Relevant Kafka/DB logs: 
- Related PR/commit: 
