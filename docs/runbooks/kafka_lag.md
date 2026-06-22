# Runbook: Kafka Consumer Lag

**Alert:** `KafkaBrokerUnreachable` | `QueueDepthHigh` | `PipelineThroughputDropped`
**Severity:** Critical / Warning
**Escalation:** On-call → Platform Lead → Kafka Admin

---

## Symptoms
- `insurance_kafka_consumer_lag` gauge is non-zero and rising
- Airflow `stream_processing` tasks timing out
- Grafana "Pipeline Throughput" panel dropping or zeroing
- Claims not appearing in PostgreSQL after expected window

---

## Immediate Triage (5 minutes)

```bash
# 1. Check broker reachability
docker exec -it kafka kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092

# 2. Check consumer group lag per topic
docker exec -it kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group insurance-pipeline-group \
  --describe

# 3. Check if the processor is running
docker ps | grep stream-processor

# 4. Check API health endpoint
curl -s http://localhost:8082/health | python3 -m json.tool
```

---

## Likely Causes and Fixes

### Cause 1: Stream processor crashed
```bash
docker logs stream-processor --tail 100
# Restart if OOM or exception
docker restart stream-processor
```

### Cause 2: Kafka broker overloaded / disk full
```bash
docker exec -it kafka df -h /var/kafka-logs
# If disk > 90%: delete old log segments
docker exec -it kafka kafka-log-dirs.sh \
  --bootstrap-server localhost:9092 --describe \
  | grep -i "size"
```

### Cause 3: Poison pill message causing processing loop
```bash
# Check DLQ for clues
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic insurance.claims.dlq \
  --from-beginning \
  --max-messages 20
```

### Cause 4: Zookeeper coordination failure
```bash
docker logs zookeeper --tail 50
docker restart zookeeper && sleep 10 && docker restart kafka
```

---

## Recovery Verification
After fix, confirm:
1. `kafka-consumer-groups.sh --describe` shows LAG = 0 or decreasing
2. Grafana throughput panel recovers to >1 claim/sec
3. No new `KafkaBrokerUnreachable` alerts in 5 minutes

---

## Escalation
- > 15 minutes unresolved → page Platform Lead
- > 30 minutes unresolved → page Kafka Admin + notify stakeholders
