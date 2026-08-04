import os

HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<title>Insurance Claims Pipeline — Phase 2 Workflow Chart</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800;900&display=swap');
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #fff;
    font-family: 'Inter', 'Segoe UI', Arial, sans-serif;
    padding: 24px;
  }

  /* ── MAIN CONTAINER ── */
  .chart { width: 1460px; margin: 0 auto; }

  /* ── COLUMN HEADER BAR ── */
  .col-hdrs {
    display: grid;
    grid-template-columns: 195px 195px 330px 210px 210px 320px;
    border-bottom: 2.5px solid #1a1a1a;
  }
  .col-hdr {
    display: flex; align-items: center; gap: 8px;
    padding: 12px 10px;
    border-right: 1.5px solid #d0d0d0;
    font-size: 11px; font-weight: 800; color: #111;
    text-transform: none;
  }
  .col-hdr:last-child { border-right: none; }
  .col-hdr img { width: 22px; height: 22px; flex-shrink: 0; }
  .col-hdr svg { width: 22px; height: 22px; flex-shrink: 0; }
  .col-hdr .hdr-main { font-size: 12.5px; font-weight: 900; }
  .col-hdr .hdr-sub { font-size: 9.5px; font-weight: 500; color: #666; }

  /* ── COLUMNS BODY ── */
  .cols {
    display: grid;
    grid-template-columns: 195px 195px 330px 210px 210px 320px;
    min-height: 820px;
    border-bottom: 2.5px solid #1a1a1a;
  }
  .col {
    border-right: 1.5px solid #e0e0e0;
    padding: 16px 10px;
    display: flex; flex-direction: column;
    gap: 8px;
    position: relative;
  }
  .col:last-child { border-right: none; }

  /* ── STEP BOX ── */
  .step {
    border: 2px solid #222;
    border-radius: 6px;
    padding: 10px 10px 12px;
    background: #fff;
    position: relative;
  }
  .step-hdr {
    font-size: 9.5px; font-weight: 900; color: #111;
    text-transform: uppercase; letter-spacing: 0.04em;
    margin-bottom: 5px;
    display: flex; align-items: center; gap: 6px;
  }
  .step-hdr .new-tag {
    font-size: 7px; font-weight: 800; background: #2563eb; color: #fff;
    padding: 1px 5px; border-radius: 3px; text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .step-body {
    font-size: 9px; color: #444; line-height: 1.45;
  }
  .step-ico {
    display: flex; align-items: center; gap: 5px;
    margin-top: 5px;
  }
  .step-ico img { width: 16px; height: 16px; }
  .step-ico svg { width: 16px; height: 16px; }
  .step-ico span { font-size: 8.5px; color: #666; }

  /* decision gate */
  .decision {
    border: 2px solid #222;
    border-radius: 6px;
    padding: 10px;
    background: #f9fafb;
    text-align: center;
  }
  .decision .step-hdr { justify-content: center; }
  .decision .step-body { font-size: 9px; color: #555; font-style: italic; }

  /* branch labels */
  .branch-row {
    display: flex; justify-content: center; gap: 28px;
    margin: -2px 0 4px;
  }
  .branch-lbl {
    font-size: 9px; font-weight: 800;
    display: flex; flex-direction: column; align-items: center; gap: 2px;
  }
  .branch-lbl.fail { color: #dc2626; }
  .branch-lbl.pass { color: #16a34a; }
  .branch-lbl.invalid { color: #dc2626; }
  .branch-lbl.valid { color: #16a34a; }

  /* vertical arrow */
  .v-arrow {
    display: flex; justify-content: center; align-items: center;
    height: 22px; position: relative;
  }
  .v-arrow::after {
    content: '';
    display: block;
    width: 0; height: 0;
    border-left: 5px solid transparent;
    border-right: 5px solid transparent;
    border-top: 8px solid #333;
  }
  .v-arrow::before {
    content: '';
    position: absolute; top: 0; left: 50%;
    width: 1.5px; height: 14px;
    background: #333;
    transform: translateX(-50%);
  }

  /* horizontal arrow label */
  .h-arrow-lbl {
    font-size: 7.5px; color: #888; font-style: italic;
    text-align: center; margin: 2px 0;
  }

  /* small inline boxes */
  .route-box {
    display: inline-flex; align-items: center; gap: 4px;
    border: 1.5px solid #333;
    border-radius: 5px; padding: 4px 8px;
    font-size: 8.5px; font-weight: 700; color: #222;
    background: #fff;
  }
  .route-box img { width: 14px; height: 14px; }
  .route-box.dlq { border-color: #dc2626; color: #dc2626; }

  /* side arrow (horizontal connector) */
  .side-arr {
    font-size: 8px; color: #888;
    display: flex; align-items: center; gap: 3px;
    position: absolute;
    white-space: nowrap;
  }
  .side-arr::after {
    content: '→'; font-size: 12px; color: #555;
  }

  /* ── BACKUP SECTION ── */
  .backup-section {
    margin-top: 18px;
    border: 2px solid #222;
    border-radius: 8px;
    padding: 14px 18px 16px;
  }
  .backup-title {
    font-size: 13px; font-weight: 900; color: #111;
    margin-bottom: 3px;
  }
  .backup-sub {
    font-size: 9.5px; color: #666; margin-bottom: 14px;
  }
  .backup-flow {
    display: flex; align-items: center; gap: 0;
    flex-wrap: nowrap;
  }
  .backup-step {
    border: 2px solid #222; border-radius: 6px;
    padding: 8px 12px; background: #fff;
    text-align: center; min-width: 100px;
  }
  .backup-step .step-hdr { font-size: 9px; justify-content: center; }
  .backup-step .step-body { font-size: 8px; }
  .b-arrow {
    height: 2px; width: 26px; background: #333; position: relative;
    flex-shrink: 0;
  }
  .b-arrow::after {
    content: ''; position: absolute; right: -5px; top: -3.5px;
    width: 0; height: 0;
    border-top: 4.5px solid transparent;
    border-bottom: 4.5px solid transparent;
    border-left: 7px solid #333;
  }
  .b-storage {
    border: 2px dashed #999; border-radius: 10px;
    padding: 8px 14px;
    font-size: 9px; font-weight: 700; color: #555; text-align: center;
  }

  /* ── DECISION PATHS FOOTER ── */
  .paths {
    margin-top: 16px;
    display: grid; grid-template-columns: 1fr 1fr;
    gap: 20px;
  }
  .paths-box {
    font-size: 9px; color: #333; line-height: 1.6;
  }
  .paths-box strong {
    font-size: 10px; font-weight: 900; color: #111;
    display: block; margin-bottom: 4px;
  }

  /* ── PHASE 2 HIGHLIGHT ── */
  .p2 { border-color: #2563eb !important; }
  .p2 .step-hdr { color: #1d4ed8; }

  /* ── TINY CONNECTOR DOTS ── */
  .conn-dot {
    width: 7px; height: 7px;
    background: #333; border-radius: 50%;
    margin: 0 auto;
  }
</style>
</head>
<body>
<div class="chart">

  <!-- ══════════════════════════════════════════════════ -->
  <!-- COLUMN HEADERS                                      -->
  <!-- ══════════════════════════════════════════════════ -->
  <div class="col-hdrs">
    <div class="col-hdr">
      <img src="https://cdn.simpleicons.org/apacheairflow/017CEE" alt="Airflow"/>
      <div>
        <div class="hdr-main">Orchestrator</div>
        <div class="hdr-sub">(Airflow)</div>
      </div>
    </div>
    <div class="col-hdr">
      <img src="https://cdn.simpleicons.org/python/3776AB" alt="Python"/>
      <div>
        <div class="hdr-main">Producer</div>
        <div class="hdr-sub">(Claims Gen)</div>
      </div>
    </div>
    <div class="col-hdr">
      <img src="https://cdn.simpleicons.org/apachekafka/231F20" alt="Kafka"/>
      <div>
        <div class="hdr-main">Stream Processor</div>
        <div class="hdr-sub">(Kafka + Schema Registry)</div>
      </div>
    </div>
    <div class="col-hdr">
      <img src="https://cdn.simpleicons.org/postgresql/336791" alt="PG"/>
      <div>
        <div class="hdr-main">Storage</div>
        <div class="hdr-sub">(Postgres/MinIO/Iceberg)</div>
      </div>
    </div>
    <div class="col-hdr">
      <svg viewBox="0 0 24 24" fill="none"><circle cx="5" cy="12" r="2.2" fill="#333"/><circle cx="19" cy="5" r="2.2" fill="#333"/><circle cx="19" cy="19" r="2.2" fill="#333"/><line x1="7" y1="11" x2="17" y2="6" stroke="#333" stroke-width="1.5"/><line x1="7" y1="13" x2="17" y2="18" stroke="#333" stroke-width="1.5"/></svg>
      <div>
        <div class="hdr-main">Lineage Tracker</div>
        <div class="hdr-sub">&nbsp;</div>
      </div>
    </div>
    <div class="col-hdr">
      <img src="https://cdn.simpleicons.org/prometheus/E6522C" alt="Prom"/>
      <div>
        <div class="hdr-main">Observability</div>
        <div class="hdr-sub">(Prometheus/Grafana/OTel)</div>
      </div>
    </div>
  </div>

  <!-- ══════════════════════════════════════════════════ -->
  <!-- COLUMNS BODY                                        -->
  <!-- ══════════════════════════════════════════════════ -->
  <div class="cols">

    <!-- ── COL 1: ORCHESTRATOR ── -->
    <div class="col">
      <div class="step">
        <div class="step-hdr">STEP 1: TRIGGER (15-min cycle)</div>
        <div class="step-body">DAG trigger</div>
      </div>

      <div class="v-arrow"></div>

      <div class="decision">
        <div class="step-hdr">STEP 2: HEALTH GATE</div>
        <div class="step-body">DECISION — Health check passes?</div>
      </div>

      <div class="branch-row">
        <div class="branch-lbl fail">
          FAIL<br>
          <span style="font-size:16px;">↓</span>
        </div>
        <div class="branch-lbl pass">
          PASS<br>
          <span style="font-size:16px;">↓</span>
        </div>
      </div>

      <div style="display:flex; gap:10px; justify-content:center;">
        <div class="step" style="text-align:center;padding:8px;">
          <div class="step-hdr" style="justify-content:center;">End + Alert 🔴</div>
        </div>
        <div class="step" style="text-align:center;padding:8px;">
          <div class="step-hdr" style="justify-content:center;">Continue</div>
        </div>
      </div>

      <div style="flex:1;"></div>

      <!-- Phase 2: Circuit Breaker info -->
      <div class="step p2" style="margin-top:auto;">
        <div class="step-hdr">CIRCUIT BREAKER <span class="new-tag">P2</span></div>
        <div class="step-body">
          PG: 5 fails → OPEN (30s)<br>
          MinIO: 3 fails → OPEN (20s)<br>
          3-state: CLOSED → OPEN → HALF-OPEN
        </div>
      </div>
    </div>

    <!-- ── COL 2: PRODUCER ── -->
    <div class="col">
      <div class="step">
        <div class="step-hdr">STEP 3: GENERATE CLAIMS</div>
        <div class="step-ico">
          <svg viewBox="0 0 20 20" fill="none"><rect x="2" y="2" width="16" height="16" rx="2" stroke="#555" stroke-width="1.5"/><path d="M5 6h10M5 10h6M5 14h8" stroke="#555" stroke-width="1.2" stroke-linecap="round"/></svg>
          <span>Batch processing<br>(50 claims per run)</span>
        </div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr">STEP 4: PRODUCE TO KAFKA</div>
        <div class="step-ico">
          <img src="https://cdn.simpleicons.org/apachekafka/231F20" alt="Kafka"/>
          <span>Publish to raw-claims topic<br>+ trace context</span>
        </div>
      </div>

      <div class="v-arrow"></div>

      <!-- Phase 2: Schema + Idempotency -->
      <div class="step p2">
        <div class="step-hdr">STEP 4A: SCHEMA TAG <span class="new-tag">P2</span></div>
        <div class="step-body">
          Attach headers:<br>
          <code style="font-size:8px;background:#f1f5f9;padding:1px 4px;border-radius:2px;">correlation_id</code> (UUID) +
          <code style="font-size:8px;background:#f1f5f9;padding:1px 4px;border-radius:2px;">schema_version</code> ("v1")<br>
          Generate idempotency key:<br>
          SHA256(policy|loss_date|amount)
        </div>
      </div>

      <div style="flex:1;"></div>

      <!-- Phase 2: PII Masking -->
      <div class="step p2" style="margin-top:auto;">
        <div class="step-hdr">PII MASKING <span class="new-tag">P2</span></div>
        <div class="step-body">
          name → J.D.<br>
          VIN → ***-4352<br>
          policy → ABC-***456<br>
          address → city, state
        </div>
      </div>
    </div>

    <!-- ── COL 3: STREAM PROCESSOR ── -->
    <div class="col">
      <div class="step">
        <div class="step-hdr">STEP 5: CONSUME RAW</div>
        <div class="step-body">Subscribe to raw-claims topic</div>
      </div>

      <div class="v-arrow"></div>

      <!-- Phase 2: Schema Validation -->
      <div class="step p2">
        <div class="step-hdr">STEP 5A: SCHEMA VALIDATE <span class="new-tag">P2</span></div>
        <div class="step-body">
          Check schema_version header<br>
          Validate against Schema Registry<br>
          Unsupported version → DLQ
        </div>
        <div class="step-ico">
          <img src="https://cdn.simpleicons.org/apacheavro/CC0909" alt="Avro"/>
          <span>Confluent Schema Registry (Avro)</span>
        </div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr">STEP 6: VALIDATE CLAIMS</div>
        <div class="step-body">
          Check required fields,<br>
          format, amount, dates
        </div>
      </div>

      <div class="v-arrow"></div>

      <!-- Phase 2: DQ Scorecard -->
      <div class="step p2">
        <div class="step-hdr">STEP 6A: DQ SCORECARD <span class="new-tag">P2</span></div>
        <div class="step-body">
          4-dimension quality score:<br>
          Completeness 35% · Validity 35%<br>
          Timeliness 15% · Consistency 15%<br>
          Published every 10 claims
        </div>
      </div>

      <div class="v-arrow"></div>

      <div class="decision">
        <div class="step-hdr">STEP 7: VALIDATION GATE</div>
        <div class="step-body">DECISION — Claim valid?<br>(4 severity levels: CRITICAL/HIGH/MEDIUM/LOW)</div>
      </div>

      <div class="branch-row">
        <div class="branch-lbl invalid">Invalid</div>
        <div class="branch-lbl valid">Valid</div>
      </div>

      <div style="display:flex; gap:8px; justify-content:center; align-items:center;">
        <div class="route-box dlq">
          Route to<br>DLQ
          <img src="https://cdn.simpleicons.org/apachekafka/dc2626" alt="K"/>
        </div>
        <div class="route-box">
          Continue to<br>fraud detection
        </div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr">STEP 8: FRAUD DETECTION</div>
        <div class="step-body">
          Apply 8 weighted rules +<br>
          sigmoid scoring → RiskLevel<br>
          <span style="font-size:8px;color:#888;">CRITICAL ≥0.8 · HIGH 0.6 · MED 0.3 · LOW &lt;0.3</span>
        </div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr">STEP 9: ENRICHMENT</div>
        <div class="step-body">
          Enrich (policy, history, geo<br>
          risk, adjuster, reserve)
        </div>
        <div class="step-ico">
          <img src="https://cdn.simpleicons.org/redis/DC382D" alt="Redis"/>
          <span>Redis cache lookups</span>
        </div>
      </div>

      <!-- Phase 2: Graceful Drain -->
      <div class="step p2" style="margin-top:6px;">
        <div class="step-hdr">GRACEFUL DRAIN <span class="new-tag">P2</span></div>
        <div class="step-body" style="font-size:8px;">
          SIGTERM → flush PG batch →<br>
          producer.flush → commit offsets →<br>
          consumer.close
        </div>
      </div>
    </div>

    <!-- ── COL 4: STORAGE ── -->
    <div class="col">

      <div style="height:132px;"></div>

      <div class="step" style="position:relative;">
        <div class="step-hdr" style="font-size:8.5px;">Send valid → validated-claims topic</div>
        <div class="step-ico">
          <img src="https://cdn.simpleicons.org/apachekafka/231F20" alt="Kafka"/>
          <span>insurance.claims.validated</span>
        </div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr" style="font-size:8.5px;">Publish to scored-claims topic</div>
        <div class="step-ico">
          <img src="https://cdn.simpleicons.org/apachekafka/231F20" alt="Kafka"/>
          <span>insurance.claims.scored</span>
        </div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr" style="font-size:8.5px;">Publish to enriched-claims topic</div>
        <div class="step-ico">
          <img src="https://cdn.simpleicons.org/apachekafka/231F20" alt="Kafka"/>
          <span>insurance.claims.enriched</span>
        </div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr">STEP 10: STORE PIPELINE STATE</div>
        <div class="step-body">
          <div class="step-ico"><img src="https://cdn.simpleicons.org/postgresql/336791" alt="PG"/> <span>Postgres processed_claims<br>(batch upsert, 50 buffer)</span></div>
          <div class="step-ico" style="margin-top:3px;"><img src="https://cdn.simpleicons.org/minio/C72C48" alt="MinIO"/> <span>MinIO buckets<br>(raw→validated→scored→enriched)</span></div>
        </div>
      </div>

      <div class="v-arrow"></div>

      <!-- Phase 2: Iceberg Write -->
      <div class="step p2">
        <div class="step-hdr">STEP 10A: ICEBERG WRITE <span class="new-tag">P2</span></div>
        <div class="step-body">
          ACID lakehouse append<br>
          Apache Parquet format<br>
          Time-travel queries enabled
        </div>
        <div class="step-ico">
          <img src="https://cdn.simpleicons.org/apacheparquet/50ABF1" alt="Parquet"/>
          <span>iceberg/insurance/claims/</span>
        </div>
      </div>

      <!-- Phase 2: Idempotency -->
      <div class="step p2" style="margin-top:6px;">
        <div class="step-hdr">IDEMPOTENCY <span class="new-tag">P2</span></div>
        <div class="step-body" style="font-size:8px;">
          SHA256 dedup key<br>
          DB-level duplicate detection<br>
          Exactly-once semantics
        </div>
      </div>
    </div>

    <!-- ── COL 5: LINEAGE TRACKER ── -->
    <div class="col">

      <div style="height:132px;"></div>

      <div class="step">
        <div class="step-hdr" style="font-size:8.5px;">Aggregate lineage<br>coverage report</div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr" style="font-size:8.5px;">Publish to scored-<br>claims topic</div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr" style="font-size:8.5px;">Generate lineage<br>enriched-claims report</div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr">STEP 11: LINEAGE REPORT</div>
        <div class="step-body">
          Aggregate lineage<br>coverage report<br>
          Generate node/edge/<br>event statistics
        </div>
      </div>

      <!-- Phase 2: Audit Trail -->
      <div class="step p2" style="margin-top:auto;">
        <div class="step-hdr">AUDIT TRAIL <span class="new-tag">P2</span></div>
        <div class="step-body">
          Immutable append-only log<br>
          per claim: stage → status →<br>
          timestamp → correlation_id<br>
          Full provenance graph
        </div>
      </div>
    </div>

    <!-- ── COL 6: OBSERVABILITY ── -->
    <div class="col">
      <div class="step">
        <div class="step-hdr">STEP 1: TRIGGER</div>
        <div class="step-body">Record trigger event</div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr">STEP 2: HEALTH GATE</div>
        <div class="step-body">
          Get component status<br>
          (Kafka/Postgres/MinIO/Jaeger)
        </div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr">STEP 3: GENERATE CLAIMS</div>
        <div class="step-body">Track producer latency</div>
      </div>

      <div class="v-arrow"></div>

      <!-- Phase 2: OTel Collector -->
      <div class="step p2">
        <div class="step-hdr">STEP 3A: OTEL TRACES <span class="new-tag">P2</span></div>
        <div class="step-body">
          correlation_id → OTel spans<br>
          validate_claim → score_fraud<br>
          → enrich_claim<br>
          Export to Jaeger (:16686)
        </div>
        <div class="step-ico">
          <img src="https://cdn.simpleicons.org/opentelemetry/425CC7" alt="OTel"/>
          <span>OpenTelemetry Collector</span>
        </div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr">STEP 8: GENERATE REPORT</div>
        <div class="step-body">Track processing latency</div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr">STEP 9: STORE SUMMARY</div>
        <div class="step-body">Track producer latency</div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr">STEP 10: STORE SUMMARY</div>
        <div class="step-body">Track 4 trigger to Prometheus</div>
      </div>

      <div class="v-arrow"></div>

      <div class="step">
        <div class="step-hdr">STEP 12: STORE SUMMARY</div>
        <div class="step-body">
          Push health snapshot + all<br>
          metrics to Prometheus
        </div>
        <div class="step-ico">
          <img src="https://cdn.simpleicons.org/grafana/F46800" alt="Grafana"/>
          <img src="https://cdn.simpleicons.org/prometheus/E6522C" alt="Prom"/>
          <img src="https://cdn.simpleicons.org/jaeger/0a7ab0" alt="Jaeger"/>
        </div>
      </div>

      <!-- Phase 2: DQ Metrics -->
      <div class="step p2" style="margin-top:6px;">
        <div class="step-hdr">DQ METRICS <span class="new-tag">P2</span></div>
        <div class="step-body" style="font-size:8px;">
          completeness / validity /<br>
          timeliness / consistency /<br>
          overall weighted score<br>
          + circuit breaker trip count<br>
          + consumer lag per partition<br>
          + schema validation errors<br>
          + duplicate claims detected
        </div>
      </div>
    </div>

  </div><!-- /cols -->

  <!-- ══════════════════════════════════════════════════ -->
  <!-- BACKUP WORKFLOW                                     -->
  <!-- ══════════════════════════════════════════════════ -->
  <div class="backup-section">
    <div class="backup-title">BACKUP WORKFLOW</div>
    <div class="backup-sub">(Daily 2 AM UTC)</div>
    <div class="backup-flow">
      <div class="backup-step">
        <div class="step-hdr">STEP 1:<br>TRIGGER</div>
      </div>
      <div class="b-arrow"></div>
      <div class="backup-step">
        <div class="step-hdr">STEP 2:<br>BACKUP DB</div>
        <div class="step-body"><code style="font-size:7.5px;">pg_dump → .sql.gz</code></div>
      </div>
      <div class="b-arrow"></div>
      <div class="backup-step">
        <div class="step-hdr">STEP 3:<br>UPLOAD to MinIO</div>
        <div class="step-ico" style="justify-content:center;">
          <img src="https://cdn.simpleicons.org/minio/C72C48" alt="MinIO"/>
        </div>
      </div>
      <div class="b-arrow"></div>
      <div class="b-storage">Storage<br><span style="font-size:7.5px;color:#888;">backups/postgres/</span></div>
      <div class="b-arrow"></div>
      <div class="backup-step">
        <div class="step-hdr">STEP 4:<br>VERIFY INTEGRITY</div>
        <div class="step-body">Check file exists, log size</div>
      </div>
      <div class="b-arrow"></div>
      <div class="backup-step">
        <div class="step-hdr">STEP 5:<br>RETENTION CHECK</div>
        <div class="step-body">Delete backups &gt; 30 days</div>
      </div>
    </div>
  </div>

  <!-- ══════════════════════════════════════════════════ -->
  <!-- DECISION PATHS FOOTER                               -->
  <!-- ══════════════════════════════════════════════════ -->
  <div class="paths">
    <div class="paths-box">
      <strong>DECISION PATHS:</strong>
      [Health Check Fails] → Alert: component_down → STOP<br>
      [Invalid Claim] → Route to DLQ → Log reason → Continue<br>
      [Enrichment Timeout] → Fallback enrichment → Set reserve = 0 → Alert: latency_high<br>
      <span style="color:#2563eb;font-weight:700;">[Schema Version Unknown] → Route to DLQ → Log schema_error</span><br>
      <span style="color:#2563eb;font-weight:700;">[Circuit Breaker OPEN] → Skip component → Alert: breaker_tripped</span><br>
      <span style="color:#2563eb;font-weight:700;">[Duplicate Detected] → Skip → Log idempotency_key match</span>
    </div>
    <div class="paths-box">
      <strong>DECISION PATHS (continued):</strong>
      [Health Check Fails] → Alert: component_down → STOP<br>
      [Invalid Claim] → Route to DLQ → Log reason → Continue<br>
      [Enrichment Timeout] → Fallback enrichment → Set reserve = 0 → Alert: latency_high<br>
      <span style="color:#2563eb;font-weight:700;">[DQ Score &lt; Threshold] → Flag for review → Metric: dq_below_threshold</span><br>
      <span style="color:#2563eb;font-weight:700;">[PII Detected in Logs] → Auto-mask → Audit: pii_masked</span><br>
      <span style="color:#2563eb;font-weight:700;">[Graceful Drain] → Flush batch → Commit offsets → Close</span>
    </div>
  </div>

</div><!-- /chart -->
</body>
</html>
'''

path = "/Users/tarun/Desktop/Data Obs/insurance-pipeline-observability/assets/linkedin/phase2_workflow_chart.html"
with open(path, "w", encoding="utf-8") as f:
    f.write(HTML)

import os
print(f"Written {os.path.getsize(path):,} bytes to {path}")
