#!/usr/bin/env python3
"""Generate LinkedIn-friendly Phase 2 workflow chart."""

HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=1200"/>
<title>Insurance Claims Pipeline — Phase 2 Workflow</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
*{box-sizing:border-box;margin:0;padding:0;}
body{background:#f8f9fb;font-family:'Inter','Segoe UI',sans-serif;}

/* ── CANVAS ── */
.canvas{width:1200px;margin:0 auto;background:#fff;min-height:100vh;}

/* ── HEADER BANNER ── */
.hero{
  background:linear-gradient(135deg,#0f172a 0%,#1e293b 50%,#0f172a 100%);
  padding:40px 48px 36px;
  position:relative;
  overflow:hidden;
}
.hero::after{
  content:'';position:absolute;top:0;right:0;width:340px;height:100%;
  background:linear-gradient(135deg,transparent 40%,rgba(37,99,235,0.12) 100%);
}
.hero-badge{
  display:inline-block;
  background:linear-gradient(135deg,#2563eb,#3b82f6);
  color:#fff;font-size:11px;font-weight:800;letter-spacing:0.08em;
  padding:5px 14px;border-radius:20px;margin-bottom:14px;
  text-transform:uppercase;
}
.hero h1{
  font-size:32px;font-weight:900;color:#fff;line-height:1.2;
  margin-bottom:8px;letter-spacing:-0.02em;
}
.hero h1 span{color:#60a5fa;}
.hero p{
  font-size:15px;color:#94a3b8;line-height:1.5;max-width:700px;
}

/* ── TECH STRIP ── */
.tech-strip{
  display:flex;align-items:center;gap:20px;
  padding:14px 48px;
  background:#f1f5f9;border-bottom:1px solid #e2e8f0;
  flex-wrap:wrap;
}
.tech-chip{
  display:flex;align-items:center;gap:6px;
  font-size:11.5px;font-weight:600;color:#334155;
}
.tech-chip img{width:18px;height:18px;}

/* ── SECTION ── */
.section{padding:28px 48px 0;}
.section-title{
  font-size:14px;font-weight:900;color:#0f172a;
  text-transform:uppercase;letter-spacing:0.06em;
  padding-bottom:8px;border-bottom:2.5px solid #0f172a;
  margin-bottom:20px;display:flex;align-items:center;gap:10px;
}
.section-title .count{
  background:#0f172a;color:#fff;font-size:10px;font-weight:800;
  padding:2px 8px;border-radius:10px;
}

/* ── 3-COLUMN GRID ── */
.grid3{
  display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;
  margin-bottom:24px;
}

/* ── STEP CARD ── */
.card{
  border:2px solid #e2e8f0;border-radius:10px;
  padding:16px 18px;background:#fff;
  position:relative;transition:border-color 0.15s;
}
.card.p2{border-color:#2563eb;background:#f8faff;}
.card .num{
  font-size:10px;font-weight:800;color:#94a3b8;
  text-transform:uppercase;letter-spacing:0.06em;
  margin-bottom:6px;
}
.card.p2 .num{color:#2563eb;}
.card h3{
  font-size:14px;font-weight:800;color:#0f172a;
  margin-bottom:6px;display:flex;align-items:center;gap:8px;
}
.card.p2 h3{color:#1e40af;}
.p2-tag{
  font-size:8.5px;font-weight:800;background:#2563eb;color:#fff;
  padding:2px 7px;border-radius:3px;text-transform:uppercase;
  letter-spacing:0.04em;flex-shrink:0;
}
.card p{font-size:12px;color:#475569;line-height:1.55;}
.card .tech{
  display:flex;align-items:center;gap:5px;
  margin-top:8px;font-size:11px;color:#64748b;font-weight:500;
}
.card .tech img{width:16px;height:16px;}

/* ── DECISION GATE ── */
.gate{
  border:2.5px solid #0f172a;border-radius:10px;
  padding:16px 18px;background:#f8fafc;
  text-align:center;position:relative;
}
.gate h3{
  font-size:14px;font-weight:800;color:#0f172a;
  margin-bottom:4px;justify-content:center;display:flex;
}
.gate p{font-size:12px;color:#64748b;font-style:italic;}
.gate-labels{
  display:flex;justify-content:center;gap:24px;margin-top:10px;
}
.gate-lbl{
  font-size:11px;font-weight:800;
  padding:3px 14px;border-radius:4px;
}
.gate-lbl.fail{background:#fef2f2;color:#dc2626;border:1.5px solid #fca5a5;}
.gate-lbl.pass{background:#f0fdf4;color:#16a34a;border:1.5px solid #86efac;}

/* ── FLOW ARROW ROW ── */
.flow-arrow{
  display:flex;justify-content:center;align-items:center;
  padding:6px 0;
}
.flow-arrow svg{width:24px;height:24px;}

/* ── HORIZONTAL FLOW (BACKUP) ── */
.hflow{
  display:flex;align-items:center;gap:0;
  padding:0 0 20px;flex-wrap:nowrap;
}
.hflow .card{flex:1;min-width:0;text-align:center;}
.hflow .card h3{justify-content:center;font-size:12.5px;}
.hflow .card p{font-size:11px;}
.h-arr{
  width:32px;height:2px;background:#334155;position:relative;flex-shrink:0;
}
.h-arr::after{
  content:'';position:absolute;right:-5px;top:-4px;
  border-top:5px solid transparent;border-bottom:5px solid transparent;
  border-left:8px solid #334155;
}

/* ── DECISION PATHS BOX ── */
.dpaths{
  padding:20px 48px 28px;
}
.dpaths-grid{
  display:grid;grid-template-columns:1fr 1fr;gap:24px;
}
.dpath-box{
  background:#f8fafc;border:1.5px solid #e2e8f0;border-radius:8px;
  padding:16px 18px;
}
.dpath-box strong{
  font-size:12px;font-weight:800;color:#0f172a;
  display:block;margin-bottom:8px;
}
.dpath-box .rule{
  font-size:11.5px;color:#475569;line-height:1.7;
}
.dpath-box .rule.p2-rule{color:#2563eb;font-weight:600;}

/* ── FOOTER ── */
.footer{
  background:#0f172a;padding:28px 48px;
  display:flex;align-items:center;justify-content:space-between;
}
.footer .tags{
  font-size:12.5px;color:#60a5fa;font-weight:600;
  line-height:1.6;
}
.footer .author{
  text-align:right;color:#94a3b8;font-size:12px;
}
.footer .author strong{color:#fff;font-size:14px;display:block;margin-bottom:2px;}
</style>
</head>
<body>
<div class="canvas">

  <!-- ═══════════════ HERO BANNER ═══════════════ -->
  <div class="hero">
    <div class="hero-badge">Phase 2 — Production Grade</div>
    <h1>Insurance Claims Pipeline<br><span>End-to-End Workflow</span></h1>
    <p>Real-time streaming pipeline with Schema Registry, Circuit Breakers,
       DQ Governance, PII Masking, Iceberg Lakehouse, and full OpenTelemetry observability.</p>
  </div>

  <!-- ═══════════════ TECH STRIP ═══════════════ -->
  <div class="tech-strip">
    <div class="tech-chip"><img src="https://cdn.simpleicons.org/apacheairflow/017CEE" alt=""/>Airflow</div>
    <div class="tech-chip"><img src="https://cdn.simpleicons.org/apachekafka/231F20" alt=""/>Kafka</div>
    <div class="tech-chip"><img src="https://cdn.simpleicons.org/postgresql/336791" alt=""/>Postgres</div>
    <div class="tech-chip"><img src="https://cdn.simpleicons.org/minio/C72C48" alt=""/>MinIO</div>
    <div class="tech-chip"><img src="https://cdn.simpleicons.org/redis/DC382D" alt=""/>Redis</div>
    <div class="tech-chip"><img src="https://cdn.simpleicons.org/prometheus/E6522C" alt=""/>Prometheus</div>
    <div class="tech-chip"><img src="https://cdn.simpleicons.org/grafana/F46800" alt=""/>Grafana</div>
    <div class="tech-chip"><img src="https://cdn.simpleicons.org/opentelemetry/425CC7" alt=""/>OTel</div>
    <div class="tech-chip"><img src="https://cdn.simpleicons.org/jaeger/0a7ab0" alt=""/>Jaeger</div>
    <div class="tech-chip"><img src="https://cdn.simpleicons.org/python/3776AB" alt=""/>Python</div>
    <div class="tech-chip"><img src="https://cdn.simpleicons.org/docker/2496ED" alt=""/>Docker</div>
    <div class="tech-chip"><img src="https://cdn.simpleicons.org/fastapi/009688" alt=""/>FastAPI</div>
    <div class="tech-chip"><img src="https://cdn.simpleicons.org/apacheavro/CC0909" alt=""/>Schema Registry</div>
  </div>

  <!-- ═══════════════ ORCHESTRATION & INGESTION ═══════════════ -->
  <div class="section">
    <div class="section-title">
      <img src="https://cdn.simpleicons.org/apacheairflow/017CEE" style="width:20px;height:20px;" alt=""/>
      Orchestration &amp; Ingestion
      <span class="count">4 steps</span>
    </div>
    <div class="grid3">
      <div class="card">
        <div class="num">Step 1</div>
        <h3>DAG Trigger</h3>
        <p>Airflow fires every 15 minutes. Kicks off the full pipeline cycle.</p>
        <div class="tech"><img src="https://cdn.simpleicons.org/apacheairflow/017CEE" alt=""/>15-min cron schedule</div>
      </div>
      <div class="gate">
        <div class="num" style="font-size:10px;font-weight:800;color:#94a3b8;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:6px;">Step 2</div>
        <h3>Health Gate</h3>
        <p>Check Kafka, Postgres, MinIO, Jaeger availability</p>
        <div class="gate-labels">
          <div class="gate-lbl fail">FAIL → Stop + Alert</div>
          <div class="gate-lbl pass">PASS → Continue</div>
        </div>
      </div>
      <div class="card">
        <div class="num">Step 3</div>
        <h3>Generate Claims</h3>
        <p>Produce 50 synthetic insurance claims per batch with realistic data distributions.</p>
        <div class="tech"><img src="https://cdn.simpleicons.org/python/3776AB" alt=""/>Batch producer (50/run)</div>
      </div>
    </div>
    <div class="grid3">
      <div class="card">
        <div class="num">Step 4</div>
        <h3>Publish to Kafka</h3>
        <p>Push raw claims to <code style="background:#f1f5f9;padding:1px 5px;border-radius:3px;font-size:11px;">insurance.claims.raw</code> with trace context headers.</p>
        <div class="tech"><img src="https://cdn.simpleicons.org/apachekafka/231F20" alt=""/>Kafka raw topic</div>
      </div>
      <div class="card p2">
        <div class="num">Step 4A</div>
        <h3>Schema Tag + Dedup Key <span class="p2-tag">P2</span></h3>
        <p>Attach <code style="background:#eff6ff;padding:1px 4px;border-radius:2px;font-size:11px;">correlation_id</code> + <code style="background:#eff6ff;padding:1px 4px;border-radius:2px;font-size:11px;">schema_version</code> headers.<br>
        Generate SHA-256 idempotency key from policy|loss_date|amount.</p>
        <div class="tech"><img src="https://cdn.simpleicons.org/apacheavro/CC0909" alt=""/>Schema Registry v1</div>
      </div>
      <div class="card p2">
        <div class="num">Phase 2</div>
        <h3>PII Masking <span class="p2-tag">P2</span></h3>
        <p>Auto-mask sensitive fields before storage:<br>
        name → J.D. &nbsp;|&nbsp; VIN → ***-4352<br>
        policy → ABC-***456 &nbsp;|&nbsp; address → city, state</p>
      </div>
    </div>
  </div>

  <!-- ═══════════════ STREAM PROCESSING ═══════════════ -->
  <div class="section">
    <div class="section-title">
      <img src="https://cdn.simpleicons.org/apachekafka/231F20" style="width:20px;height:20px;" alt=""/>
      Stream Processing
      <span class="count">5 steps</span>
    </div>
    <div class="grid3">
      <div class="card p2">
        <div class="num">Step 5A</div>
        <h3>Schema Validate <span class="p2-tag">P2</span></h3>
        <p>Validate each message against Confluent Schema Registry.
        Unsupported schema version → route to DLQ.</p>
        <div class="tech"><img src="https://cdn.simpleicons.org/apacheavro/CC0909" alt=""/>4 registered JSON schemas</div>
      </div>
      <div class="card">
        <div class="num">Step 6</div>
        <h3>Validate Claims</h3>
        <p>Check required fields, format compliance, amount ranges, date consistency.</p>
      </div>
      <div class="card p2">
        <div class="num">Step 6A</div>
        <h3>DQ Scorecard <span class="p2-tag">P2</span></h3>
        <p>4-dimension quality score published every 10 claims:<br>
        Completeness 35% · Validity 35%<br>
        Timeliness 15% · Consistency 15%</p>
      </div>
    </div>

    <div class="flow-arrow">
      <svg viewBox="0 0 24 24" fill="none"><path d="M12 4v14M5 13l7 7 7-7" stroke="#334155" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/></svg>
    </div>

    <div class="grid3">
      <div class="gate" style="grid-column:1/2;">
        <div class="num" style="font-size:10px;font-weight:800;color:#94a3b8;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:6px;">Step 7</div>
        <h3>Validation Gate</h3>
        <p>CRITICAL / HIGH / MEDIUM / LOW severity</p>
        <div class="gate-labels">
          <div class="gate-lbl fail">Invalid → DLQ</div>
          <div class="gate-lbl pass">Valid → Fraud Check</div>
        </div>
      </div>
      <div class="card">
        <div class="num">Step 8</div>
        <h3>Fraud Detection</h3>
        <p>8 weighted rules + sigmoid scoring → Risk Level.<br>
        <span style="font-size:10.5px;color:#94a3b8;">CRITICAL ≥0.8 · HIGH 0.6 · MED 0.3 · LOW &lt;0.3</span></p>
      </div>
      <div class="card">
        <div class="num">Step 9</div>
        <h3>Enrichment</h3>
        <p>Policy lookup, claims history, geo-risk scoring, adjuster assignment, reserve calculation.</p>
        <div class="tech"><img src="https://cdn.simpleicons.org/redis/DC382D" alt=""/>Redis cache lookups</div>
      </div>
    </div>
  </div>

  <!-- ═══════════════ STORAGE & PERSISTENCE ═══════════════ -->
  <div class="section">
    <div class="section-title">
      <img src="https://cdn.simpleicons.org/postgresql/336791" style="width:20px;height:20px;" alt=""/>
      Storage &amp; Persistence
      <span class="count">4 sinks</span>
    </div>
    <div class="grid3" style="grid-template-columns:1fr 1fr 1fr 1fr;gap:14px;">
      <div class="card">
        <h3 style="font-size:13px;">
          <img src="https://cdn.simpleicons.org/postgresql/336791" style="width:18px;height:18px;" alt=""/>
          PostgreSQL
        </h3>
        <p>Batch upsert to processed_claims (50-record buffer). Lineage metadata persisted.</p>
      </div>
      <div class="card">
        <h3 style="font-size:13px;">
          <img src="https://cdn.simpleicons.org/minio/C72C48" style="width:18px;height:18px;" alt=""/>
          MinIO S3
        </h3>
        <p>Object storage buckets: validated/ scored/ enriched/ rejected/ reports/</p>
      </div>
      <div class="card p2">
        <h3 style="font-size:13px;">
          <svg viewBox="0 0 24 24" style="width:18px;height:18px;" fill="none"><polygon points="12,2 22,8 22,16 12,22 2,16 2,8" stroke="#2563eb" stroke-width="1.5" fill="#eff6ff"/><text x="12" y="15" text-anchor="middle" font-size="7" font-weight="800" fill="#2563eb">ICE</text></svg>
          Iceberg <span class="p2-tag">P2</span>
        </h3>
        <p>ACID lakehouse append.<br>
        Parquet format. Time-travel queries enabled.</p>
      </div>
      <div class="card p2">
        <h3 style="font-size:13px;">
          <img src="https://cdn.simpleicons.org/apachekafka/dc2626" style="width:18px;height:18px;" alt=""/>
          DLQ
        </h3>
        <p>Dead Letter Queue for invalid claims, schema failures, and processing errors.</p>
      </div>
    </div>
    <div class="grid3" style="margin-top:0;">
      <div class="card p2">
        <div class="num">Phase 2</div>
        <h3>Circuit Breakers <span class="p2-tag">P2</span></h3>
        <p>PostgreSQL: 5 failures → OPEN (30s recovery)<br>
        MinIO: 3 failures → OPEN (20s recovery)<br>
        3-state: CLOSED → OPEN → HALF-OPEN</p>
      </div>
      <div class="card p2">
        <div class="num">Phase 2</div>
        <h3>Idempotency <span class="p2-tag">P2</span></h3>
        <p>SHA-256 dedup key per claim.<br>
        DB-level duplicate detection.<br>
        Exactly-once write semantics.</p>
      </div>
      <div class="card p2">
        <div class="num">Phase 2</div>
        <h3>Graceful Drain <span class="p2-tag">P2</span></h3>
        <p>SIGTERM → flush PG batch → producer.flush() → commit offsets → consumer.close()</p>
      </div>
    </div>
  </div>

  <!-- ═══════════════ LINEAGE & OBSERVABILITY ═══════════════ -->
  <div class="section">
    <div class="section-title">
      <img src="https://cdn.simpleicons.org/prometheus/E6522C" style="width:20px;height:20px;" alt=""/>
      Lineage &amp; Observability
      <span class="count">Full Stack</span>
    </div>
    <div class="grid3">
      <div class="card">
        <div class="num">Step 11</div>
        <h3>Lineage Report</h3>
        <p>Aggregate coverage report with node/edge/event statistics.
        Full data provenance graph per claim lifecycle.</p>
      </div>
      <div class="card p2">
        <div class="num">Phase 2</div>
        <h3>Audit Trail <span class="p2-tag">P2</span></h3>
        <p>Immutable append-only event log per claim:<br>
        stage → status → timestamp → correlation_id<br>
        Complete provenance chain.</p>
      </div>
      <div class="card p2">
        <div class="num">Phase 2</div>
        <h3>OTel Traces <span class="p2-tag">P2</span></h3>
        <p>correlation_id → OTel spans across validate → score → enrich.<br>
        Exported to Jaeger (:16686) for distributed trace visualization.</p>
        <div class="tech">
          <img src="https://cdn.simpleicons.org/opentelemetry/425CC7" alt=""/>OTel Collector
          <img src="https://cdn.simpleicons.org/jaeger/0a7ab0" alt="" style="margin-left:8px;"/>Jaeger
        </div>
      </div>
    </div>
    <div class="grid3">
      <div class="card">
        <div class="num">Step 12</div>
        <h3>Metrics Push</h3>
        <p>Push health snapshot + all counters, histograms, and gauges to Prometheus.</p>
        <div class="tech">
          <img src="https://cdn.simpleicons.org/prometheus/E6522C" alt=""/>Prometheus
          <img src="https://cdn.simpleicons.org/grafana/F46800" alt="" style="margin-left:8px;"/>Grafana
        </div>
      </div>
      <div class="card p2">
        <div class="num">Phase 2</div>
        <h3>DQ Metrics <span class="p2-tag">P2</span></h3>
        <p>completeness / validity / timeliness / consistency scores +
        circuit breaker trip count + consumer lag + schema validation errors + duplicates detected</p>
      </div>
      <div class="card">
        <div class="num">Step 13</div>
        <h3>S3 Report Upload</h3>
        <p>Store pipeline summary JSON to MinIO <code style="background:#f1f5f9;padding:1px 4px;border-radius:2px;font-size:11px;">reports/</code> bucket for historical record.</p>
        <div class="tech"><img src="https://cdn.simpleicons.org/minio/C72C48" alt=""/>MinIO S3</div>
      </div>
    </div>
  </div>

  <!-- ═══════════════ BACKUP WORKFLOW ═══════════════ -->
  <div class="section" style="padding-bottom:0;">
    <div class="section-title">
      <svg viewBox="0 0 24 24" style="width:20px;height:20px;" fill="none"><rect x="3" y="3" width="18" height="18" rx="3" stroke="#0f172a" stroke-width="2"/><path d="M8 12l3 3 5-5" stroke="#16a34a" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/></svg>
      Backup &amp; Disaster Recovery
      <span class="count">Daily 2 AM UTC</span>
    </div>
    <div class="hflow">
      <div class="card">
        <div class="num">Step 1</div>
        <h3>Trigger</h3>
        <p>Daily cron</p>
      </div>
      <div class="h-arr"></div>
      <div class="card">
        <div class="num">Step 2</div>
        <h3>Backup DB</h3>
        <p>pg_dump → .sql.gz</p>
      </div>
      <div class="h-arr"></div>
      <div class="card">
        <div class="num">Step 3</div>
        <h3>Upload to MinIO</h3>
        <p>backups/postgres/</p>
      </div>
      <div class="h-arr"></div>
      <div class="card">
        <div class="num">Step 4</div>
        <h3>Verify Integrity</h3>
        <p>Check file, log size</p>
      </div>
      <div class="h-arr"></div>
      <div class="card">
        <div class="num">Step 5</div>
        <h3>Retention</h3>
        <p>Delete > 30 days</p>
      </div>
    </div>
  </div>

  <!-- ═══════════════ DECISION PATHS ═══════════════ -->
  <div class="dpaths">
    <div class="dpaths-grid">
      <div class="dpath-box">
        <strong>Decision Paths</strong>
        <div class="rule">[Health Check Fails] → Alert: component_down → STOP</div>
        <div class="rule">[Invalid Claim] → Route to DLQ → Log reason → Continue</div>
        <div class="rule">[Enrichment Timeout] → Fallback → Set reserve = 0 → Alert</div>
        <div class="rule p2-rule">[Schema Version Unknown] → DLQ → Log schema_error</div>
        <div class="rule p2-rule">[Circuit Breaker OPEN] → Skip → Alert: breaker_tripped</div>
        <div class="rule p2-rule">[Duplicate Detected] → Skip → Log idempotency_key match</div>
      </div>
      <div class="dpath-box">
        <strong>Decision Paths (continued)</strong>
        <div class="rule p2-rule">[DQ Score < Threshold] → Flag for review → Metric alert</div>
        <div class="rule p2-rule">[PII Detected in Logs] → Auto-mask → Audit: pii_masked</div>
        <div class="rule p2-rule">[Graceful Drain] → Flush batch → Commit → Close</div>
        <div class="rule">[Fraud CRITICAL ≥0.8] → Flag → Supervisor review queue</div>
        <div class="rule">[MinIO Unavailable] → Circuit breaker OPEN → Retry 20s</div>
        <div class="rule">[Backup Failure] → Alert: backup_failed → Retry next cycle</div>
      </div>
    </div>
  </div>

  <!-- ═══════════════ FOOTER ═══════════════ -->
  <div class="footer">
    <div class="tags">
      #DataEngineering &nbsp; #ApacheKafka &nbsp; #StreamProcessing &nbsp; #Observability<br>
      #Python &nbsp; #Airflow &nbsp; #OpenTelemetry &nbsp; #ApacheIceberg &nbsp; #DataQuality
    </div>
    <div class="author">
      <strong>Insurance Claims Pipeline — Phase 2</strong>
      Real-time · Observable · Production-grade
    </div>
  </div>

</div>
</body>
</html>
'''

import os

out = "/Users/tarun/Desktop/Data Obs/insurance-pipeline-observability/assets/linkedin/phase2_workflow_chart.html"
os.makedirs(os.path.dirname(out), exist_ok=True)
with open(out, "w", encoding="utf-8") as f:
    f.write(HTML)
print(f"Written {os.path.getsize(out):,} bytes → {out}")
