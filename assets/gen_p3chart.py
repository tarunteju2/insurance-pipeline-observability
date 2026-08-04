import os

HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<title>Insurance Claims Pipeline — Phase 3 Enterprise Workflow</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800;900&family=JetBrains+Mono:wght@500;700&display=swap');
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #090d16;
    color: #f1f5f9;
    font-family: 'Inter', -apple-system, sans-serif;
    padding: 28px;
    width: 1480px;
    margin: 0 auto;
  }

  /* ── HEADER BAR ── */
  .header-box {
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
    border: 1.5px solid #334155;
    border-radius: 12px;
    padding: 24px 32px;
    margin-bottom: 24px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.5);
  }
  .phase-pill {
    display: inline-block;
    background: #2563eb;
    color: #fff;
    font-size: 11px;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    padding: 4px 12px;
    border-radius: 20px;
    margin-bottom: 8px;
  }
  .main-title {
    font-size: 26px;
    font-weight: 900;
    color: #f8fafc;
    letter-spacing: -0.02em;
  }
  .main-subtitle {
    font-size: 13.5px;
    color: #94a3b8;
    margin-top: 4px;
    font-weight: 500;
  }

  /* ── TECH BADGES ── */
  .tech-bar {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 16px;
  }
  .tech-badge {
    background: rgba(30, 41, 59, 0.8);
    border: 1px solid #334155;
    color: #cbd5e1;
    font-size: 11px;
    font-weight: 600;
    padding: 4px 10px;
    border-radius: 6px;
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .tech-badge.new {
    background: rgba(37, 99, 235, 0.2);
    border-color: #3b82f6;
    color: #60a5fa;
  }

  /* ── SECTION PANELS ── */
  .section-card {
    background: #0f172a;
    border: 1px solid #1e293b;
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 20px;
  }
  .section-hdr {
    display: flex;
    align-items: center;
    justify-content: space-between;
    font-size: 12px;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #38bdf8;
    border-bottom: 1px solid #1e293b;
    padding-bottom: 10px;
    margin-bottom: 16px;
  }
  .step-count {
    background: #1e293b;
    color: #94a3b8;
    padding: 2px 8px;
    border-radius: 10px;
    font-size: 10px;
  }

  /* ── GRID LAYOUT ── */
  .grid-4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; }
  .grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }

  /* ── NODE CARDS ── */
  .node {
    background: #1e293b;
    border: 1.5px solid #334155;
    border-radius: 8px;
    padding: 14px;
    position: relative;
  }
  .node.highlight {
    border-color: #38bdf8;
    box-shadow: 0 0 15px rgba(56, 189, 248, 0.15);
  }
  .node-title {
    font-size: 12px;
    font-weight: 700;
    color: #f8fafc;
    margin-bottom: 6px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .tag-p3 {
    background: #0284c7;
    color: #fff;
    font-size: 8px;
    font-weight: 800;
    padding: 2px 6px;
    border-radius: 4px;
    text-transform: uppercase;
  }
  .node-desc {
    font-size: 11px;
    color: #94a3b8;
    line-height: 1.45;
  }
  .node-meta {
    margin-top: 10px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    color: #38bdf8;
    background: #090d16;
    padding: 6px 8px;
    border-radius: 4px;
    border: 1px solid #1e293b;
  }

  /* ── FOOTER ── */
  .footer {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 11px;
    color: #64748b;
    border-top: 1px solid #1e293b;
    padding-top: 16px;
    margin-top: 10px;
  }
</style>
</head>
<body>

<div class="header-box">
  <span class="phase-pill">Phase 3 — Enterprise Platform</span>
  <div class="main-title">Insurance Claims Pipeline — End-to-End Modern Architecture</div>
  <div class="main-subtitle">Python 3.11 Runtime • Linux Foundation OpenLineage 1.0 • DuckDB OLAP Engine • Apache Iceberg Catalog • Stateful Window Analytics • Dynamic RBAC Column Masking</div>
  
  <div class="tech-bar">
    <div class="tech-badge new">Python 3.11</div>
    <div class="tech-badge new">Airflow 2.11.1</div>
    <div class="tech-badge new">OpenLineage 1.0</div>
    <div class="tech-badge new">DuckDB OLAP</div>
    <div class="tech-badge new">Apache Iceberg</div>
    <div class="tech-badge">Apache Kafka</div>
    <div class="tech-badge">PostgreSQL 15</div>
    <div class="tech-badge">MinIO S3</div>
    <div class="tech-badge">Redis</div>
    <div class="tech-badge">Prometheus</div>
    <div class="tech-badge">Grafana</div>
    <div class="tech-badge">OpenTelemetry</div>
    <div class="tech-badge">Jaeger</div>
    <div class="tech-badge">FastAPI</div>
  </div>
</div>

<!-- SECTION 1 -->
<div class="section-card">
  <div class="section-hdr">
    <span>1. Orchestration & High-Throughput Stream Ingestion</span>
    <span class="step-count">4 Steps</span>
  </div>
  <div class="grid-4">
    <div class="node">
      <div class="node-title">Step 1: Airflow 2.11.1 Trigger <span class="tag-p3">P3</span></div>
      <div class="node-desc">Airflow 2.11.1 DAG on Python 3.11 runtime executes 15-minute ingestion cycles.</div>
      <div class="node-meta">Cron: */15 * * * *</div>
    </div>
    <div class="node">
      <div class="node-title">Step 2: Pre-Flight Health Gate</div>
      <div class="node-desc">Probes Kafka, Postgres, MinIO, Redis & Jaeger before processing starts.</div>
      <div class="node-meta">FAIL -> Alert | PASS -> Proceed</div>
    </div>
    <div class="node">
      <div class="node-title">Step 3: Claims Batch Producer</div>
      <div class="node-desc">Generates 50 realistic synthetic insurance claims with realistic statistical distributions.</div>
      <div class="node-meta">Batch Size: 50 claims</div>
    </div>
    <div class="node highlight">
      <div class="node-title">Step 4: Schema Tag & Dedup <span class="tag-p3">P3</span></div>
      <div class="node-desc">Attaches schema_version: v1, correlation_id, and SHA-256 idempotency key to Kafka headers.</div>
      <div class="node-meta">Kafka: raw-claims</div>
    </div>
  </div>
</div>

<!-- SECTION 2 -->
<div class="section-card">
  <div class="section-hdr">
    <span>2. Stream Processing, Stateful Windowing & DQ Governance</span>
    <span class="step-count">5 Steps</span>
  </div>
  <div class="grid-4">
    <div class="node">
      <div class="node-title">Step 5: Schema Validation</div>
      <div class="node-desc">Validates payload against registered JSON schema contracts; invalid events route to DLQ.</div>
      <div class="node-meta">Contract: v1 JSON Schema</div>
    </div>
    <div class="node highlight">
      <div class="node-title">Step 6: Stateful Window Engine <span class="tag-p3">P3</span></div>
      <div class="node-desc">1-min sliding & 5-min tumbling windows detecting velocity anomaly spikes (&ge; 3 claims/min).</div>
      <div class="node-meta">Sliding: 60s | Tumbling: 300s</div>
    </div>
    <div class="node highlight">
      <div class="node-title">Step 7: Declarative DQ Scorecard <span class="tag-p3">P3</span></div>
      <div class="node-desc">Evaluates completeness, validity, timeliness, and consistency metrics per claim batch.</div>
      <div class="node-meta">Scorecard: /quality/scorecard</div>
    </div>
    <div class="node">
      <div class="node-title">Step 8: Fraud Detection & SIU</div>
      <div class="node-desc">Bi-weighted scoring rules + sigmoid transformation flag high-risk claims for SIU audit.</div>
      <div class="node-meta">Fraud Score > 0.7 -> Flag</div>
    </div>
  </div>
</div>

<!-- SECTION 3 -->
<div class="section-card">
  <div class="section-hdr">
    <span>3. Storage, Lakehouse Catalog & Vectorized Analytics</span>
    <span class="step-count">4 Engines</span>
  </div>
  <div class="grid-4">
    <div class="node">
      <div class="node-title">PostgreSQL 15 Operational Store</div>
      <div class="node-desc">Batch upsert processed claims into relational Postgres tables for transaction processing.</div>
      <div class="node-meta">Table: processed_claims</div>
    </div>
    <div class="node">
      <div class="node-title">MinIO S3 Data Lake</div>
      <div class="node-desc">Object storage buckets storing partitioned Parquet datasets (validated, scored, enriched).</div>
      <div class="node-meta">Format: Parquet Partitioned</div>
    </div>
    <div class="node highlight">
      <div class="node-title">Apache Iceberg Catalog <span class="tag-p3">P3</span></div>
      <div class="node-desc">ACID table snapshot history, manifest list logs, and FOR SYSTEM_TIME AS OF queries.</div>
      <div class="node-meta">Catalog: /iceberg/snapshots</div>
    </div>
    <div class="node highlight">
      <div class="node-title">DuckDB Vectorized OLAP <span class="tag-p3">P3</span></div>
      <div class="node-desc">Zero-copy vectorized SQL engine executing analytical queries over S3 Parquet lakes.</div>
      <div class="node-meta">Engine: /analytics/duckdb</div>
    </div>
  </div>
</div>

<!-- SECTION 4 -->
<div class="section-card">
  <div class="section-hdr">
    <span>4. Lineage Metadata, Security Governance & Resilience</span>
    <span class="step-count">4 Modules</span>
  </div>
  <div class="grid-4">
    <div class="node highlight">
      <div class="node-title">OpenLineage 1.0 Protocol <span class="tag-p3">P3</span></div>
      <div class="node-desc">Generates Linux Foundation standard RunEvents tracking dataset inputs, outputs & DQ facets.</div>
      <div class="node-meta">Protocol: /lineage/openlineage</div>
    </div>
    <div class="node highlight">
      <div class="node-title">RBAC Column Governance <span class="tag-p3">P3</span></div>
      <div class="node-desc">Dynamic column-level security applying PII masking for PUBLIC, ADJUSTER, EXECUTIVE & AUDITOR.</div>
      <div class="node-meta">Policy: /governance/query</div>
    </div>
    <div class="node highlight">
      <div class="node-title">Chaos Engineering Harness <span class="tag-p3">P3</span></div>
      <div class="node-desc">Simulates network latency spikes, DLQ schema corruption, and circuit breaker trip tests.</div>
      <div class="node-meta">CLI: chaos_injection.py</div>
    </div>
    <div class="node">
      <div class="node-title">Disaster Recovery & Retention</div>
      <div class="node-desc">Automated daily 2 AM UTC pg_dump backup to MinIO with 30-day retention purging.</div>
      <div class="node-meta">DAG: backup_disaster_recovery</div>
    </div>
  </div>
</div>

<div class="footer">
  <div>Insurance Claims Pipeline & Observability Engine — 2026 Production Edition</div>
  <div>Python 3.11 • Airflow 2.11.1 • OpenLineage 1.0 • DuckDB • Apache Iceberg • 83 Passing Tests</div>
</div>

</body>
</html>
'''

def main():
    out_dir = os.path.join(os.path.dirname(__file__), 'linkedin')
    os.makedirs(out_dir, exist_ok=True)
    html_path = os.path.join(out_dir, 'phase3_workflow_chart.html')
    
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(HTML)
    
    print(f"✅ Created Phase 3 workflow infographic HTML at: {html_path}")

if __name__ == '__main__':
    main()
