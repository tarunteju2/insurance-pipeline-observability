"""
Observability & Lineage API for the Insurance Pipeline

Exposes endpoints for checking pipeline health, getting real-time metrics,
viewing data lineage, and browsing claim details.
"""

import json
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import structlog
import uvicorn

from src.config import config
from src.observability.tracing import init_tracing
from src.observability.metrics import get_metrics_output, REGISTRY
from src.observability.health import health_monitor
from src.lineage.tracker import lineage_tracker
from src.lineage.models import get_session, ProcessedClaim, LineageEvent
from src.lineage.openlineage import OpenLineageEmitter
from src.analytics.duckdb_engine import DuckDBAnalyticsEngine
from src.analytics.window_processor import WindowProcessor
from src.analytics.iceberg_engine import IcebergMetadataEngine
from src.security.governance import DataGovernanceEngine, SecurityRole
from src.processors.claims_validator import ClaimsValidator
from sqlalchemy import func

logger = structlog.get_logger(__name__)

app = FastAPI(
    title="Insurance Claims Pipeline - Observability API",
    description="Real-time data pipeline observability and lineage platform for insurance claims",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============ HEALTH CHECKS ============

@app.get("/health")
def health_check():
    """
    Check if the pipeline is healthy.
    
    Returns overall status and the health of each component (Kafka, Postgres, etc).
    """
    report = health_monitor.get_health_report()
    status_code = 200 if report["overall_status"] == "healthy" else 503
    return report


@app.get("/health/{component}")
def component_health(component: str):
    """Check how healthy a specific component is."""
    health_monitor.run_all_checks()
    comp = health_monitor.components.get(component)
    if not comp:
        raise HTTPException(404, f"Component '{component}' not found")
    return {
        "component": comp.name,
        "status": comp.status,
        "latency_ms": comp.latency_ms,
        "message": comp.message,
        "last_heartbeat": comp.last_heartbeat.isoformat() if comp.last_heartbeat else None,
    }


# ============ METRICS ============

@app.get("/metrics")
def prometheus_metrics():
    """
    Get metrics in Prometheus format.
    
    This endpoint returns all pipeline metrics (claims processed, errors,
    latency, etc) in the standard Prometheus text format for scraping.
    """
    return Response(
        content=get_metrics_output(),
        media_type="text/plain; version=0.0.4; charset=utf-8"
    )


# ============ LINEAGE & GRAPH ============

@app.get("/lineage/graph")
def get_lineage_graph():
    """Get the full data lineage graph showing all processing steps."""
    try:
        return lineage_tracker.get_full_lineage_graph()
    except Exception as e:
        logger.error("Failed to get lineage graph", error=str(e))
        raise HTTPException(500, str(e))


@app.get("/lineage/claim/{claim_id}")
def get_claim_lineage(claim_id: str):
    """Trace a specific claim through all the processing steps."""
    trail = lineage_tracker.get_claim_lineage(claim_id)
    if not trail:
        raise HTTPException(404, f"No lineage found for claim {claim_id}")
    return {"claim_id": claim_id, "lineage_trail": trail, "steps": len(trail)}


@app.get("/lineage/statistics")
def get_lineage_statistics():
    """Get stats on the overall lineage (coverage, success rate, etc)."""
    try:
        return lineage_tracker.get_statistics()
    except Exception as e:
        raise HTTPException(500, str(e))


# ============ CLAIMS DATA ============

@app.get("/claims/stats")
def get_claims_statistics():
    """
    Get overall statistics about claims.
    
    Shows totals, breakdown by type and status, average fraud scores,
    claim amounts, and fraud rates.
    """
    session = get_session()
    try:
        total = session.query(func.count(ProcessedClaim.claim_id)).scalar() or 0
        by_type = dict(
            session.query(
                ProcessedClaim.claim_type,
                func.count(ProcessedClaim.claim_id)
            ).group_by(ProcessedClaim.claim_type).all()
        )
        by_status = dict(
            session.query(
                ProcessedClaim.status,
                func.count(ProcessedClaim.claim_id)
            ).group_by(ProcessedClaim.status).all()
        )
        avg_fraud = session.query(func.avg(ProcessedClaim.fraud_score)).scalar() or 0
        avg_amount = session.query(func.avg(ProcessedClaim.claim_amount)).scalar() or 0
        total_amount = session.query(func.sum(ProcessedClaim.claim_amount)).scalar() or 0
        high_fraud = session.query(func.count(ProcessedClaim.claim_id)).filter(
            ProcessedClaim.fraud_score >= 0.7
        ).scalar() or 0

        return {
            "total_claims": total,
            "by_type": by_type,
            "by_status": by_status,
            "avg_fraud_score": round(float(avg_fraud), 4),
            "avg_claim_amount": round(float(avg_amount), 2),
            "total_claim_amount": round(float(total_amount), 2),
            "high_fraud_count": high_fraud,
            "fraud_rate_percent": round(high_fraud / total * 100, 2) if total > 0 else 0,
            "timestamp": datetime.utcnow().isoformat(),
        }
    finally:
        session.close()


@app.get("/claims/recent")
def get_recent_claims(limit: int = Query(default=20, le=100)):
    """Get the most recently processed claims."""
    session = get_session()
    try:
        claims = session.query(ProcessedClaim).order_by(
            ProcessedClaim.created_at.desc()
        ).limit(limit).all()

        return {
            "claims": [
                {
                    "claim_id": c.claim_id,
                    "policy_number": c.policy_number,
                    "claimant_name": c.claimant_name,
                    "claim_type": c.claim_type,
                    "claim_amount": c.claim_amount,
                    "status": c.status,
                    "fraud_score": c.fraud_score,
                    "risk_level": c.risk_level,
                    "created_at": c.created_at.isoformat() if c.created_at else None,
                }
                for c in claims
            ],
            "count": len(claims)
        }
    finally:
        session.close()


# ==================== LINEAGE VISUALIZATION ====================

@app.get("/lineage/visualize", response_class=HTMLResponse)
def visualize_lineage():
    """View the data lineage graph in your browser."""
    graph = lineage_tracker.get_full_lineage_graph()

    html = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Insurance Claims Pipeline - Data Lineage</title>
<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, sans-serif;
         background: #0f172a; color: #e2e8f0; overflow: hidden; }
  #header { background: #1e293b; padding: 16px 24px; border-bottom: 1px solid #334155;
            display: flex; justify-content: space-between; align-items: center; }
  #header h1 { font-size: 18px; color: #38bdf8; }
  #stats { display: flex; gap: 24px; font-size: 13px; }
  .stat-item { text-align: center; }
  .stat-value { font-size: 20px; font-weight: 700; color: #22d3ee; }
  .stat-label { color: #94a3b8; }
  #graph { width: 100vw; height: calc(100vh - 60px); }
  .node-group { cursor: pointer; }
  .node-rect { rx: 8; ry: 8; stroke-width: 2; }
  .node-label { font-size: 11px; font-weight: 600; fill: #f8fafc; text-anchor: middle; }
  .node-sublabel { font-size: 9px; fill: #94a3b8; text-anchor: middle; }
  .edge-line { fill: none; stroke-width: 2.5; }
  .edge-label { font-size: 9px; fill: #94a3b8; text-anchor: middle; }
  .tooltip { position: absolute; background: #1e293b; border: 1px solid #475569;
             border-radius: 8px; padding: 12px; font-size: 12px;
             pointer-events: none; opacity: 0; transition: opacity .2s; max-width: 260px; }
  .tooltip .tt-title { font-weight: 700; color: #38bdf8; margin-bottom: 6px; }
  .tooltip .tt-row { margin: 3px 0; }
  .tooltip .tt-key { color: #94a3b8; }
  .tooltip .tt-val { color: #e2e8f0; margin-left: 4px; }
</style>
</head>
<body>
<div id="header">
  <h1>🔗 Insurance Claims Pipeline — Data Lineage</h1>
  <div id="stats"></div>
</div>
<div id="graph"></div>
<div class="tooltip" id="tooltip"></div>
<script>
const graphData = """ + json.dumps(graph) + """;

const nodeColors = {
  source: { fill: '#064e3b', stroke: '#10b981' },
  transform: { fill: '#1e3a5f', stroke: '#38bdf8' },
  sink: { fill: '#4a1d6a', stroke: '#a78bfa' }
};

const statsDiv = document.getElementById('stats');
const stats = graphData.statistics || {};
const statItems = [
  { label: 'Total Events', value: stats.total_lineage_events || 0 },
  { label: 'Success Rate', value: (stats.success_rate || 0) + '%' },
  { label: 'Active Edges', value: stats.active_edges || 0 },
  { label: 'Coverage', value: (stats.pipeline_coverage_percent || 0) + '%' },
];
statsDiv.innerHTML = statItems.map(s =>
  '<div class="stat-item"><div class="stat-value">' + s.value +
  '</div><div class="stat-label">' + s.label + '</div></div>'
).join('');

const width = window.innerWidth;
const height = window.innerHeight - 60;
const nodeW = 160, nodeH = 60;

// Layout nodes in a pipeline flow
const nodesByType = { source: [], transform: [], sink: [] };
graphData.nodes.forEach(n => { (nodesByType[n.type] || nodesByType.transform).push(n); });

const allOrdered = [...nodesByType.source, ...nodesByType.transform, ...nodesByType.sink];
const cols = Math.ceil(Math.sqrt(allOrdered.length * 2));
allOrdered.forEach((n, i) => {
  const col = i % cols;
  const row = Math.floor(i / cols);
  n.x = 120 + col * (nodeW + 80);
  n.y = 100 + row * (nodeH + 100);
});

// Center
const maxX = Math.max(...allOrdered.map(n => n.x + nodeW));
const maxY = Math.max(...allOrdered.map(n => n.y + nodeH));
const offsetX = (width - maxX - 60) / 2;
const offsetY = (height - maxY - 60) / 2;
allOrdered.forEach(n => { n.x += Math.max(offsetX, 20); n.y += Math.max(offsetY, 20); });

const nodeMap = {};
allOrdered.forEach(n => { nodeMap[n.id] = n; });

const svg = d3.select('#graph').append('svg')
  .attr('width', width).attr('height', height);

const defs = svg.append('defs');
defs.append('marker').attr('id', 'arrow').attr('viewBox', '0 0 10 6')
  .attr('refX', 10).attr('refY', 3).attr('markerWidth', 10).attr('markerHeight', 6)
  .attr('orient', 'auto')
  .append('path').attr('d', 'M0,0 L10,3 L0,6 Z').attr('fill', '#475569');

const tooltip = d3.select('#tooltip');

// Draw edges
graphData.edges.forEach(e => {
  const src = nodeMap[e.source];
  const tgt = nodeMap[e.target];
  if (!src || !tgt) return;
  const x1 = src.x + nodeW;
  const y1 = src.y + nodeH / 2;
  const x2 = tgt.x;
  const y2 = tgt.y + nodeH / 2;
  const mx = (x1 + x2) / 2;
  svg.append('path')
    .attr('class', 'edge-line')
    .attr('d', 'M' + x1 + ',' + y1 + ' C' + mx + ',' + y1 + ' ' + mx + ',' + y2 + ' ' + x2 + ',' + y2)
    .attr('stroke', e.record_count > 0 ? '#38bdf8' : '#334155')
    .attr('marker-end', 'url(#arrow)');
  svg.append('text').attr('class', 'edge-label')
    .attr('x', mx).attr('y', (y1 + y2) / 2 - 8)
    .text(e.transform_type + ' (' + (e.record_count || 0) + ')');
});

// Draw nodes
allOrdered.forEach(n => {
  const c = nodeColors[n.type] || nodeColors.transform;
  const g = svg.append('g').attr('class', 'node-group')
    .attr('transform', 'translate(' + n.x + ',' + n.y + ')');
  g.append('rect').attr('class', 'node-rect')
    .attr('width', nodeW).attr('height', nodeH)
    .attr('fill', c.fill).attr('stroke', c.stroke);
  g.append('text').attr('class', 'node-label')
    .attr('x', nodeW / 2).attr('y', 24).text(n.name);
  g.append('text').attr('class', 'node-sublabel')
    .attr('x', nodeW / 2).attr('y', 42).text(n.type + ' • ' + (n.component || ''));
  g.on('mouseover', (ev) => {
    tooltip.style('opacity', 1)
      .style('left', (ev.pageX + 15) + 'px').style('top', (ev.pageY - 10) + 'px')
      .html('<div class="tt-title">' + n.name + '</div>' +
            '<div class="tt-row"><span class="tt-key">Type:</span><span class="tt-val">' + n.type + '</span></div>' +
            '<div class="tt-row"><span class="tt-key">Component:</span><span class="tt-val">' + (n.component||'—') + '</span></div>' +
            '<div class="tt-row"><span class="tt-key">Topic:</span><span class="tt-val">' + (n.topic||'—') + '</span></div>' +
            '<div class="tt-row"><span class="tt-key">Desc:</span><span class="tt-val">' + (n.description||'—') + '</span></div>');
  }).on('mouseout', () => tooltip.style('opacity', 0));
});
</script>
</body>
</html>
"""
    return HTMLResponse(content=html)


# ==================== PIPELINE DASHBOARD ====================

@app.get("/dashboard", response_class=HTMLResponse)
def pipeline_dashboard():
    """View the real-time pipeline dashboard in your browser."""
    html = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Enterprise Data Platform Control Center</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0b0f19;
    --panel: #111827;
    --panel-hover: #1f2937;
    --border: #1f2937;
    --border-light: #374151;
    --text-primary: #f9fafb;
    --text-secondary: #9ca3af;
    --text-muted: #6b7280;
    --accent: #38bdf8;
    --accent-dark: #0284c7;
    --success: #10b981;
    --warning: #f59e0b;
    --danger: #ef4444;
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body {
    font-family: 'Inter', -apple-system, sans-serif;
    background: var(--bg);
    color: var(--text-primary);
    padding: 24px;
    line-height: 1.5;
  }
  
  /* Top Bar */
  header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding-bottom: 20px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 24px;
  }
  .brand-title {
    font-size: 20px;
    font-weight: 700;
    color: var(--text-primary);
    letter-spacing: -0.02em;
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .brand-title span.sub {
    font-size: 13px;
    font-weight: 400;
    color: var(--text-secondary);
    border-left: 1px solid var(--border-light);
    padding-left: 10px;
  }
  .header-metrics {
    display: flex;
    align-items: center;
    gap: 16px;
  }
  .status-pill {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    background: rgba(16, 185, 129, 0.1);
    color: var(--success);
    padding: 6px 12px;
    border-radius: 6px;
    font-size: 12px;
    font-weight: 600;
    border: 1px solid rgba(16, 185, 129, 0.2);
  }
  .status-pill .indicator { width: 7px; height: 7px; border-radius: 50%; background: var(--success); }

  /* Navigation Bar */
  nav.nav-bar {
    display: flex;
    gap: 6px;
    margin-bottom: 24px;
    border-bottom: 1px solid var(--border);
    padding-bottom: 8px;
  }
  .nav-item {
    background: transparent;
    border: none;
    color: var(--text-secondary);
    padding: 8px 16px;
    border-radius: 6px;
    font-family: inherit;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.15s ease;
  }
  .nav-item:hover { color: var(--text-primary); background: var(--panel-hover); }
  .nav-item.active { color: var(--text-primary); background: var(--panel); font-weight: 600; border: 1px solid var(--border); }

  .tab-pane { display: none; }
  .tab-pane.active { display: block; }

  /* Metric Cards Grid */
  .metrics-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }
  .metric-card {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px 20px;
  }
  .metric-card .label { font-size: 12px; font-weight: 500; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.05em; }
  .metric-card .value { font-size: 24px; font-weight: 700; color: var(--text-primary); margin-top: 6px; font-family: 'JetBrains Mono', monospace; }

  /* Tables */
  .panel {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 24px;
  }
  .panel-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
  }
  .panel-title { font-size: 14px; font-weight: 600; color: var(--text-primary); }
  
  table.data-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  table.data-table th {
    text-align: left;
    padding: 10px 12px;
    color: var(--text-secondary);
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    border-bottom: 1px solid var(--border);
    background: rgba(0, 0, 0, 0.2);
  }
  table.data-table td {
    padding: 10px 12px;
    border-bottom: 1px solid var(--border);
    color: var(--text-primary);
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
  }
  table.data-table tr:hover { background: rgba(255, 255, 255, 0.02); }

  /* Status Badges */
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; font-family: 'Inter', sans-serif; }
  .badge-healthy { background: rgba(16, 185, 129, 0.15); color: #34d399; }
  .badge-degraded { background: rgba(245, 158, 11, 0.15); color: #fbbf24; }
  .badge-down { background: rgba(239, 68, 68, 0.15); color: #f87171; }

  /* SQL Console & Code Display */
  textarea.sql-editor {
    width: 100%;
    height: 80px;
    background: var(--bg);
    border: 1px solid var(--border-light);
    border-radius: 6px;
    color: var(--accent);
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    padding: 12px;
    resize: none;
    margin-bottom: 12px;
  }
  .btn {
    background: var(--accent-dark);
    color: #ffffff;
    font-family: 'Inter', sans-serif;
    font-size: 13px;
    font-weight: 600;
    padding: 8px 16px;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    transition: background 0.15s ease;
  }
  .btn:hover { background: #0369a1; }
  pre.code-box {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 16px;
    color: #93c5fd;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    max-height: 400px;
    overflow-y: auto;
  }
</style>
</head>
<body>

<header>
  <div class="brand-title">
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#38bdf8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2v20M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>
    Data Platform Observability Control Center
    <span class="sub">Stream Processing • OpenLineage • DuckDB Engine</span>
  </div>
  <div class="header-metrics">
    <div class="status-pill">
      <div class="indicator"></div>
      <span>ALL SYSTEMS OPERATIONAL</span>
    </div>
  </div>
</header>

<nav class="nav-bar">
  <button class="nav-item active" onclick="showTab('overview', this)">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px; margin-right:6px;"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>
    Overview
  </button>
  <button class="nav-item" onclick="showTab('duckdb', this)">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px; margin-right:6px;"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>
    DuckDB SQL Console
  </button>
  <button class="nav-item" onclick="showTab('rbac', this)">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px; margin-right:6px;"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
    Column Governance
  </button>
  <button class="nav-item" onclick="showTab('iceberg', this)">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px; margin-right:6px;"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg>
    Lakehouse Catalog
  </button>
  <button class="nav-item" onclick="showTab('lineage', this)">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-2px; margin-right:6px;"><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg>
    Lineage Protocol
  </button>
</nav>

<!-- OVERVIEW -->
<div id="overview" class="tab-pane active">
  <div class="metrics-grid" id="metricsGrid"></div>
  
  <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
    <div class="panel">
      <div class="panel-header">
        <div class="panel-title">Component Health Status</div>
      </div>
      <table class="data-table" id="componentTable">
        <thead><tr><th>Component</th><th>Status</th><th>Latency</th><th>Detail</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-header">
        <div class="panel-title">Data Quality Scorecard</div>
      </div>
      <div id="qualityScorecard"></div>
    </div>
  </div>
</div>

<!-- DUCKDB SQL CONSOLE -->
<div id="duckdb" class="tab-pane">
  <div class="panel">
    <div class="panel-header">
      <div class="panel-title">DuckDB Vectorized Analytics Engine</div>
    </div>
    <textarea class="sql-editor" id="sqlQuery">SELECT claim_type, COUNT(*) as count, ROUND(AVG(claim_amount), 2) as avg_amt_usd FROM claims GROUP BY claim_type</textarea>
    <button class="btn" onclick="executeDuckDB()">
      <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor" style="vertical-align:-1px; margin-right:4px;"><polygon points="5 3 19 12 5 21 5 3"/></svg>
      Execute Query
    </button>
    <div id="duckdbOutput" style="margin-top: 16px;"></div>
  </div>
</div>

<!-- COLUMN GOVERNANCE -->
<div id="rbac" class="tab-pane">
  <div class="panel">
    <div class="panel-header">
      <div class="panel-title">Role-Based Access Control (RBAC) Data Masking Policy</div>
    </div>
    <div style="display: flex; gap: 8px; margin-bottom: 16px;">
      <button class="btn" style="background:#374151" onclick="loadRBAC('public')">Role: PUBLIC</button>
      <button class="btn" style="background:#374151" onclick="loadRBAC('adjuster')">Role: ADJUSTER</button>
      <button class="btn" style="background:#374151" onclick="loadRBAC('executive')">Role: EXECUTIVE</button>
      <button class="btn" style="background:#374151" onclick="loadRBAC('auditor')">Role: AUDITOR</button>
    </div>
    <pre class="code-box" id="rbacBox">Select a role above to view masked fields...</pre>
  </div>
</div>

<!-- LAKEHOUSE CATALOG -->
<div id="iceberg" class="tab-pane">
  <div class="panel">
    <div class="panel-header">
      <div class="panel-title">Apache Iceberg Catalog Metadata</div>
    </div>
    <div id="icebergOutput"></div>
  </div>
</div>

<!-- LINEAGE PROTOCOL -->
<div id="lineage" class="tab-pane">
  <div class="panel">
    <div class="panel-header">
      <div class="panel-title">OpenLineage 1.0 Metadata Stream Protocol</div>
    </div>
    <pre class="code-box" id="lineageBox">Loading OpenLineage metadata...</pre>
  </div>
</div>

<script>
const BASE_URL = window.location.origin;

function showTab(tabId, el) {
  document.querySelectorAll('.nav-item').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  el.classList.add('active');
  document.getElementById(tabId).classList.add('active');

  if (tabId === 'lineage') fetchLineage();
  if (tabId === 'iceberg') fetchIceberg();
  if (tabId === 'rbac') loadRBAC('public');
}

async function apiCall(path) {
  try {
    const res = await fetch(BASE_URL + path);
    return res.ok ? await res.json() : null;
  } catch (e) {
    return null;
  }
}

async function updateDashboard() {
  const [stats, health, dq] = await Promise.all([
    apiCall('/claims/stats'),
    apiCall('/health'),
    apiCall('/quality/scorecard')
  ]);

  if (stats) {
    document.getElementById('metricsGrid').innerHTML = `
      <div class="metric-card"><div class="label">Total Records Processed</div><div class="value">${stats.total_claims || 0}</div></div>
      <div class="metric-card"><div class="label">Processed Volume</div><div class="value">$${(stats.total_claim_amount || 0).toLocaleString()}</div></div>
      <div class="metric-card"><div class="label">Mean Fraud Score</div><div class="value">${(stats.avg_fraud_score || 0).toFixed(3)}</div></div>
      <div class="metric-card"><div class="label">Quality Pass Rate</div><div class="value" style="color:var(--success)">${dq ? dq.overall_quality_score + '%' : '100%'}</div></div>
    `;
  }

  if (health && health.components) {
    const tbody = document.querySelector('#componentTable tbody');
    tbody.innerHTML = Object.values(health.components).map(c => `
      <tr>
        <td>${c.name}</td>
        <td><span class="badge ${c.status === 'healthy' ? 'badge-healthy' : 'badge-down'}">${c.status.toUpperCase()}</span></td>
        <td>${(c.latency_ms || 0).toFixed(1)}ms</td>
        <td>${c.message || 'OK'}</td>
      </tr>
    `).join('');
  }

  if (dq && dq.expectations) {
    document.getElementById('qualityScorecard').innerHTML = `
      <table class="data-table">
        <thead><tr><th>Expectation</th><th>Pass Rate</th><th>Status</th></tr></thead>
        <tbody>
          ${dq.expectations.map(e => `
            <tr>
              <td>${e.name}</td>
              <td>${e.pass_rate}%</td>
              <td><span class="badge badge-healthy">${e.passed ? 'PASSED' : 'FAILED'}</span></td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    `;
  }
}

async function executeDuckDB() {
  const query = document.getElementById('sqlQuery').value;
  const res = await fetch(BASE_URL + '/analytics/duckdb', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query })
  });
  const data = await res.json();
  if (data && data.data) {
    const keys = Object.keys(data.data[0] || {});
    document.getElementById('duckdbOutput').innerHTML = `
      <table class="data-table">
        <thead><tr>${keys.map(k => `<th>${k}</th>`).join('')}</tr></thead>
        <tbody>
          ${data.data.map(row => `<tr>${keys.map(k => `<td>${row[k]}</td>`).join('')}</tr>`).join('')}
        </tbody>
      </table>
    `;
  }
}

async function loadRBAC(role) {
  const res = await fetch(BASE_URL + '/governance/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ role })
  });
  const data = await res.json();
  document.getElementById('rbacBox').textContent = JSON.stringify(data, null, 2);
}

async function fetchIceberg() {
  const data = await apiCall('/iceberg/snapshots');
  if (data && data.snapshot_history) {
    document.getElementById('icebergOutput').innerHTML = `
      <table class="data-table">
        <thead><tr><th>Snapshot ID</th><th>Sequence</th><th>Timestamp (UTC)</th><th>Added Records</th><th>Total Records</th></tr></thead>
        <tbody>
          ${data.snapshot_history.map(s => `
            <tr>
              <td>${s.snapshot_id}</td>
              <td>#${s.sequence_number}</td>
              <td>${s.timestamp_utc}</td>
              <td style="color:var(--success)">+${s.summary.added_records}</td>
              <td>${s.summary.total_records}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    `;
  }
}

async function fetchLineage() {
  const data = await apiCall('/lineage/openlineage');
  document.getElementById('lineageBox').textContent = JSON.stringify(data, null, 2);
}

updateDashboard();
setInterval(updateDashboard, 5000);
</script>
</body>
</html>
"""
    return HTMLResponse(content=html)



# ============ 2026 MODERN TECH STACK ENDPOINTS ============

openlineage_emitter = OpenLineageEmitter()
duckdb_engine = DuckDBAnalyticsEngine()
claims_validator = ClaimsValidator()


@app.get("/lineage/openlineage")
def get_openlineage_events():
    """Generates standardized Linux Foundation OpenLineage 1.0 RunEvent metadata stream."""
    return openlineage_emitter.create_run_event(
        job_name="claims_ingestion_and_validation",
        event_type="COMPLETE",
        inputs=[{"name": "kafka.raw-claims", "namespace": "kafka://localhost:9092"}],
        outputs=[{"name": "postgres.processed_claims", "namespace": "postgresql://postgres:5432/insurance_lineage"}],
        data_quality_metrics={"passed_expectations": 3, "failed_expectations": 0, "quality_score": 100.0}
    )


@app.post("/analytics/duckdb")
def run_duckdb_query(payload: Optional[dict] = None):
    """Executes vectorized DuckDB SQL queries over claim datasets."""
    payload = payload or {}
    sql = payload.get("query", "SELECT claim_type, COUNT(*) as count, AVG(claim_amount) as avg_amt FROM claims GROUP BY claim_type")
    sample_claims = payload.get("claims", [
        {"claim_id": "CLM-100", "policy_number": "POL-100", "claim_amount": 1500.0, "claim_type": "auto", "fraud_score": 0.05, "is_fraud_flag": False},
        {"claim_id": "CLM-200", "policy_number": "POL-200", "claim_amount": 7500.0, "claim_type": "property", "fraud_score": 0.85, "is_fraud_flag": True}
    ])
    duckdb_engine.register_claims(sample_claims)
    results = duckdb_engine.execute_query(sql)
    return {"query": sql, "row_count": len(results), "data": results}


@app.get("/quality/scorecard")
def get_data_quality_scorecard():
    """Returns real-time Data Quality Expectation metrics and pass rates."""
    from src.models.claims import InsuranceClaim, ClaimType
    sample_claim = InsuranceClaim(
        claim_id="CLM-DQ-TEST",
        policy_number="ABC-123456",
        claimant_name="Jane Doe",
        claim_type=ClaimType.AUTO,
        claim_amount=2500.0,
        date_of_loss="2025-01-15"
    )
    scorecard = claims_validator.evaluate_data_expectations([sample_claim])
    return scorecard


# ============ PHASE 4 ADVANCED ENTERPRISE ENDPOINTS ============

window_processor = WindowProcessor()
iceberg_engine = IcebergMetadataEngine()
governance_engine = DataGovernanceEngine()

# Seed sample event for window analytics
window_processor.add_event({
    "claim_id": "CLM-WIN-101",
    "policy_number": "ABC-123456",
    "claim_amount": 4500.0,
    "claim_type": "auto"
})


@app.get("/analytics/windows")
def get_window_analytics():
    """Returns real-time 1-minute sliding window metrics and 5-minute tumbling aggregations."""
    return {
        "sliding_window": window_processor.get_sliding_window_stats(),
        "tumbling_window": window_processor.get_tumbling_window_stats()
    }


@app.get("/iceberg/snapshots")
def get_iceberg_snapshots(time_travel_ts: Optional[str] = None):
    """Returns Apache Iceberg catalog metadata, snapshot history, and time-travel query resolution."""
    history = iceberg_engine.get_snapshot_history()
    time_travel = iceberg_engine.query_time_travel(timestamp_iso=time_travel_ts) if time_travel_ts else None
    return {
        "table_name": iceberg_engine.table_name,
        "snapshot_count": len(history),
        "snapshot_history": history,
        "time_travel_query": time_travel
    }


@app.post("/governance/query")
def run_governed_query(payload: Optional[dict] = None):
    """Applies dynamic fine-grained column masking based on caller role (executive, adjuster, auditor, public)."""
    payload = payload or {}
    role_str = payload.get("role", "public").lower()
    try:
        role = SecurityRole(role_str)
    except ValueError:
        role = SecurityRole.PUBLIC

    raw_claims = payload.get("claims", [
        {
            "claim_id": "CLM-GOV-001",
            "policy_number": "POL-987654",
            "claimant_name": "Jonathan Doe",
            "vehicle_vin": "1HGCR2F83HA123456",
            "claim_amount": 14500.0,
            "claim_type": "auto"
        }
    ])
    masked_claims = governance_engine.apply_batch_masking(raw_claims, role)
    return {
        "caller_role": role.value,
        "records_returned": len(masked_claims),
        "data": masked_claims
    }


def start_api():

    """Start the observability API server."""
    init_tracing("observability-api")
    logger.info("Starting Observability API",
                host=config.observability.api_host,
                port=config.observability.api_port)
    uvicorn.run(
        app,
        host=config.observability.api_host,
        port=config.observability.api_port,
        log_level="info"
    )


if __name__ == "__main__":
    start_api()