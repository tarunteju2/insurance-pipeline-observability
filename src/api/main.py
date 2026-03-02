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
<title>Insurance Claims Pipeline Dashboard</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family: -apple-system,BlinkMacSystemFont,sans-serif; background:#0f172a; color:#e2e8f0; padding:20px; }
  h1 { color:#38bdf8; margin-bottom:20px; font-size:22px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(280px,1fr)); gap:16px; margin-bottom:24px; }
  .card { background:#1e293b; border-radius:12px; padding:20px; border:1px solid #334155; }
  .card h3 { color:#94a3b8; font-size:12px; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px; }
  .card .value { font-size:28px; font-weight:700; color:#22d3ee; }
  .card .sub { font-size:12px; color:#64748b; margin-top:4px; }
  .status-healthy { color:#22c55e; }
  .status-degraded { color:#f59e0b; }
  .status-down { color:#ef4444; }
  table { width:100%; border-collapse:collapse; }
  th { text-align:left; padding:10px; color:#94a3b8; font-size:12px; border-bottom:1px solid #334155; }
  td { padding:10px; font-size:13px; border-bottom:1px solid #1e293b; }
  .badge { padding:3px 8px; border-radius:4px; font-size:11px; font-weight:600; }
  .badge-low { background:#064e3b; color:#34d399; }
  .badge-medium { background:#78350f; color:#fbbf24; }
  .badge-high { background:#7f1d1d; color:#f87171; }
  .badge-critical { background:#581c87; color:#c084fc; }
  #error { color:#f87171; padding:10px; display:none; }
  .refresh-info { color:#475569; font-size:11px; text-align:right; margin-bottom:12px; }
</style>
</head>
<body>
<h1>📊 Insurance Claims Pipeline — Real-Time Dashboard</h1>
<div class="refresh-info">Auto-refreshes every 5 seconds | <span id="lastUpdate"></span></div>
<div id="error"></div>
<div class="grid" id="statsGrid"></div>
<div class="card" style="margin-bottom:16px;">
  <h3>Component Health</h3>
  <table id="healthTable"><thead><tr><th>Component</th><th>Status</th><th>Latency</th><th>Message</th></tr></thead><tbody></tbody></table>
</div>
<div class="card">
  <h3>Recent Claims</h3>
  <table id="claimsTable"><thead><tr><th>Claim ID</th><th>Type</th><th>Amount</th><th>Status</th><th>Fraud Score</th><th>Risk</th></tr></thead><tbody></tbody></table>
</div>
<script>
const API = window.location.origin;

async function fetchData(url) {
  try { const r=await fetch(url); return r.ok ? await r.json() : null; }
  catch(e) { return null; }
}

function riskBadge(level) {
  const cls = { low:'badge-low', medium:'badge-medium', high:'badge-high', critical:'badge-critical' };
  return '<span class="badge ' + (cls[level]||'badge-low') + '">' + (level||'low').toUpperCase() + '</span>';
}

function statusClass(s) {
  if (s==='healthy') return 'status-healthy';
  if (s==='degraded') return 'status-degraded';
  return 'status-down';
}

async function refresh() {
  const [stats,health,claims] = await Promise.all([
    fetchData(API+'/claims/stats'),
    fetchData(API+'/health'),
    fetchData(API+'/claims/recent?limit=15'),
  ]);

  document.getElementById('lastUpdate').textContent = new Date().toLocaleTimeString();

  if (stats) {
    document.getElementById('statsGrid').innerHTML = [
      { label:'Total Claims Processed', value: stats.total_claims },
      { label:'Total Claim Amount', value: '$' + (stats.total_claim_amount||0).toLocaleString() },
      { label:'Avg Fraud Score', value: (stats.avg_fraud_score||0).toFixed(3) },
      { label:'High Fraud Claims', value: stats.high_fraud_count + ' (' + stats.fraud_rate_percent + '%)' },
      { label:'Avg Claim Amount', value: '$' + (stats.avg_claim_amount||0).toLocaleString() },
      { label:'Pipeline Status', value: health ? health.overall_status.toUpperCase() : 'UNKNOWN' },
    ].map(s => '<div class="card"><h3>' + s.label + '</h3><div class="value">' + s.value + '</div></div>').join('');
  }

  if (health && health.components) {
    const tbody = document.querySelector('#healthTable tbody');
    tbody.innerHTML = Object.values(health.components).map(c =>
      '<tr><td>' + c.status + '</td><td class="' + statusClass(c.status) + '">' +
      c.status.toUpperCase() + '</td><td>' + (c.latency_ms||0).toFixed(1) + 'ms</td><td>' +
      (c.message||'—') + '</td></tr>'
    ).join('');
  }

  if (claims && claims.claims) {
    const tbody = document.querySelector('#claimsTable tbody');
    tbody.innerHTML = claims.claims.map(c =>
      '<tr><td>' + c.claim_id + '</td><td>' + c.claim_type + '</td><td>$' +
      (c.claim_amount||0).toLocaleString() + '</td><td>' + c.status + '</td><td>' +
      (c.fraud_score||0).toFixed(3) + '</td><td>' + riskBadge(c.risk_level) + '</td></tr>'
    ).join('');
  }
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""
    return HTMLResponse(content=html)


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