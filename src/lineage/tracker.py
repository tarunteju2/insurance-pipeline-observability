"""
Data lineage tracker for insurance claims pipeline.
Records every data transformation with full provenance.
"""

import time
from datetime import datetime
from typing import Optional, List, Dict

import structlog
from sqlalchemy import func
from opentelemetry import trace

from src.lineage.models import (
    LineageNode, LineageEdge, LineageEvent,
    ProcessedClaim, get_session
)
from src.observability.metrics import LINEAGE_EVENTS_RECORDED, LINEAGE_COVERAGE_PERCENT
from src.observability.tracing import get_tracer

logger = structlog.get_logger(__name__)


class LineageTracker:
    """Tracks end-to-end data lineage across the insurance claims pipeline."""

    # Mapping of which data flows to which (node pairs and their transformation type)
    EDGE_MAP = {
        ("src_claims_ingestion", "tx_validation"): "ingest",
        ("tx_validation", "tx_fraud_detection"): "validate",
        ("tx_validation", "sink_dlq"): "reject",
        ("tx_fraud_detection", "tx_enrichment"): "score",
        ("tx_enrichment", "sink_s3_datalake"): "store",
        ("tx_enrichment", "sink_postgres"): "store",
    }

    def __init__(self):
        self.tracer = get_tracer("lineage-tracker")
        # Keep frequently accessed edge IDs in memory for faster lookups
        self._edge_cache: Dict[tuple, int] = {}

    def _get_edge_id(self, source_id: str, target_id: str) -> Optional[int]:
        """Get or cache edge ID for a source-target pair."""
        cache_key = (source_id, target_id)
        if cache_key in self._edge_cache:
            return self._edge_cache[cache_key]

        session = get_session()
        try:
            edge = session.query(LineageEdge).filter(
                LineageEdge.source_node_id == source_id,
                LineageEdge.target_node_id == target_id
            ).first()
            if edge:
                self._edge_cache[cache_key] = edge.edge_id
                return edge.edge_id
            return None
        finally:
            session.close()

    def record_event(
        self,
        source_node_id: str,
        target_node_id: str,
        claim_id: str,
        latency_ms: float,
        status: str = "success",
        error_message: str = None,
        record_count: int = 1,
        metadata: dict = None
    ):
        """Record a lineage event for a data transformation."""
        with self.tracer.start_as_current_span("lineage.record_event") as span:
            session = get_session()
            try:
                edge_id = self._get_edge_id(source_node_id, target_node_id)
                if not edge_id:
                    logger.warning("Edge not found for lineage event",
                                   source=source_node_id, target=target_node_id)
                    return

                # Get current trace context
                current_span = trace.get_current_span()
                ctx = current_span.get_span_context()
                trace_id = format(ctx.trace_id, '032x') if ctx.trace_id else None
                span_id_val = format(ctx.span_id, '016x') if ctx.span_id else None

                event = LineageEvent(
                    edge_id=edge_id,
                    trace_id=trace_id,
                    span_id=span_id_val,
                    claim_id=claim_id,
                    event_type="record_processed" if status == "success" else "error",
                    record_count=record_count,
                    latency_ms=latency_ms,
                    status=status,
                    error_message=error_message,
                    metadata=metadata or {},
                    created_at=datetime.utcnow()
                )
                session.add(event)

                # Update edge statistics
                edge = session.query(LineageEdge).filter(
                    LineageEdge.edge_id == edge_id
                ).first()
                if edge:
                    edge.record_count += record_count
                    edge.last_active_at = datetime.utcnow()
                    if status != "success":
                        edge.error_count += 1
                    # Running average latency
                    if edge.avg_latency_ms > 0:
                        edge.avg_latency_ms = (edge.avg_latency_ms + latency_ms) / 2
                    else:
                        edge.avg_latency_ms = latency_ms

                session.commit()

                # Update metrics
                transform_type = self.EDGE_MAP.get(
                    (source_node_id, target_node_id), "unknown"
                )
                LINEAGE_EVENTS_RECORDED.labels(edge_type=transform_type).inc()

                span.set_attribute("lineage.source", source_node_id)
                span.set_attribute("lineage.target", target_node_id)
                span.set_attribute("lineage.claim_id", claim_id)

                logger.debug("Lineage event recorded",
                             source=source_node_id, target=target_node_id,
                             claim_id=claim_id, latency_ms=latency_ms)

            except Exception as e:
                session.rollback()
                logger.error("Failed to record lineage event", error=str(e))
                raise
            finally:
                session.close()

    def get_claim_lineage(self, claim_id: str) -> List[dict]:
        """Get complete lineage trail for a specific claim."""
        session = get_session()
        try:
            events = session.query(LineageEvent).filter(
                LineageEvent.claim_id == claim_id
            ).order_by(LineageEvent.created_at).all()

            lineage_trail = []
            for event in events:
                edge = event.edge
                lineage_trail.append({
                    "event_id": event.event_id,
                    "source": edge.source_node_id if edge else "unknown",
                    "target": edge.target_node_id if edge else "unknown",
                    "transform_type": edge.transform_type if edge else "unknown",
                    "claim_id": event.claim_id,
                    "trace_id": event.trace_id,
                    "latency_ms": event.latency_ms,
                    "status": event.status,
                    "error_message": event.error_message,
                    "timestamp": event.created_at.isoformat(),
                    "metadata": event.meta_data,
                })

            return lineage_trail
        finally:
            session.close()

    def get_full_lineage_graph(self) -> dict:
        """Get the full pipeline lineage graph with statistics."""
        session = get_session()
        try:
            nodes = session.query(LineageNode).all()
            edges = session.query(LineageEdge).all()

            graph = {
                "nodes": [
                    {
                        "id": n.node_id,
                        "type": n.node_type,
                        "name": n.node_name,
                        "description": n.description,
                        "component": n.component,
                        "topic": n.kafka_topic,
                    }
                    for n in nodes
                ],
                "edges": [
                    {
                        "id": e.edge_id,
                        "source": e.source_node_id,
                        "target": e.target_node_id,
                        "transform_type": e.transform_type,
                        "description": e.description,
                        "record_count": e.record_count,
                        "avg_latency_ms": round(e.avg_latency_ms, 2),
                        "error_count": e.error_count,
                        "last_active": e.last_active_at.isoformat() if e.last_active_at else None,
                    }
                    for e in edges
                ],
                "statistics": self._compute_statistics(session),
            }
            return graph
        finally:
            session.close()

    def _compute_statistics(self, session) -> dict:
        """Compute pipeline-wide lineage statistics."""
        total_events = session.query(func.count(LineageEvent.event_id)).scalar() or 0
        success_events = session.query(func.count(LineageEvent.event_id)).filter(
            LineageEvent.status == 'success'
        ).scalar() or 0
        total_edges = session.query(func.count(LineageEdge.edge_id)).scalar() or 0
        active_edges = session.query(func.count(LineageEdge.edge_id)).filter(
            (LineageEdge.record_count > 0) | (LineageEdge.last_active_at.isnot(None))
        ).scalar() or 0

        # Calculate coverage as percentage of total claims with lineage tracking
        total_claims = session.query(func.count(ProcessedClaim.claim_id)).scalar() or 0
        tracked_claims = session.query(func.count(ProcessedClaim.claim_id)).filter(
            ProcessedClaim.trace_id.isnot(None)
        ).scalar() or 0
        coverage = (tracked_claims / total_claims * 100) if total_claims > 0 else 0
        LINEAGE_COVERAGE_PERCENT.set(coverage)

        return {
            "total_lineage_events": total_events,
            "successful_events": success_events,
            "error_events": total_events - success_events,
            "success_rate": round(success_events / total_events * 100, 2) if total_events > 0 else 0,
            "total_edges": total_edges,
            "active_edges": active_edges,
            "pipeline_coverage_percent": round(coverage, 2),
        }

    def get_statistics(self) -> dict:
        """Public method to get lineage statistics."""
        session = get_session()
        try:
            return self._compute_statistics(session)
        finally:
            session.close()


# Global lineage tracker singleton
lineage_tracker = LineageTracker()