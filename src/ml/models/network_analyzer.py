"""
Graph Network Fraud Ring Analyzer.

Maps relationships between claimants, providers, attorneys, addresses, and phone numbers
to detect organized fraud rings and suspicious entity clusters.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass, field
from typing import Any, Dict, List, Set, Tuple

logger = structlog.get_logger(__name__)


@dataclass
class NetworkNode:
    node_id: str
    node_type: str  # claimant, provider, attorney, address, phone
    risk_score: float = 0.0


@dataclass
class NetworkEdge:
    source_id: str
    target_id: str
    relation: str  # filed_claim, treated_by, represented_by, shares_address


class NetworkGraphAnalyzer:
    """In-memory property graph analyzer for detecting organized fraud rings."""

    def __init__(self):
        self.nodes: Dict[str, NetworkNode] = {}
        self.adjacency: Dict[str, Set[str]] = {}

    def add_node(self, node_id: str, node_type: str, risk_score: float = 0.0) -> None:
        if node_id not in self.nodes:
            self.nodes[node_id] = NetworkNode(node_id, node_type, risk_score)
            self.adjacency[node_id] = set()

    def add_edge(self, source_id: str, target_id: str, relation: str) -> None:
        self.add_node(source_id, "entity")
        self.add_node(target_id, "entity")
        self.adjacency[source_id].add(target_id)
        self.adjacency[target_id].add(source_id)

    def analyze_claim_network(
        self,
        claimant_id: str,
        provider_name: Optional[str] = None,
        attorney_id: Optional[str] = None,
        address: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Analyze network density and fraud ring risk for entities associated with a claim."""
        entities = [e for e in (claimant_id, provider_name, attorney_id, address) if e]

        # Register nodes and edges
        for i in range(len(entities)):
            for j in range(i + 1, len(entities)):
                self.add_edge(entities[i], entities[j], "associated")

        # Compute degree centrality & connected component size
        connected_nodes: Set[str] = set(entities)
        for entity in entities:
            neighbors = self.adjacency.get(entity, set())
            connected_nodes.update(neighbors)

        ring_size = len(connected_nodes)
        high_risk_neighbors = sum(1 for n in connected_nodes if self.nodes.get(n, NetworkNode(n, "")).risk_score > 0.6)

        network_risk_score = round(min(1.0, (ring_size * 0.08) + (high_risk_neighbors * 0.25)), 3)
        fraud_ring_detected = ring_size >= 6 or high_risk_neighbors >= 2

        return {
            "network_risk_score": network_risk_score,
            "connected_entity_count": ring_size,
            "high_risk_connections": high_risk_neighbors,
            "fraud_ring_detected": fraud_ring_detected,
            "cluster_nodes": list(connected_nodes)[:10],
        }
