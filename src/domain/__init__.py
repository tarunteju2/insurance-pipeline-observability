"""
Multi-Line-of-Business Domain Engine.

Provides LOB-specific claim processing pipelines for enterprise insurance
operations. Each line of business has its own validation rules, fraud models,
enrichment providers, SLA targets, and regulatory requirements.
"""

from src.domain.lob_registry import LOBRegistry, LineOfBusiness

__all__ = ["LOBRegistry", "LineOfBusiness"]
