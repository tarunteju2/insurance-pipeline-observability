"""
CQRS + Event Sourcing Framework.

Provides event store, command handlers, materialized projections,
and saga orchestration for event-driven insurance claim lifecycle management.
"""

from src.cqrs.event_store import EventStore, ClaimEvent, EventType

__all__ = ["EventStore", "ClaimEvent", "EventType"]
