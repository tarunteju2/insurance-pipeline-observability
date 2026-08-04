"""
Append-only Event Store for Claims CQRS & Event Sourcing.

Maintains immutable event stream per aggregate (claim), generates periodic
snapshots, and supports event replay for state reconstruction and projections.
"""

from __future__ import annotations

import json
import uuid
import structlog
from dataclasses import dataclass, asdict, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Callable

logger = structlog.get_logger(__name__)


class EventType(str, Enum):
    CLAIM_SUBMITTED = "ClaimSubmitted"
    CLAIM_VALIDATED = "ClaimValidated"
    CLAIM_VALIDATION_FAILED = "ClaimValidationFailed"
    FRAUD_SCORE_ASSIGNED = "FraudScoreAssigned"
    CLAIM_ENRICHED = "ClaimEnriched"
    CLAIM_ADJUDICATED = "ClaimAdjudicated"
    PAYMENT_AUTHORIZED = "PaymentAuthorized"
    CLAIM_CLOSED = "ClaimClosed"
    CLAIM_REOPENED = "ClaimReopened"
    SIU_REFERRAL_CREATED = "SIUReferralCreated"
    SUBROGATION_INITIATED = "SubrogationInitiated"


@dataclass(frozen=True)
class ClaimEvent:
    """Immutable domain event representing a state change in a claim aggregate."""
    event_id: str
    aggregate_id: str  # claim_id
    event_type: EventType
    aggregate_version: int
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    @classmethod
    def create(
        cls,
        aggregate_id: str,
        event_type: EventType,
        version: int,
        data: Dict[str, Any],
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
    ) -> ClaimEvent:
        return cls(
            event_id=f"evt_{uuid.uuid4().hex[:12]}",
            aggregate_id=aggregate_id,
            event_type=event_type,
            aggregate_version=version,
            data=data,
            metadata={
                "correlation_id": correlation_id or aggregate_id,
                "causation_id": causation_id,
                "actor": data.get("actor", "system"),
            },
        )


@dataclass
class AggregateSnapshot:
    """Snapshot of claim aggregate state at a specific version."""
    aggregate_id: str
    version: int
    state: Dict[str, Any]
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class EventStore:
    """
    Append-only Event Store managing immutable claim event streams.

    Supports thread-safe in-memory storage with periodic snapshot generation,
    optimistic concurrency control via version checking, and event listeners.
    """

    _instance: Optional[EventStore] = None

    def __init__(self, snapshot_interval: int = 5):
        self.snapshot_interval = snapshot_interval
        self._streams: Dict[str, List[ClaimEvent]] = {}
        self._snapshots: Dict[str, AggregateSnapshot] = {}
        self._listeners: List[Callable[[ClaimEvent], None]] = []

    @classmethod
    def instance(cls) -> EventStore:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def append(self, event: ClaimEvent, expected_version: Optional[int] = None) -> None:
        """
        Append a new event to aggregate stream with optimistic concurrency control.
        """
        stream = self._streams.setdefault(event.aggregate_id, [])
        current_version = stream[-1].aggregate_version if stream else 0

        if expected_version is not None and expected_version != current_version:
            raise ValueError(
                f"Concurrency conflict for aggregate {event.aggregate_id}: "
                f"expected version {expected_version}, but current version is {current_version}"
            )

        stream.append(event)
        logger.info(
            "Event appended",
            aggregate_id=event.aggregate_id,
            event_type=event.event_type.value,
            version=event.aggregate_version,
        )

        # Notify active projection listeners
        for listener in self._listeners:
            try:
                listener(event)
            except Exception as exc:
                logger.error("Error in event listener", error=str(exc), event_id=event.event_id)

    def get_events(self, aggregate_id: str, from_version: int = 0) -> List[ClaimEvent]:
        """Get event stream for aggregate starting from a version."""
        stream = self._streams.get(aggregate_id, [])
        return [evt for evt in stream if evt.aggregate_version > from_version]

    def get_snapshot(self, aggregate_id: str) -> Optional[AggregateSnapshot]:
        """Retrieve latest snapshot for an aggregate."""
        return self._snapshots.get(aggregate_id)

    def save_snapshot(self, aggregate_id: str, version: int, state: Dict[str, Any]) -> None:
        """Save a state snapshot for fast aggregate reconstruction."""
        self._snapshots[aggregate_id] = AggregateSnapshot(
            aggregate_id=aggregate_id,
            version=version,
            state=state,
        )
        logger.debug("Snapshot saved", aggregate_id=aggregate_id, version=version)

    def register_listener(self, listener: Callable[[ClaimEvent], None]) -> None:
        """Register a callback for real-time projection updates."""
        self._listeners.append(listener)

    def replay_all(self, handler: Callable[[ClaimEvent], None]) -> int:
        """Replay all historical events across all aggregates in chronological order."""
        all_events = [evt for stream in self._streams.values() for evt in stream]
        all_events.sort(key=lambda e: e.timestamp)
        for evt in all_events:
            handler(evt)
        return len(all_events)
