"""
Stateful Tumbling & Sliding Window Stream Processing Engine
Computes real-time streaming window metrics, rolling averages, claim velocity spikes, and anomaly indicators.
"""

import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class WindowProcessor:
    """
    In-memory stateful window engine supporting tumbling and sliding windows over event streams.
    """

    def __init__(self, tumbling_window_sec: int = 300, sliding_window_sec: int = 60):
        self.tumbling_window_sec = tumbling_window_sec
        self.sliding_window_sec = sliding_window_sec
        # Deque storing (timestamp_epoch, claim_dict)
        self.events: deque = deque()
        self.ip_velocity: Dict[str, deque] = defaultdict(deque)

    def add_event(self, claim: Dict[str, Any], timestamp_epoch: Optional[float] = None):
        """
        Appends an event to the stateful window buffer.
        """
        ts = timestamp_epoch or time.time()
        self.events.append((ts, claim))
        
        # Track velocity per policy/claimant IP or policy
        policy = claim.get("policy_number", "UNKNOWN")
        self.ip_velocity[policy].append(ts)
        self._cleanup_old_events(ts)

    def _cleanup_old_events(self, current_ts: float):
        """
        Evicts events older than the largest window scope.
        """
        max_age = max(self.tumbling_window_sec, self.sliding_window_sec)
        cutoff = current_ts - max_age

        while self.events and self.events[0][0] < cutoff:
            self.events.popleft()

        # Clean velocity state
        for key in list(self.ip_velocity.keys()):
            while self.ip_velocity[key] and self.ip_velocity[key][0] < (current_ts - self.sliding_window_sec):
                self.ip_velocity[key].popleft()
            if not self.ip_velocity[key]:
                del self.ip_velocity[key]

    def get_sliding_window_stats(self) -> Dict[str, Any]:
        """
        Computes real-time rolling metrics over the last 1-minute sliding window.
        """
        now = time.time()
        cutoff = now - self.sliding_window_sec
        recent_claims = [c for ts, c in self.events if ts >= cutoff]

        if not recent_claims:
            return {
                "window_type": "sliding_1min",
                "claim_count": 0,
                "total_amount_usd": 0.0,
                "avg_amount_usd": 0.0,
                "velocity_anomalies": [],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

        amounts = [float(c.get("claim_amount", 0.0)) for c in recent_claims]
        total_amt = sum(amounts)
        avg_amt = round(total_amt / len(amounts), 2)

        # Detect velocity anomaly: > 3 claims from same policy in 1 minute
        velocity_anomalies = [
            policy for policy, timestamps in self.ip_velocity.items()
            if len(timestamps) >= 3
        ]

        return {
            "window_type": "sliding_1min",
            "claim_count": len(recent_claims),
            "total_amount_usd": round(total_amt, 2),
            "avg_amount_usd": avg_amt,
            "max_amount_usd": round(max(amounts), 2),
            "velocity_anomalies": velocity_anomalies,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def get_tumbling_window_stats(self) -> Dict[str, Any]:
        """
        Computes aggregated metrics over the 5-minute tumbling window.
        """
        now = time.time()
        cutoff = now - self.tumbling_window_sec
        window_claims = [c for ts, c in self.events if ts >= cutoff]

        if not window_claims:
            return {
                "window_type": "tumbling_5min",
                "claim_count": 0,
                "total_amount_usd": 0.0,
                "fraud_flag_count": 0,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

        amounts = [float(c.get("claim_amount", 0.0)) for c in window_claims]
        fraud_flags = sum(1 for c in window_claims if c.get("is_fraud_flag") or float(c.get("fraud_score", 0.0)) > 0.7)

        return {
            "window_type": "tumbling_5min",
            "claim_count": len(window_claims),
            "total_amount_usd": round(sum(amounts), 2),
            "fraud_flag_count": fraud_flags,
            "fraud_rate_percent": round((fraud_flags / len(window_claims)) * 100, 2),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
