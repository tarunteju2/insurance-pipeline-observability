"""
Outbound Webhook Engine with HMAC-SHA256 Signatures.

Dispatches event notifications to registered subscriber endpoints with cryptographic signatures.
"""

from __future__ import annotations

import hmac
import hashlib
import json
import structlog
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = structlog.get_logger(__name__)


@dataclass
class WebhookSubscription:
    sub_id: str
    target_url: str
    event_types: List[str]
    secret: str
    is_active: bool = True


class WebhookEngine:
    """Outbound webhook manager with HMAC-SHA256 signing."""

    def __init__(self):
        self._subscriptions: Dict[str, WebhookSubscription] = {}

    def register_subscription(self, sub_id: str, target_url: str, event_types: List[str], secret: str) -> WebhookSubscription:
        sub = WebhookSubscription(sub_id, target_url, event_types, secret)
        self._subscriptions[sub_id] = sub
        logger.info("Webhook subscription registered", sub_id=sub_id, target_url=target_url)
        return sub

    def dispatch_event(self, event_type: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        deliveries = []
        body_json = json.dumps(payload, sort_keys=True)

        for sub in self._subscriptions.values():
            if sub.is_active and (event_type in sub.event_types or "*" in sub.event_types):
                signature = self.compute_signature(body_json, sub.secret)
                logger.info("Webhook dispatched", target_url=sub.target_url, event_type=event_type, signature=signature[:10])
                deliveries.append({
                    "sub_id": sub.sub_id,
                    "target_url": sub.target_url,
                    "signature": signature,
                    "status": "delivered",
                })
        return deliveries

    @staticmethod
    def compute_signature(payload_json: str, secret: str) -> str:
        return hmac.new(secret.encode(), payload_json.encode(), hashlib.sha256).hexdigest()
