#!/usr/bin/env python3
"""
Dead-Letter Queue (DLQ) Replay Tool
====================================
Inspect, filter, and replay messages from the DLQ topic back into the main
pipeline.  Supports dry-run mode so you can validate before committing.

Usage
-----
  # Inspect recent DLQ messages (default: last 50)
  python scripts/dlq_replay.py inspect --limit 50

  # Dry-run: show what would be replayed for a specific DLQ reason
  python scripts/dlq_replay.py replay --reason validation_failed --dry-run

  # Actually replay all DLQ messages back into the raw topic
  python scripts/dlq_replay.py replay --confirm

  # Replay only messages for a specific policy
  python scripts/dlq_replay.py replay --policy AUT-123456 --confirm

  # Replay a single claim by ID
  python scripts/dlq_replay.py replay --claim-id CLM-ABC123 --confirm
"""

import argparse
import json
import sys
import os
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_BEGINNING
from src.config import config

import structlog
logger = structlog.get_logger("dlq-replay")


def _make_consumer():
    return Consumer({
        "bootstrap.servers": config.kafka.bootstrap_servers,
        "group.id": "dlq-replay-tool",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def _make_producer():
    return Producer({
        "bootstrap.servers": config.kafka.bootstrap_servers,
        "client.id": "dlq-replay-producer",
        "enable.idempotence": True,
    })


def _consume_dlq(limit: int = 100, timeout_s: float = 10.0):
    """Read up to *limit* messages from the DLQ topic."""
    consumer = _make_consumer()
    topic = config.kafka.topics["dlq"]
    consumer.subscribe([topic])
    messages = []
    deadline = time.time() + timeout_s
    while len(messages) < limit and time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            data = json.loads(msg.value().decode("utf-8"))
            data["_dlq_partition"] = msg.partition()
            data["_dlq_offset"] = msg.offset()
            messages.append(data)
        except json.JSONDecodeError:
            pass
    consumer.close()
    return messages


def cmd_inspect(args):
    """Print a summary table of DLQ messages."""
    msgs = _consume_dlq(limit=args.limit)
    if not msgs:
        print("No messages found in DLQ.")
        return

    print(f"\n{'='*90}")
    print(f"{'CLAIM ID':<22} {'REASON':<28} {'POLICY':<14} {'AMOUNT':>10}  {'TIMESTAMP':<20}")
    print(f"{'-'*90}")
    for m in msgs:
        meta = m.get("processing_metadata", {})
        reason = meta.get("dlq_reason", "unknown")
        ts = meta.get("dlq_at", m.get("timestamp", ""))[:19]
        print(f"{m.get('claim_id','?'):<22} {reason:<28} "
              f"{m.get('policy_number','?'):<14} "
              f"${m.get('claim_amount', 0):>9,.2f}  {ts}")
    print(f"{'='*90}")
    print(f"Total: {len(msgs)} DLQ messages\n")


def cmd_replay(args):
    """Replay DLQ messages back into the raw topic."""
    msgs = _consume_dlq(limit=args.limit)
    if not msgs:
        print("No messages found in DLQ.")
        return

    # Apply filters
    filtered = msgs
    if args.reason:
        filtered = [m for m in filtered
                    if m.get("processing_metadata", {}).get("dlq_reason") == args.reason]
    if args.policy:
        filtered = [m for m in filtered if m.get("policy_number") == args.policy]
    if args.claim_id:
        filtered = [m for m in filtered if m.get("claim_id") == args.claim_id]

    if not filtered:
        print("No messages match the given filters.")
        return

    print(f"\n{len(filtered)} message(s) matched for replay.")

    if args.dry_run or not args.confirm:
        print("\n** DRY RUN — no messages will be replayed **\n")
        for m in filtered:
            meta = m.get("processing_metadata", {})
            print(f"  Would replay: {m.get('claim_id')} "
                  f"(reason={meta.get('dlq_reason','?')}, "
                  f"policy={m.get('policy_number','?')})")
        print(f"\nTo actually replay, add --confirm")
        return

    # Actually replay
    producer = _make_producer()
    raw_topic = config.kafka.topics["raw"]
    replayed = 0
    for m in filtered:
        # Strip DLQ metadata so the message is treated as fresh
        m.get("processing_metadata", {}).pop("dlq_reason", None)
        m.get("processing_metadata", {}).pop("dlq_at", None)
        m.get("processing_metadata", {}).pop("dlq_error_codes", None)
        m.pop("_dlq_partition", None)
        m.pop("_dlq_offset", None)
        # Add replay marker
        m.setdefault("processing_metadata", {})["replayed_from_dlq"] = True
        m["processing_metadata"]["replayed_at"] = datetime.utcnow().isoformat()

        value = json.dumps(m).encode("utf-8")
        headers = [("correlation_id", m.get("correlation_id", "").encode("utf-8"))]
        producer.produce(
            topic=raw_topic,
            key=m.get("claim_id", "").encode("utf-8"),
            value=value,
            headers=headers,
        )
        replayed += 1

    producer.flush(timeout=10)
    print(f"\n✓ Replayed {replayed} message(s) to {raw_topic}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Insurance Pipeline — Dead-Letter Queue Replay Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # inspect
    inspect_p = sub.add_parser("inspect", help="Inspect DLQ messages")
    inspect_p.add_argument("--limit", type=int, default=50, help="Max messages to read")

    # replay
    replay_p = sub.add_parser("replay", help="Replay DLQ messages to raw topic")
    replay_p.add_argument("--limit", type=int, default=500, help="Max messages to scan")
    replay_p.add_argument("--reason", type=str, help="Filter by dlq_reason")
    replay_p.add_argument("--policy", type=str, help="Filter by policy_number")
    replay_p.add_argument("--claim-id", type=str, help="Filter by specific claim_id")
    replay_p.add_argument("--dry-run", action="store_true", help="Show what would be replayed")
    replay_p.add_argument("--confirm", action="store_true", help="Actually perform the replay")

    args = parser.parse_args()
    if args.command == "inspect":
        cmd_inspect(args)
    elif args.command == "replay":
        cmd_replay(args)


if __name__ == "__main__":
    main()
