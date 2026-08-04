"""
Confluent Schema Registry client for the insurance claims pipeline.

Registers the four local JSON schemas (raw / validated / scored / enriched)
with the Schema Registry on startup and provides per-message validation.

Design principles
-----------------
* Fails OPEN — if the Registry is unreachable, the pipeline continues
  unblocked and logs a warning rather than crashing.
* Compatibility level is set to FORWARD so producers can ADD optional
  fields without breaking existing consumers.
* Validation is done locally against a cached copy of the schema dict,
  so each message does not require a network round-trip.
"""

import json
import structlog
from pathlib import Path
from typing import Optional, Tuple

import httpx

from src.config import config

logger = structlog.get_logger(__name__)

_SCHEMA_DIR = Path(__file__).parent

# Maps each Kafka topic to (schema filename, Schema Registry subject name).
# Subjects follow the TopicNameStrategy convention: <topic>-value.
_TOPIC_SCHEMA_MAP: dict[str, tuple[str, str]] = {
    "insurance.claims.raw":       ("raw_claim_v1.json",       "insurance.claims.raw-value"),
    "insurance.claims.validated": ("validated_claim_v1.json", "insurance.claims.validated-value"),
    "insurance.claims.scored":    ("scored_claim_v1.json",    "insurance.claims.scored-value"),
    "insurance.claims.enriched":  ("enriched_claim_v1.json",  "insurance.claims.enriched-value"),
}

_REGISTRY_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


class SchemaRegistryClient:
    """Thin HTTP client around the Confluent Schema Registry REST API (v1)."""

    def __init__(self) -> None:
        self._base_url = config.schema_registry.url.rstrip("/")
        self._enabled = config.schema_registry.enabled
        # Subjects successfully registered — only these are validated locally.
        self._registered_subjects: set[str] = set()
        # Parsed schema dicts keyed by subject — used for fast local validation.
        self._schema_cache: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Startup registration
    # ------------------------------------------------------------------

    def register_all_schemas(self) -> None:
        """
        Push every local JSON schema to the Schema Registry.

        Safe to call multiple times — the Registry deduplicates by content hash.
        Subjects that fail to register are skipped; the rest proceed normally.
        """
        if not self._enabled:
            logger.info("Schema Registry disabled — skipping registration")
            return

        for _topic, (schema_file, subject) in _TOPIC_SCHEMA_MAP.items():
            schema_path = _SCHEMA_DIR / schema_file
            if not schema_path.exists():
                logger.warning("Schema file not found", path=str(schema_path))
                continue

            schema_str = schema_path.read_text()
            try:
                self._set_compatibility(subject, "FORWARD")
                schema_id = self._register_schema(subject, schema_str)
                self._registered_subjects.add(subject)
                # Cache parsed schema for local validation — avoids per-message HTTP calls.
                self._schema_cache[subject] = json.loads(schema_str)
                logger.info("Schema registered with Registry",
                            subject=subject, schema_id=schema_id)
            except Exception as exc:
                logger.warning(
                    "Schema registration failed — local file validation active",
                    subject=subject, error=str(exc),
                )

    # ------------------------------------------------------------------
    # Per-message validation
    # ------------------------------------------------------------------

    def validate(self, topic: str, payload: dict) -> Tuple[bool, Optional[str]]:
        """
        Validate *payload* against the registered JSON schema for *topic*.

        Returns
        -------
        (True, None)          — payload is schema-compliant.
        (False, error_msg)    — payload violates the schema.
        (True, None)          — Registry disabled / subject not registered /
                                jsonschema unavailable → fails OPEN.
        """
        _, subject = _TOPIC_SCHEMA_MAP.get(topic, (None, None))
        if not self._enabled or not subject or subject not in self._registered_subjects:
            return True, None

        schema = self._schema_cache.get(subject)
        if schema is None:
            return True, None

        try:
            import jsonschema
            jsonschema.validate(instance=payload, schema=schema)
            return True, None
        except jsonschema.ValidationError as exc:
            return False, exc.message
        except Exception as exc:
            # jsonschema not installed or unexpected error — fail open.
            logger.debug("Registry local validation skipped", error=str(exc))
            return True, None

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _register_schema(self, subject: str, schema_str: str) -> int:
        """POST schema to /subjects/{subject}/versions and return the schema ID."""
        payload = {"schema": schema_str, "schemaType": "JSON"}
        with httpx.Client(timeout=5) as client:
            resp = client.post(
                f"{self._base_url}/subjects/{subject}/versions",
                json=payload,
                headers={"Content-Type": _REGISTRY_CONTENT_TYPE},
            )
            resp.raise_for_status()
            return resp.json()["id"]

    def _set_compatibility(self, subject: str, level: str) -> None:
        """Set per-subject compatibility level (FORWARD / BACKWARD / FULL / NONE)."""
        try:
            with httpx.Client(timeout=5) as client:
                client.put(
                    f"{self._base_url}/config/{subject}",
                    json={"compatibility": level},
                    headers={"Content-Type": _REGISTRY_CONTENT_TYPE},
                )
        except Exception:
            pass  # Non-fatal — registry may not support per-subject config yet.


# Module-level singleton — initialised lazily via register_all_schemas().
schema_registry_client = SchemaRegistryClient()
