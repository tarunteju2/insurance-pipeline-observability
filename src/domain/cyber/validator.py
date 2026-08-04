"""
Cyber Claims Validator.

Validates cyber liability claims:
  - Incident type classification (ransomware, breach, social engineering, etc.)
  - NIST Cybersecurity Framework mapping
  - Notification requirement checks (state breach notification laws)
  - Forensics provider verification
  - Business interruption period validation
  - Ransom payment compliance (OFAC screening)
"""

import re
import structlog
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)

# Cyber incident type taxonomy
_INCIDENT_TYPES = {
    "ransomware": {
        "description": "Ransomware / extortion attack",
        "typical_range": (50_000, 5_000_000),
        "notification_required": True,
        "forensics_required": True,
        "ofac_screening_required": True,
    },
    "data_breach": {
        "description": "Unauthorized data access or exfiltration",
        "typical_range": (25_000, 10_000_000),
        "notification_required": True,
        "forensics_required": True,
        "ofac_screening_required": False,
    },
    "social_engineering": {
        "description": "Business email compromise / wire fraud",
        "typical_range": (10_000, 2_000_000),
        "notification_required": False,
        "forensics_required": False,
        "ofac_screening_required": False,
    },
    "ddos": {
        "description": "Distributed denial of service attack",
        "typical_range": (5_000, 500_000),
        "notification_required": False,
        "forensics_required": True,
        "ofac_screening_required": False,
    },
    "malware": {
        "description": "Malware infection (non-ransomware)",
        "typical_range": (10_000, 1_000_000),
        "notification_required": False,
        "forensics_required": True,
        "ofac_screening_required": False,
    },
    "insider_threat": {
        "description": "Insider threat / unauthorized access by employee",
        "typical_range": (25_000, 3_000_000),
        "notification_required": True,
        "forensics_required": True,
        "ofac_screening_required": False,
    },
    "system_failure": {
        "description": "IT system failure / technology error",
        "typical_range": (10_000, 2_000_000),
        "notification_required": False,
        "forensics_required": False,
        "ofac_screening_required": False,
    },
}

# State breach notification laws (days to notify)
_STATE_NOTIFICATION_DAYS = {
    "CA": 45, "NY": 30, "TX": 60, "FL": 30, "IL": 45,
    "MA": 30, "PA": 60, "OH": 45, "NJ": 30, "GA": 30,
    "VA": 60, "WA": 30, "CO": 30, "CT": 60, "MD": 45,
    "DEFAULT": 60,
}

# NIST CSF categories
_NIST_FUNCTIONS = {
    "identify": "Develop understanding of cybersecurity risk management",
    "protect": "Implement safeguards for critical services",
    "detect": "Develop capabilities to identify cybersecurity events",
    "respond": "Develop capabilities for response to detected events",
    "recover": "Develop capabilities for resilience and restoration",
}

# OFAC sanctioned entities list
_OFAC_INDICATORS = {
    "Lazarus", "Conti", "REvil", "DarkSide", "BlackCat",
    "ALPHV", "LockBit", "Hive", "Clop", "Vice Society",
}


class CyberClaimValidator:
    """LOB-specific validation for cyber liability claims."""

    def validate(
        self, claim: InsuranceClaim
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        errors: List[Dict[str, Any]] = []

        errors.extend(self._classify_incident(claim))
        errors.extend(self._validate_notification_compliance(claim))
        errors.extend(self._validate_forensics(claim))
        errors.extend(self._validate_ofac_compliance(claim))
        errors.extend(self._validate_business_interruption(claim))
        errors.extend(self._validate_incident_timeline(claim))
        errors.extend(self._validate_nist_mapping(claim))

        has_critical = any(e["severity"] == "critical" for e in errors)
        return not has_critical, errors

    def _classify_incident(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        desc = (claim.description or "").lower()
        meta = claim.enrichment_data or {}
        incident_type = meta.get("incident_type")

        if not incident_type:
            # Attempt auto-classification
            for itype, config in _INCIDENT_TYPES.items():
                keywords = itype.replace("_", " ").split()
                if any(kw in desc for kw in keywords):
                    incident_type = itype
                    break

        if not incident_type:
            errors.append({
                "code": "CYBER_INCIDENT_TYPE_UNKNOWN",
                "field": "enrichment_data.incident_type",
                "message": "Unable to classify cyber incident type from description",
                "severity": "high",
            })
        else:
            config = _INCIDENT_TYPES.get(incident_type, {})
            low, high = config.get("typical_range", (0, float("inf")))
            if claim.claim_amount > high * 1.5:
                errors.append({
                    "code": "CYBER_AMOUNT_EXCEEDS_NORM",
                    "field": "claim_amount",
                    "message": (
                        f"Claim ${claim.claim_amount:,.2f} significantly exceeds "
                        f"typical range for {incident_type} incidents "
                        f"(${low:,.0f} - ${high:,.0f})"
                    ),
                    "severity": "medium",
                })

        return errors

    def _validate_notification_compliance(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        incident_type = meta.get("incident_type")
        config = _INCIDENT_TYPES.get(incident_type, {})

        if not config.get("notification_required"):
            return errors

        records_affected = meta.get("records_affected", 0)
        if records_affected > 0:
            state = meta.get("primary_state", "DEFAULT")
            notify_days = _STATE_NOTIFICATION_DAYS.get(
                state, _STATE_NOTIFICATION_DAYS["DEFAULT"]
            )
            notification_sent = meta.get("notification_sent", False)

            try:
                incident_date = date.fromisoformat(claim.date_of_loss)
                days_since = (date.today() - incident_date).days
            except (ValueError, TypeError):
                days_since = 0

            if not notification_sent and days_since > notify_days:
                errors.append({
                    "code": "CYBER_NOTIFICATION_OVERDUE",
                    "field": "enrichment_data.notification_sent",
                    "message": (
                        f"Breach notification deadline exceeded. State {state} "
                        f"requires notification within {notify_days} days. "
                        f"{days_since} days have elapsed since incident."
                    ),
                    "severity": "critical",
                })
            elif not notification_sent and days_since > notify_days * 0.7:
                errors.append({
                    "code": "CYBER_NOTIFICATION_APPROACHING",
                    "field": "enrichment_data.notification_sent",
                    "message": (
                        f"Breach notification deadline approaching. "
                        f"{notify_days - days_since} days remaining."
                    ),
                    "severity": "high",
                })

        return errors

    def _validate_forensics(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        incident_type = meta.get("incident_type")
        config = _INCIDENT_TYPES.get(incident_type, {})

        if config.get("forensics_required"):
            forensics_firm = meta.get("forensics_provider")
            if not forensics_firm:
                errors.append({
                    "code": "CYBER_FORENSICS_NOT_ENGAGED",
                    "field": "enrichment_data.forensics_provider",
                    "message": (
                        f"Digital forensics investigation required for "
                        f"{incident_type} incidents but no forensics "
                        f"provider has been engaged"
                    ),
                    "severity": "high",
                })

        return errors

    def _validate_ofac_compliance(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        incident_type = meta.get("incident_type")
        config = _INCIDENT_TYPES.get(incident_type, {})

        if not config.get("ofac_screening_required"):
            return errors

        threat_actor = meta.get("threat_actor_attribution", "")
        ofac_screened = meta.get("ofac_screening_completed", False)

        if not ofac_screened:
            errors.append({
                "code": "CYBER_OFAC_NOT_SCREENED",
                "field": "enrichment_data.ofac_screening_completed",
                "message": (
                    "OFAC screening required before ransom payment "
                    "authorization. Treasury Department regulations prohibit "
                    "payments to sanctioned entities."
                ),
                "severity": "critical",
            })

        if threat_actor:
            for indicator in _OFAC_INDICATORS:
                if indicator.lower() in threat_actor.lower():
                    errors.append({
                        "code": "CYBER_OFAC_SANCTIONED_ENTITY",
                        "field": "enrichment_data.threat_actor_attribution",
                        "message": (
                            f"Threat actor '{threat_actor}' matches OFAC "
                            f"sanctioned entity '{indicator}'. Ransom payment "
                            f"PROHIBITED under US Treasury regulations."
                        ),
                        "severity": "critical",
                    })
                    break

        return errors

    def _validate_business_interruption(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        bi_hours = meta.get("business_interruption_hours")
        waiting_period = meta.get("bi_waiting_period_hours", 8)

        if bi_hours is not None:
            if bi_hours < waiting_period:
                errors.append({
                    "code": "CYBER_BI_BELOW_WAITING_PERIOD",
                    "field": "enrichment_data.business_interruption_hours",
                    "message": (
                        f"Business interruption ({bi_hours}h) is below policy "
                        f"waiting period ({waiting_period}h). No BI coverage applies."
                    ),
                    "severity": "medium",
                })
            elif bi_hours > 720:  # 30 days
                errors.append({
                    "code": "CYBER_BI_EXTENDED_PERIOD",
                    "field": "enrichment_data.business_interruption_hours",
                    "message": (
                        f"Extended business interruption ({bi_hours}h / "
                        f"{bi_hours / 24:.0f} days). Verify BI sublimit "
                        f"and maximum indemnity period."
                    ),
                    "severity": "medium",
                })

        return errors

    def _validate_incident_timeline(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}

        detection_date = meta.get("detection_date")
        if detection_date:
            try:
                detection = date.fromisoformat(detection_date)
                incident = date.fromisoformat(claim.date_of_loss)
                dwell_time = (detection - incident).days

                if dwell_time > 90:
                    errors.append({
                        "code": "CYBER_LONG_DWELL_TIME",
                        "field": "enrichment_data.detection_date",
                        "message": (
                            f"Dwell time of {dwell_time} days between incident "
                            f"and detection. Extended dwell time increases "
                            f"potential data exposure and loss severity."
                        ),
                        "severity": "medium",
                    })
            except (ValueError, TypeError):
                pass

        return errors

    def _validate_nist_mapping(
        self, claim: InsuranceClaim
    ) -> List[Dict[str, Any]]:
        errors = []
        meta = claim.enrichment_data or {}
        nist_gaps = meta.get("nist_framework_gaps", [])

        if nist_gaps and len(nist_gaps) >= 3:
            errors.append({
                "code": "CYBER_NIST_MATERIAL_GAPS",
                "field": "enrichment_data.nist_framework_gaps",
                "message": (
                    f"Insured has {len(nist_gaps)} material NIST CSF gaps: "
                    f"{', '.join(nist_gaps[:3])}. This may affect coverage "
                    f"determination."
                ),
                "severity": "medium",
            })

        return errors
