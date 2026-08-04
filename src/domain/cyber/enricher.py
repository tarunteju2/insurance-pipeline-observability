"""
Cyber Claims Enricher.

Enriches cyber claims with:
  - Threat intelligence correlation (simulated)
  - Dark web exposure assessment
  - Forensics provider assignment
  - NIST CSF maturity assessment
  - Breach cost estimation (per-record cost model)
  - Business interruption quantification
  - Incident response vendor panel matching
"""

import hashlib
import random
import structlog
from datetime import date, timedelta
from typing import Any, Dict

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)

# Threat intelligence feeds (simulated)
_THREAT_ACTORS = [
    {"group": "APT28", "origin": "Russia", "ttps": ["spear_phishing", "zero_day"], "severity": "critical"},
    {"group": "APT41", "origin": "China", "ttps": ["supply_chain", "backdoor"], "severity": "critical"},
    {"group": "LockBit 3.0", "origin": "Russia", "ttps": ["ransomware", "data_exfil"], "severity": "critical"},
    {"group": "BlackCat/ALPHV", "origin": "Russia", "ttps": ["ransomware", "triple_extortion"], "severity": "critical"},
    {"group": "Scattered Spider", "origin": "US/UK", "ttps": ["social_engineering", "sim_swap"], "severity": "high"},
    {"group": "Cl0p", "origin": "Russia", "ttps": ["ransomware", "zero_day_exploit"], "severity": "critical"},
    {"group": "Opportunistic", "origin": "Various", "ttps": ["phishing", "credential_stuffing"], "severity": "medium"},
    {"group": "Insider", "origin": "Internal", "ttps": ["unauthorized_access", "data_theft"], "severity": "high"},
]

# Forensics vendor panel
_FORENSICS_PANEL = [
    {"vendor_id": "FOR-001", "name": "CrowdStrike Services", "specialties": ["ransomware", "apt"], "tier": 1},
    {"vendor_id": "FOR-002", "name": "Mandiant (Google Cloud)", "specialties": ["breach", "apt", "insider"], "tier": 1},
    {"vendor_id": "FOR-003", "name": "Unit 42 (Palo Alto)", "specialties": ["ransomware", "malware"], "tier": 1},
    {"vendor_id": "FOR-004", "name": "Kroll Cyber", "specialties": ["breach", "social_engineering", "forensics"], "tier": 2},
    {"vendor_id": "FOR-005", "name": "Stroz Friedberg (Aon)", "specialties": ["litigation_support", "breach"], "tier": 2},
    {"vendor_id": "FOR-006", "name": "Secureworks", "specialties": ["ransomware", "malware", "detection"], "tier": 2},
]

# Breach cost model (per-record costs by industry)
_PER_RECORD_COSTS = {
    "healthcare": 429.0,
    "financial": 352.0,
    "technology": 298.0,
    "professional_services": 275.0,
    "education": 245.0,
    "retail": 189.0,
    "manufacturing": 172.0,
    "public_sector": 158.0,
    "default": 195.0,
}

# Incident response cost breakdown
_IR_COST_COMPONENTS = {
    "forensics_investigation": 0.25,
    "legal_counsel": 0.15,
    "notification_costs": 0.12,
    "credit_monitoring": 0.10,
    "public_relations": 0.05,
    "regulatory_fines": 0.08,
    "business_interruption": 0.15,
    "remediation": 0.10,
}


class CyberClaimEnricher:
    """Enriches cyber claims with threat intelligence and cost modeling."""

    def enrich(self, claim: InsuranceClaim) -> Dict[str, Any]:
        enrichment: Dict[str, Any] = {}

        enrichment.update(self._threat_intelligence(claim))
        enrichment.update(self._dark_web_check(claim))
        enrichment.update(self._assign_forensics(claim, enrichment))
        enrichment.update(self._nist_assessment(claim))
        enrichment.update(self._estimate_breach_cost(claim, enrichment))
        enrichment.update(self._quantify_bi(claim))
        enrichment.update(self._incident_response_plan(claim, enrichment))

        return enrichment

    def _threat_intelligence(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Correlate claim with known threat intelligence."""
        seed = int(hashlib.md5(claim.claim_id.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)

        actor = rng.choice(_THREAT_ACTORS)
        ioc_count = rng.randint(0, 25)
        attribution_confidence = rng.choice([0.3, 0.5, 0.6, 0.75, 0.85, 0.95])

        return {
            "threat_intelligence": {
                "threat_actor_attribution": actor["group"],
                "actor_origin": actor["origin"],
                "ttps_observed": actor["ttps"],
                "threat_severity": actor["severity"],
                "indicators_of_compromise": ioc_count,
                "attribution_confidence": attribution_confidence,
                "mitre_attack_techniques": self._map_mitre(actor["ttps"]),
            },
            "threat_actor_attribution": actor["group"],
        }

    def _map_mitre(self, ttps: list) -> list:
        """Map TTPs to MITRE ATT&CK technique IDs."""
        mapping = {
            "spear_phishing": "T1566.001",
            "phishing": "T1566",
            "zero_day": "T1190",
            "zero_day_exploit": "T1190",
            "supply_chain": "T1195",
            "backdoor": "T1547",
            "ransomware": "T1486",
            "data_exfil": "T1041",
            "triple_extortion": "T1486",
            "social_engineering": "T1598",
            "sim_swap": "T1556",
            "credential_stuffing": "T1110.004",
            "unauthorized_access": "T1078",
            "data_theft": "T1005",
            "malware": "T1204",
        }
        return [mapping.get(ttp, f"T{hash(ttp) % 9999:04d}") for ttp in ttps]

    def _dark_web_check(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Simulated dark web exposure assessment."""
        seed = int(hashlib.md5(claim.policy_number.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)

        exposed = rng.random() < 0.35
        records = 0
        if exposed:
            records = rng.choice([100, 500, 2_500, 10_000, 50_000, 250_000, 1_000_000])

        return {
            "dark_web_exposure": {
                "data_found": exposed,
                "estimated_records_exposed": records,
                "exposure_type": rng.choice([
                    "credentials", "pii", "financial", "medical", "mixed"
                ]) if exposed else None,
                "marketplace_listings": rng.randint(0, 5) if exposed else 0,
                "scan_date": date.today().isoformat(),
            },
            "records_affected": records,
        }

    def _assign_forensics(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assign appropriate forensics vendor from panel."""
        threat = current.get("threat_intelligence", {})
        severity = threat.get("threat_severity", "medium")
        ttps = threat.get("ttps_observed", [])

        # Select vendor by severity and specialty match
        tier_1 = [v for v in _FORENSICS_PANEL if v["tier"] == 1]
        tier_2 = [v for v in _FORENSICS_PANEL if v["tier"] == 2]

        pool = tier_1 if severity == "critical" else tier_2
        if not pool:
            pool = _FORENSICS_PANEL

        # Score by specialty overlap
        best = max(pool, key=lambda v: len(set(v["specialties"]) & set(ttps)))

        return {
            "forensics_provider": best["name"],
            "forensics_vendor_id": best["vendor_id"],
            "forensics_tier": best["tier"],
            "forensics_specialties": best["specialties"],
        }

    def _nist_assessment(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Assess insured's NIST CSF maturity level."""
        seed = int(hashlib.md5(claim.policy_number.encode()).hexdigest()[:6], 16)
        rng = random.Random(seed)

        functions = ["identify", "protect", "detect", "respond", "recover"]
        maturity = {}
        gaps = []

        for func in functions:
            score = round(rng.uniform(1.0, 5.0), 1)
            maturity[func] = {
                "score": score,
                "tier": self._maturity_tier(score),
            }
            if score < 2.5:
                gaps.append(func)

        overall = sum(m["score"] for m in maturity.values()) / len(maturity)

        return {
            "nist_assessment": {
                "functions": maturity,
                "overall_maturity": round(overall, 1),
                "overall_tier": self._maturity_tier(overall),
            },
            "nist_framework_gaps": gaps,
        }

    @staticmethod
    def _maturity_tier(score: float) -> str:
        if score >= 4.0:
            return "adaptive"
        elif score >= 3.0:
            return "repeatable"
        elif score >= 2.0:
            return "risk_informed"
        else:
            return "partial"

    def _estimate_breach_cost(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Estimate total breach cost using per-record cost model."""
        records = current.get("records_affected", 0)
        meta = claim.enrichment_data or {}
        industry = meta.get("insured_industry", "default")
        per_record = _PER_RECORD_COSTS.get(industry, _PER_RECORD_COSTS["default"])

        base_cost = records * per_record
        # Apply component breakdown
        components = {}
        for comp, pct in _IR_COST_COMPONENTS.items():
            components[comp] = round(base_cost * pct, 2)

        total_estimated = round(base_cost, 2) if records > 0 else claim.claim_amount

        return {
            "breach_cost_estimate": {
                "per_record_cost": per_record,
                "records_affected": records,
                "total_estimated_cost": total_estimated,
                "cost_components": components,
                "model": "ponemon_ibm_methodology",
                "industry": industry,
            },
        }

    def _quantify_bi(self, claim: InsuranceClaim) -> Dict[str, Any]:
        """Quantify business interruption impact."""
        seed = int(hashlib.md5(claim.claim_id.encode()).hexdigest()[:6], 16)
        rng = random.Random(seed)

        downtime_hours = rng.choice([4, 8, 24, 48, 72, 168, 336, 720])
        hourly_revenue = rng.choice([
            500, 1_000, 5_000, 10_000, 25_000, 50_000, 100_000
        ])
        bi_loss = downtime_hours * hourly_revenue

        return {
            "business_interruption_hours": downtime_hours,
            "bi_hourly_revenue_impact": hourly_revenue,
            "bi_total_loss_estimate": bi_loss,
            "bi_recovery_timeline_days": max(1, downtime_hours // 24),
        }

    def _incident_response_plan(
        self, claim: InsuranceClaim, current: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate incident response action plan."""
        threat = current.get("threat_intelligence", {})
        severity = threat.get("threat_severity", "medium")

        actions = [
            {"step": 1, "action": "Engage forensics provider", "status": "pending", "priority": "critical"},
            {"step": 2, "action": "Contain affected systems", "status": "pending", "priority": "critical"},
            {"step": 3, "action": "Engage legal counsel (breach coach)", "status": "pending", "priority": "high"},
            {"step": 4, "action": "Assess data exfiltration scope", "status": "pending", "priority": "high"},
        ]

        if current.get("records_affected", 0) > 0:
            actions.extend([
                {"step": 5, "action": "Prepare breach notifications", "status": "pending", "priority": "high"},
                {"step": 6, "action": "Engage credit monitoring provider", "status": "pending", "priority": "medium"},
                {"step": 7, "action": "Notify regulators (state AG, HHS if PHI)", "status": "pending", "priority": "high"},
            ])

        if severity == "critical":
            actions.append(
                {"step": len(actions) + 1, "action": "Brief executive leadership and board", "status": "pending", "priority": "critical"},
            )

        return {
            "incident_response_plan": {
                "actions": actions,
                "severity_level": severity,
                "estimated_containment_hours": 24 if severity == "critical" else 72,
            },
        }
