"""
Feature Engineering Engine for Insurance Fraud Detection.

Computes 50+ real-time and historical features for claim risk scoring:
  - Claimant frequency metrics (30d, 90d, 365d)
  - Claim amount deviations and z-scores
  - Provider risk and historical fraud rates
  - Geographic risk scores and zip code fraud prevalence
  - Temporal patterns (time-to-file, day-of-week, month)
  - Policy age and premium-to-claim ratio
  - Network graph risk indicators
"""

from __future__ import annotations

import math
import hashlib
import random
import structlog
from datetime import date, datetime
from typing import Any, Dict, List

from src.models.claims import InsuranceClaim

logger = structlog.get_logger(__name__)


class FeatureEngineer:
    """Computes 50+ features from raw InsuranceClaim and historical context."""

    def extract_features(self, claim: InsuranceClaim, context: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
        ctx = context or {}
        features: Dict[str, float] = {}

        # 1. Claim Financial Metrics (8 features)
        amount = claim.claim_amount
        features["claim_amount"] = amount
        features["log_claim_amount"] = math.log1p(max(0, amount))
        features["amount_to_policy_limit_ratio"] = min(2.0, amount / max(1.0, ctx.get("policy_limit", 50000.0)))
        features["amount_vs_avg_paid_ratio"] = amount / max(1.0, ctx.get("avg_claim_amount_by_type", 3500.0))
        features["amount_zscore_by_type"] = (amount - ctx.get("mean_amount", 4000.0)) / max(1.0, ctx.get("std_amount", 2500.0))
        features["is_round_number_amount"] = 1.0 if amount > 100 and amount % 100 == 0 else 0.0
        features["is_just_below_threshold"] = 1.0 if (4900 <= amount < 5000 or 9900 <= amount < 10000) else 0.0
        features["deductible_amount"] = float(ctx.get("deductible", 500.0))

        # 2. Claimant History Metrics (10 features)
        claimant_history = ctx.get("claimant_history", {})
        features["claimant_freq_30d"] = float(claimant_history.get("claims_30d", 0))
        features["claimant_freq_90d"] = float(claimant_history.get("claims_90d", 0))
        features["claimant_freq_365d"] = float(claimant_history.get("claims_365d", 0))
        features["claimant_prior_fraud_flag"] = 1.0 if claimant_history.get("prior_fraud_flag") else 0.0
        features["claimant_address_change_count_12m"] = float(claimant_history.get("address_changes_12m", 0))
        features["claimant_phone_change_count_12m"] = float(claimant_history.get("phone_changes_12m", 0))
        features["claimant_avg_claim_amount"] = float(claimant_history.get("avg_amount", amount))
        features["claimant_std_claim_amount"] = float(claimant_history.get("std_amount", 0.0))
        features["claimant_total_paid_lifetime"] = float(claimant_history.get("total_paid", 0.0))
        features["claimant_account_age_days"] = float(claimant_history.get("account_age_days", 365))

        # 3. Policy & Coverage Metrics (8 features)
        policy = ctx.get("policy_info", {})
        policy_age = float(policy.get("policy_age_days", 180))
        features["policy_age_days"] = policy_age
        features["is_new_policy_under_30d"] = 1.0 if policy_age < 30 else 0.0
        features["is_new_policy_under_90d"] = 1.0 if policy_age < 90 else 0.0
        features["premium_amount"] = float(policy.get("annual_premium", 1200.0))
        features["premium_to_claim_ratio"] = features["premium_amount"] / max(1.0, amount)
        features["recent_coverage_upgrade_flag"] = 1.0 if policy.get("upgraded_recently") else 0.0
        features["cancellation_notice_pending"] = 1.0 if policy.get("cancellation_pending") else 0.0
        features["payment_lapsed_count_12m"] = float(policy.get("lapses_12m", 0))

        # 4. Temporal & Filing Delay Features (8 features)
        try:
            loss_d = date.fromisoformat(claim.date_of_loss)
            filed_d = date.fromisoformat(claim.date_filed)
            delay = (filed_d - loss_d).days
        except Exception:
            delay = 1
            loss_d = date.today()

        features["time_to_file_days"] = max(0.0, float(delay))
        features["is_same_day_filed"] = 1.0 if delay == 0 else 0.0
        features["is_delayed_over_30d"] = 1.0 if delay > 30 else 0.0
        features["day_of_week_filed"] = float(filed_d.weekday())
        features["is_weekend_filed"] = 1.0 if filed_d.weekday() in (5, 6) else 0.0
        features["month_filed"] = float(filed_d.month)
        features["is_holiday_season_filed"] = 1.0 if filed_d.month in (11, 12) else 0.0
        features["days_since_policy_inception"] = float(policy.get("days_since_inception", 100))

        # 5. Provider & Entity Network Features (8 features)
        provider = ctx.get("provider_info", {})
        features["provider_fraud_rate_historical"] = float(provider.get("fraud_rate", 0.02))
        features["provider_claim_volume_30d"] = float(provider.get("volume_30d", 15))
        features["provider_avg_billing_per_claim"] = float(provider.get("avg_billing", amount))
        features["provider_billing_zscore"] = float(provider.get("billing_zscore", 0.0))
        features["provider_network_risk_score"] = float(provider.get("network_risk", 0.1))
        features["attorney_involved_flag"] = 1.0 if ctx.get("attorney_involved") else 0.0
        features["attorney_fraud_rate_historical"] = float(ctx.get("attorney_fraud_rate", 0.01))
        features["shared_address_phone_entity_count"] = float(ctx.get("shared_entity_count", 0))

        # 6. Geographic & Location Risk Features (8 features)
        meta = claim.enrichment_data or {}
        state = meta.get("loss_state", "CA")
        features["geographic_risk_score"] = float(meta.get("geographic_risk_score", 0.5))
        features["zip_code_fraud_prevalence"] = float(ctx.get("zip_fraud_rate", 0.03))
        features["is_high_risk_state"] = 1.0 if state in ("FL", "TX", "CA", "NY") else 0.0
        features["loss_location_distance_from_home_miles"] = float(ctx.get("distance_from_home_miles", 12.0))
        features["catastrophe_event_flag"] = 1.0 if meta.get("is_catastrophe_claim") else 0.0
        features["weather_correlation_confidence"] = float(meta.get("weather_correlation", {}).get("confidence", 0.0) if meta.get("weather_correlation") else 0.0)
        features["fema_flood_zone_risk_num"] = {"low": 1.0, "moderate": 2.0, "high": 3.0}.get(meta.get("flood_zone_risk", "low"), 1.0)
        features["contractor_certified_flag"] = 1.0 if meta.get("preferred_contractors") else 0.0

        return features
