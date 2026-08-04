"""
Property-based tests using Hypothesis for the insurance claims pipeline.

These tests complement the example-based tests in test_pipeline.py by
generating hundreds of random inputs to find edge cases that hand-written
examples would miss.

Test strategy
-------------
* Idempotency key — deterministic: same inputs always produce the same key.
* PII masking     — never leaks: masked output never equals the original for
                   non-trivial inputs; always returns a string.
* Serialisation   — round-trip: to_kafka_dict → from_kafka_dict preserves
                   critical fields exactly.
* Validator       — never crashes: arbitrary valid-shaped claims do not raise
                   unhandled exceptions.
* Amount bounds   — boundary: amounts in (0, 10_000_000] always create a
                   claim; 0 and negative amounts always raise ValueError.
"""

import sys
import os
import pytest
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st

from src.models.claims import InsuranceClaim, ClaimType, ClaimStatus
from src.observability.pii_masking import (
    mask_name, mask_vin, mask_policy_number, mask_address, mask_claim_for_logging,
)
from src.observability.tracing import init_tracing
import src.processors.claims_validator as _validator_module


# ---------------------------------------------------------------------------
# Module-level setup
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True, scope="module")
def _setup():
    """Shared one-time setup: tracing + lineage stub."""
    init_tracing("test-property-based")
    # Prevent lineage tracker network calls during property tests.
    _validator_module.lineage_tracker.record_event = lambda **kwargs: None


# ---------------------------------------------------------------------------
# Reusable Hypothesis strategies
# ---------------------------------------------------------------------------

_CLAIM_TYPE_ST = st.sampled_from(list(ClaimType))

_VALID_POLICY_ST = st.from_regex(r"[A-Z]{3}-\d{6}", fullmatch=True)

_VALID_AMOUNT_ST = st.floats(
    min_value=0.01,
    max_value=9_999_999.0,
    allow_nan=False,
    allow_infinity=False,
)

# Dates within the last 5 years, excluding today (to avoid edge-cases in
# the "LOSS_DATE_TOO_OLD" validator rule).
_PAST_DATE_ST = st.dates(
    min_value=date.today() - timedelta(days=365 * 5),
    max_value=date.today() - timedelta(days=1),
).map(str)

# Names: at least 2 printable-letter characters (Pydantic allows any str;
# the validator has a regex check that is separate from construction).
_VALID_NAME_ST = st.text(
    alphabet=st.characters(whitelist_categories=["Lu", "Ll"], whitelist_characters=[" ", "-", "'"]),
    min_size=2,
    max_size=60,
)


# ---------------------------------------------------------------------------
# 1. Idempotency key — determinism
# ---------------------------------------------------------------------------

class TestIdempotencyKeyProperty:
    """The idempotency key must be a deterministic hash of (policy, date, amount)."""

    @given(
        policy=_VALID_POLICY_ST,
        date_loss=_PAST_DATE_ST,
        amount=_VALID_AMOUNT_ST,
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_same_inputs_always_same_key(self, policy, date_loss, amount):
        """Two claims with identical (policy, date_of_loss, amount) share an idempotency key."""
        c1 = InsuranceClaim(
            policy_number=policy,
            claimant_name="Alice Smith",
            claim_type=ClaimType.AUTO,
            claim_amount=amount,
            date_of_loss=date_loss,
        )
        c2 = InsuranceClaim(
            policy_number=policy,
            claimant_name="Bob Jones",          # different name — should NOT matter
            claim_type=ClaimType.HEALTH,         # different type — should NOT matter
            claim_amount=amount,
            date_of_loss=date_loss,
        )
        assert c1.idempotency_key == c2.idempotency_key

    @given(
        policy=_VALID_POLICY_ST,
        date_loss=_PAST_DATE_ST,
        amount1=_VALID_AMOUNT_ST,
        amount2=_VALID_AMOUNT_ST,
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_different_amounts_produce_different_keys(self, policy, date_loss, amount1, amount2):
        """Different amounts must produce different idempotency keys."""
        assume(abs(amount1 - amount2) > 0.005)   # skip near-equal floats
        c1 = InsuranceClaim(
            policy_number=policy, claimant_name="X Y",
            claim_type=ClaimType.AUTO, claim_amount=amount1, date_of_loss=date_loss,
        )
        c2 = InsuranceClaim(
            policy_number=policy, claimant_name="X Y",
            claim_type=ClaimType.AUTO, claim_amount=amount2, date_of_loss=date_loss,
        )
        assert c1.idempotency_key != c2.idempotency_key

    @given(policy=_VALID_POLICY_ST, date_loss=_PAST_DATE_ST, amount=_VALID_AMOUNT_ST)
    @settings(max_examples=100)
    def test_key_is_always_32_hex_chars(self, policy, date_loss, amount):
        c = InsuranceClaim(
            policy_number=policy, claimant_name="A B",
            claim_type=ClaimType.AUTO, claim_amount=amount, date_of_loss=date_loss,
        )
        assert c.idempotency_key is not None
        assert len(c.idempotency_key) == 32
        assert all(ch in "0123456789abcdef" for ch in c.idempotency_key)


# ---------------------------------------------------------------------------
# 2. PII masking — never leaks, always returns a string
# ---------------------------------------------------------------------------

class TestPIIMaskingProperty:
    """PII masking must always return a string and never expose the original value."""

    @given(name=st.text(min_size=2, max_size=80))
    @settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow])
    def test_mask_name_always_returns_string(self, name):
        result = mask_name(name)
        assert isinstance(result, str)
        assert len(result) > 0

    @given(name=_VALID_NAME_ST)
    @settings(max_examples=200)
    def test_mask_name_never_returns_full_name(self, name):
        """Masked name must be shorter than or differ from the original."""
        result = mask_name(name)
        # The full original name should never appear verbatim in the output.
        assert result != name

    @given(vin=st.text(min_size=5, max_size=20, alphabet="ABCDEFGHJKLMNPRSTUVWXYZ0123456789"))
    @settings(max_examples=200)
    def test_mask_vin_shows_only_last_4(self, vin):
        result = mask_vin(vin)
        assert isinstance(result, str)
        assert result.endswith(vin[-4:])
        # All but the last 4 characters should be masked.
        revealed = result.replace("*", "")
        assert revealed == vin[-4:]

    @given(policy=_VALID_POLICY_ST)
    @settings(max_examples=200)
    def test_mask_policy_never_exposes_middle_digits(self, policy):
        result = mask_policy_number(policy)
        # The masked form must contain '***' and never equal the original.
        assert "***" in result
        assert result != policy
        # The full 6-digit numeric part must not appear verbatim.
        full_digits = policy[4:]   # e.g. '123456'
        assert full_digits not in result

    @given(claim_dict=st.fixed_dictionaries({
        "claimant_name":   st.text(min_size=2, max_size=40),
        "policy_number":   _VALID_POLICY_ST,
        "claim_amount":    _VALID_AMOUNT_ST,
    }))
    @settings(max_examples=200)
    def test_mask_claim_for_logging_does_not_mutate_original(self, claim_dict):
        original_name = claim_dict["claimant_name"]
        original_policy = claim_dict["policy_number"]
        _ = mask_claim_for_logging(claim_dict)
        # Original dict must be unchanged.
        assert claim_dict["claimant_name"] == original_name
        assert claim_dict["policy_number"] == original_policy


# ---------------------------------------------------------------------------
# 3. Serialisation round-trip
# ---------------------------------------------------------------------------

class TestSerializationProperty:
    """to_kafka_dict → from_kafka_dict must preserve all critical fields."""

    @given(
        policy=_VALID_POLICY_ST,
        name=_VALID_NAME_ST,
        claim_type=_CLAIM_TYPE_ST,
        amount=_VALID_AMOUNT_ST,
        date_loss=_PAST_DATE_ST,
    )
    @settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow])
    def test_roundtrip_preserves_identity_fields(self, policy, name, claim_type, amount, date_loss):
        claim = InsuranceClaim(
            policy_number=policy,
            claimant_name=name,
            claim_type=claim_type,
            claim_amount=amount,
            date_of_loss=date_loss,
        )
        data = claim.to_kafka_dict()
        restored = InsuranceClaim.from_kafka_dict(data)

        assert restored.claim_id == claim.claim_id
        assert restored.policy_number == claim.policy_number
        assert restored.schema_version == claim.schema_version
        assert restored.idempotency_key == claim.idempotency_key
        assert abs(restored.claim_amount - claim.claim_amount) < 1e-6

    @given(
        policy=_VALID_POLICY_ST,
        name=_VALID_NAME_ST,
        amount=_VALID_AMOUNT_ST,
        date_loss=_PAST_DATE_ST,
    )
    @settings(max_examples=200)
    def test_kafka_dict_always_contains_schema_version(self, policy, name, amount, date_loss):
        claim = InsuranceClaim(
            policy_number=policy, claimant_name=name,
            claim_type=ClaimType.AUTO, claim_amount=amount, date_of_loss=date_loss,
        )
        data = claim.to_kafka_dict()
        assert "schema_version" in data
        assert data["schema_version"] == "v1"

    @given(
        policy=_VALID_POLICY_ST,
        name=_VALID_NAME_ST,
        amount=_VALID_AMOUNT_ST,
        date_loss=_PAST_DATE_ST,
    )
    @settings(max_examples=200)
    def test_kafka_dict_always_contains_correlation_id(self, policy, name, amount, date_loss):
        claim = InsuranceClaim(
            policy_number=policy, claimant_name=name,
            claim_type=ClaimType.AUTO, claim_amount=amount, date_of_loss=date_loss,
        )
        data = claim.to_kafka_dict()
        assert "correlation_id" in data
        assert len(data["correlation_id"]) == 32   # uuid4 hex sans dashes


# ---------------------------------------------------------------------------
# 4. Validator — never raises an unhandled exception
# ---------------------------------------------------------------------------

class TestValidatorNeverCrashesProperty:
    """The validator must return (bool, InsuranceClaim) for any valid InsuranceClaim."""

    @pytest.fixture(autouse=True)
    def _stub_lineage(self):
        _validator_module.lineage_tracker.record_event = lambda **kwargs: None

    @given(
        policy=_VALID_POLICY_ST,
        name=_VALID_NAME_ST,
        claim_type=_CLAIM_TYPE_ST,
        amount=_VALID_AMOUNT_ST,
        date_loss=_PAST_DATE_ST,
    )
    @settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow])
    def test_validator_always_returns_result_never_raises(
        self, policy, name, claim_type, amount, date_loss
    ):
        from src.processors.claims_validator import ClaimsValidator
        validator = ClaimsValidator()

        claim = InsuranceClaim(
            policy_number=policy,
            claimant_name=name,
            claim_type=claim_type,
            claim_amount=amount,
            date_of_loss=date_loss,
        )
        # Must not raise — only return (True/False, InsuranceClaim).
        is_valid, result = validator.validate(claim)
        assert isinstance(is_valid, bool)
        assert isinstance(result, InsuranceClaim)
        assert result.status in list(ClaimStatus)

    @given(
        policy=_VALID_POLICY_ST,
        name=_VALID_NAME_ST,
        amount=_VALID_AMOUNT_ST,
        date_loss=_PAST_DATE_ST,
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_valid_input_processing_metadata_always_well_formed(
        self, policy, name, amount, date_loss
    ):
        """processing_metadata must always contain the severity summary keys."""
        from src.processors.claims_validator import ClaimsValidator
        validator = ClaimsValidator()

        claim = InsuranceClaim(
            policy_number=policy, claimant_name=name,
            claim_type=ClaimType.AUTO, claim_amount=amount, date_of_loss=date_loss,
        )
        _, result = validator.validate(claim)
        summary = result.processing_metadata.get("validation_severity_summary", {})
        assert set(summary.keys()) == {"critical", "high", "medium", "low"}


# ---------------------------------------------------------------------------
# 5. Amount boundary invariants
# ---------------------------------------------------------------------------

class TestAmountBoundsProperty:
    """Pydantic model must enforce amount bounds (0 < amount ≤ 10_000_000)."""

    @given(amount=_VALID_AMOUNT_ST)
    @settings(max_examples=200)
    def test_valid_amounts_always_accepted(self, amount):
        claim = InsuranceClaim(
            policy_number="AUT-000001", claimant_name="Test User",
            claim_type=ClaimType.AUTO, claim_amount=amount, date_of_loss="2025-01-01",
        )
        # Float rounding may introduce small precision differences — use tolerance.
        assert abs(claim.claim_amount - amount) < 0.01

    @given(amount=st.floats(max_value=0.0, allow_nan=False, allow_infinity=False))
    @settings(max_examples=200)
    def test_zero_and_negative_amounts_always_rejected(self, amount):
        with pytest.raises((ValueError, Exception)):
            InsuranceClaim(
                policy_number="AUT-000001", claimant_name="Test User",
                claim_type=ClaimType.AUTO, claim_amount=amount, date_of_loss="2025-01-01",
            )

    @given(amount=st.floats(min_value=10_000_001.0, max_value=1e15,
                            allow_nan=False, allow_infinity=False))
    @settings(max_examples=100)
    def test_amounts_above_limit_always_rejected(self, amount):
        with pytest.raises((ValueError, Exception)):
            InsuranceClaim(
                policy_number="AUT-000001", claimant_name="Test User",
                claim_type=ClaimType.AUTO, claim_amount=amount, date_of_loss="2025-01-01",
            )
