import pytest

from vadimgest.search.scoring import (
    extract_document_memory_score,
    memory_boost,
    score_memory,
    source_prior,
)


def test_source_priors_separate_direct_evidence_from_ambient_noise():
    assert source_prior("vadim-said://2026-07-10") == 1.0
    assert source_prior("signal://person/123") > source_prior("bee://fact/123")
    assert source_prior("bee://fact/123") > source_prior("browser://example.com")
    assert source_prior("xnews://123") < source_prior("gmail://message-id")
    assert source_prior("vadimgest://bee/fact/123") == source_prior("bee://fact/123")


def test_external_claim_reduces_direct_message_prior():
    assert source_prior("signal://person/123", claim_scope="external") < source_prior(
        "signal://person/123", claim_scope="speaker"
    )


def test_score_and_route_high_value_fact_to_state_log():
    scored = score_memory(
        importance=9,
        confidence=9,
        durability=9,
        source_uri="signal://client/123",
    )

    assert scored.score == pytest.approx(8.1)
    assert scored.route == "state_log"


def test_low_value_ambient_fact_stays_in_lake():
    scored = score_memory(
        importance=2,
        confidence=6,
        durability=2,
        source_uri="browser://example.com",
    )

    assert scored.route == "lake"


def test_hard_keep_with_uncertain_evidence_is_log_only():
    scored = score_memory(
        importance=8,
        confidence=5,
        durability=9,
        source_uri="bee://conversation/123",
        hard_keep=True,
    )

    assert scored.route == "log_only"


def test_document_score_prefers_explicit_fact_score_and_defaults_neutral():
    content = """
- **stage:** negotiation · src: `signal://client/1` · 2026-07-10 · score: 8.4
- **memory-score:** `6.2`
"""
    assert extract_document_memory_score(content) == 8.4
    assert extract_document_memory_score("# No scored facts yet") == 5.0


def test_memory_boost_is_bounded_and_neutral_at_five():
    assert memory_boost(5.0) == pytest.approx(1.0)
    assert memory_boost(0.0) == pytest.approx(0.85)
    assert memory_boost(10.0) == pytest.approx(1.15)
