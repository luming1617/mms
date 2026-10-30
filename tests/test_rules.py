from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mms.models import CandidateInput, ExistingFact
from mms.services.normalizer import normalize_candidate

pytestmark = pytest.mark.unit

from mms.services import rules
from mms.services.rules import DECISION_RULE_SET_VERSION, RuleContext, _override_allowed, apply_decision_matrix


def make_candidate(value="zh-CN", source="USER_EXPLICIT", observed=None, valid_from=None, valid_to=None):
    now = observed or datetime.now(UTC)
    candidate = CandidateInput(
        customer_id="cust",
        source_system="test",
        source_event_id=str(uuid4()),
        source_refs=[{"ref_type": "message", "message_id": "m-1"}],
        memory_type="CUSTOMER_PROFILE",
        fact_key="communication.language",
        object={"value": value},
        content="客户明确表达了偏好",
        content_language="zh-CN",
        source_kind=source,
        observed_at=now,
        valid_from=valid_from,
        valid_to=valid_to,
        language_confidence=0.99,
    )
    registry = {
        "version": 1,
        "cardinality": "SINGLE_VALUE",
        "allowed_source_kinds": ["USER_EXPLICIT"],
        "requires_authority": False,
        "normalization_version": "norm-test",
        "validator": {"type": "object", "required": ["value"], "properties": {"value": {"type": "string"}}},
        "mutual_exclusion": [{"group": "language", "values": ["zh-CN", "en-US"]}],
        "semantic_template": "communication.language = {value}; valid_from = {valid_from}; valid_to = {valid_to}",
    }
    policy = {
        "ignore_threshold": 50,
        "auto_accept_threshold": 70,
        "semantic_match_threshold": 0.82,
        "language_confidence_min": 0.8,
        "sensitive_policy": {},
    }
    normalized, breakdown = normalize_candidate(candidate, registry, policy, "policy@1", "hash")
    return normalized, breakdown, registry, policy


def existing_for(candidate, source="USER_EXPLICIT"):
    return ExistingFact(
        memory_id=uuid4(),
        version_no=1,
        lifecycle_status="ACTIVE",
        object_json=candidate.object_json,
        object_hash=candidate.object_hash,
        semantic_hash=candidate.semantic_hash,
        content_hash="0" * 64,
        content_language="zh-CN",
        source_kind=source,
        valid_from=candidate.valid_from - timedelta(days=1),
        valid_to=None,
        evidence_languages={"zh-CN"},
    )


def decide(candidate, breakdown, registry, policy, *, exact=(), new_evidence=False, languages=(), semantic=()):
    return apply_decision_matrix(
        RuleContext(
            candidate=candidate,
            value_score=candidate.value_score,
            score_breakdown=breakdown,
            exact_matches=list(exact),
            has_new_evidence=new_evidence,
            new_evidence_languages=list(languages),
            semantic_candidates=list(semantic),
            authoritative_fact=None,
            policy=policy,
            registry=registry,
        )
    )


def test_add_when_no_exact_or_semantic_match():
    c, b, r, p = make_candidate()
    d = decide(c, b, r, p)
    assert (d.outcome, d.primary_rule_id, d.embedding_action) == ("ADD", "R070", "UPSERT")
    assert d.engine_version == DECISION_RULE_SET_VERSION


def test_noop_same_fact_without_new_evidence():
    c, b, r, p = make_candidate()
    d = decide(c, b, r, p, exact=[existing_for(c)], new_evidence=False)
    assert (d.outcome, d.primary_rule_id) == ("NOOP", "R050")


def test_reinforce_new_language_appends_view_only():
    c, b, r, p = make_candidate()
    old = existing_for(c)
    old.evidence_languages = {"en-US"}
    d = decide(c, b, r, p, exact=[old], new_evidence=True, languages=["zh-CN"])
    assert (d.outcome, d.primary_rule_id, d.embedding_action) == ("REINFORCE", "R060", "UPSERT_VIEWS")
    assert d.view_actions == [{"view_kind": "ORIGINAL", "view_language": "zh-CN", "action": "UPSERT"}]


def test_supersede_single_value_change():
    c, b, r, p = make_candidate("en-US")
    old_candidate, _, _, _ = make_candidate("zh-CN")
    old = existing_for(old_candidate)
    d = decide(c, b, r, p, exact=[old])
    assert (d.outcome, d.primary_rule_id, d.embedding_action) == ("SUPERSEDE", "R080", "UPSERT_NEW_AND_DELETE_OLD")


def test_low_score_needs_review_and_high_sensitivity_rejected():
    c, b, r, p = make_candidate()
    c.value_score = 60
    d = decide(c, b, r, p)
    assert d.outcome == "NEEDS_VERIFICATION" and d.primary_rule_id == "R041"
    c.value_score = 90
    c.sensitivity = "HIGH"
    d = decide(c, b, r, p)
    assert d.outcome == "REJECTED" and d.reason_codes == ["SENSITIVE_DATA"]


def test_multi_value_coexists_and_semantic_only_requires_review():
    c, b, r, p = make_candidate("en-US")
    c.cardinality = "MULTI_VALUE"
    old_candidate, _, _, _ = make_candidate("zh-CN")
    d = decide(c, b, r, p, exact=[existing_for(old_candidate)])
    assert (d.outcome, d.primary_rule_id) == ("ADD", "R075")
    d = decide(c, b, r, p, semantic=[{"memory_id": uuid4(), "score": 0.9}])
    assert (d.outcome, d.primary_rule_id) == ("NEEDS_VERIFICATION", "R110")


def test_temporal_non_overlap_uses_expected_source_version():
    c, b, r, p = make_candidate("en-US")
    c.cardinality = "TEMPORAL"
    old_candidate, _, _, _ = make_candidate("zh-CN")
    old = existing_for(old_candidate)
    old.valid_to = c.valid_from - timedelta(seconds=1)
    d = decide(c, b, r, p, exact=[old])
    assert (d.outcome, d.primary_rule_id) == ("ADD_TEMPORAL_VERSION", "R090")
    assert d.expected_versions == {str(old.memory_id): old.version_no}


def test_temporal_overlap_uses_supersede_and_temporal_without_matches_adds():
    c, b, r, p = make_candidate("en-US")
    c.cardinality = "TEMPORAL"
    old_candidate, _, _, _ = make_candidate("zh-CN")
    old = existing_for(old_candidate)
    old.valid_from = c.valid_from - timedelta(days=1)
    old.valid_to = c.valid_to + timedelta(days=1) if c.valid_to else c.valid_from + timedelta(days=1)
    d = decide(c, b, r, p, exact=[old])
    assert (d.outcome, d.primary_rule_id) == ("SUPERSEDE", "R080")
    d = decide(c, b, r, p)
    assert (d.outcome, d.primary_rule_id) == ("ADD", "R070")


def test_registry_without_mutual_exclusion_and_non_matching_value_require_review():
    c, b, r, p = make_candidate("en-US")
    old_candidate, _, _, _ = make_candidate("zh-CN")
    old = existing_for(old_candidate)
    registry = {key: value for key, value in r.items() if key != "mutual_exclusion"}
    d = decide(c, b, registry, p, exact=[old])
    assert (d.outcome, d.primary_rule_id) == ("NEEDS_VERIFICATION", "R076")
    registry = {**r, "mutual_exclusion": [{"group": "language", "values": ["fr-FR"]}]}
    d = decide(c, b, registry, p, exact=[old])
    assert (d.outcome, d.primary_rule_id) == ("NEEDS_VERIFICATION", "R076")


def test_auto_write_disabled_and_untrusted_source_override():
    c, b, r, p = make_candidate("en-US")
    old_candidate, _, _, _ = make_candidate("zh-CN")
    registry = {**r, "auto_write": False}
    d = decide(c, b, registry, p, exact=[existing_for(old_candidate)])
    assert (d.outcome, d.primary_rule_id, d.reason_codes) == ("NEEDS_VERIFICATION", "R041", ["AUTO_WRITE_DISABLED"])
    context = RuleContext(
        candidate=c,
        value_score=c.value_score,
        score_breakdown=b,
        exact_matches=[],
        has_new_evidence=False,
        new_evidence_languages=[],
        semantic_candidates=[],
        authoritative_fact=None,
        policy=p,
        registry=r,
    )
    c.source_kind = "MODEL_INFERRED"
    assert _override_allowed(context, existing_for(old_candidate)) is False


def test_rule_table_validation_rejects_inconsistent_tables(monkeypatch):
    original = list(rules.RULE_TABLE)
    duplicate = [*original, original[0]]
    monkeypatch.setattr(rules, "RULE_TABLE", duplicate)
    with pytest.raises(ValueError, match="duplicate rule id"):
        rules.validate_rule_table()
    monkeypatch.setattr(rules, "RULE_TABLE", list(reversed(original)))
    with pytest.raises(ValueError, match="ordered by id"):
        rules.validate_rule_table()
    same_priority = [
        rules.RuleSpec("R001", "GATE", 10, "ADD", "X"),
        rules.RuleSpec("R002", "GATE", 10, "ADD", "Y"),
    ]
    monkeypatch.setattr(rules, "RULE_TABLE", same_priority)
    with pytest.raises(ValueError, match="priorities must be unique"):
        rules.validate_rule_table()


# --- mutual exclusion semantics (doc 4.11) -------------------------------------------------


def _registry_with(exclusion, *, requires_authority=False, source_kinds=("USER_EXPLICIT", "SYSTEM_OF_RECORD")):
    registry = {
        "version": 1,
        "cardinality": "SINGLE_VALUE",
        "allowed_source_kinds": list(source_kinds),
        "requires_authority": requires_authority,
        "normalization_version": "norm-test",
        "validator": {"type": "object", "required": ["value"], "properties": {"value": {"type": "string"}}},
        "semantic_template": "communication.language = {value}; valid_from = {valid_from}; valid_to = {valid_to}",
    }
    if exclusion is not None:
        registry["mutual_exclusion"] = exclusion
    return registry


def _policy():
    return {
        "ignore_threshold": 50,
        "auto_accept_threshold": 70,
        "semantic_match_threshold": 0.82,
        "language_confidence_min": 0.8,
        "sensitive_policy": {},
    }


def _candidate(value, *, content="prefers a language"):
    return CandidateInput(
        customer_id="cust",
        source_system="test",
        source_event_id=str(uuid4()),
        source_refs=[{"ref_type": "message", "message_id": "m-1"}],
        memory_type="CUSTOMER_PROFILE",
        fact_key="communication.language",
        object={"value": value},
        content=content,
        content_language="zh-CN",
        source_kind="USER_EXPLICIT",
        observed_at=datetime.now(UTC),
        language_confidence=0.99,
    )


def test_mutual_exclusion_requires_two_distinct_declared_elements():
    registry = _registry_with([{"group": "language", "values": ["zh-CN", "en-US"]}])
    candidate, breakdown = normalize_candidate(_candidate("en-US"), registry, _policy(), "policy@1", "hash")
    same_value = existing_for(candidate).model_copy(update={"object_hash": "f" * 64, "object_json": {"value": "en-US"}})
    # Same canonical value (different object_hash) is the same element -> not exclusive.
    assert decide(candidate, breakdown, registry, _policy(), exact=[same_value]).primary_rule_id == "R076"

    current_zh, _ = normalize_candidate(_candidate("zh-CN"), registry, _policy(), "policy@1", "hash")
    uncovered = _registry_with([{"group": "language", "values": ["fr-FR"]}])
    assert (
        decide(candidate, breakdown, uncovered, _policy(), exact=[existing_for(current_zh)]).primary_rule_id == "R076"
    )

    wildcard = _registry_with([{"group": "client_version", "values": ["*"]}])
    assert decide(candidate, breakdown, wildcard, _policy(), exact=[existing_for(current_zh)]).primary_rule_id == "R080"


# --- ordered rule table: exhaustive combination assertions (doc 4.5) -----------------------

GRID = {
    "exact_kind": ("none", "same", "diff"),
    "overlap": (True, False),
    "new_evidence": (True, False),
    "cardinality": ("SINGLE_VALUE", "MULTI_VALUE", "TEMPORAL"),
    "exclusion": ("covering", "wildcard", "non_covering", "undeclared"),
    "requires_authority": (False, True),
    "old_source": ("USER_EXPLICIT", "SYSTEM_OF_RECORD"),
    "semantic": ("none", "hit"),
    "review_override": (False, True),
    "score": (90, 60, 40),
}

OUTCOME_BY_RULE = {
    "R040": "IGNORE",
    "R041": "NEEDS_VERIFICATION",
    "R050": "NOOP",
    "R060": "REINFORCE",
    "R070": "ADD",
    "R075": "ADD",
    "R076": "NEEDS_VERIFICATION",
    "R080": "SUPERSEDE",
    "R090": "ADD_TEMPORAL_VERSION",
    "R100": "CONFLICT",
    "R110": "NEEDS_VERIFICATION",
}


def build_grid_ctx(
    *,
    exact_kind="diff",
    overlap=True,
    new_evidence=True,
    cardinality="SINGLE_VALUE",
    exclusion="covering",
    requires_authority=False,
    old_source="USER_EXPLICIT",
    semantic="none",
    review_override=False,
    score=90,
):
    candidate, breakdown, _, _ = make_candidate("en-US")
    registry = _registry_with(
        {
            "covering": [{"group": "language", "values": ["zh-CN", "en-US"]}],
            "wildcard": [{"group": "language", "values": ["*"]}],
            "non_covering": [{"group": "language", "values": ["fr-FR"]}],
            "undeclared": None,
        }[exclusion],
        requires_authority=requires_authority,
    )
    registry["cardinality"] = cardinality
    candidate.cardinality = cardinality
    candidate.value_score = score
    old_candidate, _, _, _ = make_candidate("zh-CN")
    current = existing_for(old_candidate, source=old_source)
    current.valid_from = candidate.valid_from - timedelta(days=2)
    if overlap:
        current.valid_to = None
    else:
        current.valid_to = candidate.valid_from - timedelta(seconds=1)
    if exact_kind == "same":
        current = existing_for(candidate, source=old_source)
        current.valid_from = candidate.valid_from - timedelta(days=2)
        current.valid_to = None
    exact = [] if exact_kind == "none" else [current]
    semantic_candidates = [{"memory_id": uuid4(), "score": 0.91}] if semantic == "hit" else []
    return RuleContext(
        candidate=candidate,
        value_score=score,
        score_breakdown=breakdown,
        exact_matches=exact,
        has_new_evidence=new_evidence,
        new_evidence_languages=["en-US"] if new_evidence else [],
        semantic_candidates=semantic_candidates,
        authoritative_fact=None,
        policy=_policy(),
        registry=registry,
        review_override=review_override,
    )


def _selected_rule(ctx):
    """Doc 4.5: first match wins across phases; within the FACT phase the dimension split
    must be disjoint. GATE predicates may co-match but resolve deterministically by priority."""
    facts = rules.evaluate_facts(ctx)
    matched = [
        (rule.spec.priority, rule.spec.phase, rule.spec.rule_id)
        for rule in rules.DECISION_RULES
        if rule.matches(ctx, facts)
    ]
    assert matched
    fact_matches = [item for item in matched if item[1] == "FACT"]
    assert len(fact_matches) <= 1, matched
    gate_matches = [item for item in matched if item[1] == "GATE"]
    if gate_matches:
        return min(gate_matches)[2]
    assert len(fact_matches) == 1, matched
    return fact_matches[0][2]


def test_every_combination_hits_exactly_one_rule():
    from itertools import product

    seen = set()
    for combo in product(*GRID.values()):
        kwargs = dict(zip(GRID.keys(), combo, strict=True))
        ctx = build_grid_ctx(**kwargs)
        selected = _selected_rule(ctx)
        decision = rules.apply_decision_matrix(ctx)
        assert decision.primary_rule_id == selected, kwargs
        expected = OUTCOME_BY_RULE[selected]
        if selected == "R090" and kwargs["cardinality"] == "SINGLE_VALUE":
            # Doc 6.4.3 + ux_memory_single_value_active: a single-value slot cannot hold a second
            # ACTIVE row, so a value with a different window is a reviewed replacement.
            expected = "CONFLICT"
        assert decision.outcome == expected, (kwargs, decision.outcome)
        seen.add(selected)
    # Every FACT/GATE rule must be reachable from the documented dimension grid.
    assert {"R040", "R041", "R050", "R060", "R070", "R075", "R076", "R080", "R090", "R100", "R110"} <= seen


@given(
    exact_kind=st.sampled_from(GRID["exact_kind"]),
    overlap=st.booleans(),
    new_evidence=st.booleans(),
    cardinality=st.sampled_from(GRID["cardinality"]),
    exclusion=st.sampled_from(GRID["exclusion"]),
    requires_authority=st.booleans(),
    old_source=st.sampled_from(GRID["old_source"]),
    semantic=st.sampled_from(GRID["semantic"]),
    review_override=st.booleans(),
    score=st.sampled_from(GRID["score"]),
)
@settings(max_examples=400, deadline=None)
def test_rule_selection_is_total_and_single(
    exact_kind,
    overlap,
    new_evidence,
    cardinality,
    exclusion,
    requires_authority,
    old_source,
    semantic,
    review_override,
    score,
):
    ctx = build_grid_ctx(
        exact_kind=exact_kind,
        overlap=overlap,
        new_evidence=new_evidence,
        cardinality=cardinality,
        exclusion=exclusion,
        requires_authority=requires_authority,
        old_source=old_source,
        semantic=semantic,
        review_override=review_override,
        score=score,
    )
    selected = _selected_rule(ctx)
    assert rules.apply_decision_matrix(ctx).primary_rule_id == selected


def test_documented_boundaries():
    # "overlap + not mutually exclusive" must not hit R080/R100.
    assert rules.apply_decision_matrix(build_grid_ctx(exclusion="non_covering", overlap=True)).primary_rule_id == "R076"
    assert (
        rules.apply_decision_matrix(
            build_grid_ctx(exclusion="non_covering", overlap=True, cardinality="MULTI_VALUE")
        ).primary_rule_id
        == "R075"
    )
    # "non-overlap + explicit correction is irrelevant" -> R090.
    ctx = build_grid_ctx(overlap=False)
    ctx.candidate.explicit_correction = True
    assert rules.apply_decision_matrix(ctx).primary_rule_id == "R090"
    # Authoritative facts change the R080 sub-rule, not the outcome.
    ctx = replace(build_grid_ctx(), authoritative_fact={"object_hash": "a" * 64})
    decision = rules.apply_decision_matrix(ctx)
    assert (decision.primary_rule_id, decision.rule_trace[0]["sub_rule"]) == ("R080", "authoritative_changed")


def test_condition_hash_replays_and_snapshot_records_review_override():
    ctx = build_grid_ctx(exact_kind="none", score=60, review_override=True)
    first = rules.apply_decision_matrix(ctx)
    second = rules.apply_decision_matrix(ctx)
    assert first.primary_rule_id == "R070"
    assert first.rule_trace[0]["condition_hash"] == second.rule_trace[0]["condition_hash"]
    assert first.context_snapshot["review_override"] is True
    assert first.context_snapshot["condition_hash"] == first.rule_trace[0]["condition_hash"]
