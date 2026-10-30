from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from ..models import Decision, ExistingFact, NormalizedCandidate
from ..utils.hashing import iso_or_none, sha256_canonical

# v2.8 backlog: mutual exclusion now compares both canonical values (doc 4.11), and the
# matrix is a real ordered rule table (doc 4.5) instead of an if/else ladder.
DECISION_RULE_SET_VERSION = "2026-10-30.1"
PHASE_ORDER = {"GATE": 0, "FACT": 1, "LIFECYCLE": 2}


@dataclass(frozen=True)
class RuleContext:
    candidate: NormalizedCandidate
    value_score: int
    score_breakdown: dict[str, Any]
    exact_matches: list[ExistingFact]
    has_new_evidence: bool
    new_evidence_languages: list[str]
    semantic_candidates: list[dict[str, Any]]
    authoritative_fact: dict[str, Any] | None
    policy: dict[str, Any]
    registry: dict[str, Any]
    review_override: bool = False


@dataclass(frozen=True)
class RuleSpec:
    rule_id: str
    phase: str
    priority: int
    outcome: str
    reason_code: str


RULE_TABLE = [
    RuleSpec("R001", "GATE", 1, "DELETE_REQUESTED", "DELETE_REQUESTED"),
    RuleSpec("R002", "GATE", 2, "REJECTED", "POLICY_FORBIDDEN"),
    RuleSpec("R003", "GATE", 3, "REJECTED", "SENSITIVE_DATA"),
    RuleSpec("R004", "GATE", 4, "REJECTED", "SCHEMA_VALIDATION_FAILED"),
    RuleSpec("R005", "GATE", 5, "REJECTED", "CONSENT_DENIED"),
    RuleSpec("R006", "GATE", 6, "REJECTED", "SOURCE_UNTRUSTED"),
    RuleSpec("R007", "GATE", 7, "REJECTED", "MISSING_EVIDENCE"),
    RuleSpec("R010", "GATE", 10, "REJECTED", "POLICY_FORBIDDEN"),
    RuleSpec("R020", "GATE", 20, "NEEDS_VERIFICATION", "SOURCE_UNTRUSTED"),
    RuleSpec("R021", "GATE", 21, "NEEDS_VERIFICATION", "IDENTITY_UNVERIFIED"),
    RuleSpec("R030", "GATE", 30, "NEEDS_VERIFICATION", "LANGUAGE_UNSUPPORTED_LEXICAL"),
    RuleSpec("R040", "GATE", 40, "IGNORE", "LOW_MEMORY_VALUE"),
    RuleSpec("R041", "GATE", 41, "NEEDS_VERIFICATION", "BELOW_AUTO_ACCEPT_THRESHOLD"),
    RuleSpec("R050", "FACT", 50, "NOOP", "SAME_FACT_SAME_VALUE"),
    RuleSpec("R060", "FACT", 60, "REINFORCE", "SAME_FACT_NEW_EVIDENCE"),
    RuleSpec("R070", "FACT", 70, "ADD", "NO_EXISTING_FACT"),
    RuleSpec("R075", "FACT", 75, "ADD", "MULTI_VALUE_COEXIST"),
    RuleSpec("R076", "FACT", 76, "NEEDS_VERIFICATION", "REGISTRY_EXCLUSION_UNDECLARED"),
    RuleSpec("R080", "FACT", 80, "SUPERSEDE", "EXPLICIT_NEWER_USER_CORRECTION"),
    RuleSpec("R090", "FACT", 90, "ADD_TEMPORAL_VERSION", "TEMPORAL_NON_OVERLAP"),
    RuleSpec("R100", "FACT", 100, "CONFLICT", "CONFLICTING_VALUES"),
    RuleSpec("R110", "FACT", 110, "NEEDS_VERIFICATION", "SEMANTIC_MATCH_ONLY"),
]

RULES_BY_ID = {rule.rule_id: rule for rule in RULE_TABLE}
# R001-R007 are enforced upstream of the decision matrix (Normalizer hard gates plus the
# write-side consent check). They stay in the table for audit attribution: the hard-gate path
# records the rule id that matches its reason code instead of labelling every terminal REJECTED
# as R010 (docs 5.3, 6.5.1).
PRE_MATRIX_RULE_IDS = {"R001", "R002", "R003", "R004", "R005", "R006", "R007"}


class DecisionNotCovered(RuntimeError):
    pass


def _overlaps(a_from: datetime | None, a_to: datetime | None, b_from: datetime | None, b_to: datetime | None) -> bool:
    def aware(value: datetime | None, fallback: datetime) -> datetime:
        if value is None:
            return fallback
        return value if value.tzinfo else value.replace(tzinfo=fallback.tzinfo)

    lower = datetime.min.replace(tzinfo=datetime.now().astimezone().tzinfo)
    upper = datetime.max.replace(tzinfo=lower.tzinfo)
    a_start = aware(a_from, lower)
    b_start = aware(b_from, lower)
    a_end = aware(a_to, upper)
    b_end = aware(b_to, upper)
    return a_start < b_end and b_start < a_end


def _canonical_value(object_json: dict[str, Any] | None) -> str:
    return str((object_json or {}).get("value", ""))


def _mutual_exclusion(ctx: RuleContext, current: ExistingFact, overlap: bool) -> bool:
    """Doc 4.11: exclusion holds only when both canonical values sit on different
    elements of the same declared group. A `*` group expresses an unbounded value
    domain, so any two *distinct* values are different elements of that group."""
    if ctx.candidate.cardinality == "MULTI_VALUE":
        return False
    if ctx.candidate.cardinality == "TEMPORAL":
        return overlap
    groups = ctx.registry.get("mutual_exclusion") or []
    candidate_value = _canonical_value(ctx.candidate.object_json)
    current_value = _canonical_value(current.object_json)
    if candidate_value == current_value:
        return False
    for group in groups:
        values = {str(value) for value in group.get("values", [])}
        if "*" in values:
            return True
        if candidate_value in values and current_value in values:
            return True
    return False


def _override_allowed(ctx: RuleContext, current: ExistingFact) -> bool:
    if ctx.candidate.source_kind not in (ctx.registry.get("allowed_source_kinds") or []):
        return False
    if not ctx.registry.get("requires_authority"):
        return True
    rank = {
        "ASSISTANT_GENERATED": 0,
        "KNOWLEDGE_RETRIEVED": 0,
        "MODEL_INFERRED": 1,
        "USER_IMPLICIT": 2,
        "USER_EXPLICIT": 3,
        "AGENT_CONFIRMED": 4,
        "SYSTEM_OF_RECORD": 5,
    }
    return rank[ctx.candidate.source_kind] > rank[current.source_kind]


@dataclass(frozen=True)
class EvaluatedFacts:
    current: ExistingFact | None
    overlap: bool
    mutually_exclusive: bool
    override_allowed: bool
    semantic_hits: list[dict[str, Any]]


def _select_current(ctx: RuleContext) -> ExistingFact | None:
    matches = ctx.exact_matches
    if not matches:
        return None
    if ctx.candidate.cardinality == "TEMPORAL":
        return next(
            (
                item
                for item in matches
                if _overlaps(ctx.candidate.valid_from, ctx.candidate.valid_to, item.valid_from, item.valid_to)
            ),
            matches[0],
        )
    return next((item for item in matches if item.object_hash == ctx.candidate.object_hash), matches[0])


def evaluate_facts(ctx: RuleContext) -> EvaluatedFacts:
    current = _select_current(ctx)
    if current is None:
        overlap = mutually_exclusive = override_allowed = False
    else:
        changed = current.object_hash != ctx.candidate.object_hash
        overlap = bool(
            changed
            and _overlaps(ctx.candidate.valid_from, ctx.candidate.valid_to, current.valid_from, current.valid_to)
        )
        mutually_exclusive = bool(changed and overlap and _mutual_exclusion(ctx, current, overlap))
        override_allowed = bool(mutually_exclusive and _override_allowed(ctx, current))
    threshold = float(ctx.policy["semantic_match_threshold"])
    semantic_hits = [item for item in ctx.semantic_candidates if float(item.get("score", 0.0)) >= threshold]
    return EvaluatedFacts(current, overlap, mutually_exclusive, override_allowed, semantic_hits)


def _candidate_views(ctx: RuleContext) -> list[dict[str, Any]]:
    return [
        {"view_kind": "ORIGINAL", "view_language": ctx.candidate.content_language, "action": "UPSERT"},
        {"view_kind": "CANONICAL", "view_language": "und", "action": "UPSERT"},
    ]


def _condition_inputs(ctx: RuleContext, facts: EvaluatedFacts, sub_rule: str | None) -> dict[str, Any]:
    """Full decision inputs so the same context_snapshot + rule set replays identically
    (doc 4.5). Anything that can flip a branch must be in here."""
    return {
        "candidate_fingerprint": ctx.candidate.candidate_fingerprint,
        "candidate_object_hash": ctx.candidate.object_hash,
        "candidate_semantic_hash": ctx.candidate.semantic_hash,
        "normalization_version": ctx.candidate.normalization_version,
        "cardinality": ctx.candidate.cardinality,
        "source_kind": ctx.candidate.source_kind,
        "sensitivity": ctx.candidate.sensitivity,
        "language_confidence": ctx.candidate.language_confidence,
        "value_score": ctx.value_score,
        "policy_config_hash": ctx.candidate.policy_config_hash,
        "registry_version": ctx.candidate.fact_key_registry_version,
        "exact_matches": [
            {
                "memory_id": str(item.memory_id),
                "version_no": item.version_no,
                "object_hash": item.object_hash,
                "lifecycle_status": item.lifecycle_status,
                "source_kind": item.source_kind,
                "valid_from": iso_or_none(item.valid_from),
                "valid_to": iso_or_none(item.valid_to),
            }
            for item in ctx.exact_matches
        ],
        "has_new_evidence": ctx.has_new_evidence,
        "new_evidence_languages": sorted(ctx.new_evidence_languages),
        "overlap": facts.overlap,
        "mutually_exclusive": facts.mutually_exclusive,
        "override_allowed": facts.override_allowed,
        "semantic_hits": [
            {"memory_id": str(item["memory_id"]), "score": round(float(item["score"]), 6)}
            for item in facts.semantic_hits
        ],
        "authoritative_object_hash": (ctx.authoritative_fact or {}).get("object_hash"),
        "review_override": ctx.review_override,
        "sub_rule": sub_rule,
    }


def _emit(
    ctx: RuleContext,
    facts: EvaluatedFacts,
    spec: RuleSpec,
    *,
    outcome: str | None = None,
    reason_codes: list[str] | None = None,
    embedding_action: str = "NOOP",
    view_actions: list[dict[str, Any]] | None = None,
    target: ExistingFact | None = None,
    target_memory_id: UUID | None = None,
    requires_review: bool = False,
    sub_rule: str | None = None,
) -> Decision:
    condition_inputs = _condition_inputs(ctx, facts, sub_rule)
    trace = [
        {
            "rule_id": spec.rule_id,
            "priority": spec.priority,
            "condition_hash": sha256_canonical(condition_inputs),
            "outcome": outcome or spec.outcome,
            "sub_rule": sub_rule,
        }
    ]
    snapshot_matches = [
        {
            "memory_id": str(item.memory_id),
            "version_no": item.version_no,
            "content_hash": item.content_hash,
            "semantic_hash": item.semantic_hash,
            "object_hash": item.object_hash,
            "lifecycle_status": item.lifecycle_status,
            "valid_from": iso_or_none(item.valid_from),
            "valid_to": iso_or_none(item.valid_to),
        }
        for item in ctx.exact_matches
    ]
    return Decision(
        candidate_id=ctx.candidate.candidate_id,
        outcome=(outcome or spec.outcome),  # type: ignore[arg-type]  # rule table holds Outcome literals
        reason_codes=reason_codes or [spec.reason_code],
        rule_trace=trace,
        primary_rule_id=spec.rule_id,
        target_memory_id=target.memory_id if target else target_memory_id,
        matched_memory_ids=[item.memory_id for item in ctx.exact_matches],
        expected_versions={str(target.memory_id): target.version_no} if target else {},
        policy_version=ctx.candidate.policy_version,
        fact_key_registry_version=ctx.candidate.fact_key_registry_version,
        engine_version=DECISION_RULE_SET_VERSION,
        semantic_hash=ctx.candidate.semantic_hash,
        normalization_version=ctx.candidate.normalization_version,
        score_breakdown=ctx.score_breakdown,
        context_snapshot={
            "rule_set_version": DECISION_RULE_SET_VERSION,
            "candidate": {
                "object_hash": ctx.candidate.object_hash,
                "semantic_hash": ctx.candidate.semantic_hash,
                "normalization_version": ctx.candidate.normalization_version,
            },
            "policy": {"version": ctx.candidate.policy_version, "config_hash": ctx.candidate.policy_config_hash},
            "registry_version": ctx.candidate.fact_key_registry_version,
            "matches": snapshot_matches,
            "semantic_candidates": [
                {"memory_id": str(item["memory_id"]), "score": round(float(item["score"]), 6)}
                for item in facts.semantic_hits
            ],
            "authoritative": ctx.authoritative_fact,
            "review_override": ctx.review_override,
            "condition_hash": trace[0]["condition_hash"],
        },
        requires_review=requires_review,
        embedding_action=embedding_action,  # type: ignore[arg-type]  # call sites pass Decision literals
        view_actions=view_actions or [],
    )


RuleFn = Callable[[RuleContext, EvaluatedFacts], bool]
EvalFn = Callable[[RuleContext, EvaluatedFacts], Decision]


@dataclass(frozen=True)
class DecisionRule:
    spec: RuleSpec
    matches: RuleFn
    evaluate: EvalFn


def _gate_sensitive(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return ctx.candidate.sensitivity == "HIGH" and not ctx.policy.get("sensitive_policy", {}).get("high_allowed", False)


def _gate_source(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return ctx.candidate.source_kind not in (ctx.registry.get("allowed_source_kinds") or [])


def _gate_language(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return (ctx.candidate.language_confidence or 0.0) < float(ctx.policy.get("language_confidence_min", 0.8))


def _gate_identity(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    """Doc 4.5/5.3: an entity reference must be confirmed by the field's authority system.

    Only fields that declare an ``authority_source`` are affected. MMS cannot verify that the
    entity belongs to this customer on its own, so a non-authoritative source goes to review
    (``IDENTITY_UNVERIFIED``) instead of being written automatically; a reviewer can still
    approve it once the business system has confirmed the entity.
    """
    return (
        not ctx.review_override
        and bool(ctx.candidate.entity_ref)
        and bool(ctx.registry.get("authority_source"))
        and ctx.candidate.source_kind != "SYSTEM_OF_RECORD"
    )


def _gate_ignore(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return ctx.value_score < int(ctx.policy["ignore_threshold"])


def _gate_review(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    if ctx.review_override:
        return False
    below_auto = ctx.value_score < int(ctx.policy["auto_accept_threshold"])
    auto_write_disabled = not ctx.registry.get("auto_write", True) or not ctx.policy.get("auto_write", True)
    return below_auto or auto_write_disabled


def _same_value(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return facts.current is not None and facts.current.object_hash == ctx.candidate.object_hash


def _changed_value(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return facts.current is not None and facts.current.object_hash != ctx.candidate.object_hash


def _fact_same_no_evidence(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return _same_value(ctx, facts) and not ctx.has_new_evidence


def _fact_same_with_evidence(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return _same_value(ctx, facts) and ctx.has_new_evidence


def _fact_no_match_no_semantic(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return facts.current is None and not (facts.semantic_hits and not ctx.review_override)


def _fact_multi_coexist(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return (
        _changed_value(ctx, facts)
        and facts.overlap
        and not facts.mutually_exclusive
        and ctx.candidate.cardinality == "MULTI_VALUE"
    )


def _fact_exclusion_undeclared(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return (
        _changed_value(ctx, facts)
        and facts.overlap
        and not facts.mutually_exclusive
        and ctx.candidate.cardinality != "MULTI_VALUE"
    )


def _fact_override(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return (
        _changed_value(ctx, facts)
        and facts.overlap
        and facts.mutually_exclusive
        and (facts.override_allowed or ctx.review_override)
    )


def _fact_temporal_non_overlap(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return _changed_value(ctx, facts) and not facts.overlap


def _fact_conflict(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return (
        _changed_value(ctx, facts)
        and facts.overlap
        and facts.mutually_exclusive
        and not facts.override_allowed
        and not ctx.review_override
    )


def _fact_semantic_only(ctx: RuleContext, facts: EvaluatedFacts) -> bool:
    return facts.current is None and bool(facts.semantic_hits) and not ctx.review_override


def _eval_r010(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    return _emit(ctx, facts, RULES_BY_ID["R010"], reason_codes=["SENSITIVE_DATA"])


def _eval_r020(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    return _emit(ctx, facts, RULES_BY_ID["R020"], reason_codes=["SOURCE_UNTRUSTED"], requires_review=True)


def _eval_r021(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    return _emit(ctx, facts, RULES_BY_ID["R021"], requires_review=True)


def _eval_r030(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    return _emit(ctx, facts, RULES_BY_ID["R030"], requires_review=True)


def _eval_r040(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    return _emit(ctx, facts, RULES_BY_ID["R040"])


def _eval_r041(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    below_auto = ctx.value_score < int(ctx.policy["auto_accept_threshold"])
    reason_code = "BELOW_AUTO_ACCEPT_THRESHOLD" if below_auto else "AUTO_WRITE_DISABLED"
    return _emit(ctx, facts, RULES_BY_ID["R041"], reason_codes=[reason_code], requires_review=True)


def _eval_r050(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    return _emit(ctx, facts, RULES_BY_ID["R050"], target=facts.current)


def _eval_r060(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    languages = sorted(set(ctx.new_evidence_languages))
    return _emit(
        ctx,
        facts,
        RULES_BY_ID["R060"],
        embedding_action="UPSERT_VIEWS" if languages else "NOOP",
        view_actions=[
            {"view_kind": "ORIGINAL", "view_language": language, "action": "UPSERT"} for language in languages
        ],
        target=facts.current,
        sub_rule="new_evidence_language" if languages else "new_evidence_same_language",
    )


def _eval_r070(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    return _emit(ctx, facts, RULES_BY_ID["R070"], embedding_action="UPSERT", view_actions=_candidate_views(ctx))


def _eval_r075(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    return _emit(
        ctx,
        facts,
        RULES_BY_ID["R075"],
        embedding_action="UPSERT",
        view_actions=_candidate_views(ctx),
        target=facts.current,
    )


def _eval_r076(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    return _emit(ctx, facts, RULES_BY_ID["R076"], target=facts.current, requires_review=True)


def _eval_r080(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    authoritative_changed = bool(
        ctx.authoritative_fact
        and ctx.authoritative_fact.get("object_hash") != (facts.current.object_hash if facts.current else None)
    )
    return _emit(
        ctx,
        facts,
        RULES_BY_ID["R080"],
        reason_codes=["AUTHORITATIVE_SOURCE_CHANGED" if authoritative_changed else "EXPLICIT_NEWER_USER_CORRECTION"],
        embedding_action="UPSERT_NEW_AND_DELETE_OLD",
        view_actions=_candidate_views(ctx),
        target=facts.current,
        sub_rule="authoritative_changed" if authoritative_changed else "explicit_statement",
    )


def _eval_r090(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    if ctx.candidate.cardinality == "SINGLE_VALUE":
        # Doc 6.4.3 + ux_memory_single_value_active: a single-value slot cannot hold two ACTIVE
        # rows, so a value with a different window is a reviewed replacement, never a temporal
        # sibling. Without this the insert died on the unique index instead of reaching a person.
        return _emit(
            ctx,
            facts,
            RULES_BY_ID["R090"],
            outcome="CONFLICT",
            target=facts.current,
            requires_review=True,
            sub_rule="single_value_conflict",
        )
    return _emit(
        ctx,
        facts,
        RULES_BY_ID["R090"],
        embedding_action="UPSERT",
        view_actions=_candidate_views(ctx),
        target=facts.current,
    )


def _eval_r100(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    return _emit(ctx, facts, RULES_BY_ID["R100"], target=facts.current, requires_review=True)


def _eval_r110(ctx: RuleContext, facts: EvaluatedFacts) -> Decision:
    # Doc 4.5: the policy switch decides verify vs ignore deterministically.
    if str(ctx.policy.get("semantic_match_policy", "verify")) == "ignore":
        return _emit(
            ctx,
            facts,
            RULES_BY_ID["R110"],
            outcome="CONFLICT",
            target_memory_id=facts.semantic_hits[0]["memory_id"],
            requires_review=True,
            sub_rule="policy_ignore",
        )
    return _emit(ctx, facts, RULES_BY_ID["R110"], requires_review=True, sub_rule="policy_verify")


def _rule(rule_id: str, matches: RuleFn, evaluate: EvalFn) -> DecisionRule:
    return DecisionRule(RULES_BY_ID[rule_id], matches, evaluate)


DECISION_RULES = [
    _rule("R010", _gate_sensitive, _eval_r010),
    _rule("R020", _gate_source, _eval_r020),
    _rule("R021", _gate_identity, _eval_r021),
    _rule("R030", _gate_language, _eval_r030),
    _rule("R040", _gate_ignore, _eval_r040),
    _rule("R041", _gate_review, _eval_r041),
    _rule("R050", _fact_same_no_evidence, _eval_r050),
    _rule("R060", _fact_same_with_evidence, _eval_r060),
    _rule("R070", _fact_no_match_no_semantic, _eval_r070),
    _rule("R075", _fact_multi_coexist, _eval_r075),
    _rule("R076", _fact_exclusion_undeclared, _eval_r076),
    _rule("R080", _fact_override, _eval_r080),
    _rule("R090", _fact_temporal_non_overlap, _eval_r090),
    _rule("R100", _fact_conflict, _eval_r100),
    _rule("R110", _fact_semantic_only, _eval_r110),
]


def apply_decision_matrix(ctx: RuleContext) -> Decision:
    facts = evaluate_facts(ctx)
    for rule in DECISION_RULES:
        if rule.matches(ctx, facts):
            return rule.evaluate(ctx, facts)
    raise DecisionNotCovered(f"no rule matched; rule_set={DECISION_RULE_SET_VERSION}")


def validate_rule_table() -> None:
    ids = [rule.rule_id for rule in RULE_TABLE]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate rule id")
    if ids != sorted(ids):
        raise ValueError("rules must remain stable and ordered by id")
    priorities = [rule.priority for rule in RULE_TABLE]
    if len(priorities) != len(set(priorities)):
        raise ValueError("rule priorities must be unique")
    phases = [PHASE_ORDER[rule.phase] for rule in RULE_TABLE]
    if phases != sorted(phases):
        raise ValueError("rule phases must follow GATE -> FACT -> LIFECYCLE")
    implemented = [rule.spec.rule_id for rule in DECISION_RULES]
    if len(implemented) != len(set(implemented)):
        raise ValueError("duplicate decision rule implementation")
    for rule in DECISION_RULES:
        spec = RULES_BY_ID.get(rule.spec.rule_id)
        if spec is None:
            raise ValueError(f"decision rule {rule.spec.rule_id} is not declared in the rule table")
    missing = [
        rule.rule_id
        for rule in RULE_TABLE
        if rule.rule_id not in PRE_MATRIX_RULE_IDS and rule.rule_id not in implemented
    ]
    if missing:
        raise ValueError(f"rule table entries without implementation: {missing}")


validate_rule_table()
