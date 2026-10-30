from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

from jsonschema import Draft202012Validator, ValidationError

from ..models import CandidateInput, NormalizedCandidate
from ..utils.hashing import (
    canonical_json,
    detect_script,
    iso_or_none,
    normalize_language,
    sha256_canonical,
    sha256_text,
)

logger = logging.getLogger(__name__)

SECRET_PATTERNS = [
    # ASCII keywords keep \b semantics via lookarounds; the lookarounds also let
    # CJK keywords (密码/密钥/验证码) match mid-CJK text, where \b never fires
    # because CJK characters are word characters in Unicode mode.
    re.compile(
        r"(?<![0-9a-zA-Z_])(?:password|passwd|api[_-]?key|token|secret|private[_ -]?key|验证码|密码|密钥)"
        r"(?![0-9a-zA-Z_])",
        re.I,
    ),
    re.compile(r"\b\d{13,19}\b"),
    re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)"),  # mainland China resident ID (doc 4.10.7)
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
]
INJECTION_PATTERNS = [
    re.compile(r"ignore (all|previous|prior) (instructions|rules)", re.I),
    re.compile(r"disregard (all|previous|prior) (instructions|rules)", re.I),
    re.compile(r"bypass (safety|policy|permissions|controls)", re.I),
    re.compile(r"忽略.*(规则|指令|提示词|要求)"),
    re.compile(r"( disregard|无视|跳过).*(system|系统).*(prompt|提示)", re.I),
    re.compile(r"system prompt", re.I),
    re.compile(r"you are now (a|an|the)\b", re.I),
    re.compile(r"(绕过|规避).*(规则|权限|风控|审批)"),
    re.compile(r"(直接|无条件|立即)(退款|转账|提权)"),
]
DELETE_PATTERNS = [
    # The verb must govern a memory/data object in the same clause. An unbounded
    # ".*" also fires on ordinary chat ("don't forget to ... time"), and a bare
    # "删除" fires on unrelated requests ("删除好友"), so both are excluded.
    re.compile(r"(不要记|别记(住)?|不要记录|别记录|不要保存|别保存)"),
    re.compile(r"(删除|清除|删掉|抹掉|忘掉|忘记)(我的|我)?(所有|全部)?(信息|记忆|数据|记录|资料|账号|账户|个人数据)"),
    re.compile(r"(忘掉|忘记|清除|删除)(我|关于我|关于我的|这些|这段|刚才的)"),
    re.compile(
        r"\b(forget|delete|erase|remove)\s+(me|my\s+(data|information|info|memory|memories|account|profile|records?|details))\b",
        re.I,
    ),
    re.compile(r"\b(do not|don't|never)\s+(remember|store|keep|retain|track)\s+(this|that|me|my)\b", re.I),
    re.compile(r"\b(stop|quit)\s+(remembering|storing|tracking)\b", re.I),
]
SENSITIVITY_RANK = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}


def enforce_registry_sensitivity(candidate: CandidateInput, registry: dict[str, Any]) -> CandidateInput:
    registry_sensitivity = registry.get("sensitivity", "LOW")
    if SENSITIVITY_RANK[registry_sensitivity] <= SENSITIVITY_RANK[candidate.sensitivity]:
        return candidate
    return candidate.model_copy(update={"sensitivity": registry_sensitivity})


# Kept aligned with doc 2.4 source trust ordering; also used by the R080 override rule.
SOURCE_SCORE = {
    "SYSTEM_OF_RECORD": 20,
    "USER_EXPLICIT": 18,
    "AGENT_CONFIRMED": 17,
    "USER_IMPLICIT": 10,
    "MODEL_INFERRED": 6,
    "KNOWLEDGE_RETRIEVED": 0,
    "ASSISTANT_GENERATED": 0,
}


class HardGateError(ValueError):
    def __init__(self, outcome: str, reason_codes: list[str]):
        super().__init__(",".join(reason_codes))
        self.outcome = outcome
        self.reason_codes = reason_codes


def redact_text(text: str) -> str:
    """Mask credential-like fragments before content is persisted to audit columns."""
    redacted = text
    for pattern in SECRET_PATTERNS:
        redacted = pattern.sub("[REDACTED]", redacted)
    return redacted


def _normalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key).strip(): _normalize_value(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    if isinstance(value, str):
        return " ".join(value.strip().split())
    return value


LANGUAGE_VALUE_FACT_KEYS = {"communication.language"}


def _language_value_domain(fact_key: str, registry: dict[str, Any]) -> bool:
    # Only language-typed facts may rewrite their `value` into a BCP-47 code; applying
    # normalize_language to every string would corrupt unrelated canonical values (4.11).
    return fact_key in LANGUAGE_VALUE_FACT_KEYS or registry.get("normalizer") == "language-v1"


def _normalize_object(object_json: dict[str, Any], *, language_domain: bool) -> dict[str, Any]:
    normalized: dict[str, Any] = _normalize_value(object_json)
    if language_domain and isinstance(normalized.get("value"), str):
        normalized["value"] = normalize_language(normalized["value"], default=normalized["value"])
    return normalized


def validate_object(schema: dict[str, Any], value: dict[str, Any]) -> None:
    Draft202012Validator(schema).validate(value)


def semantic_text_for(
    fact_key: str,
    object_json: dict[str, Any],
    valid_from: datetime | None,
    valid_to: datetime | None,
    template: str | None,
) -> str:
    if template:
        flattened = {
            key: (canonical_json(value) if isinstance(value, (dict, list)) else str(value))
            for key, value in object_json.items()
            if key not in {"valid_from", "valid_to"}
        }
        try:
            return template.format(valid_from=iso_or_none(valid_from), valid_to=iso_or_none(valid_to), **flattened)
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            # A malformed registry template must not make normalization (including the
            # for_rejection bookkeeping path) fail: keep the deterministic canonical form.
            logger.warning("semantic_template_invalid", extra={"error": exc.__class__.__name__})
    return canonical_json(
        {
            "fact_key": fact_key,
            "object": object_json,
            "valid_from": iso_or_none(valid_from),
            "valid_to": iso_or_none(valid_to),
        }
    )


def _scaled(base: int, default_max: int, configured_max: int) -> int:
    """Rescale a component family when policy overrides its ceiling (seed weights equal
    the defaults, so the default policy is an identity transform)."""
    if default_max == 0 or configured_max == default_max:
        return base
    return int(base * configured_max / default_max)


# Docs 10.4: within-event identity comes from the fact slot plus the evidence locator
# (span/ordinal), never from the extracted text. The fact slot stays in the key because this
# service's producers do not always send a span/ordinal, and dropping it would merge two
# different facts of one source event into a single (silently discarded) candidate.
_LOCATOR_KEYS = ("ref_type", "ref_id", "message_id", "ticket_id", "span", "ordinal", "index")


def stable_candidate_ref(candidate: CandidateInput) -> dict[str, Any]:
    locators = sorted(
        (
            {key: ref[key] for key in _LOCATOR_KEYS if ref.get(key) is not None}
            for ref in candidate.source_refs
            if isinstance(ref, dict)
        ),
        key=canonical_json,
    )
    ref: dict[str, Any] = {
        "fact_key": candidate.fact_key,
        "entity_type": candidate.entity_type,
        "entity_ref": candidate.entity_ref,
        "value_slot": candidate.value_slot,
    }
    if any(locators):
        ref["evidence_locators"] = [locator for locator in locators if locator]
    return ref


def score_candidate(candidate: CandidateInput, policy_config: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    weights = (policy_config or {}).get("scoring_weights") or {}
    content_length = len(candidate.content)
    persistence_base = (
        25
        if candidate.memory_type in {"CUSTOMER_PROFILE", "ACCOUNT_FACT"}
        else 18
        if candidate.memory_type != "EPHEMERAL_CONTEXT"
        else 5
    )
    explicitness_base = 20 if candidate.source_kind == "USER_EXPLICIT" else 15 if candidate.explicit_correction else 8
    future_value_base = 20 if candidate.memory_type in {"CUSTOMER_PROFILE", "SERVICE_HISTORY", "ACTIVE_ISSUE"} else 12
    source_trust_base = SOURCE_SCORE[candidate.source_kind]
    actionability_base = 15 if candidate.fact_key.startswith(("communication.", "product.", "issue.")) else 8
    risk_penalty_base = -100 if candidate.sensitivity == "HIGH" else -20 if candidate.sensitivity == "MEDIUM" else 0
    persistence = _scaled(persistence_base, 25, int(weights.get("persistence", 25)))
    explicitness = _scaled(explicitness_base, 20, int(weights.get("explicitness", 20)))
    future_value = _scaled(future_value_base, 20, int(weights.get("future_service_value", 20)))
    source_trust = _scaled(source_trust_base, 20, int(weights.get("source_trust", 20)))
    actionability = _scaled(actionability_base, 15, int(weights.get("actionability", 15)))
    risk_penalty = _scaled(risk_penalty_base, -100, int(weights.get("risk_penalty", -100)))
    score = persistence + explicitness + future_value + source_trust + actionability + risk_penalty
    breakdown: dict[str, Any] = {
        "persistence": persistence,
        "explicitness": explicitness,
        "future_service_value": future_value,
        "source_trust": source_trust,
        "actionability": actionability,
        "risk_penalty": risk_penalty,
    }
    if candidate.confidence is not None:
        multiplier = 0.7 + 0.3 * candidate.confidence
        breakdown["confidence_multiplier"] = round(multiplier, 6)
        breakdown["pre_confidence_score"] = score
        score = int(score * multiplier)
    if content_length < 3:
        score = min(score, 49)
    return max(0, min(100, score)), breakdown


def _has_evidence_locator(candidate: CandidateInput) -> bool:
    """Doc 4.1 #3: a candidate must point back at a server-verifiable spot in the source.

    ``source_event_id`` counts because the carrier event is itself server-verifiable and the
    current rule extractor records only that locator (4.12); once extraction carries message
    spans, the stricter message_id/span form becomes the norm.
    """
    locator_keys = (*_LOCATOR_KEYS, "source_event_id")
    return any(
        isinstance(ref, dict) and any(ref.get(key) is not None for key in locator_keys) for ref in candidate.source_refs
    )


def apply_hard_gates(candidate: CandidateInput) -> None:
    if not _has_evidence_locator(candidate):
        raise HardGateError("REJECTED", ["MISSING_EVIDENCE"])
    if any(pattern.search(candidate.content) for pattern in SECRET_PATTERNS):
        raise HardGateError("REJECTED", ["SENSITIVE_DATA"])
    if any(pattern.search(candidate.content) for pattern in INJECTION_PATTERNS):
        raise HardGateError("REJECTED", ["POLICY_FORBIDDEN"])
    if any(pattern.search(candidate.content) for pattern in DELETE_PATTERNS):
        raise HardGateError("DELETE_REQUESTED", ["DELETE_REQUESTED"])
    if candidate.source_kind in {"KNOWLEDGE_RETRIEVED", "ASSISTANT_GENERATED"}:
        raise HardGateError("REJECTED", ["SOURCE_UNTRUSTED"])


def normalize_candidate(
    candidate: CandidateInput,
    registry: dict[str, Any],
    policy: dict[str, Any],
    policy_version: str,
    policy_config_hash: str,
    *,
    for_rejection: bool = False,
) -> tuple[NormalizedCandidate, dict[str, Any]]:
    """Normalize a candidate into the decision-engine input.

    ``for_rejection=True`` computes the bookkeeping fields (hashes, score, fingerprint)
    without running any check that can raise (hard gates, und-language gate, schema
    validation). It is used to persist a terminal REJECTED/NEEDS_VERIFICATION decision
    audit trail for candidates that already failed an earlier gate or the consent
    check — a rejection must always be recordable, never retried (docs 2.2/2.6).
    """
    if not for_rejection:
        apply_hard_gates(candidate)
    candidate = enforce_registry_sensitivity(candidate, registry)
    content_language = normalize_language(candidate.content_language, "und")
    language = normalize_language(candidate.language or content_language, content_language)
    if content_language == "und" and not for_rejection:
        raise HardGateError("NEEDS_VERIFICATION", ["LANGUAGE_UNSUPPORTED_LEXICAL"])
    object_json = _normalize_object(
        candidate.object_json, language_domain=_language_value_domain(candidate.fact_key, registry)
    )
    if not for_rejection:
        try:
            validate_object(registry["validator"], object_json)
        except ValidationError as exc:
            raise HardGateError("REJECTED", ["SCHEMA_VALIDATION_FAILED"]) from exc
    object_hash = sha256_canonical(object_json)
    content_hash = sha256_text(candidate.content)
    valid_from = candidate.valid_from or candidate.observed_at
    valid_to = candidate.valid_to
    semantic_text = semantic_text_for(
        candidate.fact_key, object_json, valid_from, valid_to, registry.get("semantic_template")
    )
    semantic_hash = sha256_canonical(
        {
            "fact_key": candidate.fact_key,
            "entity_type": candidate.entity_type,
            "entity_ref": candidate.entity_ref,
            "value_slot": candidate.value_slot,
            "object_hash": object_hash,
            "valid_from": iso_or_none(valid_from),
            "valid_to": iso_or_none(valid_to),
        }
    )
    value_score, breakdown = score_candidate(candidate, policy)
    enriched = candidate.model_copy(
        update={
            "content_language": content_language,
            "language": language,
            "script": candidate.script or detect_script(candidate.content),
            "object_json": object_json,
            "valid_from": valid_from,
            "valid_to": valid_to,
            "policy_version": policy_version,
        }
    )
    # Doc 10.4: the idempotency identity must not move when the extractor re-wordings the same
    # fact, so content_hash is deliberately absent; text changes are fenced by the embedding
    # job generation instead (schema ux_embedding_job_active).
    fingerprint = sha256_canonical(
        {
            "source_system": candidate.source_system,
            "source_event_id": candidate.source_event_id,
            "stable_candidate_ref": stable_candidate_ref(candidate),
        }
    )
    normalized = NormalizedCandidate(
        **enriched.model_dump(),
        object_hash=object_hash,
        content_hash=content_hash,
        semantic_text=semantic_text,
        semantic_hash=semantic_hash,
        normalization_version=registry["normalization_version"],
        fact_key_registry_version=registry["version"],
        cardinality=registry["cardinality"],
        value_score=value_score,
        policy_config_hash=policy_config_hash,
        candidate_fingerprint=fingerprint,
    )
    return normalized, breakdown
