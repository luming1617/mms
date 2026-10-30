from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.unit

from mms.models import CandidateInput
from mms.services.normalizer import HardGateError, normalize_candidate, redact_text, score_candidate
from mms.utils.hashing import normalize_language, sha256_canonical


def registry():
    return {
        "version": 1,
        "cardinality": "SINGLE_VALUE",
        "validator": {"type": "object", "required": ["value"], "properties": {"value": {"type": "string"}}},
        "mutual_exclusion": [{"group": "language", "values": ["zh-CN", "en-US"]}],
        "semantic_template": "communication.language = {value}; valid_from = {valid_from}; valid_to = {valid_to}",
        "normalization_version": "norm-test",
        "allowed_source_kinds": ["USER_EXPLICIT"],
        "requires_authority": False,
    }


def policy():
    return {
        "ignore_threshold": 50,
        "auto_accept_threshold": 70,
        "semantic_match_threshold": 0.82,
        "language_confidence_min": 0.8,
        "sensitive_policy": {},
    }


def candidate(content="以后请用中文沟通", value="zh-CN"):
    return CandidateInput(
        customer_id="cust",
        source_system="test",
        source_event_id="evt",
        source_refs=[{"ref_type": "message", "message_id": "m-1"}],
        memory_type="CUSTOMER_PROFILE",
        fact_key="communication.language",
        object={"value": value},
        content=content,
        content_language="zh",
        source_kind="USER_EXPLICIT",
        observed_at=datetime.now(UTC),
        language_confidence=0.99,
    )


def test_language_aliases_and_stable_hashes():
    assert normalize_language("zh_cn") == "zh-CN"
    assert normalize_language("Chinese") == "zh-CN"
    assert sha256_canonical({"b": 1, "a": 2}) == sha256_canonical({"a": 2, "b": 1})


def test_normalizer_uses_registry_and_no_version_in_semantic_hash():
    normalized, breakdown = normalize_candidate(candidate(), registry(), policy(), "policy@1", "hash")
    assert normalized.content_language == "zh-CN"
    assert normalized.semantic_hash == sha256_canonical(
        {
            "fact_key": "communication.language",
            "entity_type": "",
            "entity_ref": "",
            "value_slot": "",
            "object_hash": normalized.object_hash,
            "valid_from": normalized.valid_from.isoformat(),
            "valid_to": None,
        }
    )
    assert "norm-test" not in normalized.semantic_hash
    assert normalized.value_score >= 70
    assert breakdown["source_trust"] == 18


def test_secret_hard_gate():
    with pytest.raises(HardGateError) as exc:
        normalize_candidate(candidate("我的 password 是 abc", "zh-CN"), registry(), policy(), "policy@1", "hash")
    assert exc.value.outcome == "REJECTED"


def test_registry_sensitivity_cannot_be_downgraded():
    sensitive_registry = registry() | {"sensitivity": "HIGH"}
    normalized, _ = normalize_candidate(candidate(), sensitive_registry, policy(), "policy@1", "hash")
    assert normalized.sensitivity == "HIGH"


def test_registry_schema_failure_is_a_rejected_hard_gate():
    with pytest.raises(HardGateError) as exc:
        normalize_candidate(candidate(value=None), registry(), policy(), "policy@1", "hash")
    assert exc.value.outcome == "REJECTED"
    assert exc.value.reason_codes == ["SCHEMA_VALIDATION_FAILED"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "und"),
        ("", "und"),
        ("zh", "zh-CN"),
        ("zh-Hant", "zh-Hant"),
        ("en-GB", "en-GB"),
        ("es", "es-ES"),
        ("fr-CA", "fr-CA"),
        ("not a language!", "und"),
    ],
)
def test_normalize_language_variants(value, expected):
    assert normalize_language(value) == expected


def test_score_candidate_records_confidence_multiplier_and_uses_weights():
    base_policy = policy()
    c = candidate()
    reference, _ = score_candidate(c, base_policy)
    # The seeded/default weights are an identity transform...
    assert score_candidate(c, {**base_policy, "scoring_weights": {}})[0] == reference
    # ...while overriding a weight rescales that component family.
    squeezed = score_candidate(c, {**base_policy, "scoring_weights": {"persistence": 0, "source_trust": 0}})[0]
    assert squeezed < reference
    confident = candidate().model_copy(update={"confidence": 0.5})
    scored, breakdown = score_candidate(confident, base_policy)
    assert breakdown["confidence_multiplier"] == 0.85
    assert breakdown["pre_confidence_score"] is not None
    assert scored == int(breakdown["pre_confidence_score"] * 0.85)


def test_redact_text_masks_secret_fragments():
    assert redact_text("my password is hunter2") == "my [REDACTED] is hunter2"
    assert redact_text("card 4111111111111111 ok") == "card [REDACTED] ok"
    assert redact_text("nothing sensitive") == "nothing sensitive"


def test_value_language_normalization_is_fact_key_scoped():
    # Doc 4.11: object_json canonicalization is per fact key. Only language-typed facts may
    # rewrite their value into a BCP-47 code; other keys must keep the canonical literal.
    other = candidate(value="en")
    other = other.model_copy(update={"fact_key": "product.client_version"})
    normalized, _ = normalize_candidate(other, registry(), policy(), "policy@1", "hash")
    assert normalized.object_json["value"] == "en"

    language = candidate(value="en")
    normalized, _ = normalize_candidate(language, registry(), policy(), "policy@1", "hash")
    assert normalized.object_json["value"] == "en-US"


def test_scoring_weight_signs_are_validated():
    from mms.services.repository import MemoryRepository

    MemoryRepository._validate_scoring_weights({"risk_penalty": -100, "persistence": 25})
    with pytest.raises(ValueError, match="risk_penalty"):
        MemoryRepository._validate_scoring_weights({"risk_penalty": 10})
    with pytest.raises(ValueError, match="persistence"):
        MemoryRepository._validate_scoring_weights({"persistence": -1})
    with pytest.raises(ValueError, match="numeric"):
        MemoryRepository._validate_scoring_weights({"persistence": "25"})


def _fingerprint(input_candidate):
    return normalize_candidate(input_candidate, registry(), policy(), "policy@1", "hash")[0].candidate_fingerprint


def test_candidate_fingerprint_survives_extractor_rewrite():
    """Docs 10.4: re-wording the same source fact must not mint a new idempotency identity."""
    assert _fingerprint(candidate("以后请用中文沟通")) == _fingerprint(candidate("客户明确表示希望使用中文"))


def test_two_facts_from_one_message_keep_distinct_fingerprints():
    """Docs 10.4: dropping the extracted text must not merge two facts of one message."""
    message_ref = [{"message_id": "m-1"}]
    language = candidate("以后请用中文沟通").model_copy(update={"source_refs": message_ref})
    channel = language.model_copy(update={"fact_key": "communication.channel"})
    assert _fingerprint(language) != _fingerprint(channel)


def test_candidate_fingerprint_keeps_facts_and_evidence_slots_distinct():
    base = candidate("以后请用中文沟通")
    assert _fingerprint(base.model_copy(update={"fact_key": "communication.channel"})) != _fingerprint(base)
    zero = base.model_copy(update={"source_refs": [{"message_id": "m-1", "ordinal": 0}]})
    one = base.model_copy(update={"source_refs": [{"message_id": "m-1", "ordinal": 1}]})
    assert _fingerprint(zero) != _fingerprint(one)
    assert _fingerprint(zero) != _fingerprint(base)


def test_delete_intent_patterns_ignore_ordinary_chat():
    from mms.services.normalizer import DELETE_PATTERNS

    ordinary = [
        "Don't forget to be kind to yourself; building connections takes time.",
        "I deleted my old photos last year.",
        "我忘记密码了 帮我重置",
        "删除好友后还能恢复吗",
    ]
    for text in ordinary:
        assert not any(pattern.search(text) for pattern in DELETE_PATTERNS), text
    for text in ("请删除我的所有信息", "不要记我的信息", "forget me", "Please delete my information"):
        assert any(pattern.search(text) for pattern in DELETE_PATTERNS), text


def test_semantic_template_tolerates_reserved_object_keys():
    reg = registry()
    item = candidate()
    item = item.model_copy(update={"object_json": {"value": "zh-CN", "valid_from": "caller-supplied"}})
    normalized, _ = normalize_candidate(item, reg, policy(), "policy@1", "hash")
    assert normalized.semantic_text.startswith("communication.language = zh-CN")
