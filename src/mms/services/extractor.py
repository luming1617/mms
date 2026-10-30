from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Protocol
from uuid import uuid4

from ..models import CandidateInput
from ..utils.hashing import detect_script, normalize_language

CORRECTION_PATTERNS = [
    re.compile(r"(不是|不对|错了|改成|改为|我说的是|更正|纠正)"),
    re.compile(r"\b(actually|no,|i said|correction)\b", re.I),
]
LANG_PATTERNS = [
    (re.compile(r"(以后|之后|请|希望).{0,8}(中文|汉语|Chinese)", re.I), "zh-CN"),
    (re.compile(r"(以后|之后|请|希望).{0,8}(英文|英语|English)", re.I), "en-US"),
    (re.compile(r"(use|speak|reply in)\s+(chinese|mandarin)", re.I), "zh-CN"),
    (re.compile(r"(use|speak|reply in)\s+(english)", re.I), "en-US"),
]


class Extractor(Protocol):
    extractor_version: str

    def extract(
        self,
        *,
        customer_id: str,
        source_system: str,
        source_event_id: str,
        content: str,
        observed_at: datetime | None = None,
    ) -> list[CandidateInput]: ...


class RuleBasedExtractor:
    """Deterministic Phase 1 extractor.

    It deliberately extracts only high-precision language preference facts. LLM extraction can be
    added behind the same protocol after offline evaluation; LLM output must still pass contracts,
    hard gates, normalizer, policy and rules.
    """

    extractor_version = "rule-extractor-2026-10-26"

    def extract(
        self,
        *,
        customer_id: str,
        source_system: str,
        source_event_id: str,
        content: str,
        observed_at: datetime | None = None,
    ) -> list[CandidateInput]:
        observed = observed_at or datetime.now(UTC)
        candidates: list[CandidateInput] = []
        for pattern, language in LANG_PATTERNS:
            match = pattern.search(content)
            if not match:
                continue
            # content_language describes the language of the stored content (the matched
            # snippet), not the language the customer prefers — e.g. "use Chinese" is
            # English text expressing a zh-CN preference.
            script = detect_script(match.group(0))
            candidates.append(
                CandidateInput(
                    candidate_id=uuid4(),
                    customer_id=customer_id,
                    source_system=source_system,
                    source_event_id=source_event_id,
                    memory_type="CUSTOMER_PROFILE",
                    fact_key="communication.language",
                    subject="customer",
                    predicate="prefers_language",
                    object_json={"value": language},
                    content=match.group(0),
                    content_language="zh-CN" if script == "Han" else "en-US",
                    language=normalize_language(language),
                    language_confidence=0.99,
                    source_kind="USER_EXPLICIT",
                    source_refs=[{"source_event_id": source_event_id}],
                    confidence=0.99,
                    # Doc 4.10.6: only correction phrasing marks explicit_correction; a plain
                    # "please use Chinese from now on" is a preference, not a correction.
                    explicit_correction=any(pattern.search(content) for pattern in CORRECTION_PATTERNS),
                    observed_at=observed,
                )
            )
            break
        return candidates
