"""Hypothesis generation (the "reader" in LongMemEval terms).

The reader is separate from the system under test and its identity is
recorded with every result (docs/4 rule four: reader fingerprints are fixed
at dataset level so score changes stay attributable).

Offline runs use a deterministic extractive proxy - the retrieved sentence
with the highest lexical overlap with the question - keeping the whole
pipeline reproducible without any API key. An LLM reader plugs into the same
seam (pass ``reader_model`` once a generation-capable client is configured);
until then it fails loudly instead of silently degrading the benchmark.
"""

from __future__ import annotations

import re
from typing import Any

TOKEN_RE = re.compile(r"[a-z0-9']+", re.IGNORECASE)
STOPWORDS = {
    "the",
    "a",
    "an",
    "is",
    "are",
    "was",
    "were",
    "do",
    "does",
    "did",
    "what",
    "which",
    "who",
    "whom",
    "when",
    "where",
    "why",
    "how",
    "i",
    "my",
    "me",
    "we",
    "our",
    "you",
    "your",
    "he",
    "she",
    "it",
    "they",
    "them",
    "his",
    "her",
    "its",
    "their",
    "of",
    "in",
    "on",
    "at",
    "to",
    "for",
    "and",
    "or",
    "that",
    "this",
    "have",
    "has",
    "had",
    "with",
    "about",
}
REFUSAL = "I don't have enough information to answer."
READER_PROXY_ID = "extractive-proxy-v1"


def content_tokens(text: str) -> set[str]:
    return {token.lower() for token in TOKEN_RE.findall(text) if token.lower() not in STOPWORDS and len(token) > 2}


def extractive_hypothesis(question: str, retrieved: list[str], *, top_k: int = 5) -> str:
    """Deterministic reader proxy: highest-overlap sentence from the top-k
    retrieved memories. Never sees the gold answer (rule two)."""
    question_tokens = content_tokens(question)
    if not question_tokens or not retrieved:
        return REFUSAL
    best_sentence = ""
    best_score = 0
    for content in retrieved[:top_k]:
        for sentence in re.split(r"(?<=[.!?])\s+", content):
            sentence = sentence.strip()
            if len(sentence) < 3:
                continue
            overlap = len(question_tokens & content_tokens(sentence))
            if overlap > best_score:
                best_score = overlap
                best_sentence = sentence
    return best_sentence if best_score > 0 else REFUSAL


def generate_hypothesis(
    question: str,
    retrieved: list[str],
    *,
    reader_model: str | None = None,
) -> dict[str, Any]:
    """Returns {hypothesis, reader}; the reader id is stored with every result."""
    if reader_model:
        raise NotImplementedError(
            "LLM reader requires a generation-capable client; wire it here and record its "
            "fingerprint in the dataset metadata before enabling (docs/4 rule four)"
        )
    return {"hypothesis": extractive_hypothesis(question, retrieved), "reader": READER_PROXY_ID}
