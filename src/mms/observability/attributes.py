"""Span/log attribute governance: allowlist of low-cardinality, non-sensitive keys (docs/2 §6.3/§10.1)."""

from __future__ import annotations

from typing import Any

ALLOWED_ATTRIBUTES = frozenset(
    {
        "mms.purpose",
        "mms.language",
        "mms.memory_type",
        "mms.decision.outcome",
        "mms.retrieval.mode",
        "mms.retrieval.results",
        "mms.retrieval.degraded",
        "mms.rerank.used",
        "mms.model.provider",
        "mms.model.name",
        "mms.job.status",
        "mms.operation",
        "mms.extraction.candidates",
        "mms.degraded_reason",
        "error.type",
    }
)

_MAX_VALUE_LENGTH = 128


def sanitize_attributes(attributes: dict[str, Any]) -> dict[str, Any]:
    """Drop non-allowlisted keys (customer_id/query/document ids and other high-cardinality or
    sensitive values must live in logs, not in span attributes) and bound value length."""
    sanitized: dict[str, Any] = {}
    for key, value in attributes.items():
        if key not in ALLOWED_ATTRIBUTES:
            continue
        if isinstance(value, str):
            value = value[:_MAX_VALUE_LENGTH]
        elif isinstance(value, (bool, int, float)):
            pass
        elif value is None:
            continue
        else:
            value = str(value)[:_MAX_VALUE_LENGTH]
        sanitized[key] = value
    return sanitized
