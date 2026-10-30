from __future__ import annotations

import hashlib
import hmac
import json
import re
import unicodedata
from datetime import UTC, datetime
from typing import Any

_LANGUAGE_MAP = {
    "zh": "zh-CN",
    "zh_cn": "zh-CN",
    "zh-cn": "zh-CN",
    "chinese": "zh-CN",
    "中文": "zh-CN",
    "en": "en-US",
    "en_us": "en-US",
    "en-us": "en-US",
    "english": "en-US",
    "英文": "en-US",
    "es": "es-ES",
    "es_es": "es-ES",
    "es-es": "es-ES",
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_canonical(value: Any) -> str:
    return sha256_text(canonical_json(value))


def hmac_sha256_hex(key: str, value: str) -> str:
    return hmac.new(key.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()


def customer_hash(customer_id: str) -> str:
    """Doc 7.4: keyed audit hash; an unkeyed SHA-256 is dictionary-reversible."""
    from ..config import get_settings

    return hmac_sha256_hex(get_settings().customer_pepper, customer_id)


def customer_ref(customer_id: str) -> str:
    """Doc 7.9: Kafka pseudonym HMAC-SHA256(kafka_ref_key_vN, customer_id)."""
    from ..config import get_settings

    return hmac_sha256_hex(get_settings().kafka_ref_key, customer_id)


def utcnow() -> datetime:
    return datetime.now(UTC)


def normalize_language(value: str | None, default: str = "und") -> str:
    if not value:
        return default
    key = value.strip().replace("_", "-").lower()
    if key in _LANGUAGE_MAP:
        return _LANGUAGE_MAP[key]
    match = re.fullmatch(r"([a-z]{2,3})(?:-([a-z]{4}|[a-z]{2}|\d{3}))?", key)
    if not match:
        return default
    language, region = match.groups()
    if language == "zh" and not region:
        return "zh-CN"
    if language == "en" and not region:
        return "en-US"
    if language == "es" and not region:
        return "es-ES"
    if not region:
        return language
    # BCP 47: a 4-letter subtag is a script (Hant), other subtags are regions (GB).
    return f"{language}-{region.title() if len(region) == 4 else region.upper()}"


def detect_script(text: str) -> str | None:
    scripts = set()
    for char in text:
        name = unicodedata.name(char, "")
        for candidate in ("CJK", "HIRAGANA", "KATAKANA", "HANGUL", "CYRILLIC", "ARABIC", "LATIN"):
            if candidate in name:
                scripts.add(
                    "Han"
                    if candidate == "CJK"
                    else candidate.title().replace("Katakana", "Kana").replace("Hiragana", "Kana")
                )
                break
    return next(iter(sorted(scripts)), None) if scripts else None


def iso_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if value else None
