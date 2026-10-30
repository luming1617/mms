from __future__ import annotations

import unicodedata

_LANGUAGE_SCRIPTS: dict[str, frozenset[str]] = {
    "zh": frozenset({"HAN"}),
    "ja": frozenset({"HAN", "KANA"}),
    "ko": frozenset({"HANGUL"}),
    "en": frozenset({"LATIN"}),
    "es": frozenset({"LATIN"}),
    "fr": frozenset({"LATIN"}),
    "de": frozenset({"LATIN"}),
    "pt": frozenset({"LATIN"}),
    "it": frozenset({"LATIN"}),
    "nl": frozenset({"LATIN"}),
    "tr": frozenset({"LATIN"}),
    "id": frozenset({"LATIN"}),
    "vi": frozenset({"LATIN"}),
    "ru": frozenset({"CYRILLIC"}),
    "uk": frozenset({"CYRILLIC"}),
    "ar": frozenset({"ARABIC"}),
    "th": frozenset({"THAI"}),
    "hi": frozenset({"DEVANAGARI"}),
}

_SCRIPT_PREFIXES = (
    ("CJK", "HAN"),
    ("HIRAGANA", "KANA"),
    ("KATAKANA", "KANA"),
    ("HANGUL", "HANGUL"),
    ("LATIN", "LATIN"),
    ("CYRILLIC", "CYRILLIC"),
    ("ARABIC", "ARABIC"),
    ("THAI", "THAI"),
    ("DEVANAGARI", "DEVANAGARI"),
)


def detect_script(text: str) -> str | None:
    counts: dict[str, int] = {}
    for char in text:
        if not char.isalpha():
            continue
        name = unicodedata.name(char, "")
        for prefix, script in _SCRIPT_PREFIXES:
            if name.startswith(prefix):
                counts[script] = counts.get(script, 0) + 1
                break
    if not counts:
        return None
    return max(counts, key=counts.__getitem__)


def query_language_confidence(text: str, language: str | None) -> float:
    """Doc 8.15 step 1: script-level detection. 1.0 matches, 0.0 mismatch, 0.5 unknown.

    Latin-script languages cannot be told apart by script alone, so en/es/fr/... all
    accept a Latin query; a CJK query declared as es-ES is the case this catches.
    """
    script = detect_script(text)
    if script is None:
        return 0.5
    base = (language or "").split("-")[0].lower()
    expected = _LANGUAGE_SCRIPTS.get(base)
    if not expected:
        return 0.5
    return 1.0 if script in expected else 0.0
