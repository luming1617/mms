from __future__ import annotations

import math

# CJK ideographs/kana/hangul: roughly one token per character. Latin/Cyrillic/Arabic
# text averages about four characters per token under BGE-M3/e5 tokenizers.
_CJK_RANGES = (
    (0x3040, 0x30FF),
    (0x3400, 0x4DBF),
    (0x4E00, 0x9FFF),
    (0xAC00, 0xD7AF),
    (0xF900, 0xFAFF),
)


def _is_cjk(char: str) -> bool:
    code = ord(char)
    return any(start <= code <= end for start, end in _CJK_RANGES)


def estimate_tokens(text: str) -> int:
    """Doc 9.2: max_tokens is a context budget, so size by tokens, not characters."""
    if not text:
        return 0
    cjk = sum(1 for char in text if _is_cjk(char))
    rest = len(text) - cjk
    if rest == 0:
        return cjk
    return cjk + max(1, math.ceil(rest / 4))
