#!/usr/bin/env python3
"""Download the raw public benchmark data (docs/4 §二/§三).

Files land in evals/benchmarks/data/ (gitignored; the versioned asset is the
converted sample under evals/benchmarks/samples/ plus its manifest hash).
The official HuggingFace source is tried first; hf-mirror.com is a fallback
for networks where huggingface.co is unreachable.

    python evals/benchmarks/download_data.py [--all]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import httpx

DATA_DIR = Path(__file__).resolve().parent / "data"

SOURCES: list[dict] = [
    {
        "name": "longmemeval_s_cleaned.json",
        "urls": [
            "https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_s_cleaned.json",
            "https://hf-mirror.com/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_s_cleaned.json",
        ],
        "about": "LongMemEval-S 主回归集 (500 questions)",
    },
    {
        "name": "longmemeval_oracle.json",
        "urls": [
            "https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_oracle.json",
            "https://hf-mirror.com/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_oracle.json",
        ],
        "about": "LongMemEval-Oracle 诊断集 (仅证据会话)",
    },
    {
        "name": "longmemeval_m_cleaned.json",
        "urls": [
            "https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_m_cleaned.json",
            "https://hf-mirror.com/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_m_cleaned.json",
        ],
        "about": "LongMemEval-M 历史规模压力集 (~2.7GB, --all only)",
        "optional": True,
    },
    {
        "name": "locomo10.json",
        "urls": [
            "https://github.com/snap-research/LoCoMo/raw/main/data/locomo10.json",
        ],
        "about": "LoCoMo 10 段超长对话",
    },
]


def download(source: dict, *, retries: int = 3) -> bool:
    target = DATA_DIR / source["name"]
    if target.is_file() and target.stat().st_size > 0:
        print(f"already present: {target}")
        return True
    for url in source["urls"]:
        for attempt in range(retries):
            try:
                with httpx.stream("GET", url, follow_redirects=True, timeout=120) as response:
                    if response.status_code != 200:
                        print(f"  {response.status_code} {url}")
                        break
                    with target.open("wb") as handle:
                        for chunk in response.iter_bytes(1024 * 1024):
                            handle.write(chunk)
                print(f"downloaded {target.name} ({target.stat().st_size} bytes) from {url}")
                return True
            except Exception as exc:
                print(f"  retry {attempt} ({type(exc).__name__}) {url}")
                time.sleep(2)
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download raw benchmark data")
    parser.add_argument("--all", action="store_true", help="include the 2.7GB LongMemEval-M stress set")
    args = parser.parse_args(argv)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    failed = []
    for source in SOURCES:
        if source.get("optional") and not args.all:
            print(f"skipped (use --all): {source['name']}")
            continue
        print(f"==> {source['name']}: {source['about']}")
        if not download(source):
            failed.append(source["name"])
    if failed:
        print(f"failed: {failed}", file=sys.stderr)
        return 1
    print("next: python evals/benchmarks/convert.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
