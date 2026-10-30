#!/usr/bin/env python3
"""Convert raw benchmark data into versioned sample case files.

Usage (run in an environment with the raw data downloaded):
    python evals/benchmarks/convert.py --data-dir evals/benchmarks/data \
        --longmemeval-s 1 --longmemeval-oracle 1 --locomo 2

Writes evals/benchmarks/samples/*.json plus a manifest.json capturing the
dataset metadata required by docs/4 §六 (hashes, benchmark versions, language
coverage, modality limitations). Sampling is deterministic (per-type median
selection) so regenerated samples stay byte-stable for the same raw input.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from evals.benchmarks import locomo_adapter, longmemeval_adapter

DATA_DIR = Path(__file__).resolve().parent / "data"
SAMPLES_DIR = Path(__file__).resolve().parent / "samples"


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def convert_longmemeval(data_dir: Path, file_name: str, per_type: int, out_name: str, out_dir: Path) -> dict[str, Any]:
    raw_path = data_dir / file_name
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    cases = [longmemeval_adapter.convert_question(question) for question in raw]
    sample = longmemeval_adapter.select_sample(cases, per_type=per_type)
    payload = {
        "metadata": longmemeval_adapter.sample_metadata(file_name, sample, dataset_hash=sha256_of(raw_path)),
        "cases": sample,
    }
    out = out_dir / out_name
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "file": str(out.relative_to(out.parents[2])),
        "kind": "qa",
        "dataset": file_name,
        "dataset_hash": payload["metadata"]["dataset_hash"],
        "case_count": len(sample),
        "turn_count": sum(case["turn_count"] for case in sample),
        "benchmark": f"{longmemeval_adapter.BENCHMARK_NAME} {longmemeval_adapter.BENCHMARK_VERSION}",
    }


def convert_locomo(data_dir: Path, qas_per_category: int, out_name: str, out_dir: Path) -> dict[str, Any]:
    raw_path = data_dir / "locomo10.json"
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    conversation = raw[0]
    sample = locomo_adapter.convert_conversation(conversation, qas_per_category=qas_per_category)
    payload = {
        "metadata": locomo_adapter.sample_metadata(sample, dataset_hash=sha256_of(raw_path)),
        "sample": sample,
    }
    out = out_dir / out_name
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "file": str(out.relative_to(out.parents[2])),
        "kind": "locomo",
        "dataset": "locomo10.json",
        "dataset_hash": payload["metadata"]["dataset_hash"],
        "conversation": sample["sample_id"],
        "session_count": len(sample["sessions"]),
        "turn_count": sample["turn_count"],
        "qa_count": len(sample["qas"]),
        "benchmark": f"{locomo_adapter.BENCHMARK_NAME} {locomo_adapter.BENCHMARK_VERSION}",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Convert raw benchmark data into versioned sample case files")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--samples-dir", type=Path, default=SAMPLES_DIR)
    parser.add_argument("--longmemeval-s", type=int, default=1, help="questions per type from the S regression set")
    parser.add_argument(
        "--longmemeval-oracle", type=int, default=0, help="questions per type from the oracle diagnostic set"
    )
    parser.add_argument("--locomo-qas", type=int, default=2, help="QAs per category from the first conversation")
    args = parser.parse_args(argv)

    out_dir = args.samples_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, Any]] = []
    if args.longmemeval_s > 0:
        entries.append(
            convert_longmemeval(
                args.data_dir, "longmemeval_s_cleaned.json", args.longmemeval_s, "longmemeval_s_sample.json", out_dir
            )
        )
    if args.longmemeval_oracle > 0:
        entries.append(
            convert_longmemeval(
                args.data_dir,
                "longmemeval_oracle.json",
                args.longmemeval_oracle,
                "longmemeval_oracle_sample.json",
                out_dir,
            )
        )
    if args.locomo_qas > 0:
        entries.append(convert_locomo(args.data_dir, args.locomo_qas, "locomo_sample.json", out_dir))
    if not entries:
        parser.error("nothing to convert: enable at least one source")

    manifest = {
        "schema_version": 1,
        "note": "reader/judge fingerprints are recorded per run in the eval report; raw data lives in evals/benchmarks/data (not versioned)",
        "datasets": entries,
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    for entry in entries:
        print(f"wrote {entry['file']}: {json.dumps(entry, ensure_ascii=False)}")
    print(f"manifest: {out_dir / 'manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
