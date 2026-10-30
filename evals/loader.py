"""Golden dataset loading and governance validation.

Datasets live in evals/datasets/*.json as lists of DeepEval Goldens. Every
golden must carry auditable metadata (docs/1-模型质量评测体系设计.md §4.2):
case_id, category, risk_level, dataset_version, review_status and the
applicable metric names, plus the scenario fields the harness executes.
"""

from __future__ import annotations

import json
from pathlib import Path

from deepeval.dataset import EvaluationDataset, Golden

DATASET_DIR = Path(__file__).resolve().parent / "datasets"

REQUIRED_METADATA_FIELDS = (
    "case_id",
    "category",
    "risk_level",
    "dataset_version",
    "review_status",
    "scenario",
    "applicable_metrics",
)
KNOWN_SCENARIOS = {"retrieval", "ingestion_gate", "deletion_barrier"}
KNOWN_RISK_LEVELS = {"critical", "high", "medium", "low"}
KNOWN_METRICS = {
    "MemoryContextContract",
    "AnswerRelevancy",
    "AnswerCorrectness",
    "ContextualRelevancy",
    "ContextualRecall",
    "ContextualPrecision",
}


class DatasetValidationError(ValueError):
    pass


def _validate(golden: Golden, source: Path, seen_ids: set[str]) -> None:
    metadata = golden.additional_metadata or {}
    where = f"{source.name}:{golden.input!r}"
    if not isinstance(metadata, dict):
        raise DatasetValidationError(f"{where}: additional_metadata must be an object")
    missing = [field for field in REQUIRED_METADATA_FIELDS if not metadata.get(field)]
    if missing:
        raise DatasetValidationError(f"{where}: missing metadata fields {missing}")
    case_id = str(metadata["case_id"])
    if case_id in seen_ids:
        raise DatasetValidationError(f"{where}: duplicate case_id {case_id}")
    seen_ids.add(case_id)
    if metadata["scenario"] not in KNOWN_SCENARIOS:
        raise DatasetValidationError(f"{where}: unknown scenario {metadata['scenario']!r}")
    if metadata["risk_level"] not in KNOWN_RISK_LEVELS:
        raise DatasetValidationError(f"{where}: unknown risk_level {metadata['risk_level']!r}")
    unknown_metrics = set(metadata["applicable_metrics"]) - KNOWN_METRICS
    if unknown_metrics:
        raise DatasetValidationError(f"{where}: unknown applicable metrics {sorted(unknown_metrics)}")
    if metadata["review_status"] != "approved":
        raise DatasetValidationError(f"{where}: only approved goldens may run in the gate")
    if "MemoryContextContract" not in metadata["applicable_metrics"]:
        raise DatasetValidationError(f"{where}: every golden must declare the deterministic contract metric")
    if not golden.input or not golden.expected_output:
        raise DatasetValidationError(f"{where}: golden needs input and expected_output")


def load_dataset(path: Path | None = None) -> EvaluationDataset:
    source = Path(path) if path else DATASET_DIR / "core_goldens.json"
    if not source.is_file():
        raise DatasetValidationError(f"dataset file not found: {source}")
    raw = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise DatasetValidationError(f"dataset {source} must be a non-empty JSON array")
    goldens: list[Golden] = []
    seen_ids: set[str] = set()
    for item in raw:
        golden = Golden(**item)
        _validate(golden, source, seen_ids)
        goldens.append(golden)
    return EvaluationDataset(goldens=goldens)


def case_id(golden: Golden) -> str:
    metadata = golden.additional_metadata or {}
    return str(metadata.get("case_id", "case"))


def dataset_versions(dataset: EvaluationDataset) -> dict[str, str]:
    versions: dict[str, str] = {}
    for golden in dataset.goldens:
        metadata = golden.additional_metadata or {}
        versions[str(metadata.get("case_id", "case"))] = str(metadata.get("dataset_version", "unknown"))
    return versions
