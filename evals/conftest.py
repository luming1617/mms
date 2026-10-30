import hashlib
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

DSN = os.environ.get("MMS_TEST_DATABASE_URL")
requires_db = pytest.mark.skipif(not DSN, reason="set MMS_TEST_DATABASE_URL for DeepEval evaluation tests")
if DSN:
    os.environ.setdefault("MMS_DATABASE_URL", DSN)

from evals.judge import JudgeConfig
from evals.loader import load_dataset

_DATASET_FILES = (
    "core_goldens.json",
    "rag_cases.json",
    "security_cases.json",
    "regression_cases.json",
)


def dataset_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    for name in _DATASET_FILES:
        dataset = load_dataset(Path(__file__).resolve().parent / "datasets" / name)
        for golden in dataset.goldens:
            metadata = golden.additional_metadata or {}
            versions[str(metadata.get("case_id", "case"))] = str(metadata.get("dataset_version", "unknown"))
    return versions


def dataset_hashes() -> dict[str, str]:
    """SHA256 of every versioned evaluation asset used by a run.

    Covers golden datasets, lifecycle case files and the benchmark samples
    (which embed the raw-dataset hashes and conversion metadata), so any
    dataset edit changes the recorded hash even if case ids stay stable.
    """
    hashed: dict[str, str] = {}
    asset_globs = (
        ROOT / "evals" / "datasets" / "*.json",
        ROOT / "evals" / "lifecycle" / "cases" / "*.jsonl",
        ROOT / "evals" / "benchmarks" / "samples" / "*.json",
    )
    for pattern in asset_globs:
        for path in sorted(pattern.parent.glob(pattern.name)):
            hashed[path.relative_to(ROOT).as_posix()] = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    return hashed


@pytest.fixture(scope="session")
def judge() -> JudgeConfig:
    config = JudgeConfig.from_env()
    config.ensure_usable(purpose="the evaluation suite")
    return config


@pytest.fixture(scope="session")
def target():
    from evals.harness import EvalTarget

    return EvalTarget()


@pytest.fixture(scope="session")
def recorder(judge: JudgeConfig):
    from evals.benchmarks.reader import READER_PROXY_ID
    from evals.judge import judge_fingerprint
    from evals.recorder import EvalRecorder

    recorder_ = EvalRecorder(
        judge_model=judge.model if judge.available else None,
        judge_model_fingerprint=judge_fingerprint(judge.model),
        reader_model_fingerprint=READER_PROXY_ID,
        dataset_versions=dataset_versions(),
        dataset_hashes=dataset_hashes(),
    )
    yield recorder_
    report = recorder_.finish()
    print(
        f"\nevaluation report: {recorder_.report_dir / 'eval-results.json'} "
        f"(pass_rate={report['summary']['pass_rate']}, judge={report['summary']['judge_model']})"
    )


def golden_ids(dataset):
    from evals.loader import case_id

    return [case_id(golden) for golden in dataset.goldens]
