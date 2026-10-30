"""Public benchmark execution via DeepEval (docs/4 §二/§三).

LongMemEval: every question replays its own full haystack through the real
write pipeline into an isolated customer space, then queries with the
question only. LoCoMo: one conversation is replayed once per session and all
its sample QAs share that memory space. These tests are measurement sets:
metrics use threshold 0.0 and never block; per-type summaries and the
official-shape JSONL land in quality/reports/benchmark-*.

Run the full benchmark suite explicitly (the PR gate excludes it):
    pytest evals/test_benchmarks.py -m public_benchmark
"""

import pytest

from evals.benchmarks.case import SAMPLES_DIR, load_locomo_sample, load_qa_cases
from evals.benchmarks.reader import READER_PROXY_ID
from evals.benchmarks.replay import replay_locomo_conversation
from evals.benchmarks.runner import run_benchmark_case
from evals.benchmarks.scoring import BenchmarkResultWriter
from evals.conftest import requires_db
from evals.recorder import default_report_dir

pytestmark = [
    pytest.mark.evaluation,
    pytest.mark.integration,
    pytest.mark.public_benchmark,
    requires_db,
]


def _optional_qa_cases(name: str) -> list[dict]:
    path = SAMPLES_DIR / name
    if not path.is_file():
        pytest.skip(f"{name} not generated yet; run evals/benchmarks/convert.py (docs/4 §六)")
    return load_qa_cases(path)


longmemeval_s_cases = _optional_qa_cases("longmemeval_s_sample.json")
longmemeval_oracle_cases = _optional_qa_cases("longmemeval_oracle_sample.json")


@pytest.fixture(scope="module")
def result_writers(judge, recorder):
    writers = {
        suite: BenchmarkResultWriter(
            default_report_dir(),
            suite=suite,
            metadata={
                "judge_model": judge.label,
                "reader": READER_PROXY_ID,
                "scorer": "containment-proxy + f1-token-proxy (official LLM scorer requires judge credentials)",
                "target_model": recorder.target,
            },
        )
        for suite in ("longmemeval-s", "longmemeval-oracle", "locomo")
    }
    yield writers
    for writer in writers.values():
        summary = writer.finish()
        if summary.get("question_count"):
            print(
                f"\nbenchmark [{writer.suite}]: {summary['question_count']} questions, "
                f"proxy_accuracy={summary['overall']['proxy_accuracy']}, "
                f"mean_token_f1={summary['overall']['mean_token_f1']}"
            )


@pytest.mark.parametrize("case", longmemeval_s_cases, ids=lambda case: case["question_id"])
def test_longmemeval_s(case, target, recorder, judge, result_writers):
    run_benchmark_case(target, case, recorder, judge, result_writer=result_writers["longmemeval-s"])


@pytest.mark.parametrize("case", longmemeval_oracle_cases, ids=lambda case: case["question_id"])
def test_longmemeval_oracle(case, target, recorder, judge, result_writers):
    run_benchmark_case(target, case, recorder, judge, result_writer=result_writers["longmemeval-oracle"])


@pytest.fixture(scope="module")
def locomo_space(target):
    path = SAMPLES_DIR / "locomo_sample.json"
    if not path.is_file():
        pytest.skip("locomo_sample.json not generated yet; run evals/benchmarks/convert.py")
    sample = load_locomo_sample(path)
    customer = target.new_customer()
    target.grant_consent(customer)
    replayed = replay_locomo_conversation(target, customer, sample)
    replayed["customer"] = customer
    return {"sample": sample, "replayed": replayed}


def _locomo_qas() -> list[dict]:
    path = SAMPLES_DIR / "locomo_sample.json"
    if not path.is_file():
        return []
    return load_locomo_sample(path)["qas"]


locomo_qas = _locomo_qas()


@pytest.mark.skipif(not locomo_qas, reason="locomo_sample.json not generated yet; run evals/benchmarks/convert.py")
@pytest.mark.parametrize("qa", locomo_qas, ids=lambda qa: qa["question_id"])
def test_locomo_qa(qa, target, recorder, judge, locomo_space, result_writers):
    case = {
        "question_id": qa["question_id"],
        "question_type": f"locomo-{qa['category_name']}",
        "question": qa["question"],
        "answer": qa["answer"],
        "adversarial_answer": qa.get("adversarial_answer", ""),
        "evidence_dia_ids": qa["evidence_dia_ids"],
        "source": {
            "benchmark": "LoCoMo",
            "benchmark_version": locomo_space["sample"].get("metadata", {}).get("benchmark_version"),
            "dataset": "locomo10.json",
        },
    }
    run_benchmark_case(
        target, case, recorder, judge, result_writer=result_writers["locomo"], replayed=locomo_space["replayed"]
    )
