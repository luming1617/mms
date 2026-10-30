"""Official-style benchmark scoring and result persistence (docs/4 §二.4, §三.3).

Result objects keep the official field names (question_id, question_type,
question, gold_answer, hypothesis) so the JSONL output can be scored by the
original repositories' scripts as well. Official-proxy scores (token F1 /
normalized containment, deterministic) and supplementary DeepEval metrics are
computed and saved separately; per-type summaries mirror print_qa_metrics.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

TOKEN_RE = re.compile(r"[a-z0-9']+")
PUNCT = re.compile(r"[^\w\s]")


def _tokens(text: str) -> list[str]:
    return TOKEN_RE.findall(PUNCT.sub(" ", str(text).lower()))


def token_f1(hypothesis: str, gold: str) -> float:
    """SQuAD-style token F1 - the deterministic stand-in for the official
    LLM correctness judge (scorer id: f1-token-proxy)."""
    hyp_tokens, gold_tokens = _tokens(hypothesis), _tokens(gold)
    if not hyp_tokens or not gold_tokens:
        return 1.0 if not hyp_tokens and not gold_tokens else 0.0
    common: set[str] = set(hyp_tokens) & set(gold_tokens)
    if not common:
        return 0.0
    precision = len(common) / len(set(hyp_tokens))
    recall = len(common) / len(set(gold_tokens))
    return 2 * precision * recall / (precision + recall)


def containment_correct(hypothesis: str, gold: str) -> bool:
    gold_norm = " ".join(PUNCT.sub(" ", str(gold).lower()).split())
    hyp_norm = " ".join(PUNCT.sub(" ", str(hypothesis).lower()).split())
    return bool(gold_norm) and gold_norm in hyp_norm


class BenchmarkResultWriter:
    """Accumulates per-question results and writes the official-shape JSONL
    plus a per-type summary next to the other quality reports."""

    def __init__(self, report_dir: Path, *, suite: str, metadata: dict[str, Any]):
        self.report_dir = report_dir
        self.suite = suite
        self.metadata = metadata
        self.results: list[dict[str, Any]] = []

    def add(
        self,
        *,
        question_id: str,
        question_type: str,
        question: str,
        gold_answer: str,
        hypothesis: str,
        reader: str,
        retrieved: list[str],
        adversarial_answer: str = "",
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if gold_answer:
            official_proxy = containment_correct(hypothesis, gold_answer)
            f1 = token_f1(hypothesis, gold_answer)
        else:
            # adversarial QA: correct behavior = the reader did NOT assert the
            # plausible-but-wrong adversarial answer (docs/4 §三 adversarial)
            official_proxy = not (adversarial_answer and containment_correct(hypothesis, adversarial_answer))
            f1 = None
        entry = {
            "question_id": question_id,
            "question_type": question_type,
            "question": question,
            "gold_answer": gold_answer,
            "adversarial_answer": adversarial_answer,
            "hypothesis": hypothesis,
            "retrieved": retrieved,
            "reader": reader,
            "scorer": "containment-proxy",
            "official_proxy_correct": official_proxy,
            "token_f1": round(f1, 4) if f1 is not None else None,
            **(extra or {}),
        }
        self.results.append(entry)
        return entry

    def finish(self) -> dict[str, Any]:
        if not self.results:
            return {"suite": self.suite, "question_count": 0}
        by_type: dict[str, dict[str, Any]] = {}
        for entry in self.results:
            stats = by_type.setdefault(
                entry["question_type"],
                {"questions": 0, "proxy_correct": 0, "f1_sum": 0.0, "gold_in_context": 0},
            )
            stats["questions"] += 1
            stats["proxy_correct"] += int(bool(entry.get("official_proxy_correct")))
            stats["f1_sum"] += float(entry.get("token_f1") or 0.0)
            stats["gold_in_context"] += int(bool(entry.get("gold_in_context")))
        for stats in by_type.values():
            count = stats["questions"]
            stats["proxy_accuracy"] = round(stats["proxy_correct"] / count, 4)
            stats["mean_token_f1"] = round(stats["f1_sum"] / count, 4)
            stats["gold_in_context_rate"] = round(stats["gold_in_context"] / count, 4)
            del stats["proxy_correct"]
            del stats["f1_sum"]
            del stats["gold_in_context"]
        total = len(self.results)
        summary = {
            "schema_version": 1,
            "kind": "benchmark-summary",
            "suite": self.suite,
            "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "metadata": self.metadata,
            "question_count": total,
            "overall": {
                "proxy_accuracy": round(
                    sum(1 for entry in self.results if entry.get("official_proxy_correct")) / total, 4
                ),
                "mean_token_f1": round(sum(float(entry.get("token_f1") or 0.0) for entry in self.results) / total, 4),
            },
            "by_question_type": dict(sorted(by_type.items())),
        }
        self.report_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = self.report_dir / f"benchmark-{self.suite}-results.jsonl"
        with jsonl_path.open("w", encoding="utf-8") as handle:
            for entry in self.results:
                handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
        summary_path = self.report_dir / f"benchmark-{self.suite}-summary.json"
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        summary["results_path"] = str(jsonl_path)
        summary["summary_path"] = str(summary_path)
        return summary
