from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from psycopg.types.json import Jsonb

from ..utils.db import DbConnection, get_db
from ..utils.metrics import RECALL_AT_10, RECALL_BASELINE, RECALL_MRR
from .service import MemoryService


class RecallEvaluator:
    def __init__(self, memory_service: MemoryService):
        self.memory_service = memory_service
        self.repository = memory_service.repository

    def evaluate(
        self,
        conn: DbConnection,
        customer_id: str,
        cases: list[dict[str, Any]],
        *,
        baseline_id: str,
        minimum_recall: dict[str, float],
        channels: tuple[str, ...] = ("semantic", "hybrid"),
    ) -> dict[str, Any]:
        languages = sorted({case["language"] for case in cases})
        results = []
        for language in languages:
            language_cases = [case for case in cases if case["language"] == language]
            for channel in channels:
                ranks = [self._rank(conn, customer_id, case, channel) for case in language_cases]
                hits = [rank for rank in ranks if rank is not None]
                recall = len(hits) / len(ranks) if ranks else 0.0
                mrr = sum(1 / rank for rank in hits) / len(ranks) if ranks else 0.0
                minimum = minimum_recall.get(language)
                passed = minimum is not None and recall >= minimum
                results.append(
                    {
                        "language": language,
                        "channel": channel,
                        "count": len(ranks),
                        "recall_at_10": recall,
                        "mrr": mrr,
                        "minimum_recall": minimum,
                        "passed": passed,
                        "baseline_id": baseline_id,
                    }
                )
                RECALL_AT_10.labels(language=language, channel=channel).set(recall)
                RECALL_MRR.labels(language=language, channel=channel).set(mrr)
                if minimum is not None:
                    RECALL_BASELINE.labels(language=language, channel=channel).set(minimum)
        passed = bool(results) and all(item["passed"] for item in results)
        with get_db().transaction() as conn:
            run_id = uuid4()
            mismatches = sum(0 if item["passed"] else 1 for item in results)
            conn.execute(
                """INSERT INTO recon_run
                     (id,recon_type,status,started_at,completed_at,scanned_count,mismatch_count,details,triggered_by)
                   VALUES (%s,'RECALL',%s,now(),now(),%s,%s,%s,'phase3-gate')""",
                (run_id, "COMPLETED" if passed else "PARTIAL", len(results), mismatches, Jsonb(results)),
            )
        return {
            "run_id": run_id,
            "baseline_id": baseline_id,
            "status": "COMPLETED" if passed else "PARTIAL",
            "results": results,
        }

    def _rank(self, conn: DbConnection, customer_id: str, case: dict[str, Any], channel: str) -> int | None:
        if channel == "semantic":
            vector = self.memory_service.embedding_provider.embed_query(
                case["query"], self.memory_service.settings.embedding_model
            )
            rows = self.repository.retrieve(
                conn,
                customer_id,
                case["query"],
                vector,
                case["language"],
                "CURRENT_ANSWER",
                10,
                include_sensitive=True,
                parallel=False,
            )
            ids = [str(row["memory_id"]) for row in rows]
        else:
            if channel == "rerank" and not self.memory_service.reranker:
                raise ValueError("rerank channel requires a configured reranker")
            result = self.memory_service.retrieve(
                conn,
                customer_id=customer_id,
                query=case["query"],
                language=case["language"],
                purpose="CURRENT_ANSWER",
                max_items=10,
            )
            ids = [row["memory_id"] for row in result["memories"]]
        expected = str(case["memory_id"])
        return ids.index(expected) + 1 if expected in ids else None


SLO_CRITERIA = {
    "availability": ("gte", 0.999),
    "degraded_rate": ("lt", 0.01),
    "default_p95": ("lte", 0.300),
    "default_p99": ("lte", 0.800),
    "extended_p95": ("lte", 0.500),
    "extended_p99": ("lte", 1.500),
    "embedding_ready_p95": ("lte", 30.0),
    "embedding_ready_p99": ("lte", 120.0),
    "translation_ready_p95": ("lte", 120.0),
    "translation_failure_rate": ("lt", 0.01),
}


def evaluate_slo(values: dict[str, float]) -> dict[str, Any]:
    checks = {}
    for name, (operator, threshold) in SLO_CRITERIA.items():
        value = values[name]
        passed = (
            value >= threshold if operator == "gte" else value < threshold if operator == "lt" else value <= threshold
        )
        checks[name] = {"value": value, "threshold": threshold, "operator": operator, "passed": passed}
    return {"status": "COMPLETED" if all(item["passed"] for item in checks.values()) else "FAILED", "checks": checks}


class GateEvidenceWriter:
    def __init__(self, directory: Path | None = None):
        self.directory = directory or Path(tempfile.gettempdir()) / "mms-phase3-evidence"

    def write(
        self, gate: str, *, role: str, environment: str, criteria: dict[str, Any], evidence: dict[str, Any]
    ) -> Path:
        now = datetime.now(UTC)
        payload = {
            "gate": gate,
            "role": role,
            "environment": environment,
            "criteria": criteria,
            "evidence": evidence,
            "created_at": now.isoformat(),
            "retain_until": (now + timedelta(days=365)).isoformat(),
        }
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.directory / f"{gate}-{uuid4()}.json"
        with path.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, default=str, sort_keys=True)
        return path
