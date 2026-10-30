from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from mms.config import get_settings
from mms.services.quality import GateEvidenceWriter, RecallEvaluator, evaluate_slo
from mms.services.repository import MemoryRepository
from mms.services.service import MemoryService
from mms.utils.db import get_db
from mms.utils.embedding import DeterministicHashEmbeddingProvider, HttpEmbeddingProvider


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slo-input", type=Path)
    parser.add_argument("--recall-input", type=Path)
    parser.add_argument("--customer-id")
    parser.add_argument("--baseline-id", default="phase3-baseline")
    parser.add_argument("--minimum-recall", default="{}")
    parser.add_argument("--role", default="mms-sre")
    parser.add_argument("--environment", default="production")
    parser.add_argument("--evidence-dir", type=Path)
    parser.set_defaults(evidence_dir=Path(tempfile.gettempdir()) / "mms-phase3-evidence")
    args = parser.parse_args()
    if not args.slo_input and not args.recall_input:
        parser.error("at least one of --slo-input or --recall-input is required")
    writer = GateEvidenceWriter(args.evidence_dir)
    failed = False
    if args.slo_input:
        result = evaluate_slo(json.loads(args.slo_input.read_text()))
        path = writer.write(
            "phase3-slo", role=args.role, environment=args.environment, criteria=result["checks"], evidence=result
        )
        print(json.dumps({"gate": "slo", "status": result["status"], "evidence": str(path)}))
        failed |= result["status"] != "COMPLETED"
    if args.recall_input:
        if not args.customer_id:
            parser.error("--customer-id is required with --recall-input")
        cases = json.loads(args.recall_input.read_text())
        settings = get_settings()
        provider = (
            HttpEmbeddingProvider(settings.embedding_provider_url)
            if settings.embedding_provider == "http"
            else DeterministicHashEmbeddingProvider(settings.embedding_dimension)
        )
        service = MemoryService(MemoryRepository(), provider)
        with get_db().connection() as conn:
            result = RecallEvaluator(service).evaluate(
                conn,
                args.customer_id,
                cases,
                baseline_id=args.baseline_id,
                minimum_recall=json.loads(args.minimum_recall),
            )
        path = writer.write(
            "phase3-recall", role=args.role, environment=args.environment, criteria=result["results"], evidence=result
        )
        print(json.dumps({"gate": "recall", "status": result["status"], "evidence": str(path)}))
        failed |= result["status"] != "COMPLETED"
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
