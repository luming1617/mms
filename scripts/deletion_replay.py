#!/usr/bin/env python3
"""Replay WORM deletion tombstones onto a PITR-restored database.

Doc 8.4/20.1: a backup may contain rows deleted after the backup was taken. Recovery is
only complete after replaying every tombstone recorded after the backup time. The ledger
lives outside the backup domain; this tool reads it, resolves the pseudonymous customer
reference in the restored database, and re-runs the same deletion transaction.

Usage:
    MMS_DATABASE_URL=... python scripts/deletion_replay.py \
        --ledger-dir var/deletion-ledger --backup-taken-at 2026-10-30T00:00:00+00:00
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from mms.services.repository import MemoryRepository


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def load_tombstones(ledger_dir: Path, backup_taken_at: datetime) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(ledger_dir.glob("deletion-tombstones/*/*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        completed_at = payload.get("completed_at")
        if not completed_at:
            print(f"skip {path}: legacy tombstone without completed_at", file=sys.stderr)
            continue
        if _parse_time(str(completed_at)) <= backup_taken_at:
            continue
        payload["_path"] = str(path)
        records.append(payload)
    return records


def replay(dsn: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    repository = MemoryRepository()
    result: dict[str, Any] = {"replayed": [], "skipped": [], "failed": []}
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        for record in records:
            request_id = record.get("request_id")
            customer_ref = record.get("customer_ref")
            identity = conn.execute(
                "SELECT customer_id FROM customer_identity WHERE customer_ref=%s", (customer_ref,)
            ).fetchone()
            if identity is None:
                result["skipped"].append({"request_id": request_id, "reason": "customer_ref_not_found"})
                continue
            try:
                with conn.transaction():
                    outcome = repository.delete_customer_data(
                        conn,
                        identity["customer_id"],
                        "restore-replay",
                        "",
                        existing_request_id=UUID(str(request_id)),
                        approved_by="restore-replay",
                    )
                result["replayed"].append({"request_id": request_id, "status": outcome.get("status")})
            except Exception as exc:  # report every tombstone, then fail loudly
                result["failed"].append({"request_id": request_id, "error": str(exc)})
    if result["failed"]:
        raise SystemExit(json.dumps(result, ensure_ascii=False, indent=1))
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger-dir", type=Path, required=True)
    parser.add_argument("--backup-taken-at", required=True)
    parser.add_argument("--database-url", default=None)
    args = parser.parse_args()
    dsn = args.database_url or __import__("os").environ.get("MMS_DATABASE_URL")
    if not dsn:
        raise SystemExit("MMS_DATABASE_URL or --database-url is required")
    records = load_tombstones(args.ledger_dir, _parse_time(args.backup_taken_at))
    print(json.dumps({"tombstones": len(records), **replay(dsn, records)}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
