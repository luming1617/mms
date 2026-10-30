from __future__ import annotations

from typing import Any

from ..config import get_settings
from ..utils.db import DbConnection, require_row


class CapacityService:
    def report(self, conn: DbConnection, customer_id: str) -> dict[str, Any]:
        settings = get_settings()
        counts = require_row(
            conn.execute(
                """SELECT
                 (SELECT count(*) FROM customer_memory WHERE customer_id=%s) AS memories,
                 (SELECT count(*) FROM memory_embedding WHERE customer_id=%s AND status='READY') AS ready_vectors,
                 (SELECT count(*) FROM memory_search_document WHERE customer_id=%s AND status='READY') AS ready_views,
                 (SELECT count(DISTINCT tableoid::regclass::text) FROM memory_embedding WHERE customer_id=%s) AS vector_partitions,
                 (SELECT count(DISTINCT tableoid::regclass::text) FROM memory_search_document WHERE customer_id=%s) AS document_partitions""",
                (customer_id, customer_id, customer_id, customer_id, customer_id),
            ).fetchone(),
            "capacity counts",
        )
        exceeded = counts["ready_vectors"] > settings.exact_scan_soft_limit
        actions = []
        if exceeded:
            actions = [
                "inspect_view_fanout",
                "review_archive_and_retention",
                "measure_exact_p95_and_scanned_rows",
                "keep_exact_no_auto_ann",
            ]
        return {
            "customer_id": customer_id,
            **counts,
            "exact_scan_soft_limit": settings.exact_scan_soft_limit,
            "exceeded": exceeded,
            "recommended_actions": actions,
        }
