from __future__ import annotations

import time
from typing import Any

from ..config import get_settings
from ..observability.bootstrap import bootstrap_worker
from ..utils.db import DbConnection, SchedulerLock, get_db, require_row
from ..utils.metrics import (
    CUSTOMERS_OVER_EXACT_SOFT_LIMIT,
    VECTORS_PER_CUSTOMER,
    VECTORS_PER_CUSTOMER_MAX,
    VIEW_COUNT,
    VIEW_EVICTED,
)


class ViewCostService:
    def __init__(self):
        self.settings = get_settings()

    def run_once(self) -> dict[str, Any]:
        evicted = []
        with get_db().transaction() as conn:
            memories = conn.execute(
                """SELECT m.id,m.customer_id,p.config AS policy_config,count(d.*) AS view_count
                   FROM customer_memory m JOIN memory_policy p ON p.name||'@'||p.version=m.policy_version
                   JOIN memory_search_document d ON d.memory_id=m.id AND d.customer_id=m.customer_id AND d.status='READY'
                   GROUP BY m.id,m.customer_id,p.config
                   HAVING count(d.*)>LEAST(COALESCE((p.config->>'max_views_per_memory')::int,6),%s)""",
                (self.settings.max_views_per_memory,),
            ).fetchall()
            for memory in memories:
                budget = min(
                    self.settings.max_views_per_memory, int(memory["policy_config"].get("max_views_per_memory", 6))
                )
                remaining = int(memory["view_count"])
                views = conn.execute(
                    "SELECT * FROM memory_search_document WHERE memory_id=%s AND view_kind='TRANSLATED' AND status='READY'",
                    (memory["id"],),
                ).fetchall()
                ranked = sorted(views, key=lambda view: self._cost_key(conn, memory["id"], view))
                for view in ranked:
                    if remaining <= budget:
                        break
                    self._evict(conn, view)
                    evicted.append({"memory_id": view["memory_id"], "language": view["view_language"]})
                    remaining -= 1
                VIEW_COUNT.observe(remaining)
        # Doc 13.4: per-customer READY vector distribution (p50/p95/p99 from the
        # histogram, max and over-soft-limit customers as gauges) feeds capacity review.
        with get_db().connection() as conn:
            rows = conn.execute(
                "SELECT count(*) AS vectors FROM memory_embedding WHERE status='READY' GROUP BY customer_id"
            ).fetchall()
        counts = [int(row["vectors"]) for row in rows]
        if counts:
            for count in counts:
                VECTORS_PER_CUSTOMER.observe(count)
            VECTORS_PER_CUSTOMER_MAX.set(max(counts))
            CUSTOMERS_OVER_EXACT_SOFT_LIMIT.set(
                sum(1 for count in counts if count > self.settings.exact_scan_soft_limit)
            )
        return {
            "evicted": evicted,
            "evicted_count": len(evicted),
            "customers": len(counts),
            "vectors_max": max(counts) if counts else 0,
        }

    def _cost_key(self, conn: DbConnection, memory_id, view) -> tuple:
        priorities = self.settings.translation_language_priority or self.settings.translation_target_languages
        priority = priorities.index(view["view_language"]) if view["view_language"] in priorities else len(priorities)
        usage = conn.execute(
            """SELECT count(*) AS hits,max(created_at) AS last_used FROM memory_usage_event
               WHERE (matched_views->%s->>'view_kind')='TRANSLATED'
                 AND (matched_views->%s->>'view_language')=%s""",
            (str(memory_id), str(memory_id), view["view_language"]),
        ).fetchone()
        usage = require_row(usage, "view cost usage")
        last_used = usage["last_used"].timestamp() if usage["last_used"] else 0.0
        return (usage["hits"], priority, view["translation_quality"] or 0.0, last_used)

    def _evict(self, conn: DbConnection, view) -> None:
        params = (view["memory_id"], view["view_language"])
        conn.execute(
            "DELETE FROM memory_embedding WHERE memory_id=%s AND view_kind='TRANSLATED' AND view_language=%s", params
        )
        conn.execute(
            "UPDATE memory_embedding_job SET status='CANCELLED',last_error_code='VIEW_EVICTED',updated_at=now() "
            "WHERE memory_id=%s AND view_kind='TRANSLATED' AND view_language=%s AND status IN ('PENDING','RUNNING','RETRY')",
            params,
        )
        conn.execute(
            "UPDATE memory_translation_job SET status='CANCELLED',last_error_code='VIEW_EVICTED',updated_at=now() "
            "WHERE memory_id=%s AND target_language=%s AND status IN ('PENDING','RUNNING','RETRY')",
            params,
        )
        conn.execute(
            "DELETE FROM memory_search_document WHERE memory_id=%s AND view_kind='TRANSLATED' AND view_language=%s",
            params,
        )
        VIEW_EVICTED.labels(language=view["view_language"]).inc()

    def run_forever(self) -> None:
        # Doc 13.1: advisory lock keeps a standby view-cost scheduler from duplicating scans.
        guard = SchedulerLock("mms:scheduler:view-cost")
        try:
            while True:
                if guard.try_acquire():
                    try:
                        self.run_once()
                    finally:
                        guard.release()
                time.sleep(self.settings.lifecycle_poll_seconds)
        finally:
            guard.release()


def main() -> None:
    bootstrap_worker(get_settings())
    ViewCostService().run_forever()


if __name__ == "__main__":
    main()
