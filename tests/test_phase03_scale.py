import os
import re

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MMS_TEST_DATABASE_URL"),
        reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests",
    ),
]

from tests.test_phase01_integration import candidate, customer_id, delete, grant_consent, service

from mms.api.main import retrieve_with_read_routing
from mms.api.schemas import QueryRequest
from mms.config import get_settings
from mms.services.capacity import CapacityService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db
from mms.utils.metrics import EXACT_SCAN_LARGE_CUSTOMER


def add_ready(customer, svc, provider):
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "scale-ready"))
    worker = EmbeddingWorker(provider)
    while worker.process_one():
        pass
    return added


def test_exact_limit_capacity_and_single_partition_index():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    add_ready(customer, svc, provider)
    settings = get_settings()
    previous = settings.exact_scan_soft_limit
    settings.exact_scan_soft_limit = 0
    before = EXACT_SCAN_LARGE_CUSTOMER._value.get()
    try:
        with get_db().connection() as conn:
            # With a one-row table PostgreSQL legitimately prefers a sequential scan;
            # pin the planner so this test verifies the partition-local index contract.
            conn.execute("SET LOCAL enable_seqscan = off")
            result = svc.retrieve(
                conn,
                customer_id=customer,
                query="以后请用中文沟通",
                language="zh-CN",
                purpose="CURRENT_ANSWER",
                max_items=8,
            )
            assert result["memories"]
            assert EXACT_SCAN_LARGE_CUSTOMER._value.get() > before
            plan_rows = conn.execute(
                """EXPLAIN (ANALYZE,BUFFERS) SELECT * FROM memory_embedding
                   WHERE customer_id=%s AND status='READY' AND embedding_model='BAAI/bge-m3'
                     AND embedding_version=1 AND view_kind='ORIGINAL' AND view_language='zh-CN'""",
                (customer,),
            ).fetchall()
            plan = "\n".join(row["QUERY PLAN"] for row in plan_rows)
            partitions = set(re.findall(r"memory_embedding_p\d+", plan))
            assert len(partitions) == 1 and "customer_id_status_embedding_model" in plan
            capacity = CapacityService().report(conn, customer)
            assert capacity["exceeded"] and capacity["vector_partitions"] == 1
            assert "keep_exact_no_auto_ann" in capacity["recommended_actions"]
    finally:
        settings.exact_scan_soft_limit = previous
    delete(customer, "delete-" + customer)


def test_replica_failure_and_recent_write_fall_back_to_primary():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    add_ready(customer, svc, provider)
    settings = get_settings()
    settings.read_replica_enabled = True
    settings.read_replica_database_url = "postgresql://mms:mms@127.0.0.1:59999/mms"
    get_db().close_replica()
    payload = QueryRequest(query="以后请用中文沟通", language="zh-CN")
    try:
        with get_db().transaction() as conn:
            conn.execute(
                "UPDATE customer_memory SET updated_at=now()-interval '30 seconds' WHERE customer_id=%s", (customer,)
            )
        result = retrieve_with_read_routing(payload, customer)
        assert result["degraded"] and result["degraded_reason"] == "replica_unavailable"
        assert result["memories"]
        with get_db().transaction() as conn:
            conn.execute("UPDATE customer_memory SET updated_at=now() WHERE customer_id=%s", (customer,))
        recent = retrieve_with_read_routing(payload, customer)
        assert not recent["degraded"] and recent["memories"]
    finally:
        settings.read_replica_enabled = False
        settings.read_replica_database_url = None
        get_db().close_replica()
    delete(customer, "delete-" + customer)
