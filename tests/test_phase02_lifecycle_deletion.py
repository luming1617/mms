import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MMS_TEST_DATABASE_URL"),
        reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests",
    ),
]

from tests.test_phase01_integration import candidate, customer_id, delete, grant_consent, service

from mms.services.deletion import DeletionService
from mms.services.lifecycle import LifecycleService
from mms.services.repository import MemoryRepository
from mms.services.review import ReviewService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db
from mms.utils.errors import BarrierError
from mms.utils.hashing import customer_hash, customer_ref, sha256_text, utcnow
from mms.utils.metrics import BARRIER_CACHE_MISS, CONFLICT_TIMEOUT_EXPIRED


def test_candidate_review_sla_expires():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        pending = svc.process_candidate(
            conn, candidate(customer, "expired-review").model_copy(update={"confidence": 0.0})
        )
        assert pending.outcome == "NEEDS_VERIFICATION"
        conn.execute(
            "UPDATE memory_candidate SET created_at=now()-interval '31 days' WHERE customer_id=%s", (customer,)
        )
    counts = LifecycleService().run_once()
    assert counts["candidates_expired"] == 1 and counts["candidate_reviews_expired"] == 1
    with get_db().connection() as conn:
        row = conn.execute(
            "SELECT candidate_status,content_redacted,source_refs,semantic_text "
            "FROM memory_candidate WHERE customer_id=%s",
            (customer,),
        ).fetchone()
    assert row["candidate_status"] == "EXPIRED"
    # Doc 6.2: SLA expiry keeps only the fingerprint + decision audit; the body is scrubbed.
    assert row["content_redacted"] is None and row["source_refs"] == [] and row["semantic_text"] is None
    delete(customer, "delete-" + customer)


def test_memory_expiry_deletes_projections_and_valid_to_resolves():
    expired_customer = customer_id()
    grant_consent(expired_customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        expired = candidate(expired_customer, "expired-memory").model_copy(
            update={"expires_at": datetime.now(UTC) - timedelta(days=1)}
        )
        added = svc.process_candidate(conn, expired)
    worker = EmbeddingWorker(provider)
    while worker.process_one():
        pass
    counts = LifecycleService().run_once()
    assert counts["memories_expired"] >= 1 and counts["embeddings_deleted"] >= 2
    with get_db().connection() as conn:
        state = conn.execute(
            "SELECT (SELECT lifecycle_status FROM customer_memory WHERE id=%s) AS memory,"
            "(SELECT count(*) FROM memory_embedding WHERE memory_id=%s AND status='READY') AS embeddings,"
            "(SELECT count(*) FROM customer_memory_current WHERE memory_id=%s) AS pointers",
            (added.new_memory_id, added.new_memory_id, added.new_memory_id),
        ).fetchone()
    assert state == {"memory": "EXPIRED", "embeddings": 0, "pointers": 0}
    delete(expired_customer, "delete-" + expired_customer)

    resolved_customer = customer_id()
    grant_consent(resolved_customer)
    svc, provider = service()
    observed = datetime.now(UTC) - timedelta(days=2)
    with get_db().transaction() as conn:
        resolved = candidate(resolved_customer, "resolved-memory").model_copy(
            update={
                "memory_type": "ACTIVE_ISSUE",
                "observed_at": observed,
                "valid_from": observed,
                "valid_to": observed + timedelta(days=1),
            }
        )
        added = svc.process_candidate(conn, resolved)
    worker = EmbeddingWorker(provider)
    while worker.process_one():
        pass
    LifecycleService().run_once()
    with get_db().connection() as conn:
        state = conn.execute(
            """SELECT m.lifecycle_status,count(e.memory_id) AS ready FROM customer_memory m
               LEFT JOIN memory_embedding e ON e.memory_id=m.id AND e.status='READY' WHERE m.id=%s GROUP BY m.lifecycle_status""",
            (added.new_memory_id,),
        ).fetchone()
    assert state == {"lifecycle_status": "RESOLVED", "ready": 2}
    delete(resolved_customer, "delete-" + resolved_customer)


def test_reviewed_deletion_worm_tombstone_and_rejection():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    review = ReviewService(svc.repository, svc)
    deletion = DeletionService(svc.repository)
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "delete-approved"))
        requested = deletion.request(conn, customer, "customer", "delete-" + customer)
        assert requested["status"] == "RECEIVED"
        with pytest.raises(BarrierError):
            svc.process_candidate(conn, candidate(customer, "blocked-during-delete"))
    with get_db().transaction() as conn:
        task = review.queue(conn, customer)[0]
        approved = review.decide(
            conn,
            task["id"],
            customer_id=customer,
            reviewer_id="privacy-reviewer",
            co_reviewer_id="privacy-second-reviewer",
            decision_version=1,
            approve=True,
            reason_code="GDPR_APPROVED",
            note=None,
        )
        assert approved["deletion"]["status"] == "COMPLETED"
    with get_db().connection() as conn:
        evidence = conn.execute(
            "SELECT (SELECT status FROM memory_deletion_request WHERE id=%s) AS request,"
            "(SELECT count(*) FROM deletion_tombstone WHERE deletion_request_id=%s) AS tombstones,"
            "(SELECT count(*) FROM memory_deletion_item WHERE deletion_request_id=%s) AS items,"
            "(SELECT ledger_object_key FROM deletion_tombstone WHERE deletion_request_id=%s) AS ledger_key",
            (requested["request_id"], requested["request_id"], requested["request_id"], requested["request_id"]),
        ).fetchone()
    assert evidence["request"] == "COMPLETED" and evidence["tombstones"] == 1 and evidence["items"] > 0
    # Doc 6.8: fixed object key deletion-tombstones/{request_id}/{scope_hash}.json
    assert evidence["ledger_key"].startswith("deletion-tombstones/") and evidence["ledger_key"].endswith(".json")
    assert (deletion.settings.deletion_ledger_dir / evidence["ledger_key"]).exists()
    with get_db().transaction() as conn:
        assert deletion.request(conn, customer, "customer", "delete-" + customer)["status"] == "COMPLETED"

    rejected_customer = customer_id()
    grant_consent(rejected_customer)
    svc, _ = service()
    review = ReviewService(svc.repository, svc)
    deletion = DeletionService(svc.repository)
    with get_db().transaction() as conn:
        requested = deletion.request(conn, rejected_customer, "customer", "delete-" + rejected_customer)
    with get_db().transaction() as conn:
        task = review.queue(conn, rejected_customer)[0]
        rejected = review.decide(
            conn,
            task["id"],
            customer_id=rejected_customer,
            reviewer_id="privacy-reviewer",
            decision_version=1,
            approve=False,
            reason_code="NOT_VERIFIED",
            note=None,
        )
        assert rejected["deletion"]["status"] == "REJECTED"
        barrier = conn.execute(
            "SELECT status,generation FROM customer_deletion_barrier WHERE customer_id=%s", (rejected_customer,)
        ).fetchone()
        # Doc 6.7.1: a cancelled barrier bumps its generation; pre-cancel carriers stay rejected.
        assert barrier["status"] == "CANCELLED" and int(barrier["generation"]) >= 2
        assert svc.process_candidate(conn, candidate(rejected_customer, "allowed-after-reject")).outcome == "ADD"
    delete(rejected_customer, "delete-cleanup-" + rejected_customer)


def test_delete_request_candidate_enters_deletion_workflow():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        decision = svc.process_candidate(
            conn,
            candidate(customer, "delete-request").model_copy(update={"content": "请删除我的所有信息"}),
        )
    assert decision.outcome == "DELETE_REQUESTED"
    assert decision.primary_rule_id == "R001"
    with get_db().connection() as conn:
        request = conn.execute(
            "SELECT request_source,status FROM memory_deletion_request WHERE customer_id=%s", (customer,)
        ).fetchone()
        barrier = conn.execute(
            "SELECT status FROM customer_deletion_barrier WHERE customer_id=%s", (customer,)
        ).fetchone()
        candidate_status = conn.execute(
            "SELECT candidate_status FROM memory_candidate WHERE customer_id=%s", (customer,)
        ).fetchone()
    assert request["request_source"] == "DELETE_REQUESTED"
    assert barrier["status"] == "ACTIVE"
    assert candidate_status["candidate_status"] == "PENDING_REVIEW"
    delete(customer, "delete-request-cleanup")


def test_stale_carrier_is_rejected_with_stale_generation_code():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    repository = svc.repository
    review = ReviewService(repository, svc)
    with get_db().transaction() as conn:
        DeletionService(repository).request(conn, customer, "customer", "delete-" + customer)
    with get_db().transaction() as conn:
        task = review.queue(conn, customer)[0]
        review.decide(
            conn,
            task["id"],
            customer_id=customer,
            reviewer_id="privacy-reviewer",
            decision_version=1,
            approve=False,
            reason_code="NOT_VERIFIED",
            note=None,
        )
    with get_db().transaction() as conn:
        generation = conn.execute(
            "SELECT generation FROM customer_deletion_barrier WHERE customer_id=%s", (customer,)
        ).fetchone()["generation"]
        with pytest.raises(BarrierError) as excinfo:
            repository.verify_write_barrier(conn, customer, int(generation) - 1, utcnow())
    # Docs 6.7.1/6.7.6: a stale carrier is a generation rejection, not "customer deleted".
    assert excinfo.value.code == "STALE_GENERATION" and excinfo.value.status_code == 409
    delete(customer, "delete-cleanup-" + customer)


def test_ledger_failure_keeps_deletion_verifying():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "ledger-failure"))

    def broken_ledger(key, payload):
        raise RuntimeError("worm unavailable")

    with get_db().transaction() as conn:
        result = MemoryRepository().delete_customer_data(
            conn, customer, "test", "delete-" + customer, ledger_writer=broken_ledger
        )
    assert result["status"] == "VERIFYING"
    with get_db().connection() as conn:
        state = conn.execute(
            "SELECT (SELECT status FROM memory_deletion_request WHERE id=%s) AS request,"
            "(SELECT count(*) FROM deletion_tombstone WHERE deletion_request_id=%s) AS tombstones,"
            "(SELECT status FROM customer_deletion_barrier WHERE customer_id=%s) AS barrier",
            (result["request_id"], result["request_id"], customer),
        ).fetchone()
    # Doc 6.8: no tombstone and no completion without a successful WORM ledger write.
    assert state == {"request": "VERIFYING", "tombstones": 0, "barrier": "ACTIVE"}

    # Retrying the same request with a healthy ledger completes it.
    with get_db().transaction() as conn:
        retried = MemoryRepository().delete_customer_data(
            conn,
            customer,
            "test",
            "",
            existing_request_id=result["request_id"],
            approved_by="privacy-reviewer",
        )
    assert retried["status"] == "COMPLETED" and retried["request_id"] == result["request_id"]


def test_lifecycle_retries_verifying_deletion_request():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "verifying-retry"))

    def broken_ledger(key, payload):
        raise RuntimeError("worm unavailable")

    ledger_writes = []

    class HealthyLedger:
        def write(self, key, payload):
            ledger_writes.append(key)

    with get_db().transaction() as conn:
        result = MemoryRepository().delete_customer_data(
            conn, customer, "customer", "delete-" + customer, ledger_writer=broken_ledger
        )
    assert result["status"] == "VERIFYING"
    with get_db().transaction() as conn:
        conn.execute(
            "UPDATE memory_deletion_request SET updated_at=now()-interval '10 minutes' WHERE id=%s",
            (result["request_id"],),
        )
    lifecycle = LifecycleService(DeletionService(MemoryRepository(), ledger=HealthyLedger()))
    counts = lifecycle.run_once()
    assert counts["deletion_retries_completed"] >= 1
    with get_db().connection() as conn:
        state = conn.execute(
            "SELECT (SELECT status FROM memory_deletion_request WHERE id=%s) AS request,"
            "(SELECT count(*) FROM deletion_tombstone WHERE deletion_request_id=%s) AS tombstones,"
            "(SELECT status FROM customer_deletion_barrier WHERE customer_id=%s) AS barrier",
            (result["request_id"], result["request_id"], customer),
        ).fetchone()
    assert state == {"request": "COMPLETED", "tombstones": 1, "barrier": "COOLING_DOWN"}
    assert ledger_writes


def test_cooling_down_barrier_completes_after_cooldown():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "cooldown"))
    result = delete(customer, "delete-" + customer)
    assert result["status"] == "COMPLETED"
    with get_db().connection() as conn:
        barrier = conn.execute(
            "SELECT status FROM customer_deletion_barrier WHERE customer_id=%s", (customer,)
        ).fetchone()
    assert barrier["status"] == "COOLING_DOWN"
    with get_db().transaction() as conn:
        conn.execute(
            "UPDATE customer_deletion_barrier SET completed_at=now()-interval '400 days' WHERE customer_id=%s",
            (customer,),
        )
    counts = LifecycleService().run_once()
    assert counts["barriers_completed"] >= 1
    with get_db().connection() as conn:
        assert (
            conn.execute("SELECT status FROM customer_deletion_barrier WHERE customer_id=%s", (customer,)).fetchone()[
                "status"
            ]
            == "COMPLETED"
        )
    delete(customer, "delete-cleanup-" + customer)


def test_read_barrier_cache_serves_hits_and_invalidates_on_deletion():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "barrier-cache"))

    def query() -> dict:
        with get_db().connection() as conn:
            return svc.retrieve(
                conn,
                customer_id=customer,
                query="中文",
                language="zh-CN",
                purpose="CURRENT_ANSWER",
                max_items=4,
                fact_key="communication.language",
            )

    assert query()["memories"]
    misses = BARRIER_CACHE_MISS._value.get()
    assert query()["memories"]
    assert BARRIER_CACHE_MISS._value.get() == misses  # second read is served from the TTL cache
    with get_db().transaction() as conn:
        DeletionService(svc.repository).request(conn, customer, "customer", "delete-" + customer)
    with pytest.raises(BarrierError) as excinfo:
        query()
    assert excinfo.value.code == "DELETE_IN_PROGRESS"
    delete(customer, "delete-cleanup-" + customer)


def test_audit_hash_and_kafka_ref_are_keyed():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "keyed-pseudonym"))
    with get_db().connection() as conn:
        row = conn.execute(
            "SELECT d.customer_hash, i.customer_ref FROM memory_decision d "
            "JOIN customer_identity i ON i.customer_id = d.customer_id WHERE d.customer_id=%s LIMIT 1",
            (customer,),
        ).fetchone()
    assert row["customer_hash"] == customer_hash(customer)
    assert row["customer_ref"] == customer_ref(customer)
    # Docs 7.4/7.9: keyed HMAC, never an unkeyed digest of the customer id.
    assert row["customer_hash"] != sha256_text(customer)
    assert row["customer_ref"] != sha256_text(f"customer-ref:{customer}")
    delete(customer, "delete-cleanup-" + customer)


def test_consent_blocked_candidate_persists_masked_content():
    customer = customer_id()
    with get_db().transaction() as conn:
        conn.execute(
            "INSERT INTO customer_memory_consent (customer_id,purpose,status,version,effective_at,source) "
            "VALUES (%s,'MEMORY_WRITE','DENIED',1,now(),'test')",
            (customer,),
        )
    svc, _ = service()
    with get_db().transaction() as conn:
        decision = svc.process_candidate(conn, candidate(customer, "consent-denied", content="我的密码是 abc123"))
    assert decision.outcome == "REJECTED" and decision.reason_codes == ["CONSENT_DENIED"]
    with get_db().connection() as conn:
        row = conn.execute("SELECT content_redacted FROM memory_candidate WHERE customer_id=%s", (customer,)).fetchone()
    # Doc 6.2: a privacy hard block keeps only the masked excerpt.
    assert row["content_redacted"] is not None
    assert "密码" not in row["content_redacted"] and "[REDACTED]" in row["content_redacted"]
    delete(customer, "delete-cleanup-" + customer)


def test_reinforce_new_language_view_matches_evidence_excerpt():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "primary-language"))
    assert added.outcome == "ADD"
    with get_db().transaction() as conn:
        reinforced = svc.process_candidate(
            conn, candidate(customer, "second-language", language="en-US", content="Please use Chinese")
        )
    assert reinforced.outcome == "REINFORCE"
    with get_db().connection() as conn:
        rows = conn.execute(
            """SELECT d.content, e.evidence_excerpt_redacted
               FROM memory_search_document d
               JOIN memory_evidence e ON e.memory_id = d.memory_id AND e.content_language = d.view_language
               WHERE d.memory_id=%s AND d.view_kind='ORIGINAL' AND d.view_language='en-US'""",
            (added.new_memory_id,),
        ).fetchall()
    # Doc 8.4 1a + 7.13.4: the secondary ORIGINAL must equal its evidence excerpt.
    assert rows and all(row["content"] == row["evidence_excerpt_redacted"] for row in rows)
    delete(customer, "delete-cleanup-" + customer)


def test_candidate_ttl_expires_and_redacts():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "ttl-candidate").model_copy(update={"confidence": 0.0}))
        conn.execute(
            "UPDATE memory_candidate SET candidate_status='RECEIVED', expires_at=now()-interval '1 second' "
            "WHERE customer_id=%s",
            (customer,),
        )
    counts = LifecycleService().run_once()
    assert counts["candidates_expired_by_ttl"] == 1 and counts["candidate_reviews_expired"] == 1
    with get_db().connection() as conn:
        row = conn.execute(
            "SELECT candidate_status,content_redacted,source_refs,semantic_text "
            "FROM memory_candidate WHERE customer_id=%s",
            (customer,),
        ).fetchone()
        task = conn.execute("SELECT status FROM memory_review_task WHERE customer_id=%s", (customer,)).fetchone()
    # Doc 6.3: RECEIVED/VALIDATED -> EXPIRED on candidate TTL, keeping only the fingerprint.
    assert row == {
        "candidate_status": "EXPIRED",
        "content_redacted": None,
        "source_refs": [],
        "semantic_text": None,
    }
    assert task["status"] == "EXPIRED"
    delete(customer, "delete-" + customer)


def test_conflict_timeout_reason_code_and_metric():
    customer = customer_id()
    grant_consent(customer)
    with get_db().transaction() as conn:
        conn.execute(
            "INSERT INTO memory_conflict (id,customer_id,fact_key,status,risk_level,opened_at) "
            "VALUES (%s,%s,'communication.language','OPEN','LOW',now()-interval '100 days')",
            (uuid4(), customer),
        )
    with get_db().transaction() as conn:
        conn.execute(
            "INSERT INTO memory_conflict (id,customer_id,fact_key,status,risk_level,opened_at) "
            "VALUES (%s,%s,'communication.channel','OPEN','LOW',now())",
            (uuid4(), customer),
        )
    before = CONFLICT_TIMEOUT_EXPIRED._value.get()
    counts = LifecycleService().run_once()
    assert counts["conflicts_resolved_by_timeout"] == 1
    assert CONFLICT_TIMEOUT_EXPIRED._value.get() == before + 1
    with get_db().connection() as conn:
        rows = conn.execute(
            "SELECT status,resolution_reason_code,opened_at FROM memory_conflict "
            "WHERE customer_id=%s ORDER BY opened_at",
            (customer,),
        ).fetchall()
    # Doc 6.2: auto-expired conflicts carry CONFLICT_TIMEOUT_EXPIRED, not CONFLICT_TIMEOUT;
    # a fresh OPEN conflict must stay open.
    assert [(r["status"], r["resolution_reason_code"]) for r in rows] == [
        ("RESOLVED", "CONFLICT_TIMEOUT_EXPIRED"),
        ("OPEN", None),
    ]
    delete(customer, "delete-" + customer)


def test_embedding_job_priority_takes_min():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "priority-job"))
    repository = svc.repository
    with get_db().transaction() as conn:
        conn.execute("UPDATE memory_embedding_job SET priority=500 WHERE memory_id=%s", (added.new_memory_id,))
        view = conn.execute(
            "SELECT view_kind,view_language,view_text_hash FROM memory_search_document "
            "WHERE memory_id=%s ORDER BY view_kind LIMIT 1",
            (added.new_memory_id,),
        ).fetchone()
        content_hash = conn.execute(
            "SELECT content_hash FROM customer_memory WHERE id=%s", (added.new_memory_id,)
        ).fetchone()["content_hash"]
        for priority in (100, 500):
            repository.enqueue_embedding_job(
                conn,
                added.new_memory_id,
                customer,
                content_hash,
                view["view_kind"],
                view["view_language"],
                view["view_text_hash"],
                0,
                priority=priority,
            )
        stored = conn.execute(
            "SELECT priority FROM memory_embedding_job WHERE memory_id=%s AND view_kind=%s AND view_language=%s",
            (added.new_memory_id, view["view_kind"], view["view_language"]),
        ).fetchone()["priority"]
    # Doc 7.6: LEAST(old, new) — a later REBUILD must not demote the urgent UPSERT.
    assert stored == 100
    delete(customer, "delete-" + customer)


def test_candidate_discovery_status_set():
    # Doc 8.6: CANDIDATE_DISCOVERY is an internal purpose mapped to ACTIVE + CONFLICTED.
    assert MemoryRepository()._status_set("CANDIDATE_DISCOVERY") == ["ACTIVE", "CONFLICTED"]


def test_carrier_created_at_uses_carrier_row_not_observed_at():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "carrier-base"))
        conn.execute(
            """INSERT INTO customer_deletion_barrier
                 (customer_id,request_id,status,requested_at,cutover_at,generation,scope_type)
               VALUES (%s,%s,'CANCELLED',now(),now(),1,'CUSTOMER')
               ON CONFLICT (customer_id) DO UPDATE SET status='CANCELLED',cutover_at=now(),generation=1""",
            (customer, uuid4()),
        )
    backdated = datetime.now(UTC) - timedelta(days=1)
    with get_db().transaction() as conn:
        # Doc 6.7.1 ③: the carrier is created now; a submitter back-dated observed_at
        # must not be treated as the carrier time.
        decision = svc.process_candidate(
            conn, candidate(customer, "carrier-observed-old").model_copy(update={"observed_at": backdated})
        )
        assert decision.outcome
    with get_db().transaction() as conn, pytest.raises(BarrierError) as excinfo:
        svc.process_candidate(conn, candidate(customer, "carrier-explicit-old"), carrier_created_at=backdated)
    assert excinfo.value.barrier_status == "STALE_GENERATION"
    delete(customer, "delete-" + customer)
