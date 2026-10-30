from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from tests.evaluation.helpers import evaluate, requires_db

pytestmark = [pytest.mark.evaluation, pytest.mark.integration, requires_db]

from tests.test_phase01_integration import candidate, customer_id, delete, grant_consent, service
from tests.test_phase02_review import conflict_candidate

from mms.config import get_settings
from mms.services.deletion import DeletionService
from mms.services.ingestion import IngestionService
from mms.services.lifecycle import LifecycleService
from mms.services.repository import MemoryRepository
from mms.services.review import ReviewService
from mms.services.service import MemoryService
from mms.services.translation import DeterministicTranslationProvider, TranslationService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db
from mms.utils.embedding import DeterministicHashEmbeddingProvider
from mms.utils.errors import BarrierError


def test_decision_outcomes_add_noop_reinforce_supersede():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        first = candidate(customer, "decision-event").model_copy(update={"source_refs": [{"message_id": "a"}]})
        outcomes = [svc.process_candidate(conn, first).outcome]
        repeated = candidate(customer, "decision-event").model_copy(update={"source_refs": [{"message_id": "b"}]})
        outcomes.append(svc.process_candidate(conn, repeated).outcome)
        outcomes.append(
            svc.process_candidate(
                conn, candidate(customer, "reinforce-event", language="en-US", content="Please use Chinese")
            ).outcome
        )
        outcomes.append(
            svc.process_candidate(
                conn,
                candidate(customer, "supersede-event", value="en-US", language="en-US", content="Please use English"),
            ).outcome
        )
    actual = {"outcomes": outcomes, "decision_count": len(outcomes)}
    evaluate(
        "decision-state-machine",
        "ADD/NOOP/REINFORCE/SUPERSEDE",
        actual,
        {
            "outcomes": ["ADD", "NOOP", "REINFORCE", "SUPERSEDE"],
            "decision_count": 4,
        },
    )
    delete(customer, "delete-" + customer)


def test_sensitive_hard_gate_rejects_before_write():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        decision = svc.process_candidate(conn, candidate(customer, "secret-event", content="我的 password 是 abc"))
        memory_count = conn.execute(
            "SELECT count(*) AS n FROM customer_memory WHERE customer_id=%s", (customer,)
        ).fetchone()["n"]
    evaluate(
        "sensitive-hard-gate",
        "secret content must not become memory",
        {
            "outcome": decision.outcome,
            "reason": decision.reason_codes[0],
            "memory_count": memory_count,
        },
        {"outcome": "REJECTED", "reason": "SENSITIVE_DATA", "memory_count": 0},
    )
    delete(customer, "delete-" + customer)


def test_conflict_review_closes_conflict_and_supersedes():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    review = ReviewService(svc.repository, svc)
    with get_db().transaction() as conn:
        conn.execute(
            "INSERT INTO fact_key_registry (fact_key,version,cardinality,allowed_source_kinds,sensitivity,auto_write,requires_authority,validator,mutual_exclusion,semantic_template,normalization_version,status,effective_from) "
            "VALUES ('profile.conflict',1,'SINGLE_VALUE',ARRAY['USER_EXPLICIT'],'LOW',true,true,'{}','[{\"group\":\"value\",\"values\":[\"*\"]}]',NULL,'norm-test','ACTIVE',now()) "
            "ON CONFLICT (fact_key,version) DO NOTHING"
        )
        first = svc.process_candidate(conn, conflict_candidate(customer, "eval-conflict-first", "zh-CN"))
        conflict = svc.process_candidate(conn, conflict_candidate(customer, "eval-conflict-second", "en-US"))
        task = review.queue(conn, customer)[0]
    with get_db().transaction() as conn:
        reviewed = review.decide(
            conn,
            task["id"],
            customer_id=customer,
            reviewer_id="eval-reviewer",
            decision_version=1,
            approve=True,
            reason_code="EVAL_CONFIRMED",
            note=None,
        )
        state = conn.execute("SELECT status FROM memory_conflict WHERE id=%s", (task["conflict_id"],)).fetchone()
        old_state = conn.execute(
            "SELECT lifecycle_status FROM customer_memory WHERE id=%s", (first.new_memory_id,)
        ).fetchone()
    evaluate(
        "conflict-review",
        "same-authority conflict resolved by reviewer",
        {
            "initial_outcome": conflict.outcome,
            "review_status": reviewed["status"],
            "conflict_status": state["status"],
            "old_memory_status": old_state["lifecycle_status"],
        },
        {
            "initial_outcome": "CONFLICT",
            "review_status": "APPROVED",
            "conflict_status": "RESOLVED",
            "old_memory_status": "SUPERSEDED",
        },
    )
    delete(customer, "delete-" + customer)


def test_hybrid_retrieval_uses_fts_and_rrf():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        svc.process_candidate(
            conn, candidate(customer, "hybrid-event", language="en-US", content="Please use English", value="en-US")
        )
    worker = EmbeddingWorker(provider)
    while worker.process_one():
        pass
    with get_db().connection() as conn:
        result = svc.retrieve(
            conn,
            customer_id=customer,
            query="Please use English",
            language="en-US",
            purpose="CURRENT_ANSWER",
            max_items=8,
        )
    first = result["memories"][0]
    evaluate(
        "hybrid-retrieval",
        "English semantic and lexical channels",
        {
            "lexical_used": result["lexical_used"],
            "lexical_mode": result["lexical_mode"],
            "content": first["content"],
            "rrf_score": round(first["score"], 6),
        },
        {"lexical_used": True, "lexical_mode": "fts", "content": "Please use English", "rrf_score": round(2 / 61, 6)},
    )
    delete(customer, "delete-" + customer)


def test_translation_recall_preserves_original_and_provenance():
    customer = customer_id()
    grant_consent(customer)
    settings = get_settings()
    settings.translation_target_languages = ["en-US"]
    settings.translation_provider = "deterministic"
    repository = MemoryRepository()
    translation = TranslationService(repository, DeterministicTranslationProvider())
    embedding = DeterministicHashEmbeddingProvider()
    svc = MemoryService(repository, embedding, translation)
    try:
        with get_db().transaction() as conn:
            added = svc.process_candidate(conn, candidate(customer, "translation-event", content="以后请用中文"))
        translation.process_one()
        worker = EmbeddingWorker(embedding)
        while worker.process_one():
            pass
        with get_db().connection() as conn:
            result = svc.retrieve(
                conn,
                customer_id=customer,
                query="Please use Chinese",
                language="en-US",
                purpose="CURRENT_ANSWER",
                max_items=8,
            )
            view = conn.execute(
                """SELECT d.source_view_hash,d.translation_quality,p.view_text_hash AS original_hash
                   FROM memory_search_document d JOIN memory_search_document p
                     ON p.memory_id=d.memory_id AND p.view_kind='ORIGINAL' AND p.view_language='zh-CN'
                   WHERE d.memory_id=%s AND d.view_kind='TRANSLATED'""",
                (added.new_memory_id,),
            ).fetchone()
        actual = {
            "content": result["memories"][0]["content"],
            "matched_view": result["memories"][0]["matched_view_kind"],
            "provenance_matches": view["source_view_hash"] == view["original_hash"],
            "quality": view["translation_quality"],
        }
    finally:
        settings.translation_target_languages = []
        settings.translation_provider = None
    evaluate(
        "translation-view",
        "translated recall must return original content",
        actual,
        {
            "content": "以后请用中文",
            "matched_view": "TRANSLATED",
            "provenance_matches": True,
            "quality": 0.99,
        },
    )
    delete(customer, "delete-" + customer)


def test_async_inbox_replay_has_no_duplicate_decisions():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    ingestion = IngestionService(svc)
    event_id = uuid4()
    item = candidate(customer, "async-eval-event")
    with get_db().transaction() as conn:
        ingestion.accept(
            conn, item, consumer_group="deepeval", event_id=event_id, topic="candidates", partition=0, offset=1
        )
    ingestion.process_one()
    duplicates = []
    for offset in range(2, 12):
        with get_db().transaction() as conn:
            result = ingestion.accept(
                conn, item, consumer_group="deepeval", event_id=event_id, topic="candidates", partition=0, offset=offset
            )
        duplicates.append(result["duplicate"] and result["status"] == "COMPLETED")
    with get_db().connection() as conn:
        decision_count = conn.execute(
            "SELECT count(*) AS n FROM memory_decision WHERE customer_id=%s", (customer,)
        ).fetchone()["n"]
    evaluate(
        "async-idempotency",
        "ten completed inbox replays",
        {
            "decision_count": decision_count,
            "all_duplicates_skipped": all(duplicates),
            "replay_count": len(duplicates),
        },
        {"decision_count": 1, "all_duplicates_skipped": True, "replay_count": 10},
    )
    delete(customer, "delete-" + customer)


def test_reviewed_deletion_writes_worm_and_blocks_reads():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    review = ReviewService(svc.repository, svc)
    deletion = DeletionService(svc.repository)
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "deletion-eval"))
        requested = deletion.request(conn, customer, "customer", "delete-" + customer)
    with get_db().transaction() as conn:
        task = review.queue(conn, customer)[0]
        approved = review.decide(
            conn,
            task["id"],
            customer_id=customer,
            reviewer_id="eval-privacy-reviewer",
            co_reviewer_id="eval-privacy-second",
            decision_version=1,
            approve=True,
            reason_code="EVAL_DELETE_APPROVED",
            note=None,
        )
        evidence = conn.execute(
            "SELECT (SELECT status FROM memory_deletion_request WHERE id=%s) AS request_status,"
            "(SELECT count(*) FROM deletion_tombstone WHERE deletion_request_id=%s) AS tombstones,"
            "(SELECT count(*) FROM memory_deletion_item WHERE deletion_request_id=%s) AS items,"
            "(SELECT ledger_object_key FROM deletion_tombstone WHERE deletion_request_id=%s) AS ledger_key",
            (requested["request_id"], requested["request_id"], requested["request_id"], requested["request_id"]),
        ).fetchone()
    try:
        with get_db().connection() as conn:
            svc.retrieve(
                conn, customer_id=customer, query="中文", language="zh-CN", purpose="CURRENT_ANSWER", max_items=8
            )
        barrier_code = None
    except BarrierError as exc:
        barrier_code = exc.code
    ledger_exists = (deletion.settings.deletion_ledger_dir / evidence["ledger_key"]).exists()
    evaluate(
        "reviewed-deletion",
        "approve deletion then enforce barrier",
        {
            "initial_status": requested["status"],
            "final_status": approved["deletion"]["status"],
            "request_status": evidence["request_status"],
            "tombstones": evidence["tombstones"],
            "deletion_items_present": evidence["items"] > 0,
            "ledger_exists": ledger_exists,
            "barrier_code": barrier_code,
        },
        {
            "initial_status": "RECEIVED",
            "final_status": "COMPLETED",
            "request_status": "COMPLETED",
            "tombstones": 1,
            "deletion_items_present": True,
            "ledger_exists": True,
            "barrier_code": "CUSTOMER_DELETED",
        },
    )


def test_lifecycle_expiry_invalidates_all_projections():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    expired = candidate(customer, "lifecycle-expiry").model_copy(
        update={"expires_at": datetime.now(UTC) - timedelta(days=1)}
    )
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, expired)
    worker = EmbeddingWorker(provider)
    while worker.process_one():
        pass
    LifecycleService().run_once()
    with get_db().connection() as conn:
        state = conn.execute(
            "SELECT (SELECT lifecycle_status FROM customer_memory WHERE id=%s) AS status,"
            "(SELECT count(*) FROM memory_embedding WHERE memory_id=%s AND status='READY') AS vectors,"
            "(SELECT count(*) FROM memory_search_document WHERE memory_id=%s AND status='READY') AS documents,"
            "(SELECT count(*) FROM customer_memory_current WHERE memory_id=%s) AS pointers",
            (added.new_memory_id, added.new_memory_id, added.new_memory_id, added.new_memory_id),
        ).fetchone()
    evaluate(
        "lifecycle-expiry",
        "expired memory must not remain retrievable",
        dict(state),
        {
            "status": "EXPIRED",
            "vectors": 0,
            "documents": 0,
            "pointers": 0,
        },
    )
    delete(customer, "delete-" + customer)
