import os

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MMS_TEST_DATABASE_URL"),
        reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests",
    ),
]

from tests.test_phase01_integration import candidate, customer_id, delete, grant_consent, service

from mms.services.review import ReviewService
from mms.utils.db import get_db
from mms.utils.errors import MmsError


def test_candidate_review_reprocesses_with_new_attempt_and_four_eyes():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    review = ReviewService(svc.repository, svc)
    item = candidate(customer, "review-candidate").model_copy(update={"confidence": 0.0})
    with get_db().transaction() as conn:
        pending = svc.process_candidate(conn, item)
        assert pending.outcome == "NEEDS_VERIFICATION"
        task = review.queue(conn, customer)[0]
        with pytest.raises(MmsError, match="reviewer must differ"):
            review.decide(
                conn,
                task["id"],
                customer_id=customer,
                reviewer_id="mms-engine",
                decision_version=1,
                approve=True,
                reason_code="REVIEWED",
                note=None,
            )
    with get_db().transaction() as conn:
        approved = review.decide(
            conn,
            task["id"],
            customer_id=customer,
            reviewer_id="reviewer-1",
            decision_version=1,
            approve=True,
            reason_code="REVIEWED",
            note="verified evidence",
        )
        assert approved["status"] == "APPROVED"
        repeated = review.decide(
            conn,
            task["id"],
            customer_id=customer,
            reviewer_id="reviewer-1",
            decision_version=1,
            approve=True,
            reason_code="REVIEWED",
            note="verified evidence",
        )
        assert repeated["status"] == "APPROVED"
    with get_db().connection() as conn:
        counts = conn.execute(
            "SELECT (SELECT count(*) FROM customer_memory WHERE customer_id=%s) AS memories,"
            "(SELECT count(*) FROM memory_decision WHERE customer_id=%s) AS decisions",
            (customer, customer),
        ).fetchone()
    assert counts == {"memories": 1, "decisions": 2}
    delete(customer, "delete-" + customer)


def conflict_candidate(customer, event, value):
    return candidate(customer, event, value=value, content=f"Customer selected {value}").model_copy(
        update={"fact_key": "profile.conflict"}
    )


def test_conflict_review_supersedes_and_closes_conflict():
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
        first = svc.process_candidate(conn, conflict_candidate(customer, "conflict-first", "zh-CN"))
        assert first.outcome == "ADD"
        conflicted = svc.process_candidate(conn, conflict_candidate(customer, "conflict-second", "en-US"))
        assert conflicted.outcome == "CONFLICT"
        task = review.queue(conn, customer)[0]
        assert task["task_type"] == "CONFLICT" and len(task["items"]) == 2
        # Doc 5.4: the queue must expose the field authority rule and evidence links.
        assert task["authority_rule"]["allowed_source_kinds"] == ["USER_EXPLICIT"]
        assert all("source_refs" in item for item in task["items"])
        assert (
            conn.execute("SELECT lifecycle_status FROM customer_memory WHERE id=%s", (first.new_memory_id,)).fetchone()[
                "lifecycle_status"
            ]
            == "CONFLICTED"
        )
    with get_db().transaction() as conn:
        result = review.decide(
            conn,
            task["id"],
            customer_id=customer,
            reviewer_id="reviewer-2",
            decision_version=1,
            approve=True,
            reason_code="CUSTOMER_CONFIRMED",
            note=None,
        )
        assert result["status"] == "APPROVED"
    with get_db().connection() as conn:
        state = conn.execute(
            "SELECT (SELECT status FROM memory_conflict WHERE id=%s) AS conflict,"
            "(SELECT lifecycle_status FROM customer_memory WHERE id=%s) AS old_memory,"
            "(SELECT count(*) FROM customer_memory WHERE customer_id=%s AND lifecycle_status='ACTIVE') AS active",
            (task["conflict_id"], first.new_memory_id, customer),
        ).fetchone()
    assert state == {"conflict": "RESOLVED", "old_memory": "SUPERSEDED", "active": 1}
    delete(customer, "delete-" + customer)


def test_high_risk_conflict_requires_co_reviewer():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    review = ReviewService(svc.repository, svc)
    with get_db().transaction() as conn:
        conn.execute(
            "INSERT INTO fact_key_registry (fact_key,version,cardinality,allowed_source_kinds,sensitivity,auto_write,requires_authority,validator,mutual_exclusion,semantic_template,normalization_version,status,effective_from) "
            "VALUES ('profile.highrisk',1,'SINGLE_VALUE',ARRAY['USER_EXPLICIT'],'LOW',true,true,'{}','[{\"group\":\"value\",\"values\":[\"*\"]}]',NULL,'norm-test','ACTIVE',now()) "
            "ON CONFLICT (fact_key,version) DO NOTHING"
        )
        first = svc.process_candidate(
            conn,
            conflict_candidate(customer, "high-first", "zh-CN").model_copy(update={"fact_key": "profile.highrisk"}),
        )
        second = svc.process_candidate(
            conn,
            conflict_candidate(customer, "high-second", "en-US").model_copy(update={"fact_key": "profile.highrisk"}),
        )
        assert (first.outcome, second.outcome) == ("ADD", "CONFLICT")
        task = review.queue(conn, customer)[0]
        conn.execute("UPDATE memory_review_task SET risk_level='HIGH' WHERE id=%s", (task["id"],))
    with get_db().transaction() as conn, pytest.raises(MmsError, match="co-reviewer"):
        review.decide(
            conn,
            task["id"],
            customer_id=customer,
            reviewer_id="reviewer-3",
            decision_version=1,
            approve=True,
            reason_code="CUSTOMER_CONFIRMED",
            note=None,
        )
    with get_db().transaction() as conn, pytest.raises(MmsError, match="co-reviewer"):
        review.decide(
            conn,
            task["id"],
            customer_id=customer,
            reviewer_id="reviewer-3",
            decision_version=1,
            approve=True,
            reason_code="CUSTOMER_CONFIRMED",
            note=None,
            co_reviewer_id="reviewer-3",
        )
    with get_db().transaction() as conn:
        result = review.decide(
            conn,
            task["id"],
            customer_id=customer,
            reviewer_id="reviewer-3",
            decision_version=1,
            approve=True,
            reason_code="CUSTOMER_CONFIRMED",
            note=None,
            co_reviewer_id="reviewer-4",
        )
        assert (result["status"], result["co_reviewer_id"]) == ("APPROVED", "reviewer-4")
    delete(customer, "delete-" + customer)
