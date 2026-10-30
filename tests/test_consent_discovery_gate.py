import os
from datetime import UTC, datetime

import pytest

DSN = os.environ.get("MMS_TEST_DATABASE_URL")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not DSN, reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests"),
]

from tests.test_phase01_integration import candidate, customer_id, delete, grant_consent, process

from mms.models import CandidateInput
from mms.services.repository import MemoryRepository
from mms.services.service import MemoryService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db
from mms.utils.embedding import DeterministicHashEmbeddingProvider

EXTERNAL_MODEL = "external/challenger"
EXTERNAL_VERSION = 9


class CountingProvider(DeterministicHashEmbeddingProvider):
    def __init__(self):
        super().__init__()
        self.query_calls = 0

    def embed_query(self, text, model):
        self.query_calls += 1
        return super().embed_query(text, model)


def counting_service():
    provider = CountingProvider()
    return MemoryService(MemoryRepository(), provider), provider


def sensitive_candidate(customer, event):
    return CandidateInput(
        customer_id=customer,
        source_system="integration",
        source_event_id=event,
        source_refs=[{"ref_type": "message", "message_id": f"msg-{event}"}],
        memory_type="CUSTOMER_PROFILE",
        fact_key="communication.language",
        object={"value": "zh-CN"},
        content="以后请用中文沟通",
        content_language="zh-CN",
        source_kind="USER_EXPLICIT",
        sensitivity="MEDIUM",
        observed_at=datetime.now(UTC),
        language_confidence=0.99,
    )


@pytest.fixture
def external_model_row():
    with get_db().transaction() as conn:
        conn.execute(
            """INSERT INTO embedding_model_registry
                 (model_id,version,provider,dimension,distance_metric,status,config)
               VALUES (%s,%s,'external',1024,'cosine','SHADOW','{"normalize":true}'::jsonb)
               ON CONFLICT (model_id,version) DO UPDATE SET provider='external'""",
            (EXTERNAL_MODEL, EXTERNAL_VERSION),
        )
    yield
    with get_db().transaction() as conn:
        conn.execute(
            "DELETE FROM embedding_model_registry WHERE model_id=%s AND version=%s",
            (EXTERNAL_MODEL, EXTERNAL_VERSION),
        )


def decisions(conn, candidate_id):
    return conn.execute(
        "SELECT outcome, reason_codes, context_snapshot FROM memory_decision WHERE candidate_id=%s ORDER BY attempt_no",
        (candidate_id,),
    ).fetchall()


def test_missing_write_consent_never_reaches_the_provider():
    customer = customer_id()
    svc, provider = counting_service()
    item = candidate(customer, "no-consent")
    prepared = svc.prepare_candidate(item)
    assert (prepared.semantic_text, prepared.query_vector, prepared.semantic_candidates) == ("", None, [])
    assert provider.query_calls == 0
    with get_db().transaction() as conn:
        decision = svc.process_candidate(conn, item, prepared=prepared)
    assert decision.outcome == "REJECTED"
    assert decision.reason_codes == ["CONSENT_UNAVAILABLE"]
    with get_db().connection() as conn:
        rows = decisions(conn, item.candidate_id)
    assert len(rows) == 1
    assert rows[0]["context_snapshot"]["consent"] == {"purpose": "MEMORY_WRITE", "code": "CONSENT_UNAVAILABLE"}
    assert provider.query_calls == 0


def test_denied_write_consent_never_reaches_the_provider():
    customer = customer_id()
    with get_db().transaction() as conn:
        conn.execute(
            "INSERT INTO customer_memory_consent (customer_id,purpose,status,version,effective_at,source) "
            "VALUES (%s,'MEMORY_WRITE','DENIED',1,now(),'integration-test')",
            (customer,),
        )
    svc, provider = counting_service()
    item = candidate(customer, "denied")
    with get_db().transaction() as conn:
        decision = svc.process_candidate(conn, item, prepared=svc.prepare_candidate(item))
    assert decision.outcome == "REJECTED"
    assert decision.reason_codes == ["CONSENT_DENIED"]
    assert provider.query_calls == 0


def test_sensitive_candidate_needs_its_own_purpose_before_the_provider():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = counting_service()
    blocked = sensitive_candidate(customer, "sensitive-blocked")
    with get_db().transaction() as conn:
        decision = svc.process_candidate(conn, blocked, prepared=svc.prepare_candidate(blocked))
    assert decision.outcome == "REJECTED"
    assert decision.reason_codes == ["CONSENT_UNAVAILABLE"]
    assert provider.query_calls == 0

    grant_consent(customer, purposes=("SENSITIVE_MEMORY",))
    allowed = sensitive_candidate(customer, "sensitive-allowed")
    with get_db().transaction() as conn:
        admitted = svc.process_candidate(conn, allowed, prepared=svc.prepare_candidate(allowed))
    # MEDIUM passes the risk gates once its own purpose is granted, so discovery ran.
    assert admitted.outcome == "ADD"
    assert provider.query_calls == 1


def test_customer_under_a_deletion_barrier_is_not_sent_to_the_provider():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = counting_service()
    with get_db().transaction() as conn:
        assert svc.process_candidate(conn, candidate(customer, "seed")).outcome == "ADD"
    delete(customer, "barrier-gate-delete")
    calls_after_delete = provider.query_calls
    prepared = svc.prepare_candidate(candidate(customer, "after-delete"))
    assert (prepared.semantic_text, prepared.query_vector) == ("", None)
    assert provider.query_calls == calls_after_delete


def test_cross_border_purpose_gates_discovery_like_the_worker(external_model_row):
    denied = customer_id()
    grant_consent(denied)
    svc, provider = counting_service()
    svc.settings = svc.settings.model_copy(
        update={"embedding_model": EXTERNAL_MODEL, "embedding_version": EXTERNAL_VERSION}
    )
    item = candidate(denied, "cross-border-denied")
    prepared = svc.prepare_candidate(item)
    # The vector channel is closed but the fact is still admitted: exact discovery remains.
    assert prepared.semantic_text != ""
    assert prepared.query_vector is None
    assert provider.query_calls == 0
    with get_db().transaction() as conn:
        decision = svc.process_candidate(conn, item, prepared=prepared)
    assert decision.outcome == "ADD"
    assert provider.query_calls == 0

    granted = customer_id()
    grant_consent(granted, purposes=("MEMORY_WRITE", "MEMORY_READ", "CROSS_BORDER_EMBEDDING"))
    with get_db().transaction() as conn:
        worker_blocked = EmbeddingWorker()._cross_border_consent_block(
            conn, {"customer_id": granted, "embedding_model": EXTERNAL_MODEL, "embedding_version": EXTERNAL_VERSION}
        )
    assert worker_blocked is None
    item = candidate(granted, "cross-border-granted")
    with get_db().transaction() as conn:
        svc.process_candidate(conn, item, prepared=svc.prepare_candidate(item))
    assert provider.query_calls > 0


def test_self_hosted_discovery_still_calls_the_provider():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = counting_service()
    prepared = svc.prepare_candidate(candidate(customer, "granted"))
    assert provider.query_calls == 1
    assert prepared.query_vector is not None
    assert prepared.semantic_text != ""


def test_retrieval_query_vector_needs_cross_border_consent(external_model_row):
    from prometheus_client import REGISTRY

    customer = customer_id()
    grant_consent(customer)
    svc, provider = counting_service()
    with get_db().transaction() as conn:
        assert svc.process_candidate(conn, candidate(customer, "seed")).outcome == "ADD"
    svc.settings = svc.settings.model_copy(
        update={"embedding_model": EXTERNAL_MODEL, "embedding_version": EXTERNAL_VERSION}
    )
    calls_before = provider.query_calls
    before = REGISTRY.get_sample_value("mms_embedding_cross_border_blocked_total", {"stage": "retrieval"}) or 0.0
    with get_db().connection() as conn:
        result = svc.retrieve(
            conn,
            customer_id=customer,
            query="以后请用中文沟通",
            language="zh-CN",
            purpose="CURRENT_ANSWER",
            max_items=8,
            primary_conn=conn,
        )
    # A closed cross-border channel is policy, not an outage: it must not enter the
    # availability degradation signal (docs 12.5/18.4).
    assert provider.query_calls == calls_before
    assert result["degraded"] is False
    after = REGISTRY.get_sample_value("mms_embedding_cross_border_blocked_total", {"stage": "retrieval"}) or 0.0
    assert after - before == 1


def test_retrieval_uses_provider_once_cross_border_is_granted(external_model_row):
    customer = customer_id()
    grant_consent(customer, purposes=("MEMORY_WRITE", "MEMORY_READ", "CROSS_BORDER_EMBEDDING"))
    svc, provider = counting_service()
    with get_db().transaction() as conn:
        assert svc.process_candidate(conn, candidate(customer, "seed")).outcome == "ADD"
    svc.settings = svc.settings.model_copy(
        update={"embedding_model": EXTERNAL_MODEL, "embedding_version": EXTERNAL_VERSION}
    )
    calls_before = provider.query_calls
    with get_db().connection() as conn:
        svc.retrieve(
            conn,
            customer_id=customer,
            query="以后请用中文沟通",
            language="zh-CN",
            purpose="CURRENT_ANSWER",
            max_items=8,
            primary_conn=conn,
        )
    assert provider.query_calls == calls_before + 1


def test_decision_replay_does_not_re_embed_without_consent(external_model_row):
    from mms.services.admin import AdminContext, AdminService

    customer = customer_id()
    grant_consent(customer)
    svc, provider = counting_service()
    with get_db().transaction() as conn:
        decision = svc.process_candidate(conn, candidate(customer, "replay-seed"))
    admin = AdminService(MemoryRepository(), provider)
    admin.settings = admin.settings.model_copy(
        update={"embedding_model": EXTERNAL_MODEL, "embedding_version": EXTERNAL_VERSION}
    )
    calls_before = provider.query_calls
    with get_db().transaction() as conn:
        replayed = admin.replay_decision(
            conn,
            decision.decision_id,
            ctx=AdminContext(actor="admin", reason_code="TEST", ticket="T-1", role="ADMIN"),
            trace_id="replay-test",
        )
    assert provider.query_calls == calls_before
    assert replayed["replayed"]["outcome"] in {"ADD", "NOOP", "REINFORCE"}


def test_decision_replay_re_embeds_with_consent(external_model_row):
    from mms.services.admin import AdminContext, AdminService

    customer = customer_id()
    grant_consent(customer, purposes=("MEMORY_WRITE", "MEMORY_READ", "CROSS_BORDER_EMBEDDING"))
    svc, provider = counting_service()
    with get_db().transaction() as conn:
        decision = svc.process_candidate(conn, candidate(customer, "replay-granted"))
    admin = AdminService(MemoryRepository(), provider)
    admin.settings = admin.settings.model_copy(
        update={"embedding_model": EXTERNAL_MODEL, "embedding_version": EXTERNAL_VERSION}
    )
    calls_before = provider.query_calls
    with get_db().transaction() as conn:
        admin.replay_decision(
            conn,
            decision.decision_id,
            ctx=AdminContext(actor="admin", reason_code="TEST", ticket="T-1", role="ADMIN"),
            trace_id="replay-granted",
        )
    assert provider.query_calls == calls_before + 1


def test_semantic_canary_honours_the_cross_border_gate(monkeypatch):
    from mms.services.recon import ReconciliationService

    customer = customer_id()
    grant_consent(customer)
    svc, provider = counting_service()
    with get_db().transaction() as conn:
        assert svc.process_candidate(conn, candidate(customer, "canary-seed")).outcome == "ADD"
    process(EmbeddingWorker(provider))
    stages: list[str] = []

    def denied(self, conn, customer_id, *, model, version, stage="worker"):
        stages.append(stage)
        return "CROSS_BORDER_CONSENT_DENIED"

    # The verdict itself is covered by the discovery/retrieval/replay cases; this pins that the
    # canary loop asks for it and skips the provider call, which is what shrinks recall coverage.
    monkeypatch.setattr(MemoryRepository, "cross_border_embedding_blocked", denied)
    recon = ReconciliationService()
    counter = CountingProvider()
    recon.provider = counter
    stats = recon.run("RECALL")
    canary = stats["details"]["semantic_canary"]
    assert "canary" in stages
    assert canary["scanned_total"] == 0
    assert canary["skipped_cross_border_customers"] >= 1
    assert counter.query_calls == 0


def test_semantic_canary_runs_when_the_gate_is_clear(monkeypatch):
    from mms.services.recon import ReconciliationService

    customer = customer_id()
    grant_consent(customer)
    svc, provider = counting_service()
    with get_db().transaction() as conn:
        assert svc.process_candidate(conn, candidate(customer, "canary-positive")).outcome == "ADD"
    process(EmbeddingWorker(provider))
    monkeypatch.setattr(MemoryRepository, "cross_border_embedding_blocked", lambda *a, **k: None)
    recon = ReconciliationService()
    counter = CountingProvider()
    recon.provider = counter
    stats = recon.run("RECALL")
    assert stats["details"]["semantic_canary"]["scanned_total"] >= 1
    assert counter.query_calls >= 1
