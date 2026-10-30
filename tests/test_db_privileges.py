import psycopg
import pytest
from psycopg import Rollback, sql

pytestmark = pytest.mark.integration

from tests.test_phase01_integration import candidate, customer_id, delete, grant_consent, service

from mms.utils.db import get_db
from mms.utils.embedding import vector_literal


def _probe_as(role: str, statements: list[tuple[str, tuple]], *, commit: bool = False) -> list[Exception | None]:
    """Run statements as a runtime account in one transaction and report per-statement errors.

    ``SET LOCAL ROLE`` makes the account the effective user, so the ACLs the deployed
    processes face are the ACLs under test (docs 12.3). The exception has to leave the
    savepoint block for psycopg to roll back to it and keep the outer transaction usable.
    Probes roll back by default; ``commit=True`` keeps the effect (used for the erasure
    delete so the worker re-insert below is not a primary-key collision).
    """
    results: list[Exception | None] = []
    with get_db().connection() as conn, conn.transaction():
        conn.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
        for statement, params in statements:
            try:
                with conn.transaction():
                    conn.execute(statement, params)
            except Exception as exc:
                results.append(exc)
            else:
                results.append(None)
        if not commit:
            raise Rollback
    return results


def test_api_account_cannot_write_vectors_worker_can():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "privilege-vector"))
    from mms.services.worker import EmbeddingWorker

    worker = EmbeddingWorker(provider)
    while worker.process_one():
        pass
    with get_db().connection() as conn:
        row = conn.execute(
            """SELECT memory_id, customer_id, content_hash, embedding_model, embedding_version,
                      view_kind, view_language, view_text_hash, embedding_dim
               FROM memory_embedding WHERE customer_id=%s AND status='READY' LIMIT 1""",
            (customer,),
        ).fetchone()
    assert row is not None, "seed memory has no READY vector"

    insert_statement = """INSERT INTO memory_embedding
                       (memory_id,customer_id,embedding_model,embedding_version,embedding_dim,content_hash,
                        view_kind,view_language,view_text_hash,embedding,status,generated_at)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::vector,'READY',now())"""
    insert_params = (
        row["memory_id"],
        row["customer_id"],
        row["embedding_model"],
        row["embedding_version"],
        row["embedding_dim"],
        row["content_hash"],
        row["view_kind"],
        row["view_language"],
        row["view_text_hash"],
        vector_literal([1.0] + [0.0] * (int(row["embedding_dim"]) - 1)),
    )

    # Docs 12.3: the API account may neither create a vector nor rewrite one; erasure for
    # the API is the physical delete it is granted.
    app_results = _probe_as(
        "mms_app",
        [  # committed: this is the erasure the API account is allowed to perform
            (insert_statement, insert_params),
            ("UPDATE memory_embedding SET status='DELETED' WHERE customer_id=%s", (customer,)),
            ("DELETE FROM memory_embedding WHERE customer_id=%s", (customer,)),
        ],
        commit=True,
    )
    assert isinstance(app_results[0], psycopg.errors.InsufficientPrivilege), app_results[0]
    assert isinstance(app_results[1], psycopg.errors.InsufficientPrivilege), app_results[1]
    assert app_results[2] is None, app_results[2]

    # Only the worker account can put a vector back.
    assert _probe_as("mms_worker", [(insert_statement, insert_params)]) == [None]
    delete(customer, "delete-" + customer)


def test_runtime_accounts_cannot_alter_append_only_audits():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "privilege-audit"))

    probes = [
        ("UPDATE memory_decision SET outcome='ADD' WHERE customer_id=%s", (customer,)),
        ("DELETE FROM memory_decision WHERE customer_id=%s", (customer,)),
        ("UPDATE admin_access_audit SET reason_code='FORGED' WHERE TRUE", ()),
        ("DELETE FROM admin_access_audit WHERE TRUE", ()),
    ]
    for role in ("mms_app", "mms_worker"):
        results = _probe_as(role, probes)
        assert all(isinstance(result, psycopg.errors.InsufficientPrivilege) for result in results), (role, results)
    delete(customer, "delete-" + customer)
