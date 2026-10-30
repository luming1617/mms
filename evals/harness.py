"""Evaluation harness: drives the real FastAPI application.

The target under evaluation is the deployed service behavior (docs/1-模型
质量评测体系设计.md §2.2): candidates are submitted over HTTP, async
projections are drained with the production worker/lifecycle services, and
queries run against the real retrieval pipeline. Nothing is stubbed except
that everything executes in one process via TestClient.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi.testclient import TestClient

from mms.config import get_settings
from mms.utils.db import get_db
from mms.utils.embedding import provider_from_settings

INGESTION_PURPOSES = ("MEMORY_WRITE", "MEMORY_READ")


class EvalTarget:
    def __init__(self) -> None:
        from mms.api.main import app

        self.settings = get_settings()
        self.client = TestClient(app)
        self.provider = provider_from_settings(self.settings)

    # ---- identity / consent -------------------------------------------------

    def new_customer(self) -> str:
        return "eval-" + str(uuid.uuid4())

    def _headers(self, customer: str, *, reviewer: bool = False, admin: bool = False) -> dict[str, str]:
        headers = {"X-Customer-ID": customer, "X-Service-Token": self.settings.service_token}
        if reviewer:
            headers["X-Reviewer-ID"] = "eval-reviewer"
        if admin:
            # Doc 13.2/13.7: AUDIT and as_of reads use the admin plane and are audited.
            headers.update(
                {
                    "X-Admin-Token": self.settings.admin_token,
                    "X-Reviewer-ID": "eval-admin",
                    "X-Admin-Reason-Code": "EVAL_GOLDEN_AUDIT",
                    "X-Admin-Ticket": "eval-suite",
                }
            )
        return headers

    def grant_consent(self, customer: str, purposes: tuple[str, ...] = INGESTION_PURPOSES) -> None:
        for purpose in purposes:
            response = self.client.post(
                f"/internal/customers/{customer}/memory-consent",
                headers=self._headers(customer),
                json={
                    "purpose": purpose,
                    "status": "GRANTED",
                    "version": 1,
                    "effective_at": datetime.now(UTC).isoformat(),
                    "source": "eval-harness",
                },
            )
            assert response.status_code == 200, f"consent sync failed: {response.status_code} {response.text}"

    # ---- ingestion ----------------------------------------------------------

    def ingest(self, customer: str, seed: dict[str, Any], index: int) -> dict[str, Any]:
        payload = dict(seed)
        expires_offset_days = payload.pop("_expires_offset_days", None)
        payload["customer_id"] = customer
        payload["source_system"] = "eval"
        # explicit source_event_id lets goldens replay the same event (idempotency
        # semantics); otherwise each seed gets a unique event id
        payload["source_event_id"] = str(payload.get("source_event_id") or f"{customer}-{index}")
        # honor the caller's observed_at (benchmark replay preserves real
        # session timestamps, docs/4 rule three); default to write time
        payload["observed_at"] = str(payload.get("observed_at") or datetime.now(UTC).isoformat())
        if expires_offset_days is not None:
            # TTL semantics: expiry counts from the write moment, not from a
            # backdated observed_at, so short-TTL cases stay controllable
            payload["expires_at"] = (datetime.now(UTC) + timedelta(days=float(expires_offset_days))).isoformat()
        response = self.client.post("/api/v1/memories/candidates", headers=self._headers(customer), json=payload)
        assert response.status_code == 202, f"ingestion failed: {response.status_code} {response.text}"
        decision = response.json()
        return {"outcome": decision["outcome"], "reason_codes": decision["reason_codes"]}

    def drain_embeddings(self) -> None:
        from mms.services.worker import EmbeddingWorker

        worker = EmbeddingWorker(self.provider)
        while worker.process_one():
            pass

    def run_lifecycle(self) -> None:
        from mms.services.lifecycle import LifecycleService

        LifecycleService().run_once()

    def seed_fact_key_registry(
        self, fact_key: str, *, mutual_exclusion: str = '[{"group":"value","values":["*"]}]'
    ) -> None:
        """Register a fact key so goldens can ingest it.

        SINGLE_VALUE keys must declare a mutual-exclusion group (enforced by
        MemoryRepository.load_registry); the generic "any value pair excludes"
        group mirrors what scripts/init_db.py registers for shipped keys.
        ON CONFLICT keeps rows registered by init_db or earlier runs.
        """
        self.seed_fact_keys([fact_key], mutual_exclusion=mutual_exclusion)

    def seed_fact_keys(
        self,
        fact_keys: list[str],
        *,
        mutual_exclusion: str = '[{"group":"value","values":["*"]}]',
        requires_authority: bool = True,
    ) -> None:
        """Register many fact keys in one transaction.

        Benchmark replay writes thousands of per-turn keys; registering them in
        individual transactions would dominate the replay runtime.
        ``requires_authority`` encodes the conflict policy for the key (docs/4
        §四 policy_id): true -> same-rank value changes go to CONFLICT review;
        false -> an allowed source may supersede directly.
        """
        if not fact_keys:
            return
        with get_db().transaction() as conn:
            conn.cursor().executemany(
                "INSERT INTO fact_key_registry (fact_key,version,cardinality,allowed_source_kinds,sensitivity,"
                "auto_write,requires_authority,validator,mutual_exclusion,semantic_template,normalization_version,"
                "status,effective_from) "
                "VALUES (%s,1,'SINGLE_VALUE',ARRAY['USER_EXPLICIT','USER_IMPLICIT','AGENT_CONFIRMED','SYSTEM_OF_RECORD'],"
                "'LOW',true,%s,'{}',%s,NULL,'norm-test','ACTIVE',now()) "
                # re-policy only rows this harness owns (norm-test); rows
                # registered by init_db keep their production configuration
                "ON CONFLICT (fact_key,version) DO UPDATE SET "
                "allowed_source_kinds=EXCLUDED.allowed_source_kinds, "
                "requires_authority=EXCLUDED.requires_authority, "
                "mutual_exclusion=EXCLUDED.mutual_exclusion "
                "WHERE fact_key_registry.normalization_version='norm-test'",
                [(key, requires_authority, mutual_exclusion) for key in fact_keys],
            )

    # ---- retrieval ----------------------------------------------------------

    def query(self, customer: str, query: dict[str, Any]) -> tuple[int, dict[str, Any] | None, str | None]:
        endpoint = "/api/v1/agent/memory-context" if query.get("endpoint") == "agent" else "/api/v1/memories/query"
        body = {
            "query": query.get("query_text"),
            "language": query.get("language", "zh-CN"),
            "purpose": query.get("purpose", "CURRENT_ANSWER"),
            "max_items": int(query.get("max_items", 8)),
        }
        if query.get("max_tokens") is not None:
            body["max_tokens"] = int(query["max_tokens"])
        if query.get("fact_key"):
            body["fact_key"] = query["fact_key"]
        if query.get("as_of"):
            body["as_of"] = query["as_of"]
        if query.get("include_types"):
            body["include_types"] = list(query["include_types"])
        admin = query.get("purpose") == "AUDIT" or query.get("as_of") is not None
        response = self.client.post(endpoint, headers=self._headers(customer, admin=admin), json=body)
        if response.status_code >= 400:
            return response.status_code, None, str(response.json().get("code", "UNKNOWN"))
        payload = response.json()
        if endpoint.endswith("/memory-context"):
            return response.status_code, payload, None
        return response.status_code, payload, None

    # ---- review / deletion --------------------------------------------------

    def approve_first_review(self, customer: str) -> dict[str, Any] | None:
        queue = self.client.get("/api/v1/review-queue", headers=self._headers(customer), params={"status": "PENDING"})
        assert queue.status_code == 200, f"review queue failed: {queue.status_code} {queue.text}"
        items = queue.json().get("items") or []
        if not items:
            return None
        task = items[0]
        decision = self.client.post(
            f"/api/v1/review-queue/{task['id']}/decision",
            headers=self._headers(customer, reviewer=True),
            json={
                "decision": "APPROVE",
                "decision_version": task["decision_version"],
                "reason_code": "EVAL_GOLDEN_APPROVED",
                "note": None,
                # Doc 5.4/12.6: high-risk deletion approval is double-signed; the
                # golden runner plays the second reviewer explicitly.
                "co_reviewer_id": "eval-co-reviewer",
            },
        )
        assert decision.status_code == 200, f"review decision failed: {decision.status_code} {decision.text}"
        return decision.json()

    def request_deletion(self, customer: str) -> dict[str, Any]:
        response = self.client.post(
            "/api/v1/deletion-requests",
            headers=self._headers(customer),
            json={"idempotency_key": f"eval-del-{customer}", "scope": {"all": True}},
        )
        assert response.status_code == 202, f"deletion request failed: {response.status_code} {response.text}"
        return response.json()

    def deletion_status(self, customer: str, request_id: str) -> dict[str, Any]:
        response = self.client.get(f"/api/v1/deletion-requests/{request_id}", headers=self._headers(customer))
        assert response.status_code == 200, f"deletion status failed: {response.status_code} {response.text}"
        return response.json()
