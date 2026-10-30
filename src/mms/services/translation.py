from __future__ import annotations

import time
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol
from uuid import uuid4

import httpx

from ..config import get_settings
from ..observability.bootstrap import bootstrap_worker
from ..observability.llm import model_call
from ..utils.db import DbConnection, get_db, require_row
from ..utils.hashing import normalize_language, sha256_text
from ..utils.metrics import (
    TRANSLATION_FAILURES,
    TRANSLATION_JOBS,
    TRANSLATION_LATENCY,
    TRANSLATION_READY_AGE,
    VIEW_BUDGET_EXHAUSTED,
    VIEW_COUNT,
)
from ..utils.outbound import auth_headers
from .repository import MemoryRepository


@dataclass(frozen=True)
class TranslationResult:
    content: str
    model: str
    version: str
    quality: float


class TranslationProvider(Protocol):
    model: str
    version: str

    def translate(self, text: str, source_language: str, target_language: str) -> TranslationResult: ...
    def health(self) -> bool: ...


class DeterministicTranslationProvider:
    model = "deterministic-translator"
    version = "1"
    TRANSLATIONS = MappingProxyType(
        {
            ("zh-CN", "en-US", "以后请用中文"): "Please use Chinese",
            ("zh-CN", "en-US", "以后请用英文"): "Please use English",
            ("en-US", "zh-CN", "Please use Chinese"): "以后请用中文",
            ("en-US", "zh-CN", "Please use English"): "以后请用英文",
        }
    )

    def translate(self, text: str, source_language: str, target_language: str) -> TranslationResult:
        with model_call("deterministic", self.model, "translation", input_texts=[text]) as observation:
            translated = self.TRANSLATIONS.get((source_language, target_language, text))
            if observation is not None:
                observation.update(metadata={"source_language": source_language, "target_language": target_language})
            return TranslationResult(
                translated or f"[{target_language}] {text}",
                self.model,
                self.version,
                0.99 if translated else 0.75,
            )

    def health(self) -> bool:
        return True


class HttpTranslationProvider:
    def __init__(
        self,
        base_url: str,
        model: str = "http-translator",
        version: str = "1",
        timeout_ms: int = 1500,
        token: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.version = version
        self.timeout = timeout_ms / 1000
        self.headers = auth_headers(token)

    def translate(self, text: str, source_language: str, target_language: str) -> TranslationResult:
        with model_call("http", self.model, "translation", input_texts=[text]) as observation:
            response = httpx.post(
                f"{self.base_url}/translate",
                json={
                    "text": text,
                    "source_language": source_language,
                    "target_language": target_language,
                    "model": self.model,
                },
                timeout=self.timeout,
                headers=self.headers,
            )
            response.raise_for_status()
            payload = response.json()
            if observation is not None:
                observation.update(metadata={"source_language": source_language, "target_language": target_language})
            return TranslationResult(
                payload["translated_text"],
                payload.get("model", self.model),
                str(payload.get("version", self.version)),
                float(payload["quality"]),
            )

    def health(self) -> bool:
        try:
            return httpx.get(f"{self.base_url}/health", timeout=3, headers=self.headers).status_code == 200
        except httpx.HTTPError:
            return False


class TranslationService:
    def __init__(self, repository: MemoryRepository | None = None, provider: TranslationProvider | None = None):
        self.settings = get_settings()
        self.repository = repository or MemoryRepository()
        translation_url = self.settings.translation_provider_url
        self.provider = provider or (
            HttpTranslationProvider(
                translation_url,
                model=self.settings.translation_model or "http-translator",
                version=self.settings.translation_model_version or "1",
                timeout_ms=self.settings.translation_timeout_ms,
                token=self.settings.outbound_token,
            )
            if self.settings.translation_provider == "http" and translation_url
            else DeterministicTranslationProvider()
            if self.settings.translation_provider == "deterministic"
            else None
        )
        self.worker_id = f"translation-{uuid4()}"

    def enqueue_for_memory(self, conn: DbConnection, memory_id, barrier_generation: int) -> None:
        if not self.provider or not self.settings.translation_target_languages:
            return
        memory = conn.execute(
            """SELECT m.*,p.config AS policy_config FROM customer_memory m
               JOIN memory_policy p ON p.name||'@'||p.version=m.policy_version WHERE m.id=%s""",
            (memory_id,),
        ).fetchone()
        if (
            not memory
            or memory["sensitivity"] == "HIGH"
            or memory["memory_type"] not in self.settings.translation_memory_types
            or not memory["policy_config"].get("translatable", False)
        ):
            return
        source = conn.execute(
            "SELECT * FROM memory_search_document WHERE memory_id=%s AND view_kind='ORIGINAL' AND view_language=%s AND status='READY'",
            (memory_id, memory["content_language"]),
        ).fetchone()
        if not source:
            return
        view_count = require_row(
            conn.execute(
                "SELECT count(*) AS n FROM memory_search_document WHERE memory_id=%s AND status='READY'", (memory_id,)
            ).fetchone(),
            "translation view count",
        )["n"]
        budget = min(self.settings.max_views_per_memory, int(memory["policy_config"].get("max_views_per_memory", 6)))
        capacity = max(0, budget - view_count)
        targets = []
        for language in self.settings.translation_target_languages:
            target = normalize_language(language, "")
            if target and target != memory["content_language"] and target not in targets:
                targets.append(target)
        for target in targets[:capacity]:
            if self.settings.translation_provider == "http" and not self._translation_consent(
                conn, memory["customer_id"]
            ):
                continue
            conn.execute(
                """INSERT INTO memory_translation_job
                     (id,memory_id,customer_id,source_language,target_language,source_view_hash,content_hash,
                      translation_model,translation_version,barrier_generation,status,priority)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'PENDING',120)
                   ON CONFLICT (customer_id,memory_id,target_language,source_view_hash,translation_model,translation_version) DO NOTHING""",
                (
                    uuid4(),
                    memory_id,
                    memory["customer_id"],
                    memory["content_language"],
                    target,
                    source["view_text_hash"],
                    memory["content_hash"],
                    self.provider.model,
                    self.provider.version,
                    barrier_generation,
                ),
            )

    def _translation_consent(self, conn: DbConnection, customer_id: str) -> bool:
        row = conn.execute(
            """SELECT 1 FROM customer_memory_consent WHERE customer_id=%s AND purpose='CROSS_BORDER_TRANSLATION'
               AND status='GRANTED' AND (expires_at IS NULL OR expires_at>now())""",
            (customer_id,),
        ).fetchone()
        return bool(row)

    def claim(self, conn: DbConnection) -> dict[str, Any] | None:
        job = conn.execute(
            """SELECT * FROM memory_translation_job
               WHERE (status IN ('PENDING','RETRY') AND available_at<=now())
                  OR (status='RUNNING' AND lease_expires_at<=now())
               ORDER BY priority,available_at,created_at FOR UPDATE SKIP LOCKED LIMIT 1"""
        ).fetchone()
        if not job:
            return None
        return dict(
            require_row(
                conn.execute(
                    """UPDATE memory_translation_job SET status='RUNNING',locked_by=%s,locked_at=now(),
                 lease_expires_at=now()+(%s || ' seconds')::interval,attempts=attempts+1,updated_at=now()
               WHERE id=%s RETURNING *""",
                    (self.worker_id, self.settings.translation_lease_seconds, job["id"]),
                ).fetchone(),
                "translation claim",
            )
        )

    def process_one(self) -> bool:
        # The provider check must run BEFORE claiming: a claimed job that is then
        # abandoned stays RUNNING until lease expiry and gets re-claimed forever.
        if self.provider is None:
            return False
        db = get_db()
        with db.transaction() as conn:
            job = self.claim(conn)
        if not job:
            return False
        started = time.perf_counter()
        try:
            with db.connection() as conn:
                source = conn.execute(
                    """SELECT content FROM memory_search_document WHERE customer_id=%s AND memory_id=%s
                       AND view_kind='ORIGINAL' AND view_language=%s AND view_text_hash=%s AND status='READY'""",
                    (job["customer_id"], job["memory_id"], job["source_language"], job["source_view_hash"]),
                ).fetchone()
            if not source:
                with db.transaction() as conn:
                    self._cancel(conn, job, "SOURCE_VIEW_NOT_READY")
                return True
            if len(source["content"]) > self.settings.translation_max_chars:
                raise ValueError("TRANSLATION_SOURCE_TOO_LARGE")
            with db.transaction() as conn:
                blocked = self._pre_provider_block(conn, job)
            if blocked:
                with db.transaction() as conn:
                    self._cancel(conn, job, blocked)
                return True
            result = self.provider.translate(source["content"], job["source_language"], job["target_language"])
            if not result.content or not 0 <= result.quality <= 1:
                raise ValueError("INVALID_TRANSLATION_RESULT")
            if result.model != job["translation_model"] or result.version != job["translation_version"]:
                raise ValueError("TRANSLATION_MODEL_CHANGED")
            with db.transaction() as conn:
                ready = self._final_write(conn, job, result)
            if ready:
                TRANSLATION_JOBS.labels(status="READY", target_language=job["target_language"]).inc()
                TRANSLATION_READY_AGE.labels(target_language=job["target_language"]).observe(
                    max(0.0, time.time() - job["created_at"].timestamp())
                )
            TRANSLATION_LATENCY.labels(target_language=job["target_language"]).observe(time.perf_counter() - started)
            return True
        except Exception as exc:
            TRANSLATION_LATENCY.labels(target_language=job["target_language"]).observe(time.perf_counter() - started)
            TRANSLATION_FAILURES.labels(target_language=job["target_language"], error=exc.__class__.__name__).inc()
            with db.transaction() as conn:
                self._retry_or_dead(conn, job, exc)
            return True

    def _pre_provider_block(self, conn: DbConnection, job: dict[str, Any]) -> str | None:
        """Doc 12.4/8.3.4: never send text of a deleted/expired memory cross-border."""
        self.repository.lock_barrier(conn, job["customer_id"])
        barrier = self.repository.barrier_snapshot(conn, job["customer_id"])
        required = 0 if barrier is None else int(barrier["generation"])
        if barrier and barrier["status"] in {"ACTIVE", "COOLING_DOWN", "COMPLETED"}:
            return f"BARRIER_{barrier['status']}"
        if job["barrier_generation"] != required or (barrier is not None and job["created_at"] < barrier["cutover_at"]):
            return "STALE_GENERATION"
        memory = conn.execute(
            "SELECT lifecycle_status,content_hash FROM customer_memory WHERE id=%s AND customer_id=%s",
            (job["memory_id"], job["customer_id"]),
        ).fetchone()
        if (
            memory is None
            or memory["content_hash"] != job["content_hash"]
            or memory["lifecycle_status"] not in {"ACTIVE", "RESOLVED", "CONFLICTED"}
        ):
            return "STALE_GENERATION"
        if self.settings.translation_provider == "http" and not self._translation_consent(conn, job["customer_id"]):
            return "CONSENT_UNAVAILABLE"
        return None

    def _final_write(self, conn: DbConnection, job: dict[str, Any], result: TranslationResult) -> bool:
        self.repository.lock_barrier(conn, job["customer_id"])
        barrier = self.repository.barrier_snapshot(conn, job["customer_id"])
        required = 0 if barrier is None else int(barrier["generation"])
        if barrier and (
            barrier["status"] in {"ACTIVE", "COOLING_DOWN", "COMPLETED"}
            or job["barrier_generation"] != required
            or job["created_at"] < barrier["cutover_at"]
        ):
            self._cancel(conn, job, "BARRIER_BLOCKED")
            return False
        owned = conn.execute(
            "SELECT 1 FROM memory_translation_job WHERE id=%s AND status='RUNNING' AND locked_by=%s AND lease_expires_at>now() FOR UPDATE",
            (job["id"], self.worker_id),
        ).fetchone()
        if not owned:
            return False
        memory = conn.execute(
            """SELECT m.*,p.config AS policy_config FROM customer_memory m JOIN memory_policy p ON p.name||'@'||p.version=m.policy_version
               WHERE m.id=%s AND m.customer_id=%s AND m.content_hash=%s AND m.lifecycle_status IN ('ACTIVE','RESOLVED','CONFLICTED')""",
            (job["memory_id"], job["customer_id"], job["content_hash"]),
        ).fetchone()
        source = conn.execute(
            """SELECT 1 FROM memory_search_document WHERE memory_id=%s AND view_kind='ORIGINAL' AND view_language=%s
               AND view_text_hash=%s AND status='READY'""",
            (job["memory_id"], job["source_language"], job["source_view_hash"]),
        ).fetchone()
        if not memory or not source:
            self._cancel(conn, job, "STALE_GENERATION")
            return False
        if self.settings.translation_provider == "http" and not self._translation_consent(conn, job["customer_id"]):
            self._cancel(conn, job, "CONSENT_UNAVAILABLE")
            return False
        view_count = require_row(
            conn.execute(
                "SELECT count(*) AS n FROM memory_search_document WHERE memory_id=%s AND status='READY'",
                (job["memory_id"],),
            ).fetchone(),
            "translation view count",
        )["n"]
        existing = conn.execute(
            "SELECT 1 FROM memory_search_document WHERE memory_id=%s AND view_kind='TRANSLATED' AND view_language=%s",
            (job["memory_id"], job["target_language"]),
        ).fetchone()
        budget = min(self.settings.max_views_per_memory, int(memory["policy_config"].get("max_views_per_memory", 6)))
        if not existing and view_count >= budget:
            VIEW_BUDGET_EXHAUSTED.inc()
            self._cancel(conn, job, "VIEW_BUDGET_EXCEEDED")
            return False
        view_hash = sha256_text(result.content)
        search_vector = self.repository.search_vector(
            conn, self.repository.ts_config_map.get(job["target_language"]), result.content
        )
        conn.execute(
            """INSERT INTO memory_search_document
                 (memory_id,customer_id,view_kind,view_language,content,source_evidence_id,content_hash,view_text_hash,
                  translation_model,translation_version,translation_quality,source_view_hash,search_vector,analyzer_version,status)
               VALUES (%s,%s,'TRANSLATED',%s,%s,NULL,%s,%s,%s,%s,%s,%s,%s,'phase2-2026-10-26','READY')
               ON CONFLICT (customer_id,memory_id,view_kind,view_language) DO UPDATE SET
                 content=EXCLUDED.content,content_hash=EXCLUDED.content_hash,view_text_hash=EXCLUDED.view_text_hash,
                 translation_model=EXCLUDED.translation_model,translation_version=EXCLUDED.translation_version,
                 translation_quality=EXCLUDED.translation_quality,source_view_hash=EXCLUDED.source_view_hash,
                 search_vector=EXCLUDED.search_vector,status='READY',updated_at=now()""",
            (
                job["memory_id"],
                job["customer_id"],
                job["target_language"],
                result.content,
                job["content_hash"],
                view_hash,
                result.model,
                result.version,
                result.quality,
                job["source_view_hash"],
                search_vector,
            ),
        )
        VIEW_COUNT.observe(view_count if existing else view_count + 1)
        self.repository.enqueue_embedding_job(
            conn,
            job["memory_id"],
            job["customer_id"],
            job["content_hash"],
            "TRANSLATED",
            job["target_language"],
            view_hash,
            job["barrier_generation"],
            priority=120,
        )
        conn.execute(
            "UPDATE memory_translation_job SET status='READY',locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=%s AND locked_by=%s",
            (job["id"], self.worker_id),
        )
        return True

    def _cancel(self, conn: DbConnection, job: dict[str, Any], reason: str) -> None:
        conn.execute(
            "UPDATE memory_translation_job SET status='CANCELLED',last_error_code=%s,locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=now() "
            "WHERE id=%s AND status='RUNNING' AND locked_by=%s",
            (reason, job["id"], self.worker_id),
        )
        TRANSLATION_JOBS.labels(status="CANCELLED", target_language=job["target_language"]).inc()

    def _retry_or_dead(self, conn: DbConnection, job: dict[str, Any], exc: Exception) -> None:
        status = "DEAD_LETTER" if int(job["attempts"]) >= self.settings.translation_max_attempts else "RETRY"
        delay = min(300, 2 ** max(0, int(job["attempts"]) - 1))
        conn.execute(
            """UPDATE memory_translation_job SET status=%s,available_at=now()+(%s || ' seconds')::interval,
                 locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,last_error_code=%s,updated_at=now()
               WHERE id=%s AND status='RUNNING' AND locked_by=%s""",
            (status, delay, exc.__class__.__name__, job["id"], self.worker_id),
        )
        TRANSLATION_JOBS.labels(status=status, target_language=job["target_language"]).inc()

    def run_forever(self) -> None:
        while True:
            processed = 0
            for _ in range(self.settings.translation_batch_size):
                if not self.process_one():
                    break
                processed += 1
            if not processed:
                time.sleep(self.settings.worker_poll_seconds)


def main() -> None:
    bootstrap_worker(get_settings())
    TranslationService().run_forever()


if __name__ == "__main__":
    main()
