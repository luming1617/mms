"""Lifecycle case execution.

Executes setup_events -> actions -> assertions against the real service via
the harness, mirroring docs/4 §五: decisions verified from the decision log,
state verified through the public query API (CURRENT_ANSWER / AUDIT), TTL
cases use short TTLs instead of multi-day waits, deletion uses the review
flow, and cross-customer cases use a second isolated customer.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

from evals.harness import EvalTarget

QUERY_MAX_ITEMS = 10


def _now() -> datetime:
    return datetime.now(UTC)


def _event_to_seed(event: dict[str, Any]) -> dict[str, Any]:
    seed = {
        "memory_type": event.get("memory_type", "CUSTOMER_PROFILE"),
        "fact_key": event["fact_key"],
        "object": {"value": event.get("value", event["text"][:64])},
        "content": event["text"],
        "content_language": event.get("language", "zh-CN"),
        "source_kind": event.get("source_kind", "USER_EXPLICIT"),
        "observed_at": (_now() + timedelta(hours=float(event.get("occurred_at_offset_hours", 0)))).isoformat(),
        # an explicit source_event_id lets cases replay the same event (NOOP
        # semantics); source_refs change the fingerprint without new evidence
        "source_event_id": str(event.get("source_event_id", event["event_id"])),
        # Doc 4.1 #3: every candidate needs a locator; the eval harness owns the source, so the
        # event id is the evidence handle unless the case provides its own refs.
        "source_refs": event.get("source_refs") or [{"ref_type": "message", "message_id": event["event_id"]}],
    }
    if event.get("explicit_correction"):
        seed["explicit_correction"] = True
    if event.get("ttl_seconds") is not None:
        # short TTLs stand in for multi-day expiry (docs/4 §五.3)
        seed["_expires_offset_days"] = float(event["ttl_seconds"]) / 86400.0
    return seed


def _event_fact_keys(case: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for event in case.get("setup_events") or []:
        keys.append(event["fact_key"])
    for action in case.get("actions") or []:
        if action.get("type") == "ingest":
            keys.append(action["fact_key"])
    for event in case.get("other_customer_events") or []:
        keys.append(event["fact_key"])
    return keys


def run_query(target: EvalTarget, customer: str, assertion: dict[str, Any]) -> dict[str, Any]:
    query = {
        "query_text": assertion.get("query"),
        "language": assertion.get("language", "zh-CN"),
        "purpose": assertion.get("purpose", "CURRENT_ANSWER"),
        "max_items": int(assertion.get("max_items", QUERY_MAX_ITEMS)),
    }
    if assertion.get("fact_key"):
        query["fact_key"] = assertion["fact_key"]
    if assertion.get("endpoint"):
        query["endpoint"] = assertion["endpoint"]
    if assertion.get("as_of_offset_seconds") is not None:
        # historical assertions time-travel to before the update: superseded
        # memories carry valid_to=update time, so only as_of-queries reach them
        query["as_of"] = (_now() + timedelta(seconds=float(assertion["as_of_offset_seconds"]))).isoformat()
    started = time.perf_counter()
    status, payload, error_code = target.query(customer, query)
    if isinstance(payload, dict) and "memory_context" in payload:
        # Doc 11.4 agent envelope: full retrieval result lives under `retrieval`.
        context = payload.get("retrieval") or {}
        memories = context.get("memories") or payload.get("memory_context") or []
    else:
        context = payload if isinstance(payload, dict) else {}
        memories = context.get("memories") or []
    return {
        "http_status": status,
        "error_code": error_code,
        "memories": memories,
        "contents": [str(memory.get("content", "")) for memory in memories],
        "requires_confirmation": bool((context or {}).get("requires_confirmation")),
        "latency_ms": round((time.perf_counter() - started) * 1000, 3),
    }


class LifecycleRunContext:
    """Per-case execution state: the primary customer, the optional second
    customer for isolation cases, decision log and deletion evidence."""

    def __init__(self, target: EvalTarget, case: dict[str, Any]) -> None:
        self.target = target
        self.case = case
        self.customer = target.new_customer()
        self.other_customer: str | None = None
        self.decisions: list[dict[str, Any]] = []
        self.reviews: list[dict[str, Any]] = []
        self.deletion_status: str | None = None
        self.deletion_request: dict[str, Any] | None = None


def execute_case(target: EvalTarget, case: dict[str, Any]) -> dict[str, Any]:
    ctx = LifecycleRunContext(target, case)
    target.grant_consent(ctx.customer)
    # registry_requires_authority maps the case's policy_id to the registry row:
    # false -> explicit correction wins (SUPERSEDE), true -> same-rank value
    # changes require review (CONFLICT) - the policy is a case input (docs/4 §四)
    target.seed_fact_keys(
        _event_fact_keys(case),
        requires_authority=bool(case.get("registry_requires_authority", True)),
    )
    for index, event in enumerate(case.get("setup_events") or []):
        ctx.decisions.append({"action": f"setup:{event['event_id']}", "index": index, "decision": _ingest(ctx, event)})
    target.drain_embeddings()

    other_events = case.get("other_customer_events") or []
    if other_events:
        ctx.other_customer = target.new_customer()
        target.grant_consent(ctx.other_customer)
        for event in other_events:
            _ingest_other(ctx, event)
        target.drain_embeddings()

    pre_assertions = [a for a in case.get("assertions") or [] if a.get("phase") == "setup"]
    setup_results = [_evaluate_assertion(ctx, assertion) for assertion in pre_assertions]

    for action in case.get("actions") or []:
        _execute_action(ctx, action)

    final_assertions = [a for a in case.get("assertions") or [] if a.get("phase") != "setup"]
    final_results = [_evaluate_assertion(ctx, assertion) for assertion in final_assertions]
    return {
        "customer": ctx.customer,
        "other_customer": ctx.other_customer,
        "decisions": ctx.decisions,
        "reviews": ctx.reviews,
        "deletion_status": ctx.deletion_status,
        "assertions": {
            **dict(zip([a["id"] for a in pre_assertions], setup_results, strict=True)),
            **dict(zip([a["id"] for a in final_assertions], final_results, strict=True)),
        },
    }


def _ingest(ctx: LifecycleRunContext, event: dict[str, Any]) -> dict[str, Any]:
    return ctx.target.ingest(ctx.customer, _event_to_seed(event), len(ctx.decisions))


def _ingest_other(ctx: LifecycleRunContext, event: dict[str, Any]) -> dict[str, Any]:
    return ctx.target.ingest(str(ctx.other_customer), _event_to_seed(event), 0)


def _execute_action(ctx: LifecycleRunContext, action: dict[str, Any]) -> None:
    kind = action["type"]
    if kind == "ingest":
        ctx.decisions.append(
            {"action": f"action:{action['event_id']}", "index": len(ctx.decisions), "decision": _ingest(ctx, action)}
        )
        ctx.target.drain_embeddings()
    elif kind == "wait_seconds":
        time.sleep(float(action["seconds"]))
    elif kind == "run_lifecycle":
        ctx.target.run_lifecycle()
    elif kind == "request_deletion":
        ctx.deletion_request = ctx.target.request_deletion(ctx.customer)
    elif kind == "approve_review":
        review = ctx.target.approve_first_review(ctx.customer)
        if review is not None:
            ctx.reviews.append(review)
    elif kind == "assert_deletion_status":
        deletion = ctx.reviews[-1].get("deletion") if ctx.reviews else None
        if deletion and deletion.get("request_id"):
            status = ctx.target.deletion_status(ctx.customer, str(deletion["request_id"]))
            ctx.deletion_status = status.get("status")


def _evaluate_assertion(ctx: LifecycleRunContext, assertion: dict[str, Any]) -> dict[str, Any]:
    kind = assertion["type"]
    if kind == "decision":
        index = int(assertion.get("action_index", 0))
        entry = ctx.decisions[index] if index < len(ctx.decisions) else None
        decision = (entry or {}).get("decision") or {}
        ok = decision.get("outcome") == assertion["expected_outcome"]
        if assertion.get("expected_reason_codes"):
            ok = ok and set(assertion["expected_reason_codes"]) <= set(decision.get("reason_codes") or [])
        return {
            "ok": bool(ok),
            "detail": {"outcome": decision.get("outcome"), "reason_codes": decision.get("reason_codes")},
        }
    if kind in {"current_fact", "historical_fact", "isolation"}:
        purpose = "AUDIT" if kind == "historical_fact" else assertion.get("purpose", "CURRENT_ANSWER")
        query_assertion = {**assertion, "purpose": purpose}
        customer = ctx.other_customer if assertion.get("as_other_customer") else ctx.customer
        result = run_query(ctx.target, customer, query_assertion)
        joined = "\n".join(result["contents"])
        ok = True
        if assertion.get("must_contain"):
            ok = ok and all(term in joined for term in assertion["must_contain"])
        if assertion.get("must_not_contain"):
            ok = ok and not any(term in joined for term in assertion["must_not_contain"])
        if assertion.get("requires_confirmation") is not None:
            ok = ok and result["requires_confirmation"] == assertion["requires_confirmation"]
        if assertion.get("memory_count_max") is not None:
            ok = ok and len(result["contents"]) <= int(assertion["memory_count_max"])
        result["ok"] = bool(ok)
        result["query"] = assertion.get("query")
        return result
    if kind == "write_quality":
        dump = run_query(
            ctx.target,
            ctx.customer,
            {
                "query": assertion.get("query", "audit"),
                "purpose": "AUDIT",
                "max_items": 50,
                "language": assertion.get("language", "zh-CN"),
            },
        )
        contents = dump["contents"]
        joined = "\n".join(contents)
        expected = assertion.get("expected_facts") or []
        forbidden = assertion.get("forbidden_facts") or []
        tp = sum(1 for fact in expected if all(term in joined for term in fact.get("must_contain", [])))
        fp = sum(1 for fact in forbidden if any(term in joined for term in fact.get("must_not_contain", [])))
        fn = len(expected) - tp
        precision = tp / (tp + fp) if (tp + fp) else 1.0
        recall = tp / (tp + fn) if (tp + fn) else 1.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
        if assertion.get("expected_memory_count_max") is not None:
            count_ok = len(contents) <= int(assertion["expected_memory_count_max"])
        else:
            count_ok = True
        ok = f1 >= float(assertion.get("min_f1", 1.0)) and count_ok
        return {
            "ok": bool(ok),
            "detail": {
                "precision": round(precision, 4),
                "recall": round(recall, 4),
                "f1": round(f1, 4),
                "stored_count": len(contents),
                "tp": tp,
                "fp": fp,
                "fn": fn,
            },
        }
    if kind == "leak_rate":
        customer = ctx.other_customer if assertion.get("as_other_customer") else ctx.customer
        results = [
            run_query(
                ctx.target, customer, {**assertion, "query": query, "language": assertion.get("language", "zh-CN")}
            )
            for query in assertion.get("queries") or [assertion.get("query")]
        ]
        leaks = 0
        for result in results:
            joined = "\n".join(result["contents"])
            if any(term in joined for term in assertion.get("must_not_contain", [])):
                leaks += 1
        total = len(results) or 1
        leak_rate = leaks / total
        ok = leak_rate <= float(assertion.get("max_leak_rate", 0.0))
        return {"ok": bool(ok), "detail": {"leak_rate": round(leak_rate, 4), "queries": total, "leaks": leaks}}
    if kind == "barrier":
        result = run_query(ctx.target, ctx.customer, assertion)
        ok = result["http_status"] == int(assertion.get("expected_http_status", 410)) and result[
            "error_code"
        ] == assertion.get("expected_error_code", "CUSTOMER_DELETED")
        result["ok"] = bool(ok)
        return result
    if kind == "deletion_status":
        expected = assertion.get("expected_status", "COMPLETED")
        ok = ctx.deletion_status == expected
        return {"ok": bool(ok), "detail": {"deletion_status": ctx.deletion_status, "expected": expected}}
    return {"ok": False, "detail": {"error": f"unhandled assertion type {kind}"}}
