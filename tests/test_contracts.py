import json
from pathlib import Path
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator

from mms.models import CandidateInput

pytestmark = pytest.mark.unit

from tests.test_rules import decide, make_candidate

ROOT = Path(__file__).resolve().parents[1]


def validate(schema_name, value):
    schema = json.loads((ROOT / "contracts" / schema_name).read_text())
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(value)


def test_candidate_and_decision_match_frozen_contracts():
    candidate, breakdown, registry, policy = make_candidate()
    candidate_payload = candidate.model_dump(mode="json", include=set(CandidateInput.model_fields))
    validate("candidate.schema.json", candidate_payload)
    decision = decide(candidate, breakdown, registry, policy).model_dump(mode="json")
    validate("decision.schema.json", decision)


def test_event_envelope_contract():
    validate(
        "event-envelope.schema.json",
        {
            "event_id": str(uuid4()),
            "event_type": "memory.created.v1",
            "event_version": 1,
            "occurred_at": "2026-10-26T00:00:00Z",
            "customer_ref": "a" * 64,
            "source_system": "mms",
            "trace_id": "trace-1234",
            "payload": {},
            "barrier_generation": 0,
        },
    )


def test_policy_and_fact_key_schemas_are_valid():
    for name in ("policy.schema.json", "fact-key.schema.json"):
        schema = json.loads((ROOT / "contracts" / name).read_text())
        Draft202012Validator.check_schema(schema)
