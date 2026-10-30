import copy
import importlib.util
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "openapi_contract.py"
SPEC = importlib.util.spec_from_file_location("openapi_contract", MODULE_PATH)
assert SPEC and SPEC.loader
contract = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = contract
SPEC.loader.exec_module(contract)


def baseline_spec():
    return {
        "openapi": "3.1.0",
        "info": {"title": "test", "version": "1.0.0"},
        "paths": {
            "/items": {
                "post": {
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["name", "quantity"],
                                    "properties": {"name": {"type": "string"}, "quantity": {"type": "integer"}},
                                }
                            }
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {"type": "object", "properties": {"id": {"type": "string"}}}
                                }
                            },
                        }
                    },
                }
            }
        },
        "components": {"securitySchemes": {"gateway": {"type": "apiKey", "in": "header", "name": "X-Customer-ID"}}},
    }


def test_published_baseline_has_no_breaking_changes():
    published = json.loads((ROOT / "contracts" / "openapi-baseline.json").read_text())
    changes = contract.compare_specs(published, contract.current_spec())
    assert [change for change in changes if change.kind == contract.BREAKING] == []


def test_breaking_changes_are_classified():
    baseline = baseline_spec()
    current = copy.deepcopy(baseline)
    operation = current["paths"]["/items"]["post"]
    schema = operation["requestBody"]["content"]["application/json"]["schema"]
    schema["required"].append("color")
    schema["properties"]["color"] = {"type": "string"}
    schema["properties"]["quantity"] = {"type": "string"}
    del operation["responses"]["200"]["content"]["application/json"]["schema"]["properties"]["id"]
    changes = contract.compare_specs(baseline, current)
    messages = {(change.kind, change.location, change.message) for change in changes}
    assert (
        contract.BREAKING,
        "paths//items/post/requestBody/content/application/json/schema/required/color",
        "new required request field",
    ) in messages
    assert (
        contract.BREAKING,
        "paths//items/post/requestBody/content/application/json/schema/properties/quantity",
        "type changed: ['integer'] -> ['string']",
    ) in messages
    assert (
        contract.BREAKING,
        "paths//items/post/responses/200/content/application/json/schema/properties/id",
        "response property removed",
    ) in messages


def test_optional_additions_are_informational():
    baseline = baseline_spec()
    current = copy.deepcopy(baseline)
    schema = current["paths"]["/items"]["post"]["requestBody"]["content"]["application/json"]["schema"]
    schema["properties"]["note"] = {"type": "string"}
    changes = contract.compare_specs(baseline, current)
    assert any(
        change.kind == contract.INFO and change.message == "optional request property added" for change in changes
    )


def test_removed_path_is_breaking():
    baseline = baseline_spec()
    current = copy.deepcopy(baseline)
    current["paths"].clear()
    changes = contract.compare_specs(baseline, current)
    assert any(change.kind == contract.BREAKING and change.message == "path removed" for change in changes)
