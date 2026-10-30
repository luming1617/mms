import pytest
from pydantic import ValidationError

pytestmark = pytest.mark.unit

from mms.config import Settings
from mms.utils.backoff import jittered
from mms.utils.outbound import auth_headers, require_secure_endpoint
from mms.utils.tokens import estimate_tokens


def test_jittered_backoff_stays_inside_the_band():
    """Doc 10.7: retries spread over [base*(1-ratio), base] so a shared outage does not
    turn recovery into a synchronised thundering herd."""
    for _ in range(50):
        assert 4.0 <= jittered(8.0) <= 8.0
    assert jittered(0) == 0.0


def test_require_secure_endpoint_rejects_plain_http_outside_development():
    assert require_secure_endpoint("https://broker.internal/a", setting="X", environment="production").startswith(
        "https://"
    )
    assert require_secure_endpoint("http://127.0.0.1:9/a", setting="X", environment="production").startswith("http://")
    with pytest.raises(ValueError, match="must use https"):
        require_secure_endpoint("http://broker.internal/a", setting="X", environment="production")
    assert require_secure_endpoint("http://broker.internal/a", setting="X", environment="development")


def test_settings_reject_plain_http_provider_in_production():
    with pytest.raises(ValidationError, match="must use https"):
        Settings(
            _env_file=None,
            deployment_environment="production",
            reranker_enabled=True,
            reranker_url="http://reranker.internal",
        )
    allowed = Settings(
        _env_file=None,
        deployment_environment="production",
        reranker_enabled=True,
        reranker_url="https://reranker.internal",
        deletion_ledger_endpoint="https://worm.internal/deletions",
    )
    assert allowed.reranker_url.startswith("https://")
    with pytest.raises(ValidationError, match="DELETION_LEDGER_ENDPOINT"):
        Settings(
            _env_file=None,
            deployment_environment="production",
            reranker_url="https://reranker.internal",
        )


def test_auth_headers_are_optional():
    assert auth_headers(None) == {}
    assert auth_headers("token") == {"Authorization": "Bearer token"}


def test_estimate_tokens_counts_cjk_per_character():
    """Docs 11.3: the budget must not undercount CJK by four (guards against a len//4 estimate)."""
    assert estimate_tokens("以后请用中文沟通") == 8
    assert estimate_tokens("Please use English") == 5
