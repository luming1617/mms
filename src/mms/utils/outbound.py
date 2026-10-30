from __future__ import annotations

from urllib.parse import urlparse

# Doc 12.3: transport and provider endpoints carry customer text, so they must speak TLS and
# may carry a service credential. Plain http is tolerated only on loopback or when the
# deployment explicitly declares itself development.
_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


def require_secure_endpoint(url: str, *, setting: str, environment: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme == "https":
        return url
    if parsed.scheme == "http" and (parsed.hostname in _LOOPBACK_HOSTS or environment == "development"):
        return url
    raise ValueError(f"{setting} must use https (docs 12.3), got {parsed.scheme}://{parsed.hostname}")


def auth_headers(token: str | None) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"} if token else {}
