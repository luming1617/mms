"""Minimal API load profile for the primary retrieval path.

Run against a deployed or local service:

    MMS_LOAD_CUSTOMER_ID=<seeded-customer> locust -f loadtests/locustfile.py \
        --host http://127.0.0.1:8000 --headless -u 20 -r 5 -t 2m

The customer must already have consent and at least one READY memory.
This is intentionally outside the default quality gate: load results depend on
hardware, dataset, and concurrency, so they are recorded as release evidence.
"""

import os
import secrets

from locust import HttpUser, between, task

CUSTOMER_ID = os.environ.get("MMS_LOAD_CUSTOMER_ID", "load-test-customer")
SERVICE_TOKEN = os.environ.get("MMS_SERVICE_TOKEN", "dev-token")
HEADERS = {"X-Customer-ID": CUSTOMER_ID, "X-Service-Token": SERVICE_TOKEN}
QUERIES = ["以后请用中文沟通", "请使用英文", "communication.language"]


class MemoryRetrievalUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task(8)
    def query_memories(self) -> None:
        self.client.post(
            "/api/v1/memories/query",
            headers=HEADERS,
            json={"query": secrets.choice(QUERIES), "language": "zh-CN", "max_items": 8},
            name="/api/v1/memories/query",
        )

    @task(1)
    def agent_context(self) -> None:
        self.client.post(
            "/api/v1/agent/memory-context",
            headers=HEADERS,
            json={"query": secrets.choice(QUERIES), "language": "zh-CN", "max_items": 8},
            name="/api/v1/agent/memory-context",
        )
