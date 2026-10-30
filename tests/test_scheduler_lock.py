import os

import pytest

from mms.utils.db import SchedulerLock

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MMS_TEST_DATABASE_URL"),
        reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests",
    ),
]


def test_scheduler_lock_is_exclusive_per_name():
    first = SchedulerLock("mms:test:scheduler")
    second = SchedulerLock("mms:test:scheduler")
    assert first.try_acquire() is True
    try:
        assert second.try_acquire() is False
    finally:
        first.release()
    assert second.try_acquire() is True
    second.release()


def test_scheduler_lock_names_are_independent():
    first = SchedulerLock("mms:test:duty-a")
    other = SchedulerLock("mms:test:duty-b")
    assert first.try_acquire() is True
    try:
        assert other.try_acquire() is True
    finally:
        other.release()
        first.release()
