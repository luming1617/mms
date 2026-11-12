import os

import pytest

from mms.utils.db import ConvergenceLock

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MMS_TEST_DATABASE_URL"),
        reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests",
    ),
]


def test_convergence_lock_is_exclusive_per_name():
    first = ConvergenceLock("mms:test:scheduler")
    second = ConvergenceLock("mms:test:scheduler")
    assert first.try_acquire() is True
    try:
        assert second.try_acquire() is False
    finally:
        first.release()
    assert second.try_acquire() is True
    second.release()


def test_convergence_lock_names_are_independent():
    first = ConvergenceLock("mms:test:duty-a")
    other = ConvergenceLock("mms:test:duty-b")
    assert first.try_acquire() is True
    try:
        assert other.try_acquire() is True
    finally:
        other.release()
        first.release()
