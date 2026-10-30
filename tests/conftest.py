import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if os.environ.get("MMS_TEST_DATABASE_URL"):
    os.environ.setdefault("MMS_DATABASE_URL", os.environ["MMS_TEST_DATABASE_URL"])


@pytest.fixture(autouse=True)
def reset_query_cache():
    # Doc 8.5 cache is process-global; tests that simulate provider failures must start cold.
    from mms.utils.query_cache import clear_query_cache

    clear_query_cache()
    yield
    clear_query_cache()
