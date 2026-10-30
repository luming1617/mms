from __future__ import annotations

import random

# Doc 10.7: exponential backoff must carry jitter. Without it every worker that tripped on the
# same outage retries in lockstep and the recovery itself becomes the next spike.
_JITTER_RATIO = 0.5


def jittered(seconds: float, *, ratio: float = _JITTER_RATIO) -> float:
    """Uniformly pick within [seconds*(1-ratio), seconds] so retries spread out."""
    if seconds <= 0:
        return 0.0
    return round(seconds * random.SystemRandom().uniform(1 - ratio, 1.0), 3)
